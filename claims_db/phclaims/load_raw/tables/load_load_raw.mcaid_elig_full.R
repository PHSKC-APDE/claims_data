#### CODE TO LOAD MCAID ELIG TABLES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, scipen = 999)

library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue)

db_claims <- dbConnect(odbc(), "PHClaims51")

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_file.R")


#### SET UP BATCH ID ####
# Note that the delivery_date and note columns should be changed from NULL during the run
# then put back to NULL to remind people to enter details
current_batch_id <- load_metadata_etl_log_f(conn = db_claims, 
                                            batch_type = "full", 
                                            data_source = "Medicaid", 
                                            delivery_date = NULL, 
                                            note = NULL)



#### QA CHECK: ACTUAL VS EXPECTED ROW COUNTS ####
# Use the load config file for the list of tables to check and their expected row counts
qa_rows_file <- qa_file_row_count_f(config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_full.yaml",
                               overall = F, ind_yr = T)

# Report results out to SQL table
odbc::dbGetQuery(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        'load_raw.mcaid_elig',
                                        'Number of rows in source file(s) match(es) expected value', 
                                        {qa_rows_file$outcome},
                                        {Sys.time()},
                                        {qa_rows_file$note})",
                                .con = db_claims))

if (qa_rows_file$outcome == "FAIL") {
  stop(glue::glue("Mismatching row count between source file and expected number. 
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
}


#### CREATE TABLES ####
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/create_load_raw.mcaid_elig.yaml",
               overall = T, ind_yr = T)



#### QA CHECK: ORDER OF COLUMNS IN SOURCE FILE MATCH TABLE SHELLS IN SQL ###
qa_column <- qa_column_order_f(conn = db_claims,
                            config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_full.yaml",
                            overall = F, ind_yr = T)

# Report results out to SQL table
odbc::dbGetQuery(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        'load_raw.mcaid_elig',
                                        'Order of columns in source file matches SQL table', 
                                        {qa_column$outcome},
                                        {Sys.time()},
                                        {qa_column$note})",
                                .con = db_claims))

if (qa_column$outcome == "FAIL") {
  stop(glue::glue("Mismatching column order between source file and SQL table. 
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
}



#### LOAD TABLES ####
load_table_from_file_f(conn = db_claims,
                       config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_full.yaml",
                       overall = F, ind_yr = T, combine_yr = T)


#### QA CHECK: ROW COUNTS MATCH SOURCE FILE COUNT ####
# Use the load config file for the list of tables to check and their expected row counts
qa_rows_sql <- qa_sql_row_count_f(conn = db_claims,
                                  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_full.yaml",
                                  overall = F, ind_yr = T, combine_yr = T)

# Report individual results out to SQL table
odbc::dbGetQuery(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        'load_raw.mcaid_elig',
                                        'Number rows loaded to SQL vs. expected value(s)', 
                                        {qa_rows_sql$outcome[1]},
                                        {Sys.time()},
                                        {qa_rows_sql$note[1]})",
                                .con = db_claims))
# Report combined years result out to SQL table
odbc::dbGetQuery(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                'load_raw.mcaid_elig',
                                'Number rows loaded to combined SQL table vs. expected value(s)', 
                                {qa_rows_sql$outcome[2]},
                                {Sys.time()},
                                {qa_rows_sql$note[2]})",
                                .con = db_claims))

if (qa_rows_sql$outcome[1] == "FAIL") {
  stop(glue::glue("Mismatching row count between source file and SQL table. 
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
}
if (qa_rows_sql$outcome[2] == "FAIL") {
  stop(glue::glue("Mismatching row count between expected and actual for combined years SQL table. 
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
}



#### QA CHECK: COUNT OF DISTINCT ID, CLNDR_YEAR_MNTH, FROM DATE, TO DATE, SECONDARY RAC ####
# Should be no combo of ID, CLNDR_YEAR_MNTH, from_date, to_date, and secondary RAC with >1 row
distinct_rows <- as.numeric(dbGetQuery(db_claims,
                            "SELECT COUNT (*) FROM
                            (SELECT DISTINCT CLNDR_YEAR_MNTH, 
                              MEDICAID_RECIPIENT_ID, FROM_DATE, TO_DATE,
                              SECONDARY_RAC_CODE 
                              FROM load_raw.mcaid_elig) a"))

total_rows <- as.numeric(dbGetQuery(db_claims, "SELECT COUNT (*) FROM load_raw.mcaid_elig"))


if (distinct_rows != total_rows) {
  # Looks like there are 42 people with extra rows where the only difference is a NULL or different end reason
  # Still flag as a fail but account for this in the note and continue processing
  if (total_rows - distinct_rows == 42) {
    odbc::dbGetQuery(
      conn = db_claims,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                     (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({current_batch_id}, 
                             'load_raw.mcaid_elig',
                             'Distinct rows (ID, CLNDR_YEAR_MNTH, FROM/TO DATE, SECONDARY RAC)', 
                             'FAIL', 
                             {Sys.time()}, 
                             'Known issue where 42 people have duplicate rows but differing end reason. Continued with load.')",
                     .con = db_claims))
    } else if (total_rows - distinct_rows != 42) {
      odbc::dbGetQuery(conn = db_claims,
                     glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                    (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                    VALUES ({current_batch_id}, 
                                    'load_raw.mcaid_elig',
                                    'Distinct rows (ID, CLNDR_YEAR_MNTH, FROM/TO DATE, SECONDARY RAC)', 
                                    'FAIL',
                                    {Sys.time()},
                                    'Issue was not the known 42 people with duplicate rows. Investigate further.')",
                                    .con = db_claims))
    stop("Number of distinct rows does not match total expected")
    }
  } else {
  odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'load_raw.mcaid_elig',
                                  'Distinct rows (ID, CLNDR_YEAR_MNTH, FROM/TO DATE, SECONDARY RAC)', 
                                  'PASS',
                                  {Sys.time()},
                                  'Number of distinct rows equals total # rows')",
                                  .con = db_claims))
}


#### QA CHECK: DATE RANGE MATCHES EXPECTED RANGE ####
qa_date_range <- qa_date_range_f(conn = db_claims,
                                    config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_full.yaml",
                                    overall = F, ind_yr = T, combine_yr = T,
                                 date_var = "CLNDR_YEAR_MNTH")

# Report individual results out to SQL table
odbc::dbGetQuery(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        'load_raw.mcaid_elig',
                                        'Actual vs. expected date range in data', 
                                        {qa_date_range$outcome[1]},
                                        {Sys.time()},
                                        {qa_date_range$note[1]})",
                                .con = db_claims))
# Report combined years result out to SQL table
odbc::dbGetQuery(conn = db_claims,
                 glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                'load_raw.mcaid_elig',
                                'Actual vs. expected date range in combined SQL table', 
                                {qa_date_range$outcome[2]},
                                {Sys.time()},
                                {qa_date_range$note[2]})",
                                .con = db_claims))

if (qa_date_range$outcome[1] == "FAIL") {
  stop(glue::glue("Mismatching date range between source file and SQL table. 
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
}
if (qa_date_range$outcome[2] == "FAIL") {
  stop(glue::glue("Mismatching date range between expected and actual for combined years SQL table. 
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
}


#### QA CHECK: LENGTH OF MCAID ID = 11 CHARS ####
id_len <- dbGetQuery(db_claims,
                     "SELECT MIN(LEN(MEDICAID_RECIPIENT_ID)) AS min_len, 
                     MAX(LEN(MEDICAID_RECIPIENT_ID)) AS max_len 
                     FROM load_raw.mcaid_elig")

if (id_len$min_len != 11 | id_len$max_len != 11) {
  odbc::dbGetQuery(
    conn = db_claims,
    glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'load_raw.mcaid_elig',
                   'Length of Medicaid ID', 
                   'FAIL', 
                   {Sys.time()}, 
                   'Minimum ID length was {id_len$min_len}, maximum was {id_len$max_len}')",
                   .con = db_claims))
  
  stop(glue::glue("Some Medicaid IDs are not 11 characters long.  
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
} else {
  odbc::dbGetQuery(
    conn = db_claims,
    glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'load_raw.mcaid_elig',
                   'Length of Medicaid ID', 
                   'PASS', 
                   {Sys.time()}, 
                   'All Medicaid IDs were 11 characters')",
                   .con = db_claims))
}


#### QA CHECK: LENGTH OF RAC CODES = 4 CHARS ####
rac_len <- dbGetQuery(db_claims,
                     "SELECT MIN(LEN(RPRTBL_RAC_CODE)) AS min_len, 
                     MAX(LEN(RPRTBL_RAC_CODE)) AS max_len, 
                     MIN(LEN(SECONDARY_RAC_CODE)) AS min_len2, 
                     MAX(LEN(SECONDARY_RAC_CODE)) AS max_len2 
                     FROM load_raw.mcaid_elig")

if (rac_len$min_len != 4 | rac_len$max_len != 4 | 
    rac_len$min_len2 != 4 | rac_len$max_len2 != 4) {
  odbc::dbGetQuery(
    conn = db_claims,
    glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'load_raw.mcaid_elig',
                   'Length of RAC codes', 
                   'FAIL', 
                   {Sys.time()}, 
                   'Min RPRTBLE_RAC_CODE length was {rac_len$min_len}, max was {rac_len$max_len};
                   Min SECONDARY_RAC_CODE length was {rac_len$min_len2}, max was {rac_len$max_len2}')",
                   .con = db_claims))
  
  stop(glue::glue("Some RAC codes are not 4 characters long.  
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
} else {
  odbc::dbGetQuery(
    conn = db_claims,
    glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'load_raw.mcaid_elig',
                   'Length of RAC codes', 
                   'PASS', 
                   {Sys.time()}, 
                   'All RAC codes (reportable and secondary) were 4 characters')",
                   .con = db_claims))
}


#### QA CHECK: NUMBER NULLs IN FROM_DATE ####
from_nulls <- dbGetQuery(db_claims,
                      "SELECT a.null_dates, b.total_rows 
                      FROM
                      (SELECT 
                        COUNT (*) AS null_dates, ROW_NUMBER() OVER (ORDER BY NEWID()) AS seqnum
                        FROM load_raw.mcaid_elig
                        WHERE FROM_DATE IS NULL) a
                      LEFT JOIN
                      (SELECT COUNT(*) AS total_rows, ROW_NUMBER() OVER (ORDER BY NEWID()) AS seqnum
                        FROM load_raw.mcaid_elig) b
                      ON a.seqnum = b.seqnum")

pct_null <- round(from_nulls$null_dates / from_nulls$total_rows  * 100, 3)

if (pct_null > 2.0) {
  odbc::dbGetQuery(
    conn = db_claims,
    glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'load_raw.mcaid_elig',
                   'NULL from dates', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {from_nulls$null_dates} NULL from dates ({pct_null}% of total rows)')",
                   .con = db_claims))
  
  stop(glue::glue(">2% FROM_DATE rows are null.  
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
} else {
  odbc::dbGetQuery(
    conn = db_claims,
    glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'load_raw.mcaid_elig',
                   'NULL from dates', 
                   'PASS', 
                   {Sys.time()}, 
                   '<2% of from date rows were null ({pct_null}% of total rows)')",
                   .con = db_claims))
}


#### ADD BATCH ID COLUMN ####
# Add column to the SQL table and set current batch to the default
odbc::dbGetQuery(db_claims,
                 glue::glue_sql(
                   "ALTER TABLE load_raw.mcaid_elig 
                   ADD etl_batch_id INTEGER 
                   DEFAULT {current_batch_id} WITH VALUES",
                   .con = db_claims))


#### ADD VALUES TO QA_VALUES TABLE ####
odbc::dbGetQuery(
  conn = db_claims,
  glue::glue_sql("INSERT INTO metadata.qa_mcaid_values
                   (table_name, qa_item, qa_value, qa_date, note) 
                   VALUES ('load_raw.mcaid_elig',
                   'row_count', 
                   '{total_rows}', 
                   {Sys.time()}, 
                   'Count after full refresh')",
                 .con = db_claims))


#### CLEAN UP ####
rm(from_nulls)
rm(id_len, rac_len)
rm(qa_column)
rm(qa_date_range)
rm(qa_rows_file, qa_rows_sql)
rm(total_rows, distinct_rows)
rm(pct_null)
rm(list = ls(pattern = "^qa_"))
rm(list = ls(pattern = "_f$"))
