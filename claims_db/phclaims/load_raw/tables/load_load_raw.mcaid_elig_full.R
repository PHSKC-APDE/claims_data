#### CODE TO LOAD MCAID ELIG TABLES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

### Run from master_mcaid_full script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_full.R


load_load_raw.mcaid_elig_full_f <- function(etl_date_min = "2012-01-01",
                                            etl_date_max = "2019-12-31",
                                            etl_delivery_date = NULL,
                                            etl_note = NULL) {
  
  ### Check entries are in place for ETL function
  if (is.null(etl_delivery_date) | is.null(etl_note)) {
    stop("Enter a delivery date and note for the ETL batch ID function")
  }
  
  
  # Load ETL and QA functions if not already present
  if (exists("load_metadata_etl_log_f") == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")
  }
  
  if (exists("qa_file_row_count_f") == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_file.R")
  }
  
  
  
  #### SET UP BATCH ID ####
  # Eventually switch this function over to using glue_sql to stop unwanted SQL behavior
  current_batch_id <- load_metadata_etl_log_f(conn = db_claims, 
                                              batch_type = "full", 
                                              data_source = "Medicaid", 
                                              date_min = etl_date_min,
                                              date_max = etl_date_max,
                                              delivery_date = etl_delivery_date, 
                                              note = etl_note,
                                              auto_proceed = T)
  
  if (is.na(current_batch_id)) {
    stop("No etl_batch_id. Check metadata.etl_log table")
  }
  
  
  #### QA CHECK: ACTUAL VS EXPECTED ROW COUNTS ####
  print("Checking expected vs. actual row counts")
  # Use the load config file for the list of tables to check and their expected row counts
  qa_rows_file <- qa_file_row_count_f(config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_full.yaml",
                                      overall = F, ind_yr = T)
  
  # Report results out to SQL table
  DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        'load_raw.mcaid_elig',
                                        'Number of rows in source file(s) match(es) expected value', 
                                        {qa_rows_file$outcome},
                                        {format(Sys.time(), usetz = FALSE)},
                                        {qa_rows_file$note})",
                                  .con = db_claims))
  
  if (qa_rows_file$outcome == "FAIL") {
    stop(glue::glue("Mismatching row count between source file and expected number. 
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
  }
  
  

  #### QA CHECK: ORDER OF COLUMNS IN SOURCE FILE MATCH TABLE SHELLS IN SQL ###
  print("Checking column order")
  qa_column <- qa_column_order_f(conn = db_claims,
                                 config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_full.yaml",
                                 overall = F, ind_yr = T)
  
  # Report results out to SQL table
  DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        'load_raw.mcaid_elig',
                                        'Order of columns in source file matches SQL table', 
                                        {qa_column$outcome},
                                        {format(Sys.time(), usetz = FALSE)},
                                        {qa_column$note})",
                                  .con = db_claims))
  
  if (qa_column$outcome == "FAIL" | !exists(qa_column)) {
    stop(glue::glue("Mismatching column order between source file and SQL table. 
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
  }
  
  
  
  #### LOAD TABLES ####
  print("Loading tables to SQL")
  load_table_from_file_f(conn = db_claims,
                         config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_full.yaml",
                         overall = F, ind_yr = T, combine_yr = T)
  
  
  #### QA CHECK: ROW COUNTS MATCH SOURCE FILE COUNT ####
  print("Checking loaded row counts vs. expected")
  # Use the load config file for the list of tables to check and their expected row counts
  qa_rows_sql <- qa_load_row_count_f(conn = db_claims,
                                    config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_full.yaml",
                                    overall = F, ind_yr = T, combine_yr = T)
  
  # Report individual results out to SQL table
  DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        'load_raw.mcaid_elig',
                                        'Number rows loaded to SQL vs. expected value(s)', 
                                        {qa_rows_sql$outcome[1]},
                                        {format(Sys.time(), usetz = FALSE)},
                                        {qa_rows_sql$note[1]})",
                                  .con = db_claims))
  # Report combined years result out to SQL table
  DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                'load_raw.mcaid_elig',
                                'Number rows loaded to combined SQL table vs. expected value(s)', 
                                {qa_rows_sql$outcome[2]},
                                {format(Sys.time(), usetz = FALSE)},
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
  print("Running additional QA items")
  
  # Should be no combo of ID, CLNDR_YEAR_MNTH, from_date, to_date, and secondary RAC with >1 row
  # However, some months have multiple rows per person-month-RAC combo. There are currently 
  #   3 main reasons for this:
  # 1) Multiple end reasons (mostly dates prior to 2018-09)
  # 2) One row with missing HOH_ID and one row with non-missing HOH_ID (mostly mid-2019)
  # 3) RAC name (usually secondary) spelled incorrectly 
  #  (Involuntary Inpatient Psychiactric Treatment (ITA) vs Involuntary Inpatient Psychiatric Treatment (ITA))
  
  # Check for all 3 situations, they will be addressed when making stage tables
  distinct_rows <- as.numeric(dbGetQuery(
    db_claims,
    "SELECT COUNT (*) FROM
    (SELECT DISTINCT CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, 
    TO_DATE, RPRTBL_RAC_CODE, SECONDARY_RAC_CODE, HOH_ID, END_REASON, RPRTBL_RAC_NAME,
    SECONDARY_RAC_NAME
    FROM load_raw.mcaid_elig) a"))
  
  total_rows <- as.numeric(dbGetQuery(db_claims, "SELECT COUNT (*) FROM load_raw.mcaid_elig"))
  
  
  if (distinct_rows != total_rows) {
    odbc::dbGetQuery(conn = db_claims,
                     glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                    (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                    VALUES ({current_batch_id}, 
                                    'load_raw.mcaid_elig',
                                    'Distinct rows (ID, CLNDR_YEAR_MNTH, FROM/TO DATE, RPRTBL_RAC_CODE, SECONDARY RAC, END_REASON)', 
                                    'FAIL',
                                    {format(Sys.time(), usetz = FALSE)},
                                    'Number distinct rows ({distinct_rows}) != total rows ({total_rows})')",
                                    .con = db_claims))
    warning(glue("Number of distinct rows ({distinct_rows}) does not match total expected ({total_rows})"))
  } else {
    odbc::dbGetQuery(conn = db_claims,
                     glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'load_raw.mcaid_elig',
                                  'Distinct rows (ID, CLNDR_YEAR_MNTH, FROM/TO DATE, RPRTBL_RAC_CODE, SECONDARY RAC, END_REASON)', 
                                  'PASS',
                                  {format(Sys.time(), usetz = FALSE)},
                                  'Number of distinct rows equals total # rows ({total_rows})')",
                                    .con = db_claims))
  }
  
  
  #### QA CHECK: DATE RANGE MATCHES EXPECTED RANGE ####
  qa_date_range <- qa_date_range_f(conn = db_claims,
                                   config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_full.yaml",
                                   overall = F, ind_yr = T, combine_yr = T,
                                   date_var = "CLNDR_YEAR_MNTH")
  
  # Report individual results out to SQL table
  DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        'load_raw.mcaid_elig',
                                        'Actual vs. expected date range in data', 
                                        {qa_date_range$outcome[1]},
                                        {format(Sys.time(), usetz = FALSE)},
                                        {qa_date_range$note[1]})",
                                  .con = db_claims))
  # Report combined years result out to SQL table
  DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                'load_raw.mcaid_elig',
                                'Actual vs. expected date range in combined SQL table', 
                                {qa_date_range$outcome[2]},
                                {format(Sys.time(), usetz = FALSE)},
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
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'load_raw.mcaid_elig',
                   'Length of Medicaid ID', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'Minimum ID length was {id_len$min_len}, maximum was {id_len$max_len}')",
                     .con = db_claims))
    
    stop(glue::glue("Some Medicaid IDs are not 11 characters long.  
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
  } else {
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'load_raw.mcaid_elig',
                   'Length of Medicaid ID', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
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
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'load_raw.mcaid_elig',
                   'Length of RAC codes', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'Min RPRTBLE_RAC_CODE length was {rac_len$min_len}, max was {rac_len$max_len};
                   Min SECONDARY_RAC_CODE length was {rac_len$min_len2}, max was {rac_len$max_len2}')",
                     .con = db_claims))
    
    stop(glue::glue("Some RAC codes are not 4 characters long.  
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
  } else {
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'load_raw.mcaid_elig',
                   'Length of RAC codes', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
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
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'load_raw.mcaid_elig',
                   'NULL from dates', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There were {from_nulls$null_dates} NULL from dates ({pct_null}% of total rows)')",
                     .con = db_claims))
    
    stop(glue::glue(">2% FROM_DATE rows are null.  
                  Check metadata.qa_mcaid for details (etl_batch_id = {current_batch_id}"))
  } else {
    DBI::dbExecute(
      conn = db_claims,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   'load_raw.mcaid_elig',
                   'NULL from dates', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   '<2% of from date rows were null ({pct_null}% of total rows)')",
                     .con = db_claims))
  }
  
  print("All QA items passed, see results in metadata.qa_mcaid")
  
  
  #### ADD BATCH ID COLUMN ####
  print("Adding batch ID to SQL table")
  # Add column to the SQL table and set current batch to the default
  DBI::dbExecute(db_claims,
                   glue::glue_sql(
                     "ALTER TABLE load_raw.mcaid_elig 
                   ADD etl_batch_id INTEGER 
                   DEFAULT {current_batch_id} WITH VALUES",
                     .con = db_claims))
  
  
  #### ADD VALUES TO QA_VALUES TABLE ####
  print("Loading values to metadata value table")
  DBI::dbExecute(
    conn = db_claims,
    glue::glue_sql("INSERT INTO metadata.qa_mcaid_values
                   (table_name, qa_item, qa_value, qa_date, note) 
                   VALUES ('load_raw.mcaid_elig',
                   'row_count', 
                   '{total_rows}', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'Count after full refresh')",
                   .con = db_claims))
  
  print("All eligibility data loaded to SQL and QA checked")

}

