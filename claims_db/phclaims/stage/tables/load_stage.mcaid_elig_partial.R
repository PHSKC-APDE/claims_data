#### CODE TO CREATE STAGE MCAID ELIG TABLE
# Partial refresh version
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-08

### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_partial.R


#### CALL IN CONFIG FILES TO GET VARS ####
table_config_stage_elig <- yaml::yaml.load(RCurl::getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_partial.yaml"
))

table_config_load_elig <- yaml::yaml.load(RCurl::getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_partial.yaml"
))

from_schema <- table_config_stage_elig$from_schema
from_table <- table_config_stage_elig$from_table
to_schema <- table_config_stage_elig$to_schema
to_table <- table_config_stage_elig$to_table
archive_schema <- table_config_stage_elig$archive_schema

date_truncate <- table_config_load_elig$overall$date_min

vars <- unlist(names(table_config_stage_elig$vars))
# Need to specify which temp table the vars come from
# Can't handle this just with glue_sql
# (see https://community.rstudio.com/t/using-glue-sql-s-collapse-with-table-name-identifiers/11633)
var_names <- lapply(names(table_config_stage_elig$vars), 
                    function(nme) DBI::Id(table = "a", column = nme))
vars_dedup <- lapply(var_names, DBI::dbQuoteIdentifier, conn = db_claims)


#### CALL IN SCHEMA ALTER FUNCTION ####
if (exists("alter_schema_f") == F) {
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/alter_schema.R")
}

#### CALL IN INDEX FUNCTION ####
if (exists("add_index_f") == F) {
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/add_index.R")
}


#### FIND MOST RECENT BATCH ID FROM SOURCE (LOAD_RAW) ####
current_batch_id <- as.numeric(odbc::dbGetQuery(db_claims,
                                     glue::glue_sql("SELECT MAX(etl_batch_id) FROM {`from_schema`}.{`from_table`}",
                                                    .con = db_claims)))

if (is.na(current_batch_id)) {
  stop(glue::glue_sql("Missing etl_batch_id in {`from_schema`}.{`from_table`}"))
}


#### ARCHIVE EXISTING TABLE ####
alter_schema_f(conn = db_claims, from_schema = to_schema, to_schema = archive_schema,
               table_name = to_table)


#### LOAD TABLE ####
# For dates prior to 2018-09, some rows had multiple rows per person-month-RAC
# where there were multiple end reasons. Should no longer be an issue for partial
# refreshes but add in a check for duplicates just in case the issue returns.
message("Checking for multiple END_REASON rows per id/month/RAC combo")
duplicate_check <- as.numeric(dbGetQuery(
  db_claims,
  glue::glue_sql("SELECT COUNT (*) FROM
           (SELECT DISTINCT CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, 
             TO_DATE, RPRTBL_RAC_CODE, SECONDARY_RAC_CODE, END_REASON
             FROM {`from_schema`}.{`from_table`}) a",
           .con = db_claims)))

total_rows <- as.numeric(dbGetQuery(
  db_claims,
  glue::glue_sql("SELECT COUNT (*) FROM {`from_schema`}.{`from_table`}", .con = db_claims)))

if (duplicate_check != total_rows) {
  stop("There appears to be duplicate end reasons. Check and use temp table code to fix.")
}


# # Use priority set out below (higher resaon score = higher priority)
# ### Set up temporary table
# message("Setting up a temp table to remove duplicate rows")
# # This can then be used to deduplicate rows with differing end reasons
# # Remove temp table if it exists
# try(odbc::dbRemoveTable(db_claims, "##mcaid_elig_temp", temporary = T))
# 
# odbc::dbGetQuery(db_claims,
#                  glue::glue_sql("SELECT {`vars`*}, 
#                                 CASE WHEN END_REASON IS NULL THEN 1 
#                                   WHEN END_REASON = 'Other' THEN 2 
#                                   WHEN END_REASON = 'Other - For User Generation Only' THEN 3 
#                                   WHEN END_REASON = 'Review Not Complete' THEN 4 
#                                   WHEN END_REASON = 'No Eligible Household Members' THEN 5 
#                                   WHEN END_REASON = 'Already Eligible for Program in Different AU' THEN 6 
#                                   ELSE 7 END AS reason_score
#                                 INTO ##mcaid_elig_temp
#                                 FROM {`from_schema`}.{`from_table`}",
#                                 .con = db_claims))
# 
# 
# # Check no dups exist by recording row counts
# temp_rows_01 <- as.numeric(dbGetQuery(db_claims, "SELECT COUNT (*) FROM ##mcaid_elig_temp"))
# message(glue::glue("The ##mcaid_elig_temp table has {temp_rows_01} rows"))
# 
# ### Manipulate the temporary table to deduplicate
# # Remove temp table if it exists
# try(odbc::dbRemoveTable(db_claims, "##mcaid_elig_dedup", temporary = T))
# 
# dedup_sql <- glue::glue_sql(
#   'SELECT {`vars_dedup`*} 
#   INTO ##mcaid_elig_dedup 
#   FROM
#     (SELECT {`vars`*}, reason_score FROM ##mcaid_elig_temp) a
#   LEFT JOIN
#     (SELECT CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, 
#       TO_DATE, SECONDARY_RAC_CODE, MAX(reason_score) AS max_score 
#     FROM ##mcaid_elig_temp
#     GROUP BY CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, FROM_DATE, 
#     TO_DATE, SECONDARY_RAC_CODE) b
#   ON a.CLNDR_YEAR_MNTH = b.CLNDR_YEAR_MNTH AND 
#     a.MEDICAID_RECIPIENT_ID = b.MEDICAID_RECIPIENT_ID AND 
#     (a.FROM_DATE = b.FROM_DATE OR (a.FROM_DATE IS NULL AND b.FROM_DATE IS NULL)) AND 
#     (a.TO_DATE = b.TO_DATE OR (a.TO_DATE IS NULL AND b.TO_DATE IS NULL)) AND 
#     (a.SECONDARY_RAC_CODE = b.SECONDARY_RAC_CODE OR 
#       a.SECONDARY_RAC_CODE IS NULL AND b.SECONDARY_RAC_CODE IS NULL) 
#   WHERE a.reason_score = b.max_score',
#     .con = db_claims)
# 
# odbc::dbGetQuery(db_claims, dedup_sql)
# 
# temp_rows_02 <- as.numeric(dbGetQuery(db_claims, "SELECT COUNT (*) FROM ##mcaid_elig_dedup"))
# message(glue::glue("The ##mcaid_elig_dedup table has {temp_rows_02} rows"))
# 
# ### Combine relevant part of archive table and deduplicated temp data
# sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK)
#                                   SELECT {`vars`*} FROM 
#                                   archive.{`to_table`}
#                                   WHERE {`date_var`} < {date_truncate}  
#                                   UNION 
#                                   SELECT {`vars`*} FROM 
#                                   ##mcaid_elig_dedup
#                                   WHERE {`date_var`} >= {date_truncate}",
#                               .con = db_claims,
#                               date_var = table_config_stage_elig$date_var)
# 
# odbc::dbGetQuery(db_claims, sql_combine)


### Combine relevant parts of archive and new data
message("Recreating stage table")

# First create new table
create_table_f(db_claims,
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_partial.yaml",
               overall = T, ind_yr = F, overwrite = F)

sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK)
                                  SELECT {`vars`*} FROM
                                  {`archive_schema`}.{`to_table`}
                                  WHERE {`date_var`} < {date_truncate}
                                  UNION
                                  SELECT {`vars`*} FROM
                                  {`from_schema`}.{`from_table`}
                                  WHERE {`date_var`} >= {date_truncate}",
                              .con = db_claims,
                              date_var = table_config_stage_elig$date_var)

odbc::dbGetQuery(db_claims, sql_combine)


#### QA CHECK: NUMBER OF ROWS IN SQL TABLE ####
message("Running QA checks")
# Because of deduplication, should be 1 less than load_raw table + archive before date cutoff
rows_stage <- as.numeric(dbGetQuery(
  db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}", .con = db_claims)))
rows_load_raw <- as.numeric(dbGetQuery(
  db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`from_schema`}.{`from_table`}", .con = db_claims)))
rows_archive <- as.numeric(dbGetQuery(
  db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`archive_schema`}.{`to_table`} 
                            WHERE {`table_config_stage_elig$date_var`} < {date_truncate}",
                            .con = db_claims)))

as.numeric(dbGetQuery(
  db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`archive_schema`}.{`to_table`}
                                  WHERE {`date_var`} < {date_truncate}", 
                            .con = db_claims, date_var = table_config_stage_elig$date_var)))


if ((rows_archive + rows_load_raw) - rows_stage != 0) {
  odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  'Rows passed from load_raw AND archive to stage', 
                                  'FAIL',
                                  {Sys.time()},
                                  'Issue even after accounting for the 1 duplicate row. Investigate further.')",
                                  .con = db_claims))
  stop("Number of distinct rows does not match total expected")
  } else {
    odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  'Rows passed from load_raw AND archive to stage', 
                                  'PASS',
                                  {Sys.time()},
                                  'Number of rows in stage matches load_raw and archive (n = {rows_stage})')",
                                  .con = db_claims))
    }


#### QA CHECK: NULL IDs ####
null_ids <- as.numeric(dbGetQuery(db_claims, 
                                    "SELECT COUNT (*) FROM stage.mcaid_elig 
                                    WHERE MEDICAID_RECIPIENT_ID IS NULL"))

if (null_ids != 0) {
  odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  'Null Medicaid IDs', 
                                  'FAIL',
                                  {Sys.time()},
                                  'Null IDs found. Investigate further.')",
                                  .con = db_claims))
  stop("Null Medicaid IDs found in stage.mcaid_elig")
} else {
  odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_elig',
                                  'Null Medicaid IDs', 
                                  'PASS',
                                  {Sys.time()},
                                  'No null IDs found')",
                                  .con = db_claims))
}


#### ADD INDEX ####
message("Adding index")
add_index_f(conn = db_claims, table_config = table_config_stage_elig)


#### ADD VALUES TO QA_VALUES TABLE ####
odbc::dbGetQuery(
  conn = db_claims,
  glue::glue_sql("INSERT INTO metadata.qa_mcaid_values
                   (table_name, qa_item, qa_value, qa_date, note) 
                   VALUES ('stage.mcaid_elig',
                   'row_count', 
                   '{rows_stage}', 
                   {Sys.time()}, 
                   'Count after partial refresh')",
                 .con = db_claims))


#### CLEAN UP ####
# Drop global temp table
try(odbc::dbRemoveTable(db_claims, "##mcaid_elig_temp", temporary = T))
try(odbc::dbRemoveTable(db_claims, "##mcaid_elig_dedup", temporary = T))
rm(dedup_sql)
rm(vars, var_names, vars_dedup)
rm(duplicate_check, total_rows)
rm(from_schema, from_table, to_schema, to_table, archive_schema, date_truncate)
rm(rows_stage, rows_load_raw, rows_archive, null_ids)
rm(table_config_stage_elig)
rm(sql_combine, sql_archive)
rm(current_batch_id)

