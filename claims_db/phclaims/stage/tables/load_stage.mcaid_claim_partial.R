#### CODE TO CREATE STAGE MCAID CLAIM TABLE
# Partial refresh version
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_partial.R



#### CALL IN CONFIG FILE TO GET VARS ####
table_config_stage_claim <- yaml::yaml.load(RCurl::getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_partial.yaml"
))

table_config_load_claim <- yaml::yaml.load(RCurl::getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_partial.yaml"
))

from_schema <- table_config_stage_claim$from_schema
from_table <- table_config_stage_claim$from_table
to_schema <- table_config_stage_claim$to_schema
to_table <- table_config_stage_claim$to_table
archive_schema <- table_config_stage_claim$archive_schema

date_truncate <- table_config_load_claim$overall$date_min

vars <- unlist(names(table_config_stage_claim$vars))
# Need to keep only the vars that come after the named ones below
# This is because some additional casting is needed to make CLNDR_YEAR_MNTH and CLM_LINE
vars_truncated <- vars[!vars %in% c("CLNDR_YEAR_MNTH", "MBR_H_SID", 
                                    "MEDICAID_RECIPIENT_ID", "BABY_ON_MOM_IND", 
                                    "TCN", "CLM_LINE_TCN", "CLM_LINE")]


#### CALL IN FUNCTIONS ####
### alter_schema
if (exists("alter_schema_f") == F) {
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/alter_schema.R")
}

### index
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


### Check load_table function because it appears to truncate stage before moving to archive
# load_table_from_sql_f(conn = db_claims,
#                       config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_partial.yaml",
#                       truncate = F, truncate_date = T, mcaid_claim = T)


#### ARCHIVE EXISTING TABLE ####
alter_schema_f(conn = db_claims, from_schema = to_schema, to_schema = archive_schema,
               table_name = to_table)


#### LOAD TABLE ####
message("Recreating stage table")

# First create new table
create_table_f(db_claims,
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_partial.yaml",
               overall = T, ind_yr = F, overwrite = F)

# Insert data in
sql_combine <- glue::glue_sql(
  "INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK) 
        ({`vars`*}) 
        SELECT {`vars`*} FROM {`archive_schema`}.{`to_table`}
          WHERE {`date_var`} < {date_truncate}
        UNION
        SELECT CAST(YEAR([FROM_SRVC_DATE]) AS INT) * 100 + CAST(MONTH([FROM_SRVC_DATE]) AS INT) AS [CLNDR_YEAR_MNTH],
        MBR_H_SID, MEDICAID_RECIPIENT_ID, BABY_ON_MOM_IND, TCN, CLM_LINE_TCN,
        CAST(RIGHT(CLM_LINE_TCN, 3) AS INTEGER) AS CLM_LINE, {`vars_truncated`*}
        FROM {`from_schema`}.{`from_table`}",
  .con = db_claims,
  date_var = table_config_stage_claim$date_var)

DBI::dbExecute(db_claims, sql_combine)


#### ADD INDEX ####
add_index_f(conn = db_claims, table_config = table_config_stage_claim)


#### QA CHECK: NUMBER OF ROWS IN SQL TABLE ####
message("Running QA checks")
rows_stage <- as.numeric(dbGetQuery(
  db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}", .con = db_claims)))
rows_load_raw <- as.numeric(dbGetQuery(
  db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`from_schema`}.{`from_table`}", .con = db_claims)))
rows_archive <- as.numeric(dbGetQuery(
  db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`archive_schema`}.{`to_table`} 
                            WHERE {`table_config_stage_claim$date_var`} < {date_truncate}", 
                            .con = db_claims)))


if (rows_stage != (rows_load_raw + rows_archive)) {
  DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_claim',
                                  'Rows passed from load_raw AND archive to stage', 
                                  'FAIL',
                                  {Sys.time()},
                                  'Number of rows in stage ({rows_stage}) does not match \\\
                                  load_raw ({rows_load_raw}) + archive ({rows_archive})')",
                                  .con = db_claims))
  stop("Number of rows does not match total expected")
  } else {
    DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_claim',
                                  'Rows passed from load_raw AND archive to stage', 
                                  'PASS',
                                  {Sys.time()},
                                  'Number of rows in stage matches load_raw + archive ({rows_stage})')",
                                  .con = db_claims))
    }


#### QA CHECK: NULL IDs ####
null_ids <- as.numeric(dbGetQuery(
  db_claims, 
  glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`} 
                 WHERE MEDICAID_RECIPIENT_ID IS NULL", 
                 .con = db_claims)))

if (null_ids != 0) {
  DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_claim',
                                  'Null Medicaid IDs', 
                                  'FAIL',
                                  {Sys.time()},
                                  'Null IDs found. Investigate further.')",
                                  .con = db_claims))
  stop("Null Medicaid IDs found in stage.mcaid_claim")
} else {
  DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_claim',
                                  'Null Medicaid IDs', 
                                  'PASS',
                                  {Sys.time()},
                                  'No null IDs found')",
                                  .con = db_claims))
}


### Add QA check on date range?


#### ADD VALUES TO QA_VALUES TABLE ####
# Number of new rows
DBI::dbExecute(
  conn = db_claims,
  glue::glue_sql("INSERT INTO metadata.qa_mcaid_values
                   (table_name, qa_item, qa_value, qa_date, note) 
                   VALUES ('stage.mcaid_claim',
                   'row_count', 
                   '{rows_stage}', 
                   {Sys.time()}, 
                   'Count after partial refresh')",
                 .con = db_claims))




#### CLEAN UP ####
rm(from_schema, from_table, to_schema, to_table, archive_schema, date_truncate, 
   vars, vars_truncated, current_batch_id)
rm(rows_stage, rows_load_raw, rows_archive, null_ids)
rm(sql_combine)
rm(table_config_stage_claim)

