#### CODE TO CREATE STAGE MCAID CLAIM TABLE
# Monthly refresh version
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

### Run from master_mcaid_monthly script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_monthly.R



#### CALL IN CONFIG FILE TO GET VARS ####
table_config_stage_claim <- yaml::yaml.load(RCurl::getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_monthly.yaml"
))

from_schema <- table_config_stage_claim$from_schema
from_table <- table_config_stage_claim$from_table
to_schema <- table_config_stage_claim$to_schema
to_table <- table_config_stage_claim$to_table


#### FIND MOST RECENT BATCH ID FROM SOURCE (LOAD_RAW) ####
current_batch_id <- as.numeric(odbc::dbGetQuery(db_claims,
                                     glue::glue_sql("SELECT MAX(etl_batch_id) FROM {`from_schema`}.{`from_table`}",
                                                    .con = db_claims)))

if (is.na(current_batch_id)) {
  stop(glue::glue_sql("Missing etl_batch_id in {`from_schema`}.{`from_table`}"))
}

#### LOAD TABLE ####
load_table_from_sql_f(conn = db_claims,
                      config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_monthly.yaml",
                      truncate = F, truncate_date = T, mcaid_claim = T)


#### QA CHECK: NUMBER OF ROWS IN SQL TABLE ####
print("Running QA checks")
rows_stage <- as.numeric(dbGetQuery(
  db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}", .con = db_claims)))
rows_load_raw <- as.numeric(dbGetQuery(
  db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`from_schema`}.{`from_table`}", .con = db_claims)))
rows_archive <- as.numeric(dbGetQuery(
  db_claims, glue::glue_sql("SELECT COUNT (*) FROM archive.{`to_table`} 
                            WHERE {`table_config_stage_claim$date_var`} < \\\
                            {as.character(table_config_stage_claim$date_truncate)}", .con = db_claims)))


if (rows_stage != (rows_load_raw + rows_archive)) {
  odbc::dbGetQuery(conn = db_claims,
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
    odbc::dbGetQuery(conn = db_claims,
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
  odbc::dbGetQuery(conn = db_claims,
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
  odbc::dbGetQuery(conn = db_claims,
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
odbc::dbGetQuery(
  conn = db_claims,
  glue::glue_sql("INSERT INTO metadata.qa_mcaid_values
                   (table_name, qa_item, qa_value, qa_date, note) 
                   VALUES ('stage.mcaid_claim',
                   'row_count', 
                   '{rows_stage}', 
                   {Sys.time()}, 
                   'Count after monthly refresh')",
                 .con = db_claims))




#### CLEAN UP ####
rm(from_schema, from_table, to_Schema, to_table)
rm(rows_stage, rows_load_raw, rows_archive, null_ids)
rm(table_config_stage_claim)
rm(index_sql)

