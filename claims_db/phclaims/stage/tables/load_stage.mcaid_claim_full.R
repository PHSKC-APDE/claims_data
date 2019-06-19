#### CODE TO CREATE STAGE MCAID CLAIM TABLE
# Full refresh version
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

### Run from master_mcaid_full script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_full.R


#### FIND MOST RECENT BATCH ID FROM SOURCE (LOAD_RAW) ####
current_batch_id <- as.numeric(odbc::dbGetQuery(db_claims,
                                     "SELECT MAX(etl_batch_id) FROM load_raw.mcaid_claim"))


#### LOAD TABLE ####
# Can't use default load function because some transformation is needed
# Need to make two new variables

# Call in config file to get vars
table_config_stage_claim <- yaml::yaml.load(RCurl::getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_full.yaml"
))

from_schema <- table_config_stage_claim$from_schema
from_table <- table_config_stage_claim$from_table
to_schema <- table_config_stage_claim$to_schema
to_table <- table_config_stage_claim$to_table
vars <- unlist(table_config_stage_claim$vars)

# Need to keep only the vars that come after the named ones below
vars_truncated <- vars[!vars %in% c("MBR_H_SID", "MEDICAID_RECIPIENT_ID",
                                    "BABY_ON_MOM_IND", "TCN", "CLM_LINE_TCN")]

load_sql <- glue::glue_sql(
  "INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK) 
  SELECT CAST(YEAR([FROM_SRVC_DATE]) AS INT) * 100 + CAST(MONTH([FROM_SRVC_DATE]) AS INT) AS [CLNDR_YEAR_MNTH],
  MBR_H_SID, MEDICAID_RECIPIENT_ID, BABY_ON_MOM_IND, TCN, CLM_LINE_TCN,
  CAST(RIGHT(CLM_LINE_TCN, 3) AS INTEGER) AS CLM_LINE,
  {`vars_truncated`*}
  FROM {`from_schema`}.{`from_table`}",
  .con = db_claims
)


#### QA CHECK: NUMBER OF ROWS IN SQL TABLE ####
print("Running QA checks")
# Because of deduplication, should be 42 less than load_raw table
rows_stage <- as.numeric(dbGetQuery(db_claims, "SELECT COUNT (*) FROM stage.mcaid_claim"))
rows_load_raw <- as.numeric(dbGetQuery(db_claims, "SELECT COUNT (*) FROM load_raw.mcaid_claim"))

if (rows_load_raw != rows_stage) {
  odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_claim',
                                  'Rows passed from load_raw to stage', 
                                  'FAIL',
                                  {Sys.time()},
                                  'Number of rows in stage doesn't match load_raw)",
                                  .con = db_claims))
  stop("Number of rows does not match total expected")
  } else {
    odbc::dbGetQuery(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_claim',
                                  'Rows passed from load_raw to stage', 
                                  'PASS',
                                  {Sys.time()},
                                  'Number of rows in stage matches load_raw')",
                                  .con = db_claims))
    }


#### QA CHECK: NULL IDs ####
null_ids <- as.numeric(dbGetQuery(db_claims, 
                                    "SELECT COUNT (*) FROM stage.mcaid_claim 
                                    WHERE MEDICAID_RECIPIENT_ID IS NULL"))

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


#### ADD INDEX ####
print("Adding index")
if (!is.null(table_config_stage_claim$index_name)) {
  index_sql <- glue::glue_sql("CREATE CLUSTERED INDEX [{`table_config_stage_claim$index_name`}] ON 
                              {`to_schema`}.{`to_table`}({index_vars*})",
                              index_vars = dbQuoteIdentifier(db_claims, table_config_stage_claim$index),
                              .con = db_claims)
  dbGetQuery(db_claims, index_sql)
}


#### ADD VALUES TO QA_VALUES TABLE ####
odbc::dbGetQuery(
  conn = db_claims,
  glue::glue_sql("INSERT INTO metadata.qa_mcaid_values
                   (table_name, qa_item, qa_value, qa_date, note) 
                   VALUES ('stage.mcaid_claim',
                   'row_count', 
                   '{rows_stage}', 
                   {Sys.time()}, 
                   'Count after full refresh')",
                 .con = db_claims))


#### CLEAN UP ####
# Drop global temp table
rm(load_sql)
rm(vars, vars_truncated)
rm(from_schema, from_table, to_schema, to_table)
rm(rows_stage, rows_load_raw, null_ids)
rm(table_config_stage_claim)
rm(index_sql)

