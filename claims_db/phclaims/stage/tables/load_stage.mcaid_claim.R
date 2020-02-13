#### CODE TO CREATE STAGE MCAID CLAIM TABLE
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05, updated 2020-02

### Run from master_mcaid_full script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_full.R


load_stage.mcaid_claim_f <- function(full_refresh = F) {
  #### CALL IN CONFIG FILE TO GET VARS ####
  table_config_stage_claim <- yaml::yaml.load(RCurl::getURL(
    "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim.yaml"
  ))
  
  from_schema <- table_config_stage_claim$from_schema
  from_table <- table_config_stage_claim$from_table
  to_schema <- table_config_stage_claim$to_schema
  to_table <- table_config_stage_claim$to_table
  
  if (full_refresh == F) {
    archive_schema <- table_config_load_claim$archive_schema
    date_truncate <- table_config_load_claim$overall$date_min
  }
  
  vars <- unlist(names(table_config_stage_claim$vars))
  # Need to keep only the vars that come after the named ones below
  vars_truncated <- vars[!vars %in% c("CLNDR_YEAR_MNTH", "MBR_H_SID", 
                                      "MEDICAID_RECIPIENT_ID", "BABY_ON_MOM_IND", 
                                      "TCN", "CLM_LINE_TCN", "CLM_LINE")]
  
  
  #### CALL IN FUNCTIONS IF NOT ALREADY LOADED ####
  if (exists("alter_schema_f") == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/alter_schema.R")
  }
  if (exists("add_index_f") == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/add_index.R")
  }
  
  
  #### FIND MOST RECENT BATCH ID FROM SOURCE (LOAD_RAW) ####
  message("Looking up etl_batch_id")
  current_batch_id <- as.numeric(odbc::dbGetQuery(db_claims,
                                                  glue::glue_sql("SELECT MAX(etl_batch_id) FROM {`from_schema`}.{`from_table`}",
                                                                 .con = db_claims)))
  
  if (is.na(current_batch_id)) {
    stop(glue::glue_sql("Missing etl_batch_id in {`from_schema`}.{`from_table`}"))
  }

  
  #### ARCHIVE EXISTING TABLE ####
  if (full_refresh == F) {
    message("Archiving existing stage table")
    alter_schema_f(conn = db_claims, from_schema = to_schema, to_schema = archive_schema,
                   table_name = to_table)
  }
  
  #### LOAD TABLE ####
  # Can't use default load function because some transformation is needed
  # Need to make two new variables
  if (full_refresh == F) {
    load_sql <- glue::glue_sql(
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
  } else {
    load_sql <- glue::glue_sql(
      "INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK) 
      ({`vars`*}) 
      SELECT CAST(YEAR([FROM_SRVC_DATE]) AS INT) * 100 + CAST(MONTH([FROM_SRVC_DATE]) AS INT) AS [CLNDR_YEAR_MNTH],
      MBR_H_SID, MEDICAID_RECIPIENT_ID, BABY_ON_MOM_IND, TCN, CLM_LINE_TCN,
      CAST(RIGHT(CLM_LINE_TCN, 3) AS INTEGER) AS CLM_LINE,
      {`vars_truncated`*}
      FROM {`from_schema`}.{`from_table`}",
      .con = db_claims)
  }
  
  message("Loading to stage table")
  DBI::dbExecute(db_claims, load_sql)
  
  
  #### ADD INDEX ####
  add_index_f(conn = db_claims, table_config = table_config_stage_claim)
  
  
  #### QA CHECK: NUMBER OF ROWS IN SQL TABLE ####
  message("Running QA checks")
  rows_stage <- as.numeric(dbGetQuery(
    db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}", .con = db_claims)))
  rows_load_raw <- as.numeric(dbGetQuery(
    db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`from_schema`}.{`from_table`}", .con = db_claims)))
  
  if (full_refresh == F) {
    rows_archive <- as.numeric(dbGetQuery(
      db_claims, glue::glue_sql("SELECT COUNT (*) FROM {`archive_schema`}.{`to_table`} 
                            WHERE {`table_config_stage_claim$date_var`} < {date_truncate}", 
                                .con = db_claims)))
    
    rows_diff <- rows_stage - (rows_load_raw + rows_archive)
    row_diff_qa_type <- 'Rows passed from load_raw AND archive to stage'
    
    if (rows_diff != 0) {
      row_diff_qa_note <- paste0('Number of rows in stage ({rows_stage}) does not match ',
                                 'load_raw (', rows_load_raw, ') + archive (', rows_archive, ')')
    } else {
      row_diff_qa_note <- paste0('Number of rows in stage matches load_raw + archive (', rows_stage, ')')
    }
  } else {
    rows_diff <- rows_stage - rows_load_raw
    row_diff_qa_type <- 'Rows passed from load_raw to stage'
    if (rows_diff != 0) {
      row_diff_qa_note <- paste0('Number of rows in stage (', rows_stage, 
                                 ') does not match load_raw (', rows_load_raw, ')')
    } else {
      row_diff_qa_note <- paste0('Number of rows in stage matches load_raw (', rows_stage, ')')
    }
  }

  
  if (rows_diff != 0) {
    row_diff_qa_fail <- 1
    DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_claim',
                                  {row_diff_qa_type}, 
                                  'FAIL',
                                  {Sys.time()},
                                  {row_diff_qa_note})",
                                  .con = db_claims))
    warning("Number of rows does not match total expected")
  } else {
    row_diff_qa_fail <- 0
    DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_claim',
                                  {row_diff_qa_type}, 
                                  'PASS',
                                  {Sys.time()},
                                  {row_diff_qa_note})",
                                  .con = db_claims))
  }
  
  
  #### QA CHECK: NULL IDs ####
  null_ids <- as.numeric(dbGetQuery(
    db_claims, 
    glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`} 
                 WHERE MEDICAID_RECIPIENT_ID IS NULL", 
                   .con = db_claims)))
  
  if (null_ids != 0) {
    null_ids_qa_fail <- 1
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
    warning("Null Medicaid IDs found in stage.mcaid_claim")
  } else {
    null_ids_qa_fail <- 0
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
                   {refresh_type})",
                   refresh_type = ifelse(full_refresh == F, 
                                         'Count after partial refresh', 
                                         'Count after full refresh'),
                   .con = db_claims))
  
  
  #### ADD OVERALL QA RESULT ####
  # This creates an overall QA result to feed the stage.v_mcaid_status view, 
  #    which is used by the integrated data hub to check for new data to run
  if (max(row_diff_qa_fail, null_ids_qa_fail) == 1) {
    DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_claim',
                                  'Overall QA result', 
                                  'FAIL',
                                  {Sys.time()},
                                  'One or more QA steps failed')",
                                  .con = db_claims))
    stop("One or more QA steps failed. See metadata.qa_mcaid for more details")
  } else {
    DBI::dbExecute(conn = db_claims,
                   glue::glue_sql("INSERT INTO metadata.qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'stage.mcaid_claim',
                                  'Overall QA result', 
                                  'PASS',
                                  {Sys.time()},
                                  'All QA steps passed')",
                                  .con = db_claims))
  }
  
  #### CLEAN UP ####
  suppressWarnings(rm(from_schema, from_table, to_schema, to_table, archive_schema, date_truncate, 
                      vars, vars_truncated, current_batch_id))
  suppressWarnings(rm(rows_stage, rows_load_raw, rows_archive, rows_diff, null_ids))
  rm(row_diff_qa_type, row_diff_qa_note)
  rm(row_diff_qa_fail, null_ids_qa_fail)
  rm(load_sql)
  rm(table_config_stage_claim)
  
}



