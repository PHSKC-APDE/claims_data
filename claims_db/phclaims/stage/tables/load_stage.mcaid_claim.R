#### CODE TO CREATE STAGE MCAID CLAIMS TABLE
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05, updated 2020-02, 2020-07

### Run from master_mcaid_full script
# https://github.com/PHSKC-APDE/claims_data/blob/azure_migration/claims_db/db_loader/mcaid/master_mcaid_full.R


load_claims.stage_mcaid_claim_f <- function(conn = NULL, full_refresh = F, config = NULL) {
  ### Error checks
  if (is.null(conn)) {stop("No DB connection specificed")}
  if (is.null(config)) {stop("Specify a list with config details")}
  
  
  #### GET VARS FROM CONFIG FILE ####
  from_schema <- config$from_schema
  from_table <- config$from_table
  to_schema <- config$to_schema
  to_table <- config$to_table
  
  vars <- unlist(names(config$vars))
  # Need to keep only the vars that come after the named ones below
  vars_truncated <- vars[!vars %in% c("CLNDR_YEAR_MNTH", "MBR_H_SID", 
                                      "MEDICAID_RECIPIENT_ID", "BABY_ON_MOM_IND", 
                                      "TCN", "CLM_LINE_TCN", "CLM_LINE", "etl_batch_id")]
  # Adjust vars to account for different names in raw data
  # This fix isn't ideal and somewhat defeats the point of the YAML files
  vars_truncated <- str_replace(vars_truncated, "LAST_", "LT_")
  
  
  if (!is.null(config$etl$date_var)) {
    date_var <- config$etl$date_var
  } else {
    date_var <- config$date_var
  }
  
  
  if (full_refresh == F) {
    archive_schema <- config$archive_schema
    archive_table <- config$archive_table
    date_truncate <- config$etl$date_min
    
    etl_batch_type <- "incremental"
  } else {
    etl_batch_type <- "full"
  }

  
  #### FIND MOST RECENT BATCH ID FROM SOURCE (raw) ####
  # Now need to make ETL batch ID here as it is added to stage.
  # Raw data are in an external table that points to the data warehouse. Can't add an ETL column to that.

  current_batch_id <- load_metadata_etl_log_f(conn = db_claims, 
                                              batch_type = etl_batch_type,
                                              data_source = "Medicaid", 
                                              date_min = config$etl$date_min,
                                              date_max = config$etl$date_max,
                                              delivery_date = config$etl$date_delivery, 
                                              note = config$etl$note)

  
  #### ARCHIVE EXISTING TABLE ####
  # No longer switching schema, instead just renaming table. Need to drop existing archive table first
  if (full_refresh == F) {
    try(DBI::dbSendQuery(conn, glue::glue("DROP TABLE {archive_schema}.{archive_table}")))
    DBI::dbSendQuery(conn, glue::glue("EXEC sp_rename '{to_schema}.{to_table}', '{archive_table}'"))
  }
  

  #### LOAD TABLE ####
  # Need to recreate stage table first (true if full_refresh == F or T)
  # Assumes create_table_f loaded as part of the master script
  create_table_f(conn = conn, 
                 config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/stage/tables/load_stage.mcaid_claim.yaml", 
                 overwrite = T)
  
  # Can't use default load function because some transformation is needed
  # Need to make two new variables
  if (full_refresh == F) {
    load_sql <- glue::glue_sql(
      "INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK) 
        ({`vars`*}) 
        SELECT {`vars`*} FROM {`archive_schema`}.{`archive_table`}
          WHERE {`date_var`} < {date_truncate}
        UNION
        SELECT DISTINCT CAST(YEAR([FROM_SRVC_DATE]) AS INT) * 100 + CAST(MONTH([FROM_SRVC_DATE]) AS INT) AS [CLNDR_YEAR_MNTH],
        MBR_H_SID, MEDICAID_RECIPIENT_ID, BABY_ON_MOM_IND, TCN, CLM_LINE_TCN,
        CAST(RIGHT(CLM_LINE_TCN, 3) AS INTEGER) AS CLM_LINE, {`vars_truncated`*},
        {current_batch_id} AS etl_batch_id
        FROM {`from_schema`}.{`from_table`}",
      .con = conn,
      date_var = config$date_var)
  } else {
    load_sql <- glue::glue_sql(
      "INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK) 
      ({`vars`*}) 
      SELECT DISTINCT CAST(YEAR([FROM_SRVC_DATE]) AS INT) * 100 + CAST(MONTH([FROM_SRVC_DATE]) AS INT) AS [CLNDR_YEAR_MNTH],
      MBR_H_SID, MEDICAID_RECIPIENT_ID, BABY_ON_MOM_IND, TCN, CLM_LINE_TCN,
      CAST(RIGHT(CLM_LINE_TCN, 3) AS INTEGER) AS CLM_LINE,
      {`vars_truncated`*}, {current_batch_id} AS etl_batch_id
      FROM {`from_schema`}.{`from_table`}",
      .con = conn)
  }
  
  message("Loading to stage table")
  system.time(DBI::dbExecute(conn, load_sql))
  
  
  #### ADD INDEX ####
  add_index_f(conn = conn, table_config = config)
  
  
  #### QA CHECK: NUMBER OF ROWS IN SQL TABLE ####
  message("Running QA checks")
  rows_stage <- as.numeric(dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}", .con = conn)))
  rows_raw <- as.numeric(dbGetQuery(
    conn, glue::glue_sql("SELECT COUNT (*) FROM {`from_schema`}.{`from_table`}", .con = conn)))
  
  if (full_refresh == F) {
    rows_archive <- as.numeric(dbGetQuery(
      conn, glue::glue_sql("SELECT COUNT (*) FROM {`archive_schema`}.{`archive_table`} 
                            WHERE {`config$date_var`} < {date_truncate}", 
                                .con = conn)))
    
    rows_diff <- rows_stage - (rows_raw + rows_archive)
    row_diff_qa_type <- 'Rows passed from raw AND archive to stage'
    
    if (rows_diff != 0) {
      row_diff_qa_note <- paste0('Number of rows in stage ({rows_stage}) does not match ',
                                 'raw (', rows_raw, ') + archive (', rows_archive, ')')
    } else {
      row_diff_qa_note <- paste0('Number of rows in stage matches raw + archive (', rows_stage, ')')
    }
  } else {
    rows_diff <- rows_stage - rows_raw
    row_diff_qa_type <- 'Rows passed from raw to stage'
    if (rows_diff != 0) {
      row_diff_qa_note <- paste0('Number of rows in stage (', rows_stage, 
                                 ') does not match raw (', rows_raw, ')')
    } else {
      row_diff_qa_note <- paste0('Number of rows in stage matches raw (', rows_stage, ')')
    }
  }

  
  if (rows_diff != 0) {
    row_diff_qa_fail <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_claim',
                                  {row_diff_qa_type}, 
                                  'FAIL',
                                  {Sys.time()},
                                  {row_diff_qa_note})",
                                  .con = conn))
    warning("Number of rows does not match total expected")
  } else {
    row_diff_qa_fail <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_claim',
                                  {row_diff_qa_type}, 
                                  'PASS',
                                  {Sys.time()},
                                  {row_diff_qa_note})",
                                  .con = conn))
  }
  
  
  #### QA CHECK: NULL IDs ####
  null_ids <- as.numeric(dbGetQuery(
    conn, 
    glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`} 
                 WHERE MEDICAID_RECIPIENT_ID IS NULL", 
                   .con = conn)))
  
  if (null_ids != 0) {
    null_ids_qa_fail <- 1
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_claim',
                                  'Null Medicaid IDs', 
                                  'FAIL',
                                  {Sys.time()},
                                  'Null IDs found. Investigate further.')",
                                  .con = conn))
    warning("Null Medicaid IDs found in claims.stage_mcaid_claim")
  } else {
    null_ids_qa_fail <- 0
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_claim',
                                  'Null Medicaid IDs', 
                                  'PASS',
                                  {Sys.time()},
                                  'No null IDs found')",
                                  .con = conn))
  }
  
  
  #### ADD VALUES TO QA_VALUES TABLE ####
  # Number of new rows
  DBI::dbExecute(
    conn = conn,
    glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid_values
                   (table_name, qa_item, qa_value, qa_date, note) 
                   VALUES ('claims.stage_mcaid_claim',
                   'row_count', 
                   '{rows_stage}', 
                   {Sys.time()}, 
                   {refresh_type})",
                   refresh_type = ifelse(full_refresh == F, 
                                         'Count after partial refresh', 
                                         'Count after full refresh'),
                   .con = conn))
  
  
  #### ADD OVERALL QA RESULT ####
  # This creates an overall QA result to feed the stage.v_mcaid_status view, 
  #    which is used by the integrated data hub to check for new data to run
  if (max(row_diff_qa_fail, null_ids_qa_fail) == 1) {
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_claim',
                                  'Overall QA result', 
                                  'FAIL',
                                  {Sys.time()},
                                  'One or more QA steps failed')",
                                  .con = conn))
    stop("One or more QA steps failed. See claims.metadata_qa_mcaid for more details")
  } else {
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_claim',
                                  'Overall QA result', 
                                  'PASS',
                                  {Sys.time()},
                                  'All QA steps passed')",
                                  .con = conn))
  }
  
  #### CLEAN UP ####
  suppressWarnings(rm(from_schema, from_table, to_schema, to_table, archive_schema, date_truncate, 
                      vars, vars_truncated, current_batch_id))
  suppressWarnings(rm(rows_stage, rows_raw, rows_archive, rows_diff, null_ids))
  rm(rows_diff)
  rm(row_diff_qa_type, row_diff_qa_note)
  rm(row_diff_qa_fail, null_ids_qa_fail)
  rm(load_sql)
  rm(config)
  
}



