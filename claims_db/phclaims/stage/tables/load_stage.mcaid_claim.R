#### CODE TO CREATE STAGE MCAID CLAIMS TABLE
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05, updated 2020-02, 2020-07

### Run from master_mcaid_full script
# https://github.com/PHSKC-APDE/claims_data/blob/azure_migration/claims_db/db_loader/mcaid/master_mcaid_full.R


load_claims.stage_mcaid_claim_f <- function(conn_dw = NULL, conn_db = NULL, full_refresh = F, config = NULL) {
  ### Error checks
  if (is.null(conn_dw)) {stop("No DW connection specificed")}
  if (is.null(conn_db)) {stop("No DB connection specificed")}
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

  current_batch_id <- load_metadata_etl_log_f(conn = conn_db, 
                                              batch_type = etl_batch_type,
                                              data_source = "Medicaid", 
                                              date_min = config$etl$date_min,
                                              date_max = config$etl$date_max,
                                              delivery_date = config$etl$date_delivery, 
                                              note = config$etl$note)

  
  #### QA RAW DATA ####
  # Now that we have a batch_etl_id we can do some basic QA on the raw data
  
  #### QA CHECK: ROW COUNTS MATCH SOURCE FILE COUNT ####
  message("Checking loaded row counts vs. expected")
  # Use the load config file for the list of tables to check and their expected row counts
  qa_rows_sql <- qa_load_row_count_f(conn = conn_dw, schema = config$from_schema,
                                     table = config$from_table, row_count = config$etl$row_count)
  
  # Report individual results out to SQL table
  odbc::dbGetQuery(conn = conn_db,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        '{`from_schema`}.{`from_table`}',
                                        'Number rows loaded to SQL vs. expected value(s)', 
                                        {qa_rows_sql$outcome[1]},
                                        {Sys.time()},
                                        {qa_rows_sql$note[1]})",
                                  .con = conn_db))
  
  if (qa_rows_sql$outcome[1] == "FAIL") {
    qa_row_fail <- 1L
    warning(glue::glue("Mismatching row count between source file and SQL table. 
                  Check claims.metadata_qa_mcaid for details (etl_batch_id = {current_batch_id}"))
  } else {
    qa_row_fail <- 0L
  }
  
  
  
  #### ARCHIVE EXISTING TABLE ####
  # No longer switching schema, instead just renaming table. Need to drop existing archive table first
  if (full_refresh == F) {
    try(DBI::dbSendQuery(conn_dw, glue::glue("DROP TABLE {archive_schema}.{archive_table}")))
    DBI::dbSendQuery(conn_dw, glue::glue("RENAME OBJECT {`to_schema`}.{`to_table`} TO {`archive_table`}"))
  }
  

  #### LOAD TABLE ####
  # Can't use default load function because some transformation is needed
  # Need to make two new variables
  if (full_refresh == F) {
    load_sql <- glue::glue_sql(
      "CREATE TABLE {`to_schema`}.{`to_table`} 
      WITH (CLUSTERED COLUMNSTORE INDEX, 
            DISTRIBUTION = HASH ({`date_var`}))
      AS SELECT {`vars`*}  
      FROM {`archive_schema`}.{`archive_table`}
      WHERE {`date_var`} < {date_truncate}
      UNION
      SELECT DISTINCT CAST(YEAR([FROM_SRVC_DATE]) AS INT) * 100 + CAST(MONTH([FROM_SRVC_DATE]) AS INT) AS [CLNDR_YEAR_MNTH],
        MBR_H_SID, MEDICAID_RECIPIENT_ID, BABY_ON_MOM_IND, TCN, CLM_LINE_TCN,
        CAST(RIGHT(CLM_LINE_TCN, 3) AS INTEGER) AS CLM_LINE, {`vars_truncated`*},
        {current_batch_id} AS etl_batch_id FROM
      {`from_schema`}.{`from_table`}
      WHERE {`date_var`} >= {date_truncate}",
      .con = conn_dw)
  } else {
    load_sql <- glue::glue_sql(
      "CREATE TABLE {`to_schema`}.{`to_table`} 
      ({`vars`*})
      WITH (CLUSTERED COLUMNSTORE INDEX, 
            DISTRIBUTION = HASH ({`date_var`}))
      AS SELECT DISTINCT CAST(YEAR([FROM_SRVC_DATE]) AS INT) * 100 + CAST(MONTH([FROM_SRVC_DATE]) AS INT) AS [CLNDR_YEAR_MNTH],
      MBR_H_SID, MEDICAID_RECIPIENT_ID, BABY_ON_MOM_IND, TCN, CLM_LINE_TCN,
      CAST(RIGHT(CLM_LINE_TCN, 3) AS INTEGER) AS CLM_LINE,
      {`vars_truncated`*}, {current_batch_id} AS etl_batch_id
      FROM {`from_schema`}.{`from_table`}",
      .con = conn_dw)
  }
  
  
  
  
  message("Loading to stage table")
  system.time(DBI::dbExecute(conn_dw, load_sql))
  
  
  
  #### QA CHECK: NUMBER OF ROWS IN SQL TABLE ####
  message("Running QA checks")
  rows_stage <- as.numeric(dbGetQuery(
    conn_dw, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}", .con = conn_dw)))
  rows_raw <- as.numeric(dbGetQuery(
    conn_dw, glue::glue_sql("SELECT COUNT (*) FROM {`from_schema`}.{`from_table`}", .con = conn_dw)))
  
  if (full_refresh == F) {
    rows_archive <- as.numeric(dbGetQuery(
      conn_dw, glue::glue_sql("SELECT COUNT (*) FROM {`archive_schema`}.{`archive_table`} 
                            WHERE {`date_var`} < {date_truncate}", 
                                .con = conn_dw)))
    
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
    DBI::dbExecute(conn = conn_db,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_claim',
                                  {row_diff_qa_type}, 
                                  'FAIL',
                                  {Sys.time()},
                                  {row_diff_qa_note})",
                                  .con = conn_db))
    warning("Number of rows does not match total expected")
  } else {
    row_diff_qa_fail <- 0
    DBI::dbExecute(conn = conn_db,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_claim',
                                  {row_diff_qa_type}, 
                                  'PASS',
                                  {Sys.time()},
                                  {row_diff_qa_note})",
                                  .con = conn_db))
  }
  
  
  #### QA CHECK: NULL IDs ####
  null_ids <- as.numeric(dbGetQuery(
    conn_dw, 
    glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`} 
                 WHERE MEDICAID_RECIPIENT_ID IS NULL", 
                   .con = conn_dw)))
  
  if (null_ids != 0) {
    null_ids_qa_fail <- 1
    DBI::dbExecute(conn = conn_db,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_claim',
                                  'Null Medicaid IDs', 
                                  'FAIL',
                                  {Sys.time()},
                                  'Null IDs found. Investigate further.')",
                                  .con = conn_db))
    warning("Null Medicaid IDs found in claims.stage_mcaid_claim")
  } else {
    null_ids_qa_fail <- 0
    DBI::dbExecute(conn = conn_db,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_claim',
                                  'Null Medicaid IDs', 
                                  'PASS',
                                  {Sys.time()},
                                  'No null IDs found')",
                                  .con = conn_db))
  }
  
  
  #### ADD VALUES TO QA_VALUES TABLE ####
  # Number of new rows
  DBI::dbExecute(
    conn = conn_db,
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
                   .con = conn_db))
  
  
  #### ADD OVERALL QA RESULT ####
  # This creates an overall QA result to feed the stage.v_mcaid_status view, 
  #    which is used by the integrated data hub to check for new data to run
  if (max(row_diff_qa_fail, null_ids_qa_fail) == 1) {
    DBI::dbExecute(conn = conn_db,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_claim',
                                  'Overall QA result', 
                                  'FAIL',
                                  {Sys.time()},
                                  'One or more QA steps failed')",
                                  .con = conn_db))
    stop("One or more QA steps failed. See claims.metadata_qa_mcaid for more details")
  } else {
    DBI::dbExecute(conn = conn_db,
                   glue::glue_sql("INSERT INTO claims.metadata_qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  'claims.stage_mcaid_claim',
                                  'Overall QA result', 
                                  'PASS',
                                  {Sys.time()},
                                  'All QA steps passed')",
                                  .con = conn_db))
  }
  
  #### CLEAN UP ####
  suppressWarnings(rm(from_schema, from_table, to_schema, to_table, archive_schema, date_truncate, 
                      vars, vars_truncated, current_batch_id))
  suppressWarnings(rm(rows_stage, rows_raw, rows_archive, rows_diff, null_ids))
  rm(row_diff_qa_type, row_diff_qa_note)
  rm(row_diff_qa_fail, null_ids_qa_fail)
  rm(load_sql)
  rm(config)
  
}



