#### CODE TO CREATE STAGE MCAID CLAIMS TABLE
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05, updated 2020-02, 2020-07

### Run from master_mcaid_full script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_full.R

load_claims.stage_mcaid_claim_f <- function(conn_dw = NULL, 
                                            conn_db = NULL, 
                                            server = NULL,
                                            full_refresh = F, 
                                            config = NULL) {
  ### Error checks
  if (is.null(conn_dw)) {stop("No DW connection specificed")}
  if (is.null(conn_db)) {stop("No DB connection specificed")}
  if (is.null(config)) {stop("Specify a list with config details")}
  
  #### SET UP SERVER ####
  if (is.null(server)) {
    server <- NA
  } else if (server %in% c("phclaims", "hhsaw")) {
    server <- server
  } else if (!server %in% c("phclaims", "hhsaw")) {
    stop("Server must be NULL, 'phclaims', or 'hhsaw'")
  }
  
  # Set up both connections so they work in either server
  if (server == "phclaims") {conn_dw <- conn_db}
  
  
  #### GET VARS FROM CONFIG FILE ####
  from_schema <- config[[server]][["from_schema"]]
  from_table <- ifelse(full_refresh == F, 
                       config[[server]][["from_table_incr"]],
                       config[[server]][["from_table_init"]])
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  archive_schema <- config[[server]][["archive_schema"]]
  archive_table <- ifelse(is.null(config[[server]][["archive_table"]]), '',
                      config[[server]][["archive_table"]])
  qa_schema <- config[[server]][["qa_schema"]]
  qa_table <- ifelse(is.null(config[[server]][["qa_table"]]), '',
                     config[[server]][["qa_table"]])
  
  if (full_refresh == T) {
    bho_archive_schema <- config[[server]][["bho_archive_schema"]]
    bho_archive_table <- ifelse(is.null(config[[server]][["bho_archive_table"]]), '',
                                config[[server]][["bho_archive_table"]])
  }
  
  vars <- unlist(names(config$vars))
  # Need to keep only the vars that come after the named ones below because some
  # of these are transformed
  vars_truncated <- vars[!vars %in% c("CLNDR_YEAR_MNTH", "MBR_H_SID", 
                                      "MEDICAID_RECIPIENT_ID", "BABY_ON_MOM_IND", 
                                      "TCN", "CLM_LINE_TCN", "CLM_LINE")]
  
  
  if (!is.null(config$etl$date_var)) {
    date_var <- config$etl$date_var
  } else {
    date_var <- config$date_var
  }
  
  
  if (full_refresh == F) {
    etl_batch_type <- "incremental"
    date_truncate <- as.Date(DBI::dbGetQuery(conn_dw,
      glue::glue_sql("SELECT MIN({`date_var`}) FROM {`from_schema`}.{`from_table`}",
                     .con = conn_dw))[[1]])
  } else {
    etl_batch_type <- "full"
  }

  #### FIND MOST RECENT BATCH ID FROM SOURCE (LOAD_RAW) ####
  current_batch_id <- as.numeric(odbc::dbGetQuery(
    conn_dw,
    glue::glue_sql("SELECT MAX(etl_batch_id) FROM {`from_schema`}.{`from_table`}",
                   .con = conn_dw)))
  
  if (is.na(current_batch_id)) {
    stop(glue::glue_sql("Missing etl_batch_id in {`from_schema`}.{`from_table`}"))
  }
  
  #### ARCHIVE EXISTING TABLE ####
  # Different approaches between Azure data warehouse (rename) and on-prem SQL DB (alter schema)
  # Check that the stage table actually exists so we don't accidentally wipe the archive table
  if (full_refresh == F & DBI::dbExistsTable(conn_dw, DBI::Id(schema = to_schema, table = to_table))) {
    if (server == "hhsaw") {
      if(DBI::dbExistsTable(conn_dw, DBI::Id(schema = to_schema, table = paste0(archive_table, '_bak')))) {
        try(DBI::dbSendQuery(conn_dw, 
                             glue::glue_sql("DROP TABLE {`to_schema`}.{`paste0(archive_table, '_bak')`}", 
                                            .con = conn_dw)))
      }
      try(DBI::dbSendQuery(conn_dw, 
                           glue::glue_sql("RENAME OBJECT {`to_schema`}.{`archive_table`} TO {`paste0(archive_table, '_bak')`}",
                                          .con = conn_dw)))
      try(DBI::dbSendQuery(conn_dw, 
                           glue::glue_sql("RENAME OBJECT {`to_schema`}.{`to_table`} TO {`archive_table`}",
                                          .con = conn_dw)))
    } else if (server == "phclaims") {
      if(DBI::dbExistsTable(conn_dw, DBI::Id(schema = archive_schema, table = paste0(archive_table, '_bak')))) {
        try(DBI::dbSendQuery(conn_db, 
                             glue::glue_sql("DROP TABLE {`archive_schema`}.{`paste0(archive_table, '_bak')`}", 
                                            .con = conn_db)))
      }
      try(DBI::dbSendQuery(conn_db, 
                           glue::glue("EXEC sp_rename '{archive_schema}.{archive_table}',  '{paste0(archive_table, '_bak')}'")))
      alter_schema_f(conn = conn_db, 
                     from_schema = to_schema, 
                     to_schema = archive_schema,
                     table_name = to_table, 
                     rename_index = F)
    }
  }
  

  #### LOAD TABLE ####
  # Can't use default load function because some transformation is needed
  # Need to make two new variables
  
  # Different start to the SQL depending on server
  if (server == "hhsaw") {
    load_intro <- glue::glue_sql("CREATE TABLE {`to_schema`}.{`to_table`} 
                                   WITH (CLUSTERED COLUMNSTORE INDEX, 
                                         DISTRIBUTION = HASH ({`date_var`}))
                                   AS ",
                                 .con = conn_dw)
  } else if (server == "phclaims") {
    create_table_f(conn = conn_dw, 
                   server = server,
                   config = config,
                   overwrite = T)
    load_intro <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table`} WITH (TABLOCK) 
                                   ({`vars`*})",
                                 .con = conn_dw)
  }
  
  if (full_refresh == F) {
    load_sql <- glue::glue_sql(
      "{DBI::SQL(load_intro)}  
        SELECT {`vars`*} 
        FROM {`archive_schema`}.{`archive_table`}
          WHERE {`date_var`} < {date_truncate}
        UNION
        SELECT DISTINCT CAST(YEAR([FROM_SRVC_DATE]) AS INT) * 100 + CAST(MONTH([FROM_SRVC_DATE]) AS INT) AS [CLNDR_YEAR_MNTH],
        MBR_H_SID, MEDICAID_RECIPIENT_ID, BABY_ON_MOM_IND, TCN, CLM_LINE_TCN,
        CAST(RIGHT(CLM_LINE_TCN, 3) AS INTEGER) AS CLM_LINE, {`vars_truncated`*}
        FROM {`from_schema`}.{`from_table`} 
      WHERE {`date_var`} >= {date_truncate}",
      .con = conn_dw)
  } else if (full_refresh == T) {
    load_sql <- glue::glue_sql(
      "{DBI::SQL(load_intro)} 
      SELECT DISTINCT CAST(YEAR([FROM_SRVC_DATE]) AS INT) * 100 + CAST(MONTH([FROM_SRVC_DATE]) AS INT) AS [CLNDR_YEAR_MNTH],
      MBR_H_SID, MEDICAID_RECIPIENT_ID, BABY_ON_MOM_IND, TCN, CLM_LINE_TCN,
      CAST(RIGHT(CLM_LINE_TCN, 3) AS INTEGER) AS CLM_LINE,
      {`vars_truncated`*}, {current_batch_id} AS etl_batch_id
      FROM {`from_schema`}.{`from_table`}
      UNION
      SELECT {`vars`*} FROM {`bho_archive_schema`}.{`bho_archive_table`}",
      .con = conn_dw)
  }
  
  message("Loading to stage table")
  system.time(DBI::dbExecute(conn_dw, load_sql))
  
  
  ### Add index if needed
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/add_index.R")
  if (server == "phclaims") {
    add_index_f(conn = conn_db, server = server, table_config = config)
  }
  
  
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
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  {row_diff_qa_type}, 
                                  'FAIL',
                                  {Sys.time()},
                                  {row_diff_qa_note})",
                                  .con = conn_db))
    warning("Number of rows does not match total expected")
  } else {
    row_diff_qa_fail <- 0
    DBI::dbExecute(conn = conn_db,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
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
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'Null Medicaid IDs', 
                                  'FAIL',
                                  {Sys.time()},
                                  'Null IDs found. Investigate further.')",
                                  .con = conn_db))
    warning("Null Medicaid IDs found in claims.stage_mcaid_claim")
  } else {
    null_ids_qa_fail <- 0
    DBI::dbExecute(conn = conn_db,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
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
    glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid_values
                   (table_name, qa_item, qa_value, qa_date, note) 
                   VALUES ('{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
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
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'Overall QA result', 
                                  'FAIL',
                                  {Sys.time()},
                                  'One or more QA steps failed')",
                                  .con = conn_db))
    stop("One or more QA steps failed. See claims.metadata_qa_mcaid for more details")
  } else {
    DBI::dbExecute(conn = conn_db,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
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



