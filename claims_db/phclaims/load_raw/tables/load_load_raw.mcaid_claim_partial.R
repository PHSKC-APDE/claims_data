#### CODE TO LOAD MCAID CLAIMS TABLES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-08

### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_partial.R


load_load_raw.mcaid_claim_partial_f <- function(conn = NULL,
                                                conn_dw = NULL,
                                                server = NULL,
                                                config = NULL,
                                                config_url = NULL,
                                                config_file = NULL,
                                                batch = NULL,
                                                qa_file_row = F) {
  
  # qa_file_row flag will determine whether to count the number of rows in the txt files
  # Note this is VERY slow over the network so better to check row counts once in SQL
  
  
  #### ERROR CHECKS ####
  ### Check entries are in place for ETL function
  if (is.null(batch)) {
    stop("Please select a file to be loaded.")
  }
  
  # Check if the config provided is a local object, file, or on a web page
  if (!is.null(config) & !is.null(config_url) & !is.null(config_file)) {
    stop("Specify either a local config object, config_url, or config_file but only one")
  }
  
  
  #### SET UP SERVER ####
  if (is.null(server)) {
    server <- NA
  } else if (server %in% c("phclaims", "hhsaw")) {
    server <- server
  } else if (!server %in% c("phclaims", "hhsaw")) {
    stop("Server must be NULL, 'phclaims', or 'hhsaw'")
  }
  
  
  #### READ IN CONFIG FILE ####
  if (!is.null(config)) {
    table_config <- config
  } else if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(httr::GET(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }
  
  
  #### LOAD FUNCTIONS IF NEEDED ####
  if (exists("load_metadata_etl_log_f") == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")
  }
  
  if (exists("qa_file_row_count_f") == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_file.R")
  }
  
  
  #### SET UP VARIABLES ####
  to_schema <- table_config[[server]][["to_schema"]]
  to_table <- table_config[[server]][["to_table"]]
  qa_schema <- table_config[[server]][["qa_schema"]]
  qa_table <- table_config[[server]][["qa_table"]]
  vars = table_config$vars
  vars_distinct = vars[!names(vars) %in% c("BILLING_PRVDR_ADDRESS",
                                    "SERVICING_PRVDR_ADDRESS",
                                    "MCO_PRVDR_ADDRESS", "MCO_PRVDR_COUNTY")]
  
  # Set up both connections so they work in either server
  if (server == "phclaims") {conn_dw <- conn}
  
  
  #### SET UP BATCH ID ####
  # Eventually switch this function over to using glue_sql to stop unwanted SQL behavior
  current_batch_id <- batch$etl_batch_id
  
  if (is.na(current_batch_id)) {
    stop("No etl_batch_id. Check metadata.etl_log table")
  }
  
#### SKIP THIS QA, DONE BEFORE FILE IS LOADED ####  
  #### INITAL QA (PHCLAIMS ONLY) ####
#  if (server == "phclaims") {
    #### QA CHECK: ACTUAL VS EXPECTED ROW COUNTS ####
#    if (qa_file_row == T) {
#      message("Checking expected vs. actual row counts (will take a while")
      # Use the load config file for the list of tables to check and their expected row counts
#      qa_rows_file <- qa_file_row_count_f(config = table_config, 
#                                          server = server,
#                                          overall = T, 
#                                          ind_yr = F)
      
      # Report results out to SQL table
#      DBI::dbExecute(conn = conn,
#                       glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
#                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
#                                VALUES ({current_batch_id}, 
#                                        '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
#                                        'Number of rows in source file(s) match(es) expected value', 
#                                        {qa_rows_file$outcome},
#                                        {format(Sys.time(), usetz = FALSE)},
#                                        {qa_rows_file$note})",
#                                      .con = conn))
      
#      if (qa_rows_file$outcome == "FAIL") {
#        stop(glue::glue("Mismatching row count between source file and expected number. 
#                  Check {qa_schema}.{qa_table} for details (etl_batch_id = {current_batch_id}"))
#      }
#    }
    
    
    #### QA CHECK: ORDER OF COLUMNS IN SOURCE FILE MATCH TABLE SHELLS IN SQL ####
#    message("Checking column order")
#    qa_column <- qa_column_order_f(conn = conn_dw,
#                                   config = table_config, 
#                                   server = server,
#                                   overall = T, 
#                                   ind_yr = F)
    
    # Report results out to SQL table
#    DBI::dbExecute(conn = conn,
#                     glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
#                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
#                                VALUES ({current_batch_id}, 
#                                        '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
#                                        'Order of columns in source file matches SQL table', 
#                                        {qa_column$outcome},
#                                        {format(Sys.time(), usetz = FALSE)},
#                                        {qa_column$note})",
#                                    .con = conn))
    
#    if (qa_column$outcome == "FAIL") {
#      stop(glue::glue("Mismatching column order between source file and SQL table. 
#                  Check {qa_schema}.{qa_table} for details (etl_batch_id = {current_batch_id})"))
#    }
#  }
  
  
  
  #### LOAD TABLES ####
  message("Loading tables to SQL")
  
  if (server == "hhsaw") {
    copy_into_f(conn = conn_dw, 
                server = server,
                config = table_config,
                dl_path = paste0(table_config[[server]][["base_url"]], batch["file_location"], batch["file_name"]),
                file_type = "csv", compression = "gzip",
                identity = "Storage Account Key", secret = key_get("inthealth_edw"),
                overwrite = T,
                rodbc = F)
  } else if (server == "phclaims") {
    load_table_from_file_f(conn = conn_dw,
                           server = server,
                           config = table_config,
                           filepath = paste0(batch$file_location, batch$file_name),
                           overall = T, ind_yr = F, combine_yr = F)
  }
  
  
  #### QA CHECK: ROW COUNTS MATCH SOURCE FILE COUNT ####
  message("Checking loaded row counts vs. expected")
  # Use the load config file for the list of tables to check and their expected row counts
  qa_rows_sql <- qa_load_row_count_f(conn = conn_dw,
                                     server = server,
                                     config = table_config,
                                     row_count = batch$row_count,
                                     overall = T, ind_yr = F)
  
  # Report individual results out to SQL table
  DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                        'Number rows loaded to SQL vs. expected value(s)', 
                                        {qa_rows_sql$outcome[1]},
                                        {format(Sys.time(), usetz = FALSE)},
                                        {qa_rows_sql$note[1]})",
                                  .con = conn))
  
  if (qa_rows_sql$outcome[1] == "FAIL") {
    stop(glue::glue("Mismatching row count between source file and SQL table. 
                  Check {qa_schema}.{qa_table} for details (etl_batch_id = {current_batch_id}"))
  }
  
  
  
  #### QA CHECK: COUNT OF DISTINCT ROWS (MINUS ADDRESS FIELDS) ####
  message("Running additional QA items")
  # Should be no duplicate TCNs once address fields are ignored
  
  # Currently fields are hard coded. Switch over to reading in YAML file and 
  # excluding the address fields
  
  distinct_rows <- as.numeric(DBI::dbGetQuery(
    conn_dw,
    glue::glue_sql(
    "SELECT COUNT (*) FROM (SELECT DISTINCT {`names(vars_distinct)`*} FROM {`to_schema`}.{`to_table`}) a",
    .con = conn_dw)))
  
  distinct_tcn <- as.numeric(DBI::dbGetQuery(
    conn_dw, 
    glue::glue_sql("SELECT COUNT (DISTINCT CLM_LINE_TCN) FROM {`to_schema`}.{`to_table`}",
                   .con = conn_dw)))
  
  
  if (distinct_rows != distinct_tcn) {
    DBI::dbExecute(conn = conn,
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                                    (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                    VALUES ({current_batch_id}, 
                                    '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                    'Distinct TCNs', 
                                    'FAIL',
                                    {format(Sys.time(), usetz = FALSE)},
                                    'No. distinct TCNs did not match rows even after excluding addresses')",
                                    .con = conn))
    stop("Number of distinct rows does not match total expected")
    } else {
    DBI::dbExecute(conn = conn,
                     glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'Distinct TCNs', 
                                  'PASS',
                                  {format(Sys.time(), usetz = FALSE)},
                                  'Number of distinct TCNs equals total # rows (after excluding address fields)')",
                                    .con = conn))
  }
  
  
  #### QA CHECK: DATE RANGE MATCHES EXPECTED RANGE ####
  qa_date_range <- qa_date_range_f(conn = conn_dw,
                                   server = server,
                                   config = table_config,
                                   overall = T, ind_yr = F,
                                   date_min_exp = format(as.Date(batch$date_min), "%Y-%m-%d"),
                                   date_max_exp = format(as.Date(batch$date_max), "%Y-%m-%d"),
                                   date_var = "FROM_SRVC_DATE")
  
  # Report individual results out to SQL table
  DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                        'Actual vs. expected date range in data', 
                                        {qa_date_range$outcome[1]},
                                        {format(Sys.time(), usetz = FALSE)},
                                        {qa_date_range$note[1]})",
                                  .con = conn))
  
  if (qa_date_range$outcome[1] == "FAIL") {
    stop(glue::glue("Mismatching date range between source file and SQL table. 
                  Check {qa_schema}.{qa_table} for details (etl_batch_id = {current_batch_id})"))
  }
  

  message("All QA items passed, see results in metadata.qa_mcaid")
  
  
  #### ADD BATCH ID COLUMN ####
  message("Adding batch ID to SQL table")
  # Add column to the SQL table and set current batch to the default
  # NB. In Azure data warehouse, the WITH VALUES code failed so split into 
  #      two statements, one to make the column and one to update it to default
  DBI::dbExecute(conn_dw,
                 glue::glue_sql("ALTER TABLE {`to_schema`}.{`to_table`} 
                  ADD etl_batch_id INTEGER DEFAULT {current_batch_id}",
                                .con = conn_dw))
  DBI::dbExecute(conn_dw,
                 glue::glue_sql("UPDATE {`to_schema`}.{`to_table`} 
                  SET etl_batch_id = {current_batch_id}",
                                .con = conn_dw))
  
  if (server == "phclaims") {
    meta_schema <- "metadata"
    meta_table <- "etl_log"
  } else if (server == "hhsaw") {
    meta_schema <- "claims"
    meta_table <- "metadata_etl_log"
  }
  
  DBI::dbExecute(conn,
                 glue::glue_sql("UPDATE {`meta_schema`}.{`meta_table`} 
                               SET date_load_raw = GETDATE() 
                               WHERE etl_batch_id = {current_batch_id}",
                               .con = conn))

  message("All claims data loaded to SQL and QA checked")

}

