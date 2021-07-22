#### CODE TO LOAD MCAID ELIG TABLES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-08

### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_partial.R


load_load_raw.mcaid_elig_partial_f <- function(conn = NULL,
                                               conn_dw = NULL,
                                               server = NULL,
                                               config = NULL,
                                               config_url = NULL,
                                               config_file = NULL,
                                               batch = NULL,
                                               interactive_auth = F) {
  
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
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/etl_log.R")
  }
  
  if (exists("qa_file_row_count_f") == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_file.R")
  }
  
  
  #### SET UP VARIABLES ####
  to_schema <- table_config[[server]][["to_schema"]]
  to_table <- table_config[[server]][["to_table"]]
  qa_schema <- table_config[[server]][["qa_schema"]]
  qa_table <- table_config[[server]][["qa_table"]]
  
  # If using interactive auth, don't use RODBC in COPY INTO
  
  if (interactive_auth == F) {
    rodbc <- F
  } else {
    rodbc <- T
  }
  
  # Set up both connections so they work in either server
  if (server == "phclaims") {conn_dw <- conn}
  
  
  #### SET UP BATCH ID ####
  # Eventually switch this function over to using glue_sql to stop unwanted SQL behavior
  current_batch_id <- batch$etl_batch_id
  
  if (is.na(current_batch_id)) {
    stop("No etl_batch_id. Check metadata etl_log table")
  }
  
#### SKIP THIS QA, DONE BEFORE FILE IS LOADED ####
  #### INITAL QA (PHCLAIMS ONLY) ####
#  if (server == "phclaims") {
    #### QA CHECK: ACTUAL VS EXPECTED ROW COUNTS ####
#    message("Checking expected vs. actual row counts")
    # Use the load config file for the list of tables to check and their expected row counts
#    qa_rows_file <- qa_file_row_count_f(config = table_config, 
#                                        server = server,
#                                        overall = T, 
#                                        ind_yr = F)
    
    # Report results out to SQL table
#    DBI::dbExecute(conn = conn,
#                   glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
#                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
#                                VALUES ({current_batch_id}, 
#                                        '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
#                                        'Number of rows in source file(s) match(es) expected value', 
#                                        {qa_rows_file$outcome},
#                                        {Sys.time()},
#                                        {qa_rows_file$note})",
#                                  .con = conn))
    
#    if (qa_rows_file$outcome == "FAIL") {
#      stop(glue::glue("Mismatching row count between source file and expected number. 
#                  Check {qa_schema}.{qa_table} for details (etl_batch_id = {current_batch_id}"))
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
#                   glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
#                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
#                                VALUES ({current_batch_id}, 
#                                        '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
#                                        'Order of columns in source file matches SQL table', 
#                                        {qa_column$outcome},
#                                        {Sys.time()},
#                                        {qa_column$note})",
#                                  .con = conn))
    
#    if (qa_column$outcome == "FAIL") {
#      stop(glue::glue("Mismatching column order between source file and SQL table. 
#                  Check {qa_schema}.{qa_table} for details (etl_batch_id = {current_batch_id}"))
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
                rodbc = rodbc)
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
                                        {Sys.time()},
                                        {qa_rows_sql$note[1]})",
                                .con = conn))
  
  if (qa_rows_sql$outcome[1] == "FAIL") {
    stop(glue::glue("Mismatching row count between source file and SQL table. 
                  Check {qa_schema}.{qa_table} for details (etl_batch_id = {current_batch_id}"))
  }
  
  
  
  #### QA CHECK: COUNT OF DISTINCT ID, CLNDR_YEAR_MNTH, FROM DATE, TO DATE, SECONDARY RAC ####
  message("Running additional QA items")
  # Should be no combo of ID, CLNDR_YEAR_MNTH, from_date, to_date, and secondary RAC with >1 row
  # However, there are cases where there is a duplicate row but the only difference is
  # a NULL or different END_REASON. Include END_REASON to account for this.
  distinct_rows <- as.numeric(DBI::dbGetQuery(
    conn_dw,
    glue::glue_sql("SELECT COUNT (*) FROM 
                   (SELECT DISTINCT CLNDR_YEAR_MNTH, MEDICAID_RECIPIENT_ID, 
                     FROM_DATE, TO_DATE, RPRTBL_RAC_CODE, SECONDARY_RAC_CODE, END_REASON 
                     FROM {`to_schema`}.{`to_table`}) a",
                   .con = conn_dw)))
  
  total_rows <- as.numeric(dbGetQuery(
    conn_dw, 
    glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}",
                   .con = conn_dw)))
  
  
  if (distinct_rows != total_rows) {
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                                    (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                    VALUES ({current_batch_id}, 
                                    '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                    'Distinct rows (ID, CLNDR_YEAR_MNTH, FROM/TO DATE, RPRTBL_RAC_CODE, SECONDARY RAC, END_REASON)', 
                                    'FAIL',
                                    {Sys.time()},
                                    'Number distinct rows ({distinct_rows}) != total rows ({total_rows})')",
                                  .con = conn))
    warning(glue("Number of distinct rows ({distinct_rows}) does not match total expected ({total_rows})"))
  } else {
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                  '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                  'Distinct rows (ID, CLNDR_YEAR_MNTH, FROM/TO DATE, RPRTBL_RAC_CODE, SECONDARY RAC, END_REASON)', 
                                  'PASS',
                                  {Sys.time()},
                                  'Number of distinct rows equals total # rows ({total_rows})')",
                                  .con = conn))
  }
  
  
  #### QA CHECK: DATE RANGE MATCHES EXPECTED RANGE ####
  qa_date_range <- qa_date_range_f(conn = conn_dw,
                                   server = server,
                                   config = table_config,
                                   overall = T, ind_yr = F,
                                   date_min_exp = format(as.Date(batch$date_min), "%Y%m"),
                                   date_max_exp = format(as.Date(batch$date_max), "%Y%m"),
                                   date_var = "CLNDR_YEAR_MNTH")
  
  # Report individual results out to SQL table
  DBI::dbExecute(conn = conn,
                 glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                                (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                VALUES ({current_batch_id}, 
                                        '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                        'Actual vs. expected date range in data', 
                                        {qa_date_range$outcome[1]},
                                        {Sys.time()},
                                        {qa_date_range$note[1]})",
                                .con = conn))
  
  if (qa_date_range$outcome[1] == "FAIL") {
    stop(glue::glue("Mismatching date range between source file and SQL table. 
                  Check {qa_schema}.{qa_table} for details (etl_batch_id = {current_batch_id}"))
  }
  
  
  #### QA CHECK: LENGTH OF MCAID ID = 11 CHARS ####
  id_len <- dbGetQuery(conn_dw,
                       glue::glue_sql("SELECT MIN(LEN(MEDICAID_RECIPIENT_ID)) AS min_len, 
                     MAX(LEN(MEDICAID_RECIPIENT_ID)) AS max_len 
                     FROM {`to_schema`}.{`to_table`}",
                                      .con = conn_dw))
  
  if (id_len$min_len != 11 | id_len$max_len != 11) {
    DBI::dbExecute(
      conn = conn,
      glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Length of Medicaid ID', 
                   'FAIL', 
                   {Sys.time()}, 
                   'Minimum ID length was {id_len$min_len}, maximum was {id_len$max_len}')",
                     .con = conn))
    
    stop(glue::glue("Some Medicaid IDs are not 11 characters long.  
                  Check {qa_schema}.{qa_table} for details (etl_batch_id = {current_batch_id}"))
  } else {
    DBI::dbExecute(
      conn = conn,
      glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Length of Medicaid ID', 
                   'PASS', 
                   {Sys.time()}, 
                   'All Medicaid IDs were 11 characters')",
                     .con = conn))
  }
  
  
  #### QA CHECK: LENGTH OF RAC CODES = 4 CHARS ####
  rac_len <- dbGetQuery(conn_dw,
                        glue::glue_sql("SELECT MIN(LEN(RPRTBL_RAC_CODE)) AS min_len, 
                     MAX(LEN(RPRTBL_RAC_CODE)) AS max_len, 
                     MIN(LEN(SECONDARY_RAC_CODE)) AS min_len2, 
                     MAX(LEN(SECONDARY_RAC_CODE)) AS max_len2 
                     FROM {`to_schema`}.{`to_table`}",
                                       .con = conn_dw))
  
  if (rac_len$min_len != 4 | rac_len$max_len != 4 | 
      rac_len$min_len2 != 4 | rac_len$max_len2 != 4) {
    DBI::dbExecute(
      conn = conn,
      glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Length of RAC codes', 
                   'FAIL', 
                   {Sys.time()}, 
                   'Min RPRTBLE_RAC_CODE length was {rac_len$min_len}, max was {rac_len$max_len};
                   Min SECONDARY_RAC_CODE length was {rac_len$min_len2}, max was {rac_len$max_len2}')",
                     .con = conn))
    
    stop(glue::glue("Some RAC codes are not 4 characters long.  
                  Check {qa_schema}.{qa_table} for details (etl_batch_id = {current_batch_id}"))
  } else {
    DBI::dbExecute(
      conn = conn,
      glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Length of RAC codes', 
                   'PASS', 
                   {Sys.time()}, 
                   'All RAC codes (reportable and secondary) were 4 characters')",
                     .con = conn))
  }
  
  
  #### QA CHECK: NUMBER NULLs IN FROM_DATE ####
  from_nulls <- dbGetQuery(conn_dw,
                           glue::glue_sql("SELECT a.null_dates, b.total_rows 
                      FROM
                      (SELECT 
                        COUNT (*) AS null_dates, ROW_NUMBER() OVER (ORDER BY NEWID()) AS seqnum
                        FROM {`to_schema`}.{`to_table`}
                        WHERE FROM_DATE IS NULL) a
                      LEFT JOIN
                      (SELECT COUNT(*) AS total_rows, ROW_NUMBER() OVER (ORDER BY NEWID()) AS seqnum
                        FROM {`to_schema`}.{`to_table`}) b
                      ON a.seqnum = b.seqnum",
                                          .con = conn_dw))
  
  pct_null <- round(from_nulls$null_dates / from_nulls$total_rows  * 100, 3)
  
  if (pct_null > 2.0) {
    DBI::dbExecute(
      conn = conn,
      glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'NULL from dates', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {from_nulls$null_dates} NULL from dates ({pct_null}% of total rows)')",
                     .con = conn))
    
    stop(glue::glue(">2% FROM_DATE rows are null.  
                  Check {qa_schema}.{qa_table} for details (etl_batch_id = {current_batch_id}"))
  } else {
    DBI::dbExecute(
      conn = conn,
      glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                   (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({current_batch_id}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'NULL from dates', 
                   'PASS', 
                   {Sys.time()}, 
                   '<2% of from date rows were null ({pct_null}% of total rows)')",
                     .con = conn))
  }
  
  message("All QA items complete, see results in metadata.qa_mcaid")
  
  
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
  
  message("All eligibility data loaded to SQL and QA checked")
  
}

