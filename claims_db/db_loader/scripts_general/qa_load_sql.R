#### FUNCTIONS TO RUN QA PROCESSES ON SQL TABLES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

#### FUNCTION TO CHECK ROW COUNTS MATCH in FROM and TO TABLES ####
qa_sql_row_count_f <- function(conn = db_claims,
                               server = NULL,
                               config = NULL,
                               config_url = NULL,
                               config_file = NULL) {
  
  #### BASIC ERROR CHECKS ####
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
  
  
  ### VARIABLES
  if (!is.na(server)) {
    from_schema <- table_config[[server]][["from_schema"]]
    from_table <- table_config[[server]][["from_table"]]
    to_schema <- table_config[[server]][["to_schema"]]
    to_table <- table_config[[server]][["to_table"]]}
  else {
    from_schema <- table_config$from_schema
    from_table <- table_config$from_table
    to_schema <- table_config$to_schema
    to_table <- table_config$to_table
  }

  
  ### VALUES
  rows_from <- odbc::dbGetQuery(conn = conn,
                                glue::glue_sql(
                                  "SELECT COUNT_BIG (*) FROM {`from_schema`}.{`from_table`}",
                                  .con = conn))
  
  rows_to <- odbc::dbGetQuery(conn = conn,
                                glue::glue_sql(
                                  "SELECT COUNT_BIG (*) FROM {`to_schema`}.{`to_table`}",
                                  .con = conn))
  
  
  if (rows_from != rows_to) {
    qa_result <- "FAIL"
    note <- glue::glue("Mismatched number of rows ({from_schema}.{from_table}: {rows_from} 
                       vs. {to_schema}.{to_table}: {rows_to})")
  } else {
    qa_result <- "PASS"
    note <- glue::glue("Number of rows equal in both {from_schema}.{from_table} and 
                       {to_schema}.{to_table} ({rows_to})")
  }
  
  result <- list(qa_result = qa_result, note = note)
  return(result)
}

