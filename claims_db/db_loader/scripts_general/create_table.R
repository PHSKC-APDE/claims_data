#### FUNCTION TO CREATE TABLES IN SQL
# Alastair Matheson
# Created:        2019-04-04


### Plans for future improvements:
# Add warning when table is about to be overwritten


#### PARAMETERS ####
# conn = name of the connection to the SQL database
# server = name of server being used (if using newer YAML format)
# config = config file already in memory
# config_url = URL location of YAML config file (should be blank if using config_file)
# config_file = path + file name of YAML config file (should be blank if using config_url)
# overwrite = drop table first before creating it, if it exists (default is TRUE)
# external = create external table (requires specifying data source details)
# test_mode = write things to the tmp schema to test out functions (default is FALSE)
# overall/ind_yr = old code used to load multiple years of data, now deprecated


#### FUNCTION ####
create_table_f <- function(
  conn,
  server = NULL,
  config = NULL,
  config_url = NULL,
  config_file = NULL,
  overwrite = T,
  external = F,
  test_mode = F,
  overall = NULL,
  ind_yr = NULL
) {
  
  
  ### SET UP SERVER
  if (server %in% c("phclaims", "hhsaw")) {
    server <- server
  } else if (!is.null(server)) {
    message("Server must be phclaims or hhsaw")
    server <- NA
  }
  
  #### INITIAL ERROR CHECK ####
  # Check if the config provided is a local file or on a webpage
  if (!is.null(config) & !is.null(config_url) & !is.null(config_file)) {
    stop("Specify either alocal config object, config_url, or config_file but only one")
  }
  
  if (!is.null(config_url)) {
    message("Warning: YAML configs pulled from a URL are subject to fewer error checks")
  }
  
  if (!is.null(config_file)) {
    # Check that the yaml config file exists in the right format
    if (file.exists(config_file) == F) {
      stop("Config file does not exist, check file name")
    }
    
    if (is.yaml.file(config_file) == F) {
      stop(glue::glue("Config file is not a YAML config file. ", 
                      "Check there are no duplicate variables listed"))
    }
  }
  
  # Let people know not to use these options any more
  if (!is.null(overall) | !is.null(ind_yr)) {
    warning("overall and ind_yr are deprecated and should be removed from code")
  }
  
  
  #### READ IN CONFIG FILE ####
  if (!is.null(config)) {
    table_config <- config
  } else if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }

  #### ERROR CHECKS AND OVERALL MESSAGES ####
  # Make sure a valid URL was found
  if ('404' %in% names(table_config)) {
    stop("Invalid URL for YAML file")
  }
  
  # Alert users they are in test mode
  if (test_mode == T) {
    message("FUNCTION WILL BE RUN IN TEST MODE, WRITING TO TMP SCHEMA")
    test_msg <- " (function is in test mode)"
  } else {
    test_msg <- ""
  }
  
  
  #### VARIABLES ####
  if (server %in% names(table_config)) {
    to_schema <- table_config[[server]][["to_schema"]]
    to_table <- table_config[[server]][["to_table"]]}
  else {
    # Set up to work with both new and old way of using YAML files
    if (!is.null(table_config$to_schema)) {
      to_schema <- table_config$to_schema
    } else {
      to_schema <- table_config$schema
    }
    
    if (!is.null(table_config$to_table)) {
      to_table <- table_config$to_table
    } else {
      to_table <- table_config$table
    }
  }
  
  
  vars <- unlist(table_config$vars)  

  
  if (test_mode == T) {
    to_table <- glue::glue("{to_schema}_{to_table}")
    to_schema <- "tmp"
  }
  
  if (external == T) {
    external_setup <- glue::glue_sql(" EXTERNAL ")
    external_text <- glue::glue_sql(" WITH (DATA_SOURCE = {DBI::SQL(table_config$ext_data_source)}, 
                                    SCHEMA_NAME = {table_config$ext_schema},
                                    OBJECT_NAME = {table_config$ext_object_name})", .con = conn)
  } else {
    external_setup <- DBI::SQL("")
    external_text <- DBI::SQL("")
  }
  
  
  #### CREATE TABLE ####
  message(glue::glue("Creating [{to_schema}].[{to_table}] table", test_msg))
  
  tbl_name <- DBI::Id(schema = to_schema, table = to_table)
  
  if (overwrite == T) {
    if (DBI::dbExistsTable(conn, tbl_name)) {
      DBI::dbExecute(conn, 
                     glue::glue_sql("DROP {external_setup} TABLE {`to_schema`}.{`to_table`}",
                                    .con = conn))
    }
  }
  
  create_code <- glue::glue_sql(
    "CREATE {external_setup} TABLE {`to_schema`}.{`to_table`} (
      {DBI::SQL(glue_collapse(glue_sql('{`names(table_config$vars)`} {DBI::SQL(table_config$vars)}', 
      .con = conn), sep = ', \n'))}
      ) {external_text}", 
    .con = conn)
  
  DBI::dbExecute(conn, create_code)
}