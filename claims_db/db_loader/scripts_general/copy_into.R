#### FUNCTION TO COPY DATA FROM THE DATA LAKE TO THE DATA WAREHOUSE
# Alastair Matheson
# Created:        2019-04-04


### Plans for future improvements:
# Add warning when table is about to be overwritten
# Add in other options for things we're not using (e.g., file_format)


#### PARAMETERS ####
# conn = name of the connection to the SQL database
# config_url = URL location of YAML config file (should be blank if using config_file)
# config_file = path + file name of YAML config file (should be blank if using config_url)
# overwrite = truncate table first before creating it, if it exists (default is TRUE)
# rodbc = if wanting to use RODBC package to run query (avoids encoding error if using a secret key)


#### FUNCTION ####
copy_into_f <- function(
  conn,
  server = NULL,
  config = NULL,
  config_url = NULL,
  config_file = NULL,
  file_type = c("csv", "parquet", "orc"),
  identity = NULL,
  secret = NULL,
  max_errors = 100,
  compression = c("none", "gzip", "defaultcodec", "snappy"),
  field_quote = "",
  field_terminator = "\\t",
  row_terminator = "\\n",
  first_row = 2,
  overwrite = T,
  rodbc = F) {
  
  
  #### SET UP SERVER ####
  if (is.null(server)) {
    server <- NA
  } else if (server %in% c("phclaims", "hhsaw")) {
    server <- server
  } else if (!server %in% c("phclaims", "hhsaw")) {
    stop("Server must be NULL, 'phclaims', or 'hhsaw'")
  }
  
  
  #### TEMPORARY FIX FOR ODBC ISSUES ####
  # The odbc package isn't encoding the secret key properly right now so produces
  # a Base-64 error. The RODBC doesn't seem to have that issue so for now we are
  # forcing the COPY INTO statement to use an RODBC connection
  if (rodbc == T) {
    conn_rodbc <- RODBC::odbcConnect(dsn = "int_edw_20", 
                                     uid = keyring::key_list("hhsaw_dev")[["username"]])
  }
  
 
  #### INITIAL ERROR CHECK ####
  # Check if the config provided is a local object, file, or on a web page
  if (!is.null(config) & !is.null(config_url) & !is.null(config_file)) {
    stop("Specify either a local config object, config_url, or config_file but only one")
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
  
  
  #### READ IN CONFIG FILE ####
  if (!is.null(config)) {
    table_config <- config
  } else if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(httr::GET(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }
  

  #### ERROR CHECKS AND OVERALL MESSAGES ####
  # Make sure a valid URL was found
  if ('404' %in% names(table_config)) {
    stop("Invalid URL for YAML file")
  }
  
  # Check that the yaml config file has necessary components
  if (!"vars" %in% names(table_config)) {
    stop("YAML file is missing a list of variables")
  } else {
    if (is.null(table_config$vars)) {
      stop("No variables specified in config file")
    }
  }
  
  # Check for issues with numeric values
  if (!is.numeric(max_errors)) {
    stop("max_errors must be numeric")
  }
  
  if (!is.numeric(first_row)) {
    stop("first_row must be numeric")
  }

  
  
  #### VARIABLES ####
  file_type <- match.arg(file_type)
  max_errors <- round(max_errors, 0)
  compression <- match.arg(compression)
  first_row <- round(first_row, 0)
  
  if (!is.null(server)) {
    to_schema <- table_config[[server]][["to_schema"]]
    to_table <- table_config[[server]][["to_table"]]
    dl_path <- table_config[[server]][["dl_path"]]
  } else {
    to_schema <- table_config$to_schema
    to_table <- table_config$to_table
    dl_path <- tabel_config$dl_path
  }
  
  
  if (compression == "none") {
    compression <- DBI::SQL("")
  }
  
  if (!is.null(identity) | !is.null(secret)) {
    auth_sql <- glue::glue_sql("CREDENTIAL = (IDENTITY = {identity},
                               SECRET = {secret}),", .con = conn)
  } else {
    auth_sql <- DBI::SQL("")
  }

  #### RUN CODE ####
  if (overwrite == T) {
    message("Removing existing table and creating new one")
    # Need to drop and recreate so that the field types are what is desired
    if (DBI::dbExistsTable(conn, DBI::Id(schema = to_schema, table = to_table))) {
      DBI::dbExecute(conn,
                     glue::glue_sql("DROP TABLE {`to_schema`}.{`to_table`}",
                                    .con = conn))
    }
  }
  
  ### Create table if it doesn't exist
  if (DBI::dbExistsTable(conn, DBI::Id(schema = to_schema, table = to_table)) == F) {
    DBI::dbExecute(conn, glue::glue_sql(
      "CREATE TABLE {`to_schema`}.{`to_table`} (
          {DBI::SQL(glue_collapse(glue_sql('{`names(table_config$vars)`} {DBI::SQL(table_config$vars)}',
                                           .con = conn), sep = ', \n'))}
        )", .con = conn))
  }

  
  message(glue::glue("Creating [{to_schema}].[{to_table}] table"))
  
  
  # Set up SQL
  load_sql <- glue::glue_sql(
    "COPY INTO {`to_schema`}.{`to_table`}
    ({`names(table_config$vars)`*})
    FROM {dl_path}
    WITH (
      FILE_TYPE = {file_type},
      {auth_sql}
      MAXERRORS = {max_errors},
      COMPRESSION = {compression},
      FIELDQUOTE = {field_quote}  ,
      FIELDTERMINATOR = {field_terminator},
      ROWTERMINATOR = {row_terminator},
      FIRSTROW = {first_row}
    );",
    .con = conn)
  
  if (rodbc == T) {
    RODBC::sqlQuery(channel = conn_rodbc, query = load_sql)
  } else {
    DBI::dbExecute(conn, load_sql)
  }
  
  
}