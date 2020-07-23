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


#### FUNCTION ####
copy_into_f <- function(
  conn,
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
  overwrite = T) {
  
 
  #### INITIAL ERROR CHECK ####
  # Check if the config provided is a local file or on a webpage
  if (!is.null(config_url) & !is.null(config_file)) {
    stop("Specify either a config_url or config_file but not both")
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
  if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }

  #### ERROR CHECKS AND OVERALL MESSAGES ####
  # Make sure a valid URL was found
  if ('404' %in% names(table_config)) {
    stop("Invalid URL for YAML file")
  }
  
  # Check that the yaml config file has necessary components
  if (!"dl_path" %in% names(table_config)) {
    stop("YAML file is missing a data lake file_path (dl_path)")
  } else {
    if (is.null(table_config$dl_path)) {
      stop("Data lake file path (dl_path) is blank in config file")
    }
  }
  
  if (!"ext_schema" %in% names(table_config)) {
    stop("YAML file is missing a data warehouse schema")
    } else {
      if (is.null(table_config$ext_schema)) {
        stop("Data warehouse schema is blank in config file")
        }
    }
  
  if (!"ext_table" %in% names(table_config)) {
    stop("YAML file is missing a data warehouse table")
  } else {
    if (is.null(table_config$ext_table)) {
      stop("Data warehouse table is blank in config file")
    }
  }
  
  if (!"vars" %in% names(table_config)) {
    stop("YAML file is missing a list of variables")
  } else {
    if (is.null(table_config$vars)) {
      stop("No variables specified in config file")
    }
  }
  
  # Check for issues with numberic values
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
  dw_schema <- table_config$ext_schema
  dw_table <- table_config$ext_table
  
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
    if (DBI::dbExistsTable(conn, DBI::Id(schema = dw_schema, table = dw_table))) {
      DBI::dbExecute(conn,
                     glue::glue_sql("DROP TABLE {`dw_schema`}.{`dw_table`}",
                                    .con = conn))

      DBI::dbExecute(conn, glue::glue_sql(
        "CREATE TABLE {`dw_schema`}.{`dw_table`} (
          {DBI::SQL(glue_collapse(glue_sql('{`names(table_config$vars)`} {DBI::SQL(table_config$vars)}',
                                           .con = conn), sep = ', \n'))}
        )", .con = conn))
    }
  }
  
  

  
  message(glue::glue("Creating [{dw_schema}].[{dw_table}] table"))
  
  DBI::dbExecute(conn, glue::glue_sql(
    "COPY INTO {`dw_schema`}.{`dw_table`}
    ({`names(table_config$vars)`*})
    FROM {table_config$data_lake$file_path}
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
  )
  
}