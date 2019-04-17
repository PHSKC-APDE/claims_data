#### FUNCTION TO CREATE TABLES IN SQL
# Alastair Matheson
# Created:        2019-04-04
# Last modified:  2019-04-15


### Plans for future improvements:
# Add warning when overall table is about to be overwritten


#### PARAMETERS ####
# conn = name of the connection to the SQL database
# config_file = path + file name of YAML config file
# overall = create overall table (default is TRUE)
# ind_yr = create tables for individual years (default is TRUE)
# overwrite = drop table first before creating it, if it exists (default is TRUE)
# test_mode = write things to the tmp schema to test out functions (default is FALSE)


#### FUNCTION ####
create_table_f <- function(
  conn,
  config_file,
  overall = T,
  ind_yr = T,
  overwrite = T,
  test_mode = F
) {
  
  
  #### INITIAL ERROR CHECK ####
  # Check that the yaml config file exists in the right format
  if (file.exists(config_file) == F) {
    stop("Config file does not exist, check file name")
  }
  
  if (is.yaml.file(config_file) == F) {
    stop(paste0("Config file is not a YAML config file. \n", 
                "Check there are no duplicate variables listed"))
  }
  
  
  #### READ IN CONFIG FILE ####
  table_config <- yaml::read_yaml(config_file)
  

  #### ERROR CHECKS AND OVERALL MESSAGES ####
  # Check that the yaml config file has necessary components
  if (!"schema" %in% eval.config.sections(config_file) & test_mode == F) {
    stop("YAML file is missing a schema")
  } else {
    if (is.null(table_config$schema)) {
      stop("Schema name is blank in config file")
    }
  }
  
  if (!"table" %in% eval.config.sections(config_file)) {
    stop("YAML file is missing a table name")
  } else {
    if (is.null(table_config$table)) {
      stop("Table name is blank in config file")
    }
  }
  
  if (!"vars" %in% eval.config.sections(config_file)) {
    stop("YAML file is missing a list of variables")
  } else {
    if (is.null(table_config$vars)) {
      stop("No variables specified in config file")
    }
  }
  
  if (!"years" %in% eval.config.sections(config_file) & ind_yr == T) {
    stop("YAML file is missing a list of years")
  } else {
    if (ind_yr == T & is.null(unlist(table_config$years))) {
      stop("No years specified in config file")
    }
  }

  # Check that something will be run
  if (overall == F & ind_yr == F) {
    stop("At least one of 'overall and 'ind_yr' must be set to TRUE")
  }
  
  # Alert users they are in test mode
  if (test_mode == T) {
    print("FUNCTION WILL BE RUN IN TEST MODE, WRITING TO TMP SCHEMA")
    test_msg <- " (function is in test mode)"
  } else {
    test_msg <- ""
  }
  
  
  #### VARIABLES ####
  if (test_mode == T) {
    schema <- "tmp"
  } else {
    schema <- table_config$schema
  }
  
  table_name <- table_config$table
  vars <- unlist(table_config$vars)
  
  
  if (ind_yr == T) {
    # Use unique in case variables are repeated
    years <- sort(unique(table_config$years))
  }
  
  

  #### OVERALL TABLE ####
  if (overall == T) {
    print(paste0("Creating overall [", schema, "].[", table_name, "] table", test_msg))
    
    tbl_name <- DBI::Id(schema = schema, table = table_name)
    
    if (overwrite == T) {
      if (dbExistsTable(conn, tbl_name)) {
        dbRemoveTable(conn, tbl_name)
      }
    }

    DBI::dbCreateTable(conn, tbl_name, fields = vars)
  }
  
  
  #### CALENDAR YEAR TABLES ####
  if (ind_yr == T) {
    print(paste0("Creating calendar year [", schema, "].[", table_name, "] tables", test_msg))
    
    lapply(years, function(x) {
      tbl_name <- DBI::Id(schema = schema, table = paste0(table_name, "_", x))
      
      if (overwrite == T) {
        if (dbExistsTable(conn, tbl_name)) {
          dbRemoveTable(conn, tbl_name)
        }
      }
      
      # Add additional year-specific variables if present
      add_vars_name <- paste0("vars_", x)
      if (add_vars_name %in% eval.config.sections(config_file)) {
        vars <- c(vars, unlist(table_config[[add_vars_name]]))
      }
      
      DBI::dbCreateTable(conn, tbl_name, fields = vars)
    })
  }
  
}