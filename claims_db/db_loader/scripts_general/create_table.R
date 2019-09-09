#### FUNCTION TO CREATE TABLES IN SQL
# Alastair Matheson
# Created:        2019-04-04
# Last modified:  2019-07-25


### Plans for future improvements:
# Add warning when overall table is about to be overwritten


#### PARAMETERS ####
# conn = name of the connection to the SQL database
# config_url = URL location of YAML config file (should be blank if using config_file)
# config_file = path + file name of YAML config file (should be blank if using config_url)
# overall = create overall table (default is TRUE)
# ind_yr = create tables for individual years (default is TRUE)
# overwrite = drop table first before creating it, if it exists (default is TRUE)
# test_mode = write things to the tmp schema to test out functions (default is FALSE)


#### FUNCTION ####
create_table_f <- function(
  conn,
  config_url = NULL,
  config_file = NULL,
  overall = T,
  ind_yr = T,
  overwrite = T,
  test_mode = F
) {
  
  
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
      stop(glue("Config file is not a YAML config file. ", 
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
  if (max(c("schema", "to_schema") %in% names(table_config)) == 0 & test_mode == F) {
    stop("YAML file is missing a schema")
    } else {
      if (is.null(table_config$schema) & is.null(table_config$to_schema)) {
        stop("schema/to_schema is blank in config file")
        }
    }
  
  if (max(c("table", "to_table") %in% names(table_config)) == 0) {
    stop("YAML file is missing a table name")
    } else {
      if (is.null(table_config$table) & is.null(table_config$to_table)) {
        stop("table/to_table is blank in config file")
      }
    }
  
  if (!"vars" %in% names(table_config)) {
    stop("YAML file is missing a list of variables")
  } else {
    if (is.null(table_config$vars)) {
      stop("No variables specified in config file")
    }
  }
  
  if (!"years" %in% names(table_config) & ind_yr == T) {
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
    message("FUNCTION WILL BE RUN IN TEST MODE, WRITING TO TMP SCHEMA")
    test_msg <- " (function is in test mode)"
  } else {
    test_msg <- ""
  }
  
  
  #### VARIABLES ####
  # Set up to work with both new and old way of using YMAL files
  if (!is.null(table_config$to_table)) {
    table_name <- table_config$to_table
  } else {
    table_name <- table_config$table
  }
  
  vars <- unlist(table_config$vars)  

  
  if (test_mode == T) {
    schema <- "tmp"
    
    if (!is.null(table_config$to_schema)) {
      table_name <- glue("{table_config$to_schema}_{table_name}")
    } else {
      table_name <- glue("{table_config$schema}_{table_name}")
    }
  } else if (!is.null(table_config$to_schema)) {
    schema <- table_config$to_schema
  } else {
    schema <- table_config$schema
  }
  
  
  if (ind_yr == T) {
    # Use unique in case variables are repeated
    years <- sort(unique(table_config$years))
  }
  
  

  #### OVERALL TABLE ####
  if (overall == T) {
    message(glue("Creating overall [{schema}].[{table_name}] table", test_msg))
    
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
    message(glue("Creating calendar year [{schema}].[{table_name}] tables", test_msg))
    
    lapply(years, function(x) {
      tbl_name <- DBI::Id(schema = schema, table = paste0(table_name, "_", x))
      
      if (overwrite == T) {
        if (dbExistsTable(conn, tbl_name)) {
          dbRemoveTable(conn, tbl_name)
        }
      }
      
      # Add additional year-specific variables if present
      add_vars_name <- paste0("vars_", x)
      if (add_vars_name %in% names(table_config)) {
        vars <- c(vars, unlist(table_config[[add_vars_name]]))
      }
      
      DBI::dbCreateTable(conn, tbl_name, fields = vars)
    })
  }
}