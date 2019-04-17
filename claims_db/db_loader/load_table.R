#### FUNCTIONS TO LOAD DATA TO SQL TABLES
# Alastair Matheson
# Created:        2019-04-15
# Last modified:  2019-04-16


### Plans for future improvements:
# Add warning when overall table is about to be overwritten



#### FUNCTION TO LOAD DATA FROM LOCAL FILE ####
### PARAMETERS
# conn = name of the connection to the SQL database
# config_file = path + file name of YAML config file
# truncate = whether to truncate the table before loading
# overall = load a single non-year table (cannot be T if ind_yr = T)
# ind_yr = load tables for individual years (cannot be T if overall = T)
# combine_yr = union year-specific files into a single table
# test_mode = write things to the tmp schema to test out functions (default is FALSE)


load_table_from_file_f <- function(
  conn,
  config_file,
  truncate = T,
  overall = T,
  ind_yr = F,
  combine_yr = T,
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
  # Check that something will be run (but not both things)
  if (overall == F & ind_yr == F) {
    stop("At least one of 'overall and 'ind_yr' must be set to TRUE")
  }
  
  if (overall == T & ind_yr == T) {
    stop("Only one of 'overall and 'ind_yr' can be set to TRUE")
  }
  
  
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
  
  if (overall == T) {
    if (!"overall" %in% eval.config.sections(config_file)) {
      stop("YAML file is missing details for overall file")
    }
    
    if (is.null(table_config$overall$file_path)) {
      stop("YAML file is missing a file path to the new data")
    }
  }
  
  if (ind_yr == T) {
    if ("overall" %in% eval.config.sections(config_file)) {
      warning("YAML file has details for an overall file. \n
              This will be ignored since ind_yr == T.")
    }
    if (max(str_detect(eval.config.sections(file = config_file), 
                       "table_20[0-9]{2}")) == 0) {
      stop("YAML file is missing details for individual years")
    }
    if (combine_yr == T & is.null(unlist(table_config$combine_years))) {
      stop("No years specified for combining in config file")
    }
  }


  # Alert users they are in test mode
  if (test_mode == T) {
    print("FUNCTION WILL BE RUN IN TEST MODE, WRITING TO TMP SCHEMA")
    test_msg <- " (function is in test mode, only 1,000 rows will be loaded)"
  } else {
    test_msg <- ""
  }
  
  
  #### VARIABLES ####
  if (test_mode == T) {
    schema <- "tmp"
    load_rows <- " -L 1001 "
  } else {
    schema <- table_config$schema
    load_rows <- ""
  }
  
  table_name <- table_config$table
  vars <- unlist(table_config$vars)
  
  
  if (ind_yr == T & combine_yr == T) {
    # Use unique in case variables are repeated
    combine_years <- as.list(sort(unique(table_config$combine_years)))
  }
  
  
  #### TEST AREA ####
  years <- as.list(eval.config.sections(config_file)[str_detect(eval.config.sections(config_file), "table_")])
  

  

  
  
  #####
  
  
  

  #### SET UP A FUNCTION FOR COMMON ACTIONS ####
  # Both the overall load and year-specific loads use a similar set of code
  loading_process_f <- function(conn_inner = conn,
                                test_msg_inner = test_msg,
                                ind_yr_inner = ind_yr,
                                schema_inner = schema,
                                table_name_inner = table_name,
                                table_config_inner = table_config,
                                load_rows_inner = load_rows,
                                truncate_inner = truncate,
                                config_section) {
    
    # Set up text for message
    if (ind_yr_inner == T) {
      ind_yr_msg <- "calendar year"
    } else {
      ind_yr_msg <- "overall"
    }

    # Add message to user
    print(paste0("Loading ", ind_yr_msg, " [", schema_inner, "].[", table_name_inner, "] table", test_msg_inner))
    
    # Truncate existing table if desired
    if (truncate_inner == T) {
      dbGetQuery(conn_inner, paste0("TRUNCATE TABLE ", schema_inner, ".", table_name_inner))
    }
    
    # Remove existing index
    # This code pulls out the clustered index name
    index_name <- dbGetQuery(conn_inner,
                             paste0("SELECT DISTINCT a.index_name
                                    FROM
                                    (SELECT ind.name AS index_name
                                    FROM
                                    (SELECT object_id, name, type_desc FROM sys.indexes
                                    WHERE type_desc = 'CLUSTERED') ind
                                    INNER JOIN
                                    (SELECT name, schema_id, object_id FROM sys.tables
                                    WHERE name = '", table_name_inner, "') t
                                    ON ind.object_id = t.object_id
                                    INNER JOIN
                                    (SELECT name, schema_id FROM sys.schemas
                                    WHERE name = '", schema_inner, "') s
                                    ON t.schema_id = s.schema_id
                                    ) a"))[[1]]

    if (length(index_name) != 0) {
      dbGetQuery(conn_inner,
                 paste0("DROP INDEX [", index_name, "] ON ",
                        schema_inner, ".", table_name_inner))
    }

    # Pull out parameters for BCP load
    if (!is.null(table_config_inner[[config_section]][["field_term"]])) {
      field_term <- paste0("-t ", table_config_inner[[config_section]][["field_term"]])
    } else {
      field_term <- ""
    }
    
    if (!is.null(table_config_inner[[config_section]][["row_term"]])) {
      row_term <- paste0("-r ", table_config_inner[[config_section]][["row_term"]])
    } else {
      row_term <- ""
    }

    # Set up BCP arguments and run BCP
    bcp_args <- c(paste0("PHclaims.", schema_inner, ".", table_name_inner), 
                  " IN ", table_config_inner[[config_section]][["file_path"]], 
                  field_term, row_term, "-C 65001 ", "-F 2 ",
                  "-S KCITSQLUTPDBH51 -T ", "-b 100000 ", 
                  load_rows_inner, " -c ")
    
    system2(command = "bcp", args = c(bcp_args))
  }
  
  

  #### OVERALL TABLE ####
  if (overall == T) {
    # Run loading function
    loading_process_f(config_section = "overall")
    
    # Add index to the table
    dbGetQuery(conn,
               paste0("CREATE CLUSTERED INDEX [",
                      dbQuoteString(conn, table_config$index_name),
                      "] ON [", schema, "].[", table_name, "]([",
                      paste(table_config$index, collapse = "], ["), "])"))
  }
  
  
  #### CALENDAR YEAR TABLES ####
  if (ind_yr == T) {
    # Find which years have details
    years <- as.list(eval.config.sections(config_file)[str_detect(eval.config.sections(config_file), "table_")])

    lapply(years, function(x) {
      
      table_name_new <- paste0(table_name, "_", str_sub(x, -4, -1))
      
      # Run loading function
      loading_process_f(config_section = x, table_name_inner = table_name_new)
      
      # Add index to the table
      dbGetQuery(conn,
                 paste0("CREATE CLUSTERED INDEX [",
                        dbQuoteString(conn, table_config$index_name),
                        "] ON [", schema, "].[", table_name_new, "]([",
                        paste(table_config$index, collapse = "], ["), "])"))
    })
    
    # Combine individual years into a single table if desired
    if (combine_yr == T) {
      # Remove data from existing combined table if desired
      # if (truncate == T) {
      #   dbGetQuery(conn_inner, paste0("TRUNCATE TABLE ", schema, ".", table_name))
      # }
      
      if (truncate == T) {
        dbGetQuery(conn, paste0("TRUNCATE TABLE ", schema, ".", table_name))
      }
      
      
      # Need to find all the columns that only exist in some years
      # First find common variables
      all_vars <- unlist(table_config$vars)
      # Now find year-specific ones and add to main list
      lapply(combine_years, function(x) {
        table_name_new <- paste0("table_", x)
        add_vars_name <- paste0("vars_", x)
        all_vars <<- c(all_vars, unlist(table_config[[table_name_new]][[add_vars_name]]))
      })
      
      
      # Set up SQL code to load columns
      sql_combine <- paste0("INSERT INTO ", schema, ".", table_name, 
                            " WITH (TABLOCK) SELECT ", 
                            paste(all_vars, collapse = ", "), " FROM (")
      
      # For each year check which of the additional columns are present
      lapply(seq_along(combine_years), function(x) {
        table_name_new <- paste0(table_name, "_", combine_years[x])
        config_name_new <- paste0("table_", combine_years[x])
        add_vars_name <- paste0("vars_", combine_years[x])
        year_vars <- c(unlist(table_config$vars), unlist(table_config[[config_name_new]][[add_vars_name]]))
        
        matched_vars <- match(all_vars, year_vars)
        
        vars_to_load <- unlist(lapply(seq_along(matched_vars), function(y) {
          if (is.na(matched_vars[y])) {
            var_x <- paste0("NULL AS ", all_vars[y])
          } else {
            var_x <- all_vars[y]
          }
        }))
        
        # Add to main SQL statement
        if (x < length(combine_years)) {
          sql_combine <<- paste0(sql_combine, "SELECT ", 
                                 paste(vars_to_load, collapse = ", "), " FROM ",
                                 schema, ".", table_name_new, " UNION ALL ")
        } else {
          sql_combine <<- paste0(sql_combine, "SELECT ", 
                                 paste(vars_to_load, collapse = ", "), " FROM ",
                                 schema, ".", table_name_new, ") AS tmp")
        }
        
      })
      
      dbGetQuery(conn, sql_combine)
    }

    
  }
  
}




#### FUNCTION TO LOAD DATA FROM EXISTING SQL TABLES ####


