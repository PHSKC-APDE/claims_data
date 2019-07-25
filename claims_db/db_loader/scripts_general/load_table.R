#### FUNCTIONS TO LOAD DATA TO SQL TABLES
# Alastair Matheson
# Created:        2019-04-15
# Last modified:  2019-07-25


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
  config_url = NULL,
  config_file = NULL,
  truncate = T,
  overall = T,
  ind_yr = F,
  combine_yr = T,
  test_mode = F
  ) {
  
  
  #### INITIAL ERROR CHECK ####
  # Check if the config provided is a local file or on a webpage
  if (!is.null(config_url) & !is.null(config_file)) {
    stop("Specify either a config_url or config_file but not both")
  }
  
  if (!is.null(config_url)) {
    warning("YAML configs pulled from a URL are subject to fewer error checks")
  }
  
  # Check that the yaml config file exists in the right format
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
  # Check that something will be run (but not both things)
  if (overall == F & ind_yr == F) {
    stop("At least one of 'overall and 'ind_yr' must be set to TRUE")
  }
  
  if (overall == T & ind_yr == T) {
    stop("Only one of 'overall and 'ind_yr' can be set to TRUE")
  }
  
  
  # Check that the yaml config file has necessary components
  if (!max(c("schema", "to_schema") %in% names(table_config)) == 0 & test_mode == F) {
    stop("YAML file is missing a schema")
  } else {
    if (is.null(table_config$schema) & is.null(table_config$to_schema)) {
      stop("schema/to_schema is blank in config file")
    }
  }
  
  if (!max(c("table", "to_table") %in% names(table_config)) == 0) {
    stop("YAML file is missing a table name")
  } else {
    if (is.null(table_config$table) & is.null(table_config$to_table)) {
      stop("table/to_table is blank in config file")
    }
  }
  
  if (!is.null(table_config$index_name) & is.null(table_config$index)) {
    stop("YAML file has an index name but no index columns")
  }
  
  if (overall == T) {
    if (!"overall" %in% names(table_config)) {
      stop("YAML file is missing details for overall file")
    }
    
    if (is.null(table_config$overall$file_path)) {
      stop("YAML file is missing a file path to the new data")
    }
  }
  
  if (ind_yr == T) {
    if ("overall" %in% names(table_config)) {
      warning("YAML file has details for an overall file. \n
              This will be ignored since ind_yr == T.")
    }
    if (max(str_detect(names(table_config), "table_20[0-9]{2}")) == 0) {
      stop("YAML file is missing details for individual years")
    }
    if (combine_yr == T) {
      if (is.null(unlist(table_config$combine_years))) {
        stop("No years specified for combining in config file")
      }
      if (!"vars" %in% names(table_config)) {
        stop("YAML file is missing a variables (vars) section")
      }
      if (is.null(table_config$vars)) {
        stop("No variables specified in config file")
      }
    }
  }


  # Alert users they are in test mode
  if (test_mode == T) {
    message("FUNCTION WILL BE RUN IN TEST MODE, WRITING TO TMP SCHEMA")
    test_msg <- " (function is in test mode, only 1,000 rows will be loaded)"
  } else {
    test_msg <- ""
  }
  
  
  #### VARIABLES ####
  # Set up to work with both new and old way of using YAML files
  if (!is.null(table_config$to_table)) {
    table_name <- table_config$to_table
  } else {
    table_name <- table_config$table
  }
  
  if (!is.null(names(table_config$vars))) {
    vars <- unlist(names(table_config$vars))
  } else {
    vars <- unlist(table_config$vars)
  }
  
  if (test_mode == T) {
    schema <- "tmp"
    load_rows <- " -L 1001 "
    
    if (!is.null(table_config$to_schema)) {
      table_name <- glue("{table_config$to_schema}_{table_name}")
    } else {
      table_name <- glue("{table_config$schema}_{table_name}")
    }
  } else if (!is.null(table_config$to_schema)) {
    schema <- table_config$to_schema
    load_rows <- ""
  } else {
    schema <- table_config$schema
    load_rows <- ""
  }
  
  
  if (ind_yr == T & combine_yr == T) {
    # Use unique in case variables are repeated
    combine_years <- as.list(sort(unique(table_config$combine_years)))
  }
  
  if (!is.null(table_config$index_name)) {
    add_index <- TRUE
  } else {
    add_index <- FALSE
  }


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
                                drop_index = add_index,
                                config_section) {
    
    # Set up text for message
    if (ind_yr_inner == T) {
      ind_yr_msg <- "calendar year"
    } else {
      ind_yr_msg <- "overall"
    }

    # Add message to user
    message(glue('Loading {ind_yr_msg} [{schema_inner}].[{table_name_inner}] table(s) ',
               ' from {table_config_inner[[config_section]][["file_path"]]} {test_msg_inner}'))
    
    # Truncate existing table if desired
    if (truncate_inner == T) {
      dbGetQuery(conn_inner, glue::glue_sql("TRUNCATE TABLE {`schema_inner`}.{`table_name_inner`}", 
                                            .con = conn_inner))
    }
    
    # Remove existing clustered index if desired (and an index exists)
    if (drop_index == T) {
      # This code pulls out the clustered index name
      index_name <- dbGetQuery(conn_inner, glue::glue_sql("SELECT DISTINCT a.index_name
                                  FROM
                                  (SELECT ind.name AS index_name
                                  FROM
                                  (SELECT object_id, name, type_desc FROM sys.indexes
                                  WHERE type_desc = 'CLUSTERED') ind
                                  INNER JOIN
                                  (SELECT name, schema_id, object_id FROM sys.tables
                                  WHERE name = {`table`}) t
                                  ON ind.object_id = t.object_id
                                  INNER JOIN
                                  (SELECT name, schema_id FROM sys.schemas
                                  WHERE name = {`schema`}) s
                                  ON t.schema_id = s.schema_id
                                  ) a", .con = conn_inner,
                                table = dbQuoteString(conn_inner, table_name_inner),
                                schema = dbQuoteString(conn_inner, schema_inner)))[[1]]

      if (length(index_name) != 0) {
        dbGetQuery(conn_inner,
                   glue::glue_sql("DROP INDEX {`index_name`} ON 
                                  {`schema_inner`}.{`table_name_inner`}", .con = conn_inner))
      }
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
    bcp_args <- c(glue(' PHclaims.{schema_inner}.{table_name_inner} IN ', 
                       ' "{table_config_inner[[config_section]][["file_path"]]}" ',
                       ' {field_term} {row_term} -C 65001 -F 2 ',
                       ' -S KCITSQLUTPDBH51 -T -b 100000 {load_rows_inner} -c '))
    
    system2(command = "bcp", args = c(bcp_args))
  }
  
  

  #### OVERALL TABLE ####
  if (overall == T) {
    # Run loading function
    loading_process_f(config_section = "overall")

    if (add_index == T) {
      # Add index to the table
      dbGetQuery(conn,
                 glue::glue_sql("CREATE CLUSTERED INDEX {`table_config$index_name`} ON 
                              {`schema`}.{`table_name`}({`index_vars`*})",
                                index_vars = table_config$index,
                                .con = conn))
    }
  }
  
  #### CALENDAR YEAR TABLES ####
  if (ind_yr == T) {
    # Find which years have details
    years <- as.list(names(table_config)[str_detect(names(table_config), "table_")])

    lapply(years, function(x) {
      
      table_name_new <- glue("{table_name}_{str_sub(x, -4, -1)}")
      
      # Run loading function
      loading_process_f(config_section = x, table_name_inner = table_name_new)
      
      # Add index to the table
      dbGetQuery(conn,
                 glue::glue_sql("CREATE CLUSTERED INDEX {`table_config$index_name`} ON 
                                {`schema`}.{`table_name_new`}({`index_vars`*})",
                                index_vars = table_config$index,
                                .con = conn))
    })
    
    # Combine individual years into a single table if desired
    if (combine_yr == T) {
      message("Combining years into a single table")
      if (truncate == T) {
        # Remove data from existing combined table if desired
        dbGetQuery(conn, glue::glue_sql("TRUNCATE TABLE {`schema`}.{`table_name`}", 
                                              .con = conn))
      }
      
      if (add_index == T) {
        # Remove index from combined table if it exists
        # This code pulls out the clustered index name
        index_name <- dbGetQuery(conn, 
                                 glue::glue_sql("SELECT DISTINCT a.index_name
                                                FROM
                                                (SELECT ind.name AS index_name
                                                  FROM
                                                  (SELECT object_id, name, type_desc FROM sys.indexes
                                                    WHERE type_desc = 'CLUSTERED') ind
                                                  INNER JOIN
                                                  (SELECT name, schema_id, object_id FROM sys.tables
                                                    WHERE name = {`table`}) t
                                                  ON ind.object_id = t.object_id
                                                  INNER JOIN
                                                  (SELECT name, schema_id FROM sys.schemas
                                                    WHERE name = {`schema`}) s
                                                  ON t.schema_id = s.schema_id) a",
                                                .con = conn,
                                                table = dbQuoteString(conn, table_name),
                                                schema = dbQuoteString(conn, schema)))[[1]]
        
        if (length(index_name) != 0) {
          dbGetQuery(conn_inner,
                     glue::glue_sql("DROP INDEX {`index_name`} ON 
                                  {`schema`}.{`table_name`}", .con = conn))
        }
      }
      
      
      # Need to find all the columns that only exist in some years
      # First find common variables
      # Set up to work with old and new YAML config styles
      if (!is.null(names(table_config$vars))) {
        all_vars <- unlist(names(table_config$vars))
      } else {
        all_vars <- unlist(table_config$vars)  
      }
      
      # Now find year-specific ones and add to main list
      lapply(combine_years, function(x) {
        table_name_new <- paste0("table_", x)
        add_vars_name <- paste0("vars_", x)
        
        if (!is.null(names(table_config$vars))) {
          all_vars <<- c(all_vars, unlist(names(table_config[[table_name_new]][[add_vars_name]])))
        } else {
          all_vars <<- c(all_vars, unlist(table_config[[table_name_new]][[add_vars_name]]))
        }
      })
      # Make sure there are no duplicate variables
	  all_vars <- unique(all_vars)
	  
      
      # Set up SQL code to load columns
      sql_combine <- glue::glue_sql("INSERT INTO {`schema`}.{`table_name`} WITH (TABLOCK) 
                                    ({`vars`*}) 
                                    SELECT {`vars`*} FROM (", 
                                    .con = conn,
                                    vars = all_vars)
      
      # For each year check which of the additional columns are present
      lapply(seq_along(combine_years), function(x) {
        table_name_new <- paste0(table_name, "_", combine_years[x])
        config_name_new <- paste0("table_", combine_years[x])
        add_vars_name <- paste0("vars_", combine_years[x])
        if (!is.null(names(table_config$vars))) {
          year_vars <- c(unlist(names(table_config$vars)), 
                         unlist(names(table_config[[config_name_new]][[add_vars_name]])))
        } else {
          year_vars <- c(unlist(table_config$vars), unlist(table_config[[config_name_new]][[add_vars_name]]))
        }
        
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
          sql_combine <<- glue::glue_sql("{`sql_combine`} SELECT {`vars_to_load`*}
                                         FROM {`schema`}.{`table`} UNION ALL ",
                                         .con = conn,
                                         table = table_name_new)
        } else {
          sql_combine <<- glue::glue_sql("{`sql_combine`} SELECT {`vars_to_load`*}
                                         FROM {`schema`}.{`table`}) AS tmp",
                                         .con = conn,
                                         table = table_name_new)
        }
        
      })
      
      dbGetQuery(conn, sql_combine)
      
      if (add_index == T) {
        # Add index to the table
        dbGetQuery(conn,
                   glue::glue_sql("CREATE CLUSTERED INDEX {`table_config$index_name`} ON 
                              {`schema`}.{`table_name`}({`index_vars`*})",
                                  index_vars = table_config$index,
                                  .con = conn))
      }
    }
  }
}



#### FUNCTION TO LOAD DATA FROM EXISTING SQL TABLES ####
### PARAMETERS
# conn = name of the connection to the SQL database
# config_file = path + file name of YAML config file
# truncate = whether to FULLY truncate the table before loading
# date_truncate = whether to PARTIALLY truncate the table from a specified date
#  NOTE: This triggers an archive schema table to be truncated and loaded
#        The archive schema table must already exist or an error will be thrown.
# auto_date = whether to automatically calculate date from which to truncate data
# test_mode = write things to the tmp schema to test out functions (default is FALSE)


load_table_from_sql_f <- function(
  conn,
  config_url = NULL,
  config_file = NULL,
  truncate = F,
  truncate_date = T,
  auto_date = T,
  test_mode = F
  ) {
  
  #### INITIAL ERROR CHECK ####
  # Check if the config provided is a local file or on a webpage
  if (!is.null(config_url) & !is.null(config_file)) {
    stop("Specify either a config_url or config_file but not both")
  }
  
  if (!is.null(config_url)) {
    warning("YAML configs pulled from a URL are subject to fewer error checks")
  }
  
  # Check that the yaml config file exists in the right format
  if (!is.null(config_file)) {
    # Check that the yaml config file exists in the right format
    if (file.exists(config_file) == F) {
      stop("Config file does not exist, check file name")
    }
    
    if (is.yaml.file(config_file) == F) {
      stop(paste0("Config file is not a YAML config file. \n", 
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
  # Check that the yaml config file has necessary components
  if (!"from_schema" %in% names(table_config) & test_mode == F) {
    stop("YAML file is missing a from_schema section")
  } else {
    if (is.null(table_config$from_schema)) {
      stop("from_schema name is blank in config file")
    }
  }
  
  if (!"from_table" %in% names(table_config)) {
    stop("YAML file is missing a from_table section")
  } else {
    if (is.null(table_config$from_table)) {
      stop("from_table name is blank in config file")
    }
  }
  
  if (!"to_schema" %in% names(table_config) & test_mode == F) {
    stop("YAML file is missing a to_schema section")
  } else {
    if (is.null(table_config$to_schema)) {
      stop("to_schema name is blank in config file")
    }
  }
  
  if (!"to_table" %in% names(table_config)) {
    stop("YAML file is missing a to_table section")
  } else {
    if (is.null(table_config$to_table)) {
      stop("to_table name is blank in config file")
    }
  }
  
  if (!"vars" %in% names(table_config)) {
    stop("YAML file is missing a variables (vars) section")
  } else {
    if (is.null(table_config$vars)) {
      stop("No variables specified in config file")
    }
  }

  if (truncate == T & truncate_date == T) {
    print("Warning: truncate and truncate_date both set to TRUE. \n
          Entire table will be truncated.")
  }
  
  if (truncate_date == T) {
    if (!"date_var" %in% names(table_config)) {
      stop("YAML file is missing a date_var section")
    }
    if (is.null(table_config$date_var)) {
      stop("No date_var variable specified")
    }
    
    if (auto_date == F) {
      if (!"date_truncate" %in% names(table_config)) {
        stop("YAML file is missing a date_truncate section")
      }
      if (is.null(table_config$date_truncate)) {
        stop("No date_truncate variable specified")
      }
    }
  }
  
  # Alert users they are in test mode
  if (test_mode == T) {
    message("FUNCTION WILL BE RUN IN TEST MODE, WRITING TO TMP SCHEMA")
    test_msg <- " (function is in test mode, only 5,000 rows will be loaded)"
  } else {
    test_msg <- ""
  }
  
  
  #### VARIABLES ####
  from_table_name <- table_config$from_table
  to_table_name <- table_config$to_table
  
  if (!is.null(names(table_config$vars))) {
    vars <- unlist(names(table_config$vars))
  } else {
    vars <- unlist(table_config$vars)
  }
  
  if (test_mode == T) {
    from_schema <- "tmp"
    to_schema <- "tmp"
    archive_schema <- "tmp"
    from_table_name <- glue("{table_config$from_schema}_{from_table_name}")
    archive_table_name <- glue("archive_{to_table_name}")
    to_table_name <- glue("{table_config$to_schema}_{to_table_name}")
    load_rows <- " TOP (5000) " # Using 5,000 to better test data from multiple years
    archive_rows <- " TOP (4000) " # When unioning tables in test mode, ensure a mix from both
    new_rows <- " TOP (1000) " # When unioning tables in test mode, ensure a mix from both
  } else {
    from_schema <- table_config$from_schema
    to_schema <- table_config$to_schema
    archive_schema <- "archive"
    archive_table_name <- to_table_name
    load_rows <- ""
    archive_rows <- ""
    new_rows <- ""
  }
  
  if (!is.null(table_config$index_name)) {
    add_index <- TRUE
  } else {
    add_index <- FALSE
  }
  
  if (truncate_date == T) {
    
    date_var <- table_config$date_var
    
    if (auto_date == T) {
      # Find the most recent date in the new data
      max_date <- dbGetQuery(conn, glue::glue_sql("SELECT MAX({`table_config$date_var`})
                                 FROM {`from_schema`}.{`from_table_name`}",
                                 .con = conn))
      
      message(glue("Most recent date found in the new data: {max_date}"))
      
      # If using auto-date, assume the data run through to the end of the month
      #   even if the actual date does not
      if (nchar(max_date) == 6) {
        if (str_sub(as.character(max_date), -2, -1) == "12") {
          date_truncate <- max_date - 11
        } else {
          date_truncate <- max_date - 99
        }
      } else if (nchar(max_date %in% c(8, 10) | is.Date(max_date))) {
        # Logic for full dates is go to the first day of the next month then back a year
        #   (= going back to the first day of 11 months ago)
        date_truncate <- rollback(max_date %m+% months(1), roll_to_first = T) - years(1)
      } else {
        stop("There was an error with the format of the date_var variable")
      }
    } else {
      date_truncate <- table_config$date_truncate
    }
    
    message(glue("Date to truncate from: {date_truncate}"))
  }

  
  #### DEAL WITH EXISTING TABLE ####
  # Truncate existing table if desired
  if (truncate == T) {
    dbGetQuery(conn, glue::glue_sql("TRUNCATE TABLE {`to_schema`}.{`to_table_name`}", .con = conn))
  }
  
  # 'Truncate' from a given date if desired (really move existing data to archive then copy back)
  if (truncate == F & truncate_date == T) {
    message("Archiving existing table")
    # Check if the archive table exists and move table over. If not, show message.
    tbl_id <- DBI::Id(catalog = "PHClaims", schema = archive_schema, table = to_table_name)
    if (dbExistsTable(conn, tbl_id)) {
      dbGetQuery(conn, glue::glue_sql("TRUNCATE TABLE {`archive_schema`}.{`archive_table_name`}", .con = conn))
    } else {
      message(glue("Note: {archive_schema}.{archive_table_name} did not exist so was created"))
    }
    
    sql_archive <- glue::glue_sql("INSERT INTO {`archive_schema`}.{`archive_table_name`} WITH (TABLOCK) 
                                SELECT {`archive_rows`} {`vars`*} FROM 
                                {`to_schema`}.{`to_table_name`}", .con = conn,
                                  archive_rows = DBI::SQL(archive_rows))
    
    dbGetQuery(conn, sql_archive)
    
    # Now truncate destination table
    dbGetQuery(conn, glue::glue_sql("TRUNCATE TABLE {`to_schema`}.{`to_table_name`}", .con = conn))
    }
    
  
  # Remove existing clustered index if a new one is to be added
  if (add_index == T) {
    # This code pulls out the clustered index name
    index_sql <- glue::glue_sql("SELECT DISTINCT a.index_name
                                  FROM
                                  (SELECT ind.name AS index_name
                                  FROM
                                  (SELECT object_id, name, type_desc FROM sys.indexes
                                  WHERE type_desc = 'CLUSTERED') ind
                                  INNER JOIN
                                  (SELECT name, schema_id, object_id FROM sys.tables
                                  WHERE name = {`table`}) t
                                  ON ind.object_id = t.object_id
                                  INNER JOIN
                                  (SELECT name, schema_id FROM sys.schemas
                                  WHERE name = {`schema`}) s
                                  ON t.schema_id = s.schema_id
                                  ) a", .con = conn,
                                table = dbQuoteString(conn, to_table_name),
                                schema = dbQuoteString(conn, to_schema))
    
    
    index_name <- dbGetQuery(conn, index_sql)[[1]]
    
    if (length(index_name) != 0) {
      dbGetQuery(conn,
                 glue::glue_sql("DROP INDEX {`index_name`} ON 
                                {`to_schema`}.{`to_table_name`}", .con = conn))
    }
  }
  

  #### LOAD DATA TO TABLE ####
  # Add message to user
  message(glue("Loading to [{to_schema}].[{to_table_name} table", test_msg))
  
  # Run INSERT statement
  if (truncate_date == F) {
    sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table_name`} WITH (TABLOCK) 
                                SELECT {load_rows} {vars*} FROM 
                                {`from_schema`}.{`from_table_name`}", 
                                  .con = conn,
                                  load_rows = DBI::SQL(load_rows),
                                  vars = dbQuoteIdentifier(conn, vars))
  } else if (truncate_date == T) {
    sql_combine <- glue::glue_sql("INSERT INTO {`to_schema`}.{`to_table_name`} WITH (TABLOCK)
                                  SELECT {archive_rows} {vars*} FROM 
                                  {archive_schema}.{archive_table_name}
                                  WHERE {date_var} < {date_truncate}  
                                  UNION 
                                  SELECT {new_rows} {vars*} FROM 
                                  {from_schema}.{from_table_name}
                                  WHERE {date_var} >= {date_truncate}",
                                  .con = conn,
                                  load_rows = DBI::SQL(load_rows),
                                  archive_rows = DBI::SQL(archive_rows),
                                  vars = dbQuoteIdentifier(conn, vars),
                                  new_rows = DBI::SQL(new_rows),
                                  date_var = dbQuoteIdentifier(conn, date_var),
                                  date_truncate = dbQuoteString(conn, as.character(date_truncate)))
  }
  dbGetQuery(conn, sql_combine)
  
  
  # Add index to the table (if desired)
  if (add_index == T) {
    index_sql <- glue::glue_sql("CREATE CLUSTERED INDEX {`table_config$index_name`} ON 
                            {`to_schema`}.{`to_table_name`}({index_vars*})",
                                index_vars = dbQuoteIdentifier(conn, table_config$index),
                                .con = conn)
    dbGetQuery(conn, index_sql)
  }
}


