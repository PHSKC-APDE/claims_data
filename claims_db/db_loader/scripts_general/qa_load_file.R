#### FUNCTIONS TO RUN QA PROCESSES ON LOADING FILES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

#### FUNCTION TO CHECK ACTUAL VS EXPECT ROW COUNTS IN SOURCE FILES ####
qa_file_row_count_f <- function(server = NULL,
                                config = NULL,
                                config_url = NULL,
                                config_file = NULL,
                                schema = NULL,
                                table = NULL,
                                file_path = NULL,
                                row_count = NULL,
                                overall = T,
                                ind_yr = F) {
  
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
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }
  
  #### VARIABLES ####
  # Use the supplied values if available, otherwise use config file
  if (is.null(schema)) {
    if (!is.null(table_config$to_schema)) {
      schema <- table_config$to_schema
    } else if (!is.null(table_config[[server]][["to_schema"]])) {
      schema <- table_config[[server]][["to_schema"]]
    } else {
      schema <- table_config$schema
    }
  }
  
  if (is.null(table)) {
    if (!is.null(table_config$to_table)) {
      table <- table_config$to_table
    } else if (!is.null(table_config[[server]][["to_table"]])) {
      table <- table_config[[server]][["to_table"]]
    } else {
      table <- table_config$table
    }
  }
  
  
  #### OVERALL TABLE ####
  if (overall == T) {
    # Pull out row count and file path
    # Details could be provided when calling the function, under the overall section,
    # or generally in the YAML file
    if (!is.null(row_count)) {
      row_exp <- as.numeric(stringr::str_remove_all(as.character(row_count), "\\D"))
    } else if ("row_count" %in% names(table_config$overall)) {
      row_exp <- as.numeric(stringr::str_remove_all(as.character(table_config$overall$row_count), "\\D"))
    } else {
      row_exp <- as.numeric(stringr::str_remove_all(as.character(table_config$row_count), "\\D"))
    }
    
    
    if (is.null(file_path)) {
      if (!is.null(table_config$overall$file_path)) {
        file_path <- table_config$overall$file_path
      } else if (!is.null(table_config[[server]][["file_path"]])) {
        file_path <- table_config[[server]][["file_path"]]
      } else {
        file_path <- table_config$file_path
      }
    }
    
    ### Count the actual number of rows (subtract 1 for header row)
    row_cnt <- R.utils::countLines(file_path) - 1
    
    ### Compare counts
    count_check <- row_exp == row_cnt
    
    if (count_check == T) {qa_result <- TRUE} else {qa_result <- FALSE}
    
    qa_results <- data.frame("source_year" = "overall", 
                             "qa_result" = qa_result,
                             "expected_count" = as.numeric(row_exp),
                             "actual_count" = as.numeric(row_cnt),
                             stringsAsFactors = F)
  }


  #### INDIVIDUAL YEARS ####
  if (ind_yr == T) {
    ### Find which years have details
    years <- as.list(names(table_config)[str_detect(names(table_config), "table_")])
    
    ### Check columns for each year
    qa_results <- dplyr::bind_rows(lapply(years, function(x) {
      # Make appropriate table name to match SQL
      table_new <- paste0(table, "_", str_sub(x, -4, -1))
      
      # Force rows expected to a number
      row_exp <- as.numeric(
        stringr::str_remove_all(as.character(table_config[[x]][["row_count"]]), "\\D")
      )
      
      # Count the actual number of rows (subtract 1 for header row)
      print(glue::glue("Counting rows in {x} (could take a couple of minutes)"))
      row_cnt <- R.utils::countLines(table_config[[x]][["file_path"]]) - 1
      
      # Compare counts
      count_check <- row_exp == row_cnt
      
      if (count_check == T) {qa_result <- TRUE} else {qa_result <- FALSE}
      
      # Record counts
      qa_result_df <- data.frame("source_year" = as.character(x),
                                 "qa_result" = qa_result,
                                 "expected_count" = as.numeric(row_exp),
                                 "actual_count" = as.numeric(row_cnt),
                                 stringsAsFactors = F)
      
      return(qa_result_df)
    }))
  }
  
  
  #### REPORT RESULTS BACK ####
  if (min(qa_results$qa_result, na.rm = T) == 1) {
    outcome <- "PASS"
    note <- "Number of rows in source file(s) match(es) expected value(s)"
    result <- qa_results
  } else {
    outcome <- "FAIL"
    note <- paste0("The following table(s) had discrepancies in row counts: ",
                   paste(qa_results$source_year[qa_results$qa_result == F],
                         " (Expected: ", 
                         qa_results$expected_count[qa_results$qa_result == F],
                         ", actual: ",
                         qa_results$actual_count[qa_results$qa_result == F],
                         ")", sep = "", collapse = "; "))
    result <- qa_results
  }
  
  report <- list("outcome" = outcome, "note" = note, "result" = result)
  return(report)
}



#### FUNCTION TO CHECK COLUMNS MATCH SQL TABLES ####
qa_column_order_f <- function(conn = NULL,
                              server = NULL,
                              config = NULL,
                              config_url = NULL,
                              config_file = NULL,
                              schema = NULL,
                              table = NULL,
                              file_path = NULL,
                              overall = T,
                              ind_yr = F,
                              drop_etl = T) {
  
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
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }
  
  
  #### VARIABLES ####
  # Use the supplied values if available, otherwise use config file
  if (is.null(schema)) {
    if (!is.null(table_config$to_schema)) {
      schema <- table_config$to_schema
    } else if (!is.null(table_config[[server]][["to_schema"]])) {
      schema <- table_config[[server]][["to_schema"]]
    } else {
      schema <- table_config$schema
    }
  }
  
  if (is.null(table)) {
    if (!is.null(table_config$to_table)) {
      table <- table_config$to_table
    } else if (!is.null(table_config[[server]][["to_table"]])) {
      table <- table_config[[server]][["to_table"]]
    } else {
      table <- table_config$table
    }
  }
  
  
  #### OVERALL TABLE ####
  if (overall == T) {
    # Get file path
    if (is.null(file_path)) {
      if (!is.null(table_config$overall$file_path)) {
        file_path <- table_config$overall$file_path
      } else if (!is.null(table_config[[server]][["file_path"]])) {
        file_path <- table_config[[server]][["file_path"]]
      } else {
        file_path <- table_config$file_path
      }
    }
    
    ### Pull out names of existing table
    sql_name <- names(odbc::dbGetQuery(conn, glue::glue_sql(
      "SELECT TOP(0) * FROM {`schema`}.{`table`}", .con = conn)))
    
    if (drop_etl == T) {
      ### Remove etl_batch_id as this is not likely to be in the YAML
      sql_name <- sql_name[! sql_name %in% c("etl_batch_id")]
    }
    
    ### Pull in first few rows of the data to be loaded and get names
    load_table <- data.table::fread(file_path, nrow = 10)
    tbl_name <- names(load_table)
    
    if (drop_etl == T) {
      ### Remove etl_batch_id from this table just in case it's there
      tbl_name <- tbl_name[! tbl_name %in% c("etl_batch_id")]
    }
    
    ### Compare names
    name_check <- all(sql_name == tbl_name)
    
    if (name_check == T) {qa_result <- TRUE} else {qa_result <- FALSE}
    
    qa_results <- data.frame("source_year" = "overall", 
                             "qa_result" = qa_result,
                             stringsAsFactors = F)
  }
  
  
  #### INDIVIDUAL YEARS ####
  if (ind_yr == T) {
    ### Find which years have details
    years <- as.list(names(table_config)[str_detect(names(table_config), "table_")])
    
    ### Check columns for each year
    qa_results <- lapply(years, function(x) {
      # Make appropriate table name to match SQL
      table_new <- paste0(table, "_", str_sub(x, -4, -1))
      
      # Pull out names of existing table
      sql_name <- names(odbc::dbGetQuery(conn, glue::glue_sql(
        "SELECT TOP(0) * FROM {`schema`}.{`table_new`}", .con = conn)))
      
      # Remove etl_batch_id as this is not likely to be in the YAML
      sql_name <- sql_name[! sql_name %in% c("etl_batch_id")]
      
      # Pull in first few rows of the data to be loaded and get names
      load_table <- data.table::fread(table_config[[x]][["file_path"]], nrow = 10)
      tbl_name <- names(load_table)
      
      # Remove etl_batch_id from this table just in case it's there
      tbl_name <- tbl_name[! tbl_name %in% c("etl_batch_id")]
      
      # Compare names
      name_check <- all(sql_name == tbl_name)
      
      if (name_check == T) {qa_result <- TRUE} else {qa_result <- FALSE}
      
      return(qa_result)
    })
    
    ### Summarize results
    qa_results <- data.frame("source_year" = unlist(years), 
                             "qa_result" = unlist(qa_results),
                             stringsAsFactors = F)
  }
  
  
  #### REPORT RESULTS BACK ####
  if (min(qa_results$qa_result, na.rm = T) == 1) {
    outcome <- "PASS"
    note <- "Source file(s) columns match what exists in SQL"
    result <- qa_results
  } else {
    outcome <- "FAIL"
    note <- paste0("The following table(s) had mismatching columns: ",
                   paste(qa_results$source_year[qa_results$qa_result == F], collapse = ", "))
    result <- qa_results
  }
  
  report <- list("outcome" = outcome, "note" = note, "result" = result)
  return(report)
}



#### FUNCTION TO CHECK LOADED VS EXPECT ROW COUNTS IN SOURCE FILES ####
qa_load_row_count_f <- function(conn,
                                server = NULL,
                                config = NULL,
                                config_url = NULL,
                                config_file = NULL,
                                schema = NULL,
                                table = NULL,
                                row_count = NULL,
                                overall = T,
                                ind_yr = F) {
  
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
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }
  
  #### VARIABLES ####
  # Use the supplied values if available, otherwise use config file
  if (is.null(schema)) {
    if (!is.null(table_config$to_schema)) {
      schema <- table_config$to_schema
    } else if (!is.null(table_config[[server]][["to_schema"]])) {
      schema <- table_config[[server]][["to_schema"]]
    } else {
      schema <- table_config$schema
    }
  }
  
  if (is.null(table)) {
    if (!is.null(table_config$to_table)) {
      table <- table_config$to_table
    } else if (!is.null(table_config[[server]][["to_table"]])) {
      table <- table_config[[server]][["to_table"]]
    } else {
      table <- table_config$table
    }
  }
  
  
  
  #### OVERALL TABLE ####
  if (overall == T) {
    # Pull out row count and file path
    # Details could be provided when calling the function, under the overall section,
    # or generally in the YAML file
    if (!is.null(row_count)) {
      row_exp <- as.numeric(stringr::str_remove_all(as.character(row_count), "\\D"))
    } else if ("row_count" %in% names(table_config$overall)) {
      row_exp <- as.numeric(stringr::str_remove_all(as.character(table_config$overall$row_count), "\\D"))
    } else {
      row_exp <- as.numeric(stringr::str_remove_all(as.character(table_config$row_count), "\\D"))
    }
    
    
    ### Count the actual number of rows loaded to SQL
    row_cnt <- as.numeric(DBI::dbGetQuery(conn,
                                glue::glue_sql("SELECT COUNT (*) FROM {`schema`}.{`table`}", 
                                               .con = conn)))
    
    ### Compare counts
    count_check <- row_exp == row_cnt
    
    if (count_check == T) {qa_result <- TRUE} else {qa_result <- FALSE}
    
    qa_results <- data.frame("source_year" = "overall", 
                             "qa_result" = qa_result,
                             "expected_count" = as.numeric(row_exp),
                             "actual_count" = as.numeric(row_cnt),
                             stringsAsFactors = F)
  }
  
  
  #### INDIVIDUAL YEARS ####
  if (ind_yr == T) {
    ### Find which years have details
    years <- as.list(names(table_config)[str_detect(names(table_config), "table_")])
    
    ### Check columns for each year
    qa_results <- dplyr::bind_rows(lapply(years, function(x) {
      # Make appropriate table name to match SQL
      table_new <- paste0(table, "_", str_sub(x, -4, -1))
      
      # Force rows expected to a number
      row_exp <- as.numeric(
        stringr::str_remove_all(as.character(table_config[[x]][["row_count"]]), "\\D")
      )
      
      # Count the actual number of rows loaded to SQL
      row_cnt <- DBI::dbGetQuery(conn,
                                  glue::glue_sql("SELECT COUNT (*)
                                                 FROM {`schema`}.{`table_new`}", 
                                                 .con = conn))
      
      # Compare counts
      count_check <- row_exp == row_cnt
      
      if (count_check == T) {qa_result <- TRUE} else {qa_result <- FALSE}
      
      # Record counts
      qa_result_df <- data.frame("source_year" = as.character(x),
                                 "qa_result" = qa_result,
                                 "expected_count" = as.numeric(row_exp),
                                 "actual_count" = as.numeric(row_cnt),
                                 stringsAsFactors = F)
      
      return(qa_result_df)
    }))
  }
  
 
  #### REPORT RESULTS BACK ####
  if (min(qa_results$qa_result, na.rm = T) == 1) {
    outcome <- "PASS"
    note <- "Number of rows loaded to SQL match expected value(s)"
    result <- qa_results
  } else {
    outcome <- "FAIL"
    note <- paste0("The following table(s) had discrepancies in row counts: ",
                   paste(qa_results$source_year[qa_results$qa_result == F],
                         " (Expected: ", 
                         qa_results$expected_count[qa_results$qa_result == F],
                         ", actual: ",
                         qa_results$actual_count[qa_results$qa_result == F],
                         ")", sep = "", collapse = "; "))
    result <- qa_results
  }
  
  report <- list("outcome" = outcome, "note" = note, "result" = result)
  
  return(report)
}


#### FUNCTION TO CHECK THAT DATES MATCH EXPECTED RANGE ####
qa_date_range_f <- function(conn,
                            server = NULL,
                            config = NULL,
                            config_url = NULL,
                            config_file = NULL,
                            schema = NULL,
                            table = NULL,
                            date_min_exp = NULL,
                            date_max_exp = NULL,
                            date_var = NULL,
                            overall = T,
                            ind_yr = F) {
  
  #### BASIC ERROR CHECKS ####
  # Check if the config provided is a local object, file, or on a web page
  if (!is.null(config) & !is.null(config_url) & !is.null(config_file)) {
    stop("Specify either a local config object, config_url, or config_file but only one")
  }
  
  if (is.null(date_var)) {
    stop("Specify a date variable to check")
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
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }
  
  
  #### VARIABLES ####
  # Use the supplied values if available, otherwise use config file
  if (is.null(schema)) {
    if (!is.null(table_config$to_schema)) {
      schema <- table_config$to_schema
    } else if (!is.null(table_config[[server]][["to_schema"]])) {
      schema <- table_config[[server]][["to_schema"]]
    } else {
      schema <- table_config$schema
    }
  }
  
  if (is.null(table)) {
    if (!is.null(table_config$to_table)) {
      table <- table_config$to_table
    } else if (!is.null(table_config[[server]][["to_table"]])) {
      table <- table_config[[server]][["to_table"]]
    } else {
      table <- table_config$table
    }
  }
  
  
  #### OVERALL TABLE ####
  if (overall == T) {
    # Pull out expected date ranges
    # Details could be provided when calling the function, under the overall section,
    # or generally in the YAML file
    if (is.null(date_min_exp)) {
      if (!is.null(table_config$date_min)) {
        date_min_exp <- table_config$date_min
      } else if (!is.null(table_config[[server]][["date_min"]])) {
        date_min_exp <- table_config[[server]][["date_min"]]
      } else {
        date_min_exp <- table_config$date_min
      }
    }
    
    if (is.null(date_max_exp)) {
      if (!is.null(table_config$date_max)) {
        date_max_exp <- table_config$date_max
      } else if (!is.null(table_config[[server]][["date_max"]])) {
        date_max_exp <- table_config[[server]][["date_max"]]
      } else {
        date_max_exp <- table_config$date_max
      }
    }
    
    
    
    ### Find the actual date range loaded to SQL
    date_min <- DBI::dbGetQuery(conn,
                                 glue::glue_sql("SELECT MIN ({`date_var`})
                                                FROM {`schema`}.{`table`}", 
                                                .con = conn)) 
    date_max <- DBI::dbGetQuery(conn,
                                 glue::glue_sql("SELECT MAX ({`date_var`})
                                                FROM {`schema`}.{`table`}", 
                                                .con = conn)) 
    
    ### Compare dates
    date_min_check <- date_min_exp == date_min
    date_max_check <- date_max_exp == date_max
    
    if (date_min_check == T & date_max_check == T) {
      qa_result <- TRUE
    } else {
      qa_result <- FALSE
    }
    
    qa_results <- data.frame("source_year" = "overall", 
                             "qa_result" = qa_result,
                             "expected_date_min" = date_min_exp,
                             "actual_date_min" = date_min[[1]],
                             "expected_date_max" = date_max_exp,
                             "actual_date_max" = date_max[[1]],
                             stringsAsFactors = F)
  }
  
  
  #### INDIVIDUAL YEARS ####
  if (ind_yr == T) {
    ### Find which years have details
    years <- as.list(names(table_config)[str_detect(names(table_config), "table_")])
    
    ### Check columns for each year
    qa_results <- dplyr::bind_rows(lapply(years, function(x) {
      
      # Make appropriate table name to match SQL
      table_new <- paste0(table, "_", str_sub(x, -4, -1))
      
      ### Find the expected date range
      date_min_exp <- table_config[[x]][["date_min"]]
      date_max_exp <- table_config[[x]][["date_max"]]
      
      ### Count the actual date range loaded to SQL
      date_min <- odbc::dbGetQuery(conn,
                                   glue::glue_sql("SELECT MIN ({`date_var`})
                                                  FROM {`schema`}.{`table_new`}", 
                                                  .con = conn)) 
      date_max <- odbc::dbGetQuery(conn,
                                   glue::glue_sql("SELECT MAX ({`date_var`})
                                                  FROM {`schema`}.{`table_new`}", 
                                                  .con = conn)) 
      
      ### Compare dates
      date_min_check <- date_min_exp == date_min
      date_max_check <- date_max_exp == date_max
      
      if (date_min_check == T & date_max_check == T) {
        qa_result <- TRUE
      } else {
        qa_result <- FALSE
      }
      
      qa_result_df <- data.frame("source_year" = "overall", 
                                 "qa_result" = qa_result,
                                 "expected_date_min" = date_min_exp,
                                 "actual_date_min" = date_min[[1]],
                                 "expected_date_max" = date_max_exp,
                                 "actual_date_max" = date_max[[1]],
                                 stringsAsFactors = F)
      
      return(qa_result_df)
    }))
  }
  
  
  
  
  #### REPORT RESULTS BACK ####
  if (min(qa_results$qa_result, na.rm = T) == 1) {
    outcome <- "PASS"
    note <- "Date range of table(s) loaded to SQL match(es) expected value(s)"
    result <- qa_results
  } else {
    outcome <- "FAIL"
    note <- paste0("The following table(s) had discrepancies in date ranges: ",
                   paste(qa_results$source_year[qa_results$qa_result == F],
                         " (Expected min: ", 
                         qa_results$expected_date_min[qa_results$qa_result == F],
                         ", actual min: ",
                         qa_results$actual_date_min[qa_results$qa_result == F],
                         " / ",
                         " Expected max: ", 
                         qa_results$expected_date_max[qa_results$qa_result == F],
                         ", actual max: ",
                         qa_results$actual_date_max[qa_results$qa_result == F],
                         ")", 
                         sep = "", collapse = "; "))
    result <- qa_results
  }

  report <- list("outcome" = outcome, "note" = note, "result" = result)
  
  return(report)
}

