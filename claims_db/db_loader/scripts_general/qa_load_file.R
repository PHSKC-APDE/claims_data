#### FUNCTIONS TO RUN QA PROCESSES ON LOADING FILES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

#### CALL IN GENERAL QA FUNCTIONS IF NOT ALREADY LOADED ####
if (exists("qa_error_check_f") == F) {
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/db_loader/scripts_general/qa_general.R")
}


#### FUNCTION TO CHECK ACTUAL VS EXPECT ROW COUNTS IN SOURCE FILES ####
qa_file_row_count_f <- function(config_url = NULL,
                                config_file = NULL,
                                schema = NULL,
                                table = NULL,
                                file_path = NULL,
                                row_count = NULL
                                ) {
  
  #### BASIC ERROR CHECKS ####
  qa_error_check_f(config_url_chk = config_url,
                   config_file_chk = config_file)
  
  
  #### READ IN CONFIG FILE ####
  if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else if (!is.null(config_file)) {
    table_config <- yaml::read_yaml(config_file)
  }
  
  
  #### VARIABLES ####
  # Use the supplied values if available, otherwise use config file
  if (is.null(schema)) {
    if (!is.null(table_config$to_schema)) {
      schema <- table_config$to_schema
    } else {
      schema <- table_config$schema
    }
  }

  if (is.null(table)) {
    if (!is.null(table_config$to_table)) {
      table <- table_config$to_table
    } else {
      table <- table_config$table
    }
  }
  
  if (is.null(row_count)) {
    row_exp <- as.numeric(stringr::str_remove_all(as.character(table_config$overall$row_count), "\\D"))
  } else {
    row_exp <- row_count
  }
  
  
  if (is.null(file_path)) {
    file_path <- table_config$file_path
  }
  
  if (overall == T) {

    
    ### Count the actual number of rows (subtract 1 for header row)
    row_cnt <- R.utils::countLines(table_config$overall$file_path) - 1
    
    ### Compare counts
    count_check <- row_exp == row_cnt
    
    if (count_check == T) {
      qa_result <- TRUE
    } else {
      qa_result <- FALSE
    }
    
    qa_results <- data.frame("source_year" = "overall", 
                             "qa_result" = qa_result,
                             "expected_count" = as.numeric(row_exp),
                             "actual_count" = as.numeric(row_cnt),
                             stringsAsFactors = F)
  }
  
  ### Report results back
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
                              config_url = NULL,
                              config_file = NULL,
                              schema = NULL,
                              table = NULL,
                              drop_etl = T) {
  
  #### BASIC ERROR CHECKS ####
  qa_error_check_f(config_url_chk = config_url,
                   config_file_chk = config_file)
  
  
  #### READ IN CONFIG FILE ####
  if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else if (!is.null(config_file)) {
    table_config <- yaml::read_yaml(config_file)
  }
  
  
  #### VARIABLES ####
  # Use the supplied values if available, otherwise use config file
  if (is.null(schema)) {
    if (!is.null(table_config$to_schema)) {
      schema <- table_config$to_schema
    } else {
      schema <- table_config$schema
    }
  }
  
  if (is.null(table)) {
    if (!is.null(table_config$to_table)) {
      table <- table_config$to_table
    } else {
      table <- table_config$table
    }
  }
  
  
  if (overall == T) {
    ### Pull out names of existing table
    sql_name <- names(odbc::dbGetQuery(conn, glue::glue_sql(
      "SELECT TOP(0) * FROM {`schema`}.{`table`}", .con = conn)))
    
    if (drop_etl == T) {
      ### Remove etl_batch_id as this is not likely to be in the YAML
      sql_name <- sql_name[! sql_name %in% c("etl_batch_id")]
    }
    
    ### Pull in first few rows of the data to be loaded and get names
    load_table <- data.table::fread(table_config$overall$file_path, nrow = 10)
    tbl_name <- names(load_table)
    
    if (drop_etl == T) {
      ### Remove etl_batch_id from this table just in case it's there
      tbl_name <- tbl_name[! tbl_name %in% c("etl_batch_id")]
    }
    
    ### Compare names
    name_check <- all(sql_name == tbl_name)
    
    if (name_check == T) {
      qa_result <- TRUE
    } else {
      qa_result <- FALSE
    }
    
    qa_results <- data.frame("source_year" = "overall", 
                             "qa_result" = qa_result,
                             stringsAsFactors = F)
  }
  
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
      
      if (name_check == T) {
        qa_result <- TRUE
      } else {
        qa_result <- FALSE
      }
      return(qa_result)
    })
    
    ### Summarize results
    qa_results <- data.frame("source_year" = unlist(years), 
                             "qa_result" = unlist(qa_results),
                             stringsAsFactors = F)
  }
  
  ### Report results back
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
                               config_url = NULL,
                               config_file = NULL,
                               schema = NULL,
                               table = NULL,
                               row_count = NULL) {
  
  #### BASIC ERROR CHECKS ####
  qa_error_check_f(config_url_chk = config_url,
                   config_file_chk = config_file)
  
  
  #### READ IN CONFIG FILE ####
  if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else if (!is.null(config_file)) {
    table_config <- yaml::read_yaml(config_file)
  }
  
  #### VARIABLES ####
  # Use the supplied values if available, otherwise use config file
  if (is.null(schema)) {
    if (!is.null(table_config$to_schema)) {
      schema <- table_config$to_schema
    } else {
      schema <- table_config$schema
    }
  }
  
  if (is.null(table)) {
    if (!is.null(table_config$to_table)) {
      table <- table_config$to_table
    } else {
      table <- table_config$table
    }
  }
  
  if (is.null(row_count)) {
    row_exp <- as.numeric(stringr::str_remove_all(as.character(table_config$overall$row_count), "\\D"))
  } else {
    row_exp <- row_count
  }
  
  ### Count the actual number of rows loaded to SQL
  row_cnt <- odbc::dbGetQuery(conn,
                              glue::glue_sql("SELECT COUNT (*) FROM {`schema`}.{`table`}", 
                                             .con = conn))
  
  ### Compare counts
  count_check <- row_exp == row_cnt
  
  if (count_check == T) { qa_result <- TRUE} else {qa_result <- FALSE}
  
  qa_results <- data.frame("source_year" = "overall", 
                           "qa_result" = qa_result,
                           "expected_count" = as.numeric(row_exp),
                           "actual_count" = as.numeric(row_cnt),
                           stringsAsFactors = F)
  
 
  ### Report results back
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
                            config_url = NULL,
                            config_file = NULL,
                            schema = NULL,
                            table = NULL,
                            date_min_exp = NULL,
                            date_max_exp = NULL,
                            date_var = NULL) {
  
  #### BASIC ERROR CHECKS ####
  qa_error_check_f(config_url_chk = config_url,
                   config_file_chk = config_file)
  
  if (is.null(date_var)) {
    stop("Specify a date variable to check")
  }
  
  
  #### READ IN CONFIG FILE ####
  if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else if (!is.null(config_file)) {
    table_config <- yaml::read_yaml(config_file)
  }
  
  #### VARIABLES ####
  # Use the supplied values if available, otherwise use config file
  if (is.null(schema)) {
    if (!is.null(table_config$to_schema)) {
      schema <- table_config$to_schema
    } else {
      schema <- table_config$schema
    }
  }
  
  if (is.null(table)) {
    if (!is.null(table_config$to_table)) {
      table <- table_config$to_table
    } else {
      table <- table_config$table
    }
  }
  
  if (is.null(date_min_exp)) {
    date_min_exp <- table_config$overall$date_min
  }
  
  if (is.null(date_max_exp)) {
    date_max_exp <- table_config$overall$date_max
  }
  
  
  ### Find the actual date range loaded to SQL
  date_min <- odbc::dbGetQuery(conn,
                               glue::glue_sql("SELECT MIN ({`date_var`})
                                                FROM {`schema`}.{`table`}", 
                                              .con = conn)) 
  date_max <- odbc::dbGetQuery(conn,
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

  
  ### Report results back
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



