#### FUNCTIONS TO RUN QA PROCESSES ON LOADING FILES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

#### CALL IN GENERAL QA FUNCTIONS IF NOT ALREADY LOADED ####
if (exists("qa_error_check_f") == F) {
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_general.R")
}


#### FUNCTION TO CHECK ACTUAL VS EXPECT ROW COUNTS IN SOURCE FILES ####
qa_file_row_count_f <- function(config_url = NULL,
                           config_file = NULL,
                           overall = T,
                           ind_yr = F) {
  
  #### BASIC ERROR CHECKS ####
  qa_error_check_f(config_url_chk = config_url,
                   config_file_chk = config_file,
                   overall_chk = overall,
                   ind_yr_chk = ind_yr)
  
  
  #### READ IN CONFIG FILE ####
  if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }
  
  
  #### VARIABLES ####
  schema <- table_config$schema
  table_name <- table_config$table

  
  
  if (overall == T) {
    # Force rows expected to a number
    row_exp <- as.numeric(
      stringr::str_remove_all(as.character(table_config$overall$row_count), "\\D")
      )
    
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
  
  if (ind_yr == T) {
    ### Find which years have details
    years <- as.list(names(table_config)[str_detect(names(table_config), "table_")])
    
    ### Check columns for each year
    qa_results <- dplyr::bind_rows(lapply(years, function(x) {
      # Make appropriate table name to match SQL
      table_name_new <- paste0(table_name, "_", str_sub(x, -4, -1))
      
      # Force rows expected to a number
      row_exp <- as.numeric(
        stringr::str_remove_all(as.character(table_config[[x]][["row_count"]]), "\\D")
      )
      
      # Count the actual number of rows (subtract 1 for header row)
      print(glue::glue("Counting rows in {x} (could take a couple of minutes)"))
      row_cnt <- R.utils::countLines(table_config[[x]][["file_path"]]) - 1
      
      # Compare counts
      count_check <- row_exp == row_cnt
      
      if (count_check == T) {
        qa_result <- TRUE
      } else {
        qa_result <- FALSE
      }
      
      # Record counts
      qa_result_df <- data.frame("source_year" = as.character(x),
                                   "qa_result" = qa_result,
                                   "expected_count" = as.numeric(row_exp),
                                   "actual_count" = as.numeric(row_cnt),
                                 stringsAsFactors = F)
      
      return(qa_result_df)
    }))
    
    ### Summarize results
    print(qa_results)
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
                           overall = T,
                           ind_yr = F) {
  
  #### BASIC ERROR CHECKS ####
  qa_error_check_f(config_url_chk = config_url,
                   config_file_chk = config_file,
                   overall_chk = overall,
                   ind_yr_chk = ind_yr)
  
  
  #### READ IN CONFIG FILE ####
  if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }
  
  
  #### VARIABLES ####
  schema <- table_config$schema
  table_name <- table_config$table
  
  
  if (overall == T) {
    ### Pull out names of existing table
    sql_name <- names(odbc::dbGetQuery(conn, glue::glue_sql(
      "SELECT TOP(0) * FROM {`schema`}.{`table_name`}", .con = conn)))
    
    ### Pull in first few rows of the data to be loaded and get names
    load_table <- data.table::fread(table_config$overall$file_path, nrow = 10)
    tbl_name <- names(load_table)
    
    ### Compare names
    name_check <- names(sql_name) == names(tbl_name)
    
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
      table_name_new <- paste0(table_name, "_", str_sub(x, -4, -1))
      
      # Pull out names of existing table
      sql_name <- names(odbc::dbGetQuery(conn, glue::glue_sql(
        "SELECT TOP(0) * FROM {`schema`}.{`table_name_new`}", .con = conn)))
      
      # Pull in first few rows of the data to be loaded and get names
      load_table <- data.table::fread(table_config[[x]][["file_path"]], nrow = 10)
      tbl_name <- names(load_table)
      
      # Compare names
      name_check <- dplyr::all_equal(sql_name, tbl_name, ignore_col_order = F)
      
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
    note <- paste0("The following table(s) had mismatching columns: \n",
                   paste(qa_results$source_year[qa_results$qa_result == F], sep = ", "))
    result <- qa_results
  }
  
  report <- list("outcome" = outcome, "note" = note, "result" = result)
  return(report)
}



#### FUNCTION TO CHECK LOADED VS EXPECT ROW COUNTS IN SOURCE FILES ####
qa_sql_row_count_f <- function(conn,
                               config_url = NULL,
                               config_file = NULL,
                               overall = T,
                               ind_yr = F,
                               combine_yr = F) {
  
  #### BASIC ERROR CHECKS ####
  qa_error_check_f(config_url_chk = config_url,
                   config_file_chk = config_file,
                   overall_chk = overall,
                   ind_yr_chk = ind_yr)
  
  #### READ IN CONFIG FILE ####
  if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }
  
  #### VARIABLES ####
  schema <- table_config$schema
  table_name <- table_config$table
  
  
  if (overall == T) {
    # Force rows expected to a number
    row_exp <- as.numeric(
      stringr::str_remove_all(as.character(table_config$overall$row_count), "\\D")
    )
    
    ### Count the actual number of rows loaded to SQL
    row_cnt <- odbc::dbGetQuery(conn,
                                glue::glue_sql("SELECT COUNT (*)
                                               FROM {`schema`}.{`table_name`}", 
                                               .con = conn))
    
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
  
  if (ind_yr == T) {
    ### Find which years have details
    years <- as.list(names(table_config)[str_detect(names(table_config), "table_")])
    
    ### Check columns for each year
    qa_results <- dplyr::bind_rows(lapply(years, function(x) {
      # Make appropriate table name to match SQL
      table_name_new <- paste0(table_name, "_", str_sub(x, -4, -1))
      
      # Force rows expected to a number
      row_exp <- as.numeric(
        stringr::str_remove_all(as.character(table_config[[x]][["row_count"]]), "\\D")
      )
      
      # Count the actual number of rows loaded to SQL
      row_cnt <- odbc::dbGetQuery(conn,
                                  glue::glue_sql("SELECT COUNT (*)
                                                 FROM {`schema`}.{`table_name_new`}", 
                                                 .con = conn))
      
      # Compare counts
      count_check <- row_exp == row_cnt
      
      if (count_check == T) {
        qa_result <- TRUE
      } else {
        qa_result <- FALSE
      }
      
      # Record counts
      qa_result_df <- data.frame("source_year" = as.character(x),
                                 "qa_result" = qa_result,
                                 "expected_count" = as.numeric(row_exp),
                                 "actual_count" = as.numeric(row_cnt),
                                 stringsAsFactors = F)
      
      return(qa_result_df)
    }))
    
    ### Summarize results for individual years
    print(qa_results)
    
    
    ### Compare overall count if years were combined
    if (combine_yr == T) {
      # Sum up expected count (try config value first)
      overall_exp <- as.numeric(stringr::str_remove_all(
        as.character(table_config[["overall"]][["row_count"]]), "\\D"))
      
      if (!is.null(overall_exp)) {
        print("Calculating expected overall row count from individual years")
        overall_exp <- sum(qa_results$expected_count, na.rm = T)
      }
      
      # Find actual count
      overall_cnt <- odbc::dbGetQuery(conn,
                                  glue::glue_sql("SELECT COUNT (*)
                                               FROM {`schema`}.{`table_name`}", 
                                                 .con = conn))
      
      # Compare counts
      count_check <- overall_exp == overall_cnt
      
      if (count_check == T) {
        qa_result <- TRUE
      } else {
        qa_result <- FALSE
      }
      
      qa_results_combined <- data.frame("source_year" = "combined years",
                                       "qa_result" = qa_result,
                                       "expected_count" = as.numeric(overall_exp),
                                       "actual_count" = as.numeric(overall_cnt),
                                       stringsAsFactors = F)
      
      ### Summarize results for combined years
      print(qa_results_combined)
    }
  }
  
  
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
  
  if (combine_yr == T) {
    if (min(qa_results_combined$qa_result, na.rm = T) == 1) {
      outcome_combine <- "PASS"
      note_combine <- "Number of rows loaded to combined SQL table matches expected value"
      result_combined <- qa_results_combined
    } else {
      outcome_combine <- "FAIL"
      note_combine <- paste0("The combined years table did not match expected row counts: ",
                     paste(qa_results_combined$source_year[qa_results_combined$qa_result == F],
                           " (Expected: ", 
                           qa_results_combined$expected_count[qa_results_combined$qa_result == F],
                           ", actual: ",
                           qa_results_combined$actual_count[qa_results_combined$qa_result == F],
                           ")", sep = "", collapse = "; "))
      result_combined <- qa_results_combined
    }
  }
  
  if (combine_yr == F) {
    report <- list("outcome" = outcome, "note" = note, "result" = result)
  } else {
    report <- list("outcome" = c(outcome, outcome_combine), 
                   "note" = c(note, note_combine),
                   "result" = result,
                   "result_combined" = result_combined)
  }
  
  
  return(report)
}


#### FUNCTION TO CHECK THAT DATES MATCH EXPECTED RANGE ####
qa_date_range_f <- function(conn,
                               config_url = NULL,
                               config_file = NULL,
                               overall = T,
                               ind_yr = F,
                               combine_yr = F,
                               date_var = NULL) {
  
  #### BASIC ERROR CHECKS ####
  qa_error_check_f(config_url_chk = config_url,
                   config_file_chk = config_file,
                   overall_chk = overall,
                   ind_yr_chk = ind_yr)
  
  if (is.null(date_var)) {
    stop("Specify a date variable to check")
  }
  
  
  #### READ IN CONFIG FILE ####
  if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }
  
  #### VARIABLES ####
  schema <- table_config$schema
  table_name <- table_config$table
  
  
  if (overall == T) {
    ### Find the expected date range
    date_min_exp <- table_config$overall$date_min
    date_max_exp <- table_config$overall$date_max
    
    ### Find the actual date range loaded to SQL
    date_min <- odbc::dbGetQuery(conn,
                                 glue::glue_sql("SELECT MIN ({`date_var`})
                                                FROM {`schema`}.{`table_name`}", 
                                                .con = conn)) 
    date_max <- odbc::dbGetQuery(conn,
                                 glue::glue_sql("SELECT MAX ({`date_var`})
                                                FROM {`schema`}.{`table_name`}", 
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
  
  if (ind_yr == T) {
    ### Find which years have details
    years <- as.list(names(table_config)[str_detect(names(table_config), "table_")])
    
    ### Check columns for each year
    qa_results <- dplyr::bind_rows(lapply(years, function(x) {
      
      # Make appropriate table name to match SQL
      table_name_new <- paste0(table_name, "_", str_sub(x, -4, -1))
      
      ### Find the expected date range
      date_min_exp <- table_config[[x]][["date_min"]]
      date_max_exp <- table_config[[x]][["date_max"]]
      
      ### Count the actual date range loaded to SQL
      date_min <- odbc::dbGetQuery(conn,
                                   glue::glue_sql("SELECT MIN ({`date_var`})
                                                  FROM {`schema`}.{`table_name_new`}", 
                                                  .con = conn)) 
      date_max <- odbc::dbGetQuery(conn,
                                   glue::glue_sql("SELECT MAX ({`date_var`})
                                                  FROM {`schema`}.{`table_name_new`}", 
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
    
    ### Summarize results for individual years
    print(qa_results)
    
    
    ### Compare overall count if years were combined
    if (combine_yr == T) {
      # Find expected combined date range (try config value first)
      date_min_exp <- table_config$overall$date_min
      date_max_exp <- table_config$overall$date_max

      if (is.null(date_min_exp)) {
        print("Calculating expected overall date range from individual years")
        date_min_exp <- min(qa_results$expected_date_min, na.rm = T)
        date_max_exp <- max(qa_results$expected_date_max, na.rm = T)
      }
      
      ### Find the actual date range loaded to SQL
      date_min <- odbc::dbGetQuery(conn,
                                   glue::glue_sql("SELECT MIN ({`date_var`})
                                                FROM {`schema`}.{`table_name`}", 
                                                  .con = conn)) 
      date_max <- odbc::dbGetQuery(conn,
                                   glue::glue_sql("SELECT MAX ({`date_var`})
                                                FROM {`schema`}.{`table_name`}", 
                                                  .con = conn)) 
 
      
      ### Compare dates
      date_min_check <- date_min_exp == date_min
      date_max_check <- date_max_exp == date_max
      
      if (date_min_check == T & date_max_check == T) {
        qa_result <- TRUE
      } else {
        qa_result <- FALSE
      }
      
      qa_results_combined <- data.frame("source_year" = "combined years", 
                               "qa_result" = qa_result,
                               "expected_date_min" = date_min_exp,
                               "actual_date_min" = date_min[[1]],
                               "expected_date_max" = date_max_exp,
                               "actual_date_max" = date_max[[1]],
                               stringsAsFactors = F)
      
      ### Summarize results for combined years
      print(qa_results_combined)
    }
  }
  
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

  
  if (combine_yr == T) {
    if (min(qa_results_combined$qa_result, na.rm = T) == 1) {
      outcome_combine <- "PASS"
      note_combine <- "Date range in combined SQL table matches expected values"
      result_combined <- qa_results_combined
    } else {
      outcome_combine <- "FAIL"
      note_combine <- paste0("The combined years table did not match expected row counts: ",
                             paste(qa_results_combined$source_year[qa_results_combined$qa_result == F],
                                   " (Expected min: ", 
                                   qa_results_combined$expected_date_min[qa_results_combined$qa_result == F],
                                   ", actual min: ",
                                   qa_results_combined$actual_date_min[qa_results_combined$qa_result == F],
                                   " / ",
                                   " Expected max: ", 
                                   qa_results_combined$expected_date_max[qa_results_combined$qa_result == F],
                                   ", actual max: ",
                                   qa_results_combined$actual_date_max[qa_results_combined$qa_result == F],
                                   ")", 
                                   sep = "", collapse = "; "))
      result_combined <- qa_results_combined
    }
  }
  
  if (combine_yr == F) {
    report <- list("outcome" = outcome, "note" = note, "result" = result)
  } else {
    report <- list("outcome" = c(outcome, outcome_combine), 
                   "note" = c(note, note_combine),
                   "result" = result,
                   "result_combined" = result_combined)
  }
  
  
  return(report)
}



