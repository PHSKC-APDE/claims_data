#### FUNCTIONS TO RUN QA PROCESSES ON SQL TABLES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

#### CALL IN GENERAL QA FUNCTIONS IF NOT ALREADY LOADED ####
if (exists("qa_error_check_f") == F) {
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_general.R")
}


#### FUNCTION TO CHECK ROW COUNTS MATCH in FROM and TO TABLES ####
qa_sql_row_count_f <- function(conn = db_claims,
                               config_url = NULL,
                               config_file = NULL,
                               overall = T,
                               ind_yr = F) {
  
  # Don't really need overall and ind_yr but the error checking function
  # currently uses them
  
  ### BASIC ERROR CHECKS
  qa_error_check_f(config_url_chk = config_url,
                   config_file_chk = config_file,
                   overall_chk = overall,
                   ind_yr_chk = ind_yr)
  
  ### READ IN CONFIG FILE
  if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }
  
  
  ### VARIABLES
  from_schema <- table_config$from_schema
  from_table <- table_config$from_table
  to_schema <- table_config$to_schema
  to_table <- table_config$to_table
  
  ### VALUES
  rows_from <- odbc::dbGetQuery(conn = conn,
                                glue::glue_sql(
                                  "SELECT COUNT (*) 
                                  FROM {`from_schema`}.{`from_table`}",
                                  .con = conn))
  
  rows_to <- odbc::dbGetQuery(conn = conn,
                                glue::glue_sql(
                                  "SELECT COUNT (*) 
                                  FROM {`to_schema`}.{`to_table`}",
                                  .con = conn))
  
  
  if (rows_from != rows_to) {
    qa_result <- "FAIL"
    note <- glue::glue("Mismatched number of rows ({from_schema}.{from_table}: {rows_from} 
                       vs. {to_schema}.{to_table}: {rows_to})")
  } else {
    qa_result <- "PASS"
    note <- glue::glue("Number of rows equal in both {from_schema}.{from_table} and 
                       {to_schema}.{to_table} ({rows_to})")
  }
  
  result <- list(qa_result = qa_result, note = note)
  return(result)
}

