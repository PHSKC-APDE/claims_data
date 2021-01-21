#### CODE TO LOAD APCD CLAIM PROVIDER TABLES
# Eli Kern, PHSKC (APDE)
#
# 2020-08

### Run from master_apcd_full script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/apcd/master_apcd_full.R


load_load_raw.apcd_claim_provider_full_f <- function(etl_date_min = NULL,
                                            etl_date_max = NULL,
                                            etl_delivery_date = NULL,
                                            etl_note = NULL) {
  
  ### Set table name part
  table_name_part <- "apcd_claim_provider"
  
  ### Check entries are in place for ETL function
  if (is.null(etl_delivery_date) | is.null(etl_note)) {
    stop("Enter a delivery date and note for the ETL batch ID function")
  }
  
  
  # Load ETL and QA functions if not already present
  if (exists("load_metadata_etl_log_f") == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/etl_log.R")
  }
  
  if (exists("qa_file_row_count_f") == F) {
    devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_file.R")
  }
  
  
  
  #### SET UP BATCH ID ####
  # Eventually switch this function over to using glue_sql to stop unwanted SQL behavior
  current_batch_id <- load_metadata_etl_log_f(conn = db_claims, 
                                              batch_type = "full", 
                                              data_source = "APCD", 
                                              date_min = etl_date_min,
                                              date_max = etl_date_max,
                                              delivery_date = etl_delivery_date, 
                                              note = etl_note)
  
  if (is.na(current_batch_id)) {
    stop("No etl_batch_id. Check metadata.etl_log table")
  }
  
  
  #### LOAD TABLES ####
  print("Loading tables to SQL")
  load_table_from_file_f(conn = db_claims,
                         config_url = paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.",
                                           table_name_part, "_full.yaml"),
                         overall = F, ind_yr = T, combine_yr = T, test_mode = F)
  
  
  #### ADD BATCH ID COLUMN ####
  print("Adding batch ID to SQL table")
  # Add column to the SQL table and set current batch to the default
  odbc::dbGetQuery(db_claims,
                   glue::glue_sql(
                     "ALTER TABLE load_raw.{`table_name_part`}
                   ADD etl_batch_id INTEGER 
                   DEFAULT {current_batch_id} WITH VALUES",
                     .con = db_claims))
  
  
  #### LOAD YAML CONFIG FILE FOR FINAL COMMANDS ####
  config_url <- paste0("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.", table_name_part, "_full.yaml")
  if (!is.null(config_url)) {
    table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  } else {
    table_config <- yaml::read_yaml(config_file)
  }
  
  
  #### DROP ROW_NUMBER COLUMN FROM FINAL TABLE ####
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "ALTER TABLE load_raw.{`table_name_part`}
    DROP COLUMN row_number",
    .con = db_claims))
  
  
  #### DROP TABLE CHUNKS ####
  if (length(table_config$years) > 1) {
    lapply(table_config$years, function(x) {
      table_name <- glue::glue(table_name_part, "_", x)
      odbc::dbGetQuery(db_claims, glue::glue_sql("DROP TABLE {`table_config$schema`}.{`table_name`}", .con = db_claims))
      })
    }
}