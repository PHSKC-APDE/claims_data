#### MASTER CODE TO COPY, UNZIP, and COMBINE WA-APCD GZIP files to INTHEALTH_EDW
#
# Eli Kern, PHSKC-APDE
#
# Adapted code from Jeremy Whitehurst, PHSKC (APDE)
#
# 2024-03


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

pacman::p_load(tidyverse, odbc, configr, glue, keyring, svDialogs, R.utils) # Load list of packages


#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/alter_schema.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/copy_into.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/add_index.R")


#### STEP 1: CREATE CONNECTIONS ####

##Establish connection to inthealth_edw prod
interactive_auth <- TRUE #must be set to true if running from Azure VM
prod <- TRUE
server <- "hhsaw"
dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)


#### STEP 2: PRECURSOR TO LOOP THAT WILL EVENTUALLY HANDLE ALL TABLES ####

## Beginning message (before loop begins)
message(paste0("Beginning process to copy tables to inthealth_edw - ", Sys.time()))

#Establish list of Azuer Blob Storage folders for which GZIP files will be copied to inthealth_edw
folder_list <- list("claim_icdcm_raw", "claim_line_raw", "claim_procedure_raw", "claim_provider_raw", "dental_claim", "eligibility", "medical_claim_header",
                    "member_month_detail", "pharmacy_claim", "provider", "provider_master")

folder_list <- folder_list[[5]] #testing code

#Begin loop
lapply(folder_list, function(folder_list) {

  ##Load YAML config file (dynamic GitHub URL)
  table_name <- folder_list
  message("Loading YAML config file for: ", table_name, " - ", Sys.time())
  table_config <- yaml::yaml.load(
    httr::GET(glue(
      "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_stg_claims.apcd_",
      table_name,
      "_full.yaml")))
  
  ##Retrieve ETL batch ID or create if this is the first entry for ETL run
  message("Creating/reusing ETL batch ID for: ", table_name, " - ", Sys.time())
  existing_batch_id <- DBI::dbGetQuery(db_claims, 
                             glue::glue_sql("SELECT * FROM claims.metadata_etl_log
                                    WHERE data_source = 'APCD'
                                     AND delivery_date = '{`DBI::SQL(table_config$date_delivery)`}'",
                                            .con = db_claims))$etl_batch_id
  
  if(!identical(existing_batch_id, integer(0))){
    current_batch_id <- existing_batch_id
  } else {
    current_batch_id <- load_metadata_etl_log_file_f(conn = db_claims, 
                                                     batch_type = "full", 
                                                     data_source = "APCD", 
                                                     date_min = table_config$date_min,
                                                     date_max = table_config$date_max,
                                                     delivery_date = table_config$date_delivery, 
                                                     note = table_config$note_delivery,
                                                     row_cnt = table_config$row_count,
                                                     file_name = table_config[[server]][["to_table"]],
                                                     file_loc = table_config[[server]][["dl_path"]],
                                                     server = server)
  }
  
  ##Load data
  #Note that tables in EDW automatically have an index created
  #This function will automatically combine all GZIP files in a given folder into a single SQL table
  message("Loading data for: ", table_config[[server]][["to_table"]], " - ", Sys.time())
  to_schema <- table_config[[server]][["to_schema"]]
  to_table <- table_config[[server]][["to_table"]]
  dl_path <- table_config[[server]][["dl_path"]]
  
  system.time(copy_into_f(conn = dw_inthealth, 
              server = server,
              config = table_config,
              dl_path = dl_path,
              file_type = "csv",
              compression = "gzip",
              field_terminator = ",",
              row_terminator = "0x0A",
              overwrite = TRUE,
              rodbc = FALSE,
              batch_id_assign = FALSE,
              batch_id = current_batch_id))
  
  ##QA row and column counts
  message("Running row count comparison QA for: ", table_config[[server]][["to_table"]], " - ", Sys.time())
  qa_rows_sql <- qa_load_row_count_f(conn = dw_inthealth,
                                     server = server,
                                     config = table_config,
                                     row_count = table_config$row_count,
                                     overall = T,
                                     ind_yr = F)
  
  # Report individual results out to SQL table
  qa_schema <- table_config[[server]][["qa_schema"]]
  qa_table <- table_config[[server]][["qa_table"]]
  
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                          '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                          'Number rows loaded to SQL vs. expected value(s)', 
                                          {qa_rows_sql$outcome[1]},
                                          {format(Sys.time(), usetz = FALSE)},
                                          {qa_rows_sql$note[1]})",
                                .con = db_claims))
  
  if (qa_rows_sql$outcome[1] == "FAIL") {
    stop(glue::glue("Mismatching row count between source file and SQL table. 
                    Check {qa_schema}.{qa_table} for details (etl_batch_id = {current_batch_id}"))
  }
  
  ## Add batch ID column
  # message(paste0("Adding batch ID column to: ", table_config[[server]][["to_table"]]), " - ", Sys.time())
  # DBI::dbExecute(dw_inthealth,
  #                glue::glue_sql("ALTER TABLE {`to_schema`}.{`to_table`} 
  #                   ADD etl_batch_id INTEGER DEFAULT {current_batch_id}",
  #                               .con = dw_inthealth))
  # DBI::dbExecute(dw_inthealth,
  #                glue::glue_sql("UPDATE {`to_schema`}.{`to_table`} 
  #                   SET etl_batch_id = {current_batch_id}",
  #                               .con = dw_inthealth))
  
  ## Add date_load_raw to metadata_etl_log table
  message(paste0("Adding date_load_raw to metadata_etl_log for: ", table_config[[server]][["to_table"]]), " - ", Sys.time())
  DBI::dbExecute(db_claims,
                 glue::glue_sql("UPDATE claims.metadata_etl_log 
                                 SET date_load_raw = GETDATE() 
                                 WHERE etl_batch_id = {current_batch_id}",
                                .con = db_claims))
})


## Closing message (after loop completes)
message(paste0("All tables have been successfully copied to inthealth_edw - ", Sys.time()))