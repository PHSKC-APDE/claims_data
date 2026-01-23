#### MASTER CODE TO COPY, UNZIP, and COMBINE WA-APCD GZIP files to INTHEALTH_EDW
#
# Eli Kern, PHSKC-APDE
#
# Adapted code from Jeremy Whitehurst, PHSKC (APDE)
#
# 2024-03

#10-16-24 Added Keyring for INTHEALTH
#10-16-24 commented out step 3 which is checking that the tables are mirrored on HHSAW. This should occur as part of script 8
#1-23-26 Eli updated the  [claims].[metadata_etl_log] column [row_count] to BIGINT;


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
#Enter credentials for HHSAW
#key_set("hhsaw", username = "shernandez@kingcounty.gov") #Only run this each time password is changed
#key_set("inthealth_edw_prod", username = "shernandez@kingcounty.gov") #Only run this each time password is changed
keyring::key_list() #Run this to list all the stored usernames


interactive_auth <- FALSE
prod <- TRUE
server <- "hhsaw"
dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)


#### STEP 2: LOAD DATA FOR ALL TABLES ####

## Beginning message (before loop begins)
message(paste0("Beginning process to copy tables to inthealth_edw - ", Sys.time()))

#Establish list of Azure Blob Storage folders for which GZIP files will be copied to inthealth_edw
folder_list <- list("claim_icdcm_raw", "claim_line_raw", "claim_procedure_raw", "claim_provider_raw", "dental_claim", "eligibility", "medical_claim_header",
                    "member_month_detail", "pharmacy_claim", "provider", "provider_master")

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
  
  ##Create ETL batch ID (each table will have its own ETL batch ID)
  message("Creating ETL batch ID for: ", table_name, " - ", Sys.time())
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
                                                   server = server,
                                                   auto_proceed = TRUE)
  
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
              batch_id_assign = TRUE,
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
  
  ## Add date_load_raw to metadata_etl_log table
  message(paste0("Adding date_load_raw to metadata_etl_log for: ", table_config[[server]][["to_table"]]), " - ", Sys.time())
  DBI::dbExecute(db_claims,
                 glue::glue_sql("UPDATE claims.metadata_etl_log 
                                 SET date_load_raw = GETDATE() 
                                 WHERE etl_batch_id = {current_batch_id}
                                 AND file_name = {to_table}",
                                .con = db_claims))
})



#### STEP 3: CONFIRM EXTERNAL TABLES ON HHSAW ARE WORKING ####

##Query external tables and return row counts
#external_table_row_counts <- lapply(folder_list, function(folder_list) {
  
 # table_selected <- folder_list
  #message(paste0("Querying row count for HHSAW external table: ", table_selected))
  #sql_query <- dbGetQuery(conn = db_claims, glue_sql("SELECT count(*) as row_count FROM [claims].[stage_apcd_{DBI::SQL(`table_selected`)}];",
      #                                               .con = db_claims))
  #da_inner <- data.frame(table_name = table_selected, row_count = sql_query$row_count)
  #return(da_inner)
#}) %>%
 # bind_rows()

## QA message
#if (table(external_table_row_counts$row_count>0)["TRUE"][[1]] != length(folder_list)) {
 # stop(glue::glue("Not all external tables have non-zero row counts. Inspect manually"))
#} else {
 # message("All external tables are working properly.")
#}


## Closing message
#message(paste0("All tables have been successfully copied to inthealth_edw - ", Sys.time()))