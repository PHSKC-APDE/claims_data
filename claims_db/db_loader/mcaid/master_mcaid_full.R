#### MASTER CODE TO RUN A FULL MEDICAID DATA REFRESH
#
# Alastair Matheson, PHSKC (APDE)
#
# 2020-07


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

library(tidyverse) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code



#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/add_index.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/alter_schema.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/copy_into.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_sql.R")


#### CHOOSE SERVER AND CREATE CONNECTION ####
server <- select.list(choices = c("phclaims", "hhsaw"))
interactive_auth <- select.list(choices = c("TRUE", "FALSE"))
if (server == "hhsaw") {
  prod <- select.list(choices = c("TRUE", "FALSE"))
} else {
  prod <- F
}


db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)

if (server == "hhsaw") {
  dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
}


#### LOAD_RAW ELIGIBILITY ####
### Create tables
# Get data from data lake into data warehouse
copy_into_f(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_full.yaml",
            file_type = "csv", compression = "gzip",
            identity = "Storage Account Key", secret = key_get("inthealth_edw"),
            overwrite = T)

# Create external table in database that points to data warehouse
create_table_f(conn = db_claims,
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_full.yaml",
               overwrite = T, external = T)


#### LOAD_RAW CLAIMS ####
### Create tables
# Get data from data lake into data warehouse
copy_into_f(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_full.yaml",
            file_type = "csv", compression = "gzip",
            identity = "Storage Account Key", secret = key_get("inthealth_edw"),
            overwrite = T)

# Create external table in database that points to data warehouse
create_table_f(conn = db_claims,
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_full.yaml",
               overwrite = T, external = T)


#### STAGE ELIG ####
### Load table
# Call in config file to get vars (and check for errors)
table_config_stage_elig <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_elig.yaml"))
if (table_config_stage_elig[[1]] == "Not Found") {stop("Error in config file. Check URL")}
# Load and run function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_elig.R")
if (server == "hhsaw") {
  system.time(load_stage.mcaid_elig_f(conn_dw = dw_inthealth, 
                                      conn_db = db_claims, 
                                      server = server,
                                      full_refresh = T, 
                                      config = table_config_stage_elig))
} else if (server == "phclaims") {
  system.time(load_stage.mcaid_elig_f(conn_dw = db_claims, 
                                      conn_db = db_claims, 
                                      server = server,
                                      full_refresh = T, 
                                      config = table_config_stage_elig))
}


#### STAGE CLAIM ####
# Call in config file to get vars (and check for errors)
table_config_stage_claims <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_claim.yaml"))
if (table_config_stage_claims[[1]] == "Not Found") {stop("Error in config file. Check URL")}
# Load and run function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_claim.R")
if (server == "hhsaw") {
  system.time(load_claims.stage_mcaid_claim_f(conn_dw = dw_inthealth, 
                                              conn_db = db_claims, 
                                              server = server,
                                              full_refresh = T, 
                                              config = table_config_stage_claims))
} else if (server == "phclaims") {
  system.time(load_claims.stage_mcaid_claim_f(conn_dw = db_claims, 
                                              conn_db = db_claims, 
                                              server = server,
                                              full_refresh = T, 
                                              config = table_config_stage_claims))
}



#### ADDRESS CLEANING ####
### stage.address_clean
# Run step 1, which identifies new addresses and sets them up to be run through Informatica
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.address_clean_partial_step1.R")

# Run step 2, which processes addresses that were through Informatica and loads to SQL
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.address_clean_partial_step2.R")

# QA stage.address_clean
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/qa_stage.address_clean_partial.R")


### ref.address_clean
load_table_from_sql_f(conn = db_claims, 
                      config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.address_clean.yaml",
                      truncate = T, truncate_date = F)

# Check appropriate # rows loaded
rows_ref <- as.integer(dbGetQuery(db_claims, "SELECT COUNT (*) AS row_cnt FROM ref.address_clean"))
rows_ref_new <- as.integer(dbGetQuery(db_claims, "SELECT COUNT (*) AS row_cnt FROM stage.address_clean"))

if (rows_ref != rows_ref_new) {
  stop("Unexpected number of rows loaded to ref.address_clean")
}


### stage.address_geocode
# Currently need to run through manually until all geocoding can be done via R
# use load_stage.address_geocode_partial.R

### ref.address_geocode
# Also should only be triggered manually until automatic geocoding and QA are built in to stage above
last_run_geocode <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.address_geocode")[[1]])

load_table_from_sql_f(conn = db_claims,
                      config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.address_geocode.yaml",
                      truncate = T, truncate_date = F)

qa_rows_final <- qa_sql_row_count_f(conn = db_claims,
                                    config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.address_geocode.yaml",
                                    overall = T, ind_yr = F)

DBI::dbExecute(
  conn = db_claims,
  glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_geocode}, 
                 'ref.address_geocode',
                 'Number final rows compared to stage', 
                 {qa_rows_final$qa_result}, 
                 {Sys.time()}, 
                 {qa_rows_final$note})",
                 .con = db_claims))

rm(last_run_geocode, qa_rows_final)



#### DROP TABLES NO LONGER NEEDED ####
# Elig year tables
# Claims year tables
# Truncate stage analytic tables
