#### MASTER CODE TO RUN A MONTHYL MEDICAID DATA UPDATE
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-08


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

library(tidyverse) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code


db_claims <- dbConnect(odbc(), "PHClaims51")

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_sql.R")



#### LOAD_RAW ELIGIBILITY ####
### Bring in yaml file
load_mcaid_elig_config <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_partial.yaml"))
load_elig_date_min <- as.Date(paste0(str_sub(load_mcaid_elig_config$overall$date_min, 1, 4), "-",
                                     str_sub(load_mcaid_elig_config$overall$date_min, 5, 6), "-",
                                     "01"), format = "%Y-%m-%d")
load_elig_date_max <- as.Date(paste0(str_sub(load_mcaid_elig_config$overall$date_max, 1, 4), "-",
                                     str_sub(load_mcaid_elig_config$overall$date_max, 5, 6), "-",
                                     "01"), format = "%Y-%m-%d") %m+% months(1) - days(1)

### Create tables
# Need to do this each time because of the etl_batch_id variable
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_partial.yaml",
               overall = T, ind_yr = F, overwrite = T)


### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_partial.R")

load_load_raw.mcaid_elig_partial_f(etl_date_min = load_elig_date_min, 
                                etl_date_max = load_elig_date_max,
                                etl_delivery_date = load_mcaid_elig_config$overall$date_delivery, 
                                etl_note = "Partial refresh of Medicaid elig data")

### Clean up
rm(load_elig_date_min, load_elig_date_max, load_mcaid_elig_config)


#### LOAD_RAW CLAIMS ####
### Bring in yaml file
load_mcaid_claim_config <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_partial.yaml"))

### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_partial.yaml",
               overall = T, ind_yr = F, overwrite = T)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_partial.R")

load_load_raw.mcaid_claim_partial_f(etl_date_min = load_mcaid_claim_config$overall$date_min, 
                                    etl_date_max = load_mcaid_claim_config$overall$date_max,
                                    etl_delivery_date = load_mcaid_claim_config$overall$date_delivery, 
                                    etl_note = "Partial refresh of Medicaid claims data",
                                    qa_file_row = F)



#### STAGE ELIG ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_partial.R")


#### STAGE CLAIM ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_partial.R")



#### ADDRESS CLEANING ####
### stage.address_clean
# Run step 1, which identifies new addresses and sets them up to be run through Informatica
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.address_clean_partial_step1.R")

# Run step 2, which processes addresses that were through Informatica and loads to SQL
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.address_clean_partial_step2.R")

# QA stage.address_clean
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.address_clean_partial.R")


### ref.address_clean
load_table_from_sql_f(conn = db_claims, 
                      config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_clean.yaml",
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
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_geocode.yaml",
truncate = T, truncate_date = F)

qa_rows_final <- qa_sql_row_count_f(conn = db_claims,
                                    config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_geocode.yaml",
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
