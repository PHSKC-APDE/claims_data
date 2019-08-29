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
load_mcaid_elig_config <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_monthly.yaml"))
load_elig_date_min <- as.Date(paste0(str_sub(load_mcaid_elig_config$overall$date_min, 1, 4), "-",
                                     str_sub(load_mcaid_elig_config$overall$date_min, 5, 6), "-",
                                     "01"), format = "%Y-%m-%d")
load_elig_date_max <- as.Date(paste0(str_sub(load_mcaid_elig_config$overall$date_max, 1, 4), "-",
                                     str_sub(load_mcaid_elig_config$overall$date_max, 5, 6), "-",
                                     "01"), format = "%Y-%m-%d")

### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_monthly.yaml",
               overall = T, ind_yr = F, overwrite = T)


### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_monthly.R")

load_load_raw.mcaid_elig_monthly_f(etl_date_min = load_elig_date_min, 
                                etl_date_max = load_elig_date_max,
                                etl_delivery_date = load_mcaid_elig_config$overall$date_delivery, 
                                etl_note = "Monthly refresh of Medicaid elig data")

### Clean up
rm(load_elig_date_min, load_elig_date_max, load_mcaid_elig_config)


#### LOAD_RAW CLAIMS ####
### Bring in yaml file
load_mcaid_claim_config <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_monthly.yaml"))

### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_monthly.yaml",
               overall = T, ind_yr = F, overwrite = T)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_monthly.R")

load_load_raw.mcaid_claim_monthly_f(etl_date_min = load_mcaid_claim_config$overall$date_min, 
                                    etl_date_max = load_mcaid_claim_config$overall$date_max,
                                    etl_delivery_date = load_mcaid_claim_config$overall$date_delivery, 
                                    etl_note = "Monthly refresh of Medicaid claims data",
                                    qa_file_row = F)



#### STAGE ELIG ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_monthly.R")


#### STAGE CLAIM ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_monthly.R")

