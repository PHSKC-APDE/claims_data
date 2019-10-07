#### MASTER CODE TO RUN A FULL APCD DATA REFRESH
#
# Eli Kern, PHSKC (APDE)
# Adapted from Alastair Matheson's Medicaid script
#
# 2019-10


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, RCurl, configr, glue)

db_claims <- dbConnect(odbc(), "PHClaims51")

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
#devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/eli/claims_db/db_loader/scripts_general/load_table.R") #use eli branch for now
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/claim_ccw.R")



#### LOAD_RAW DENTAL CLAIMS ####
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_dental_claim_full.yaml",
               overall = T,
               ind_yr = T,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_dental_claim_full.R")

system.time(load_load_raw.apcd_dental_claim_full_f(etl_date_min = "2014-01-01",
                                       etl_date_max = "2019-03-31",
                                       etl_delivery_date = "2019-10-01", 
                                       etl_note = "Full refresh of APCD data using extract 187"))


#### LOAD_RAW ELIGIBILITY ####
#Run time: X min
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_eligibility_full.yaml",
               overall = T,
               ind_yr = T,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_eligibility_full.R")

system.time(load_load_raw.apcd_eligibility_full_f(etl_date_min = "2014-01-01",
                                      etl_date_max = "2019-03-31",
                                      etl_delivery_date = "2019-10-01", 
                                      etl_note = "Full refresh of APCD data using extract 187"))


#### LOAD_RAW PROVIDER ####
#Run time: 10 min
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_provider_full.yaml",
               overall = T,
               ind_yr = T,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_provider_full.R")

system.time(load_load_raw.apcd_provider_full_f(etl_date_min = "2014-01-01",
                                      etl_date_max = "2019-03-31",
                                      etl_delivery_date = "2019-10-01", 
                                      etl_note = "Full refresh of APCD data using extract 187"))


#### LOAD_RAW PROVIDER PRACTICE ROSTER ####
#Run time: 1 min
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_provider_practice_roster_full.yaml",
               overall = T,
               ind_yr = F,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_provider_practice_roster_full.R")

system.time(load_load_raw.apcd_provider_practice_roster_full_f(etl_date_min = "2014-01-01",
                                                   etl_date_max = "2019-03-31",
                                                   etl_delivery_date = "2019-10-01", 
                                                   etl_note = "Full refresh of APCD data using extract 187"))
