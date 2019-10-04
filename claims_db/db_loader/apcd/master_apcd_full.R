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
               overall = F, ind_yr = T, overwrite = T)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_dental_claim_full.R")

load_load_raw.apcd_dental_claim_full_f(etl_date_min = "2014-01-01", etl_date_max = "2019-03-31",
                                etl_delivery_date = "2019-10-01", 
                                etl_note = "Full refresh of APCD data using extract 187")


#### LOAD_RAW ELIGIBILITY ####
#Run time: X min
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_eligibility_full.yaml",
               overall = F, ind_yr = T, overwrite = T)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_eligibility_full.R")

load_load_raw.apcd_eligibility_full_f(etl_date_min = "2014-01-01", etl_date_max = "2019-03-31",
                                                   etl_delivery_date = "2019-10-01", 
                                                   etl_note = "Full refresh of APCD data using extract 187")


#### LOAD_RAW PROVIDER PRACTICE ROSTER ####
#Run time: 1 min
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_provider_practice_roster_full.yaml",
               overall = T, ind_yr = F, overwrite = T)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_provider_practice_roster_full.R")

load_load_raw.apcd_provider_practice_roster_full_f(etl_date_min = "2014-01-01", etl_date_max = "2019-03-31",
                                       etl_delivery_date = "2019-10-01", 
                                       etl_note = "Full refresh of APCD data using extract 187")



STOP


#### LOAD_RAW CLAIMS ####
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/create_load_raw.mcaid_claim.yaml",
               overall = T, ind_yr = T, overwrite = T)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_full.R")

load_load_raw.mcaid_claim_full_f(etl_date_min = "2012-01-01", etl_date_max = "2018-12-31",
                                 etl_delivery_date = "2019-06-12", 
                                 etl_note = "Updated claims data to correct missing secondary RAC claims",
                                 qa_file_row = F)






#### STAGE ELIG ####
### Create table
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.mcaid_elig.yaml",
               overall = T, ind_yr = F)

### Load table
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_full.R")



#### STAGE CLAIM ####
### Create table
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.mcaid_claim.yaml",
               overall = T, ind_yr = F)

### Load table
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_full.R")




#### CREATE ELIG ANALYTIC TABLES ####
#### MCAID_ELIG_DEMO ####
# Create and load stage version
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.mcaid_elig_demo.yaml",
               overall = T, ind_yr = F, overwrite = T)

devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_demo.R")

# Pull out run date of stage.mcaid_elig_demo
last_run_elig_demo <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_elig_demo")[[1]])

# QA stage version
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_elig_demo.R")
qa_mcaid_elig_demo_f(conn = db_claims, load_only = T)

# Create and load final table
create_table_f(
  conn = db_claims, 
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/create_final.mcaid_elig_demo.yaml",
  overall = T, ind_yr = F, overwrite = T)

load_table_from_sql_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_demo.yaml", 
  truncate = T, truncate_date = F)

# QA final table
qa_rows_final_elig_demo <- qa_sql_row_count_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_demo.yaml",
  overall = T, ind_yr = F)

odbc::dbGetQuery(
  conn = db_claims,
  glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_elig_demo}, 
                 'final.mcaid_elig_demo',
                 'Number final rows compared to stage', 
                 {qa_rows_final_elig_demo$qa_result}, 
                 {Sys.time()}, 
                 {qa_rows_final_elig_demo$note})",
                 .con = db_claims))


#### ADDRESS CLEANING ####
### Create stage table
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.address_clean.yaml",
               overall = T, ind_yr = F, overwrite = T)

### Call in and run load function
# Note: using partial load because ref.address_clean already exists
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.address_clean_partial.R")
load_stage.address_clean_partial_f(informatica = F)

#### QA (not sure what yet)


### Move existing ref to archive?


### Create and load to final
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/create_ref.address_clean.yaml",
               overall = T, ind_yr = F)


load_table_from_sql_f(conn = db_claims, 
                      config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_clean.yaml",
                      truncate = T, truncate_date = F)



#### MCAID_ELIG_TIMEVAR ####
# Create and load stage version
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.mcaid_elig_timevar.yaml",
               overall = T, ind_yr = F, overwrite = T)

time_start <- Sys.time()
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_elig_timevar.R")
time_end <- Sys.time()
print(paste0("stage.mcaid_elig_timevar took ", round(difftime(time_end, time_start, units = "mins"), 2), " mins to make"))


# Pull out run date of stage.mcaid_elig_demo
last_run_elig_timevar <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_elig_timevar")[[1]])

# QA stage version
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_elig_timevar.R")
qa_mcaid_elig_timevar_f(conn = db_claims, load_only = T)


# Create and load final table
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_timevar.yaml",
               overall = T, ind_yr = F, overwrite = T)

load_table_from_sql_f(conn = db_claims,
                      config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_timevar.yaml",
                      truncate = T, truncate_date = F)

# QA final table
qa_rows_final_elig_timevar <- qa_sql_row_count_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_timevar.yaml",
  overall = T, ind_yr = F)

odbc::dbGetQuery(
  conn = db_claims,
  glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_elig_timevar}, 
                 'final.mcaid_elig_timevar',
                 'Number final rows compared to stage', 
                 {qa_rows_final_elig_timevar$qa_result}, 
                 {Sys.time()}, 
                 {qa_rows_final_elig_timevar$note})",
                 .con = db_claims))



#### GEOCODING SETUP ####
library(sf) # Read shape files

devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.address_geocode.R")

### Create stage.address_geocode
create_table_f(db_claims,
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.address_geocode.yaml",
               overall = T, ind_yr = F)

### Run function to process geocodes
# NB. This script is still a WIP and needs to be fleshed out to 
# accommodate partial refreshes.
row_load_ref_geo <- stage_address_geocode_f(full_refresh = F)


### QA stage.address_geocode
# How many ZIP centroids?
# What else?



### Load to ref.address_geocode
# Pull out run date of stage.address_geocode
last_run <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.address_geocode")[[1]])

create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/create_ref.address_geocode.yaml",
               overall = T, ind_yr = F, overwrite = T)

load_table_from_sql_f(conn = db_claims,
                      config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_geocode.yaml",
                      truncate = T, truncate_date = F)

qa_rows_final <- qa_sql_row_count_f(conn = db_claims,
                                    config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_geocode.yaml",
                                    overall = T, ind_yr = F)

odbc::dbGetQuery(
  conn = db_claims,
  glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run}, 
                 'ref.address_geocode',
                 'Number final rows compared to stage', 
                 {qa_rows_final$qa_result}, 
                 {Sys.time()}, 
                 {qa_rows_final$note})",
                 .con = db_claims))






#### STAGE ANALYTIC TABLES ####
### CCW
# Load table to SQL
load_ccw(conn = db_claims, source = "mcaid")

# QA table
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_claim_ccw.R")

# If QA passes, load to final table
if (ccw_qa_result == "PASS") {
  
  create_table_f(
    conn = db_claims, 
    config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_claim_ccw.yaml",
    overall = T, ind_yr = F, overwrite = T)
  
  load_table_from_sql_f(
    conn = db_claims,
    config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_claim_ccw.yaml", 
    truncate = T, truncate_date = F)
  
  # QA final table
  qa_rows_final_claim_ccw <- qa_sql_row_count_f(
    conn = db_claims,
    config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_claim_ccw.yaml",
    overall = T, ind_yr = F)
  
  odbc::dbGetQuery(
    conn = db_claims,
    glue::glue_sql("INSERT INTO metadata.qa_mcaid
                 (last_run, table_name, qa_item, qa_result, qa_date, note) 
                 VALUES ({last_run_claim_ccw}, 
                 'final.mcaid_claim_ccw',
                 'Number final rows compared to stage', 
                 {qa_rows_final_claim_ccw$qa_result}, 
                 {Sys.time()}, 
                 {qa_rows_final_claim_ccw$note})",
                   .con = db_claims))
} else {
  warning("CCW table failed QA and was not loaded to final schema")
}



#### DROP TABLES NO LONGER NEEDED ####
# Elig year tables
# Claims year tables
# Truncate stage analytic tables
