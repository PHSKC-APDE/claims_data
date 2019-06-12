#### MASTER CODE TO RUN A FULL MEDICAID DATA REFRESH
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05


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
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_sql.R")



#### BATCH ID ####



#### LOAD_RAW ####
### Create 


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
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/create_final.mcaid_elig_timevar.yaml",
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




#### DROP TABLES NO LONGER NEEDED ####
# Elig year tables
# Claims year tables
# Truncate stage analytic tables
