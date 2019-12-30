#### MASTER CODE TO UPDATE COMBINED MEDICAID/MEDICARE ANALYTIC TABLES
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-12


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

library(tidyverse) # Manipulate data
library(data.table) # Manipulate data
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
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/claim_ccw.R")


#### IDENTITY LINKAGE ####
# To come


#### CREATE ELIG ANALYTIC TABLES ####
#### MCAID_MCARE_ELIG_DEMO ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_elig_demo.R")

# Pull out run date of stage.mcaid_elig_demo
last_run_elig_demo <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_elig_demo")[[1]])

# QA stage version
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_elig_demo.R")
qa_mcaid_elig_demo_f(conn = db_claims, load_only = F)

# Load final table (assumes no changes to table structure)
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

rm(qa_rows_final_elig_demo, last_run_elig_demo)


#### MCAID_ELIG_TIMEVAR ####
# Load stage version
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
load_table_from_sql_f(conn = db_claims,
                      config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/final/tables/load_final.mcaid_elig_timevar.yaml",
                      truncate = T, truncate_date = F)

# QA final table
qa_rows_final_elig_timevar <- qa_sql_row_count_f(conn = db_claims,
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

rm(last_run_elig_timevar, qa_rows_final_elig_timevar)





#### STAGE ANALYTIC TABLES ####
# Need to follow this order when making tables because of dependencies


#### MCAID_CLAIM_LINE ####



#### MCAID_CLAIM_ICDCM_HEADER ####
### Create and load table, add index
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_icdcm_header.R",
                     echo = T)

### QA table
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_icdcm_header.R",
                     echo = T)

if (fail_tot > 0) {
  message("One or more QA checks on stage.mcaid_claim_icdcm_header failed. See metadata.qa_mcaid for details")
} else {
  message("All QA checks on stage.mcaid_claim_icdcm_header passed")
}

rm(fail_tot)



#### MCAID_CLAIM_PROCEDURE ####



#### MCAID_CLAIM_PHARM ####





#### MCAID_CLAIM_HEADER ####






### CCW
# Load table to SQL
load_ccw(conn = db_claims, source = "mcaid")

# QA table
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcaid_claim_ccw.R",
                     echo = T)

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
