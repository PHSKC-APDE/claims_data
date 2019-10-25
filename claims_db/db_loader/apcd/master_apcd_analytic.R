#### MASTER CODE TO CREATE ANALYTIC TABLES FOR APCD DATA
#
# Loads and QAs data on stage schema
# Changes schema of existing final tables to archive
# Changes schema of new stage tables to final
# Adds clustered columnstore indexes to new final tables
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
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/alter_schema.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/claim_ccw.R")

## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### STEP 1: Load and QA data on stage schema ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

#### apcd_elig_demo ####
### Create table
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_elig_demo.yaml",
               overall = T,
               ind_yr = F,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
#Have to replace below script with final script (which will include load and qa functions)
#devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_dental_claim_full.R")
system.time(load_stage.apcd_elig_demo_f())

### Testing space for QA code (eventually append as qa_stage.apcd_elig_demo_f() and add to above sourced script)
res1 <- dbGetQuery(conn = db_claims, glue_sql(
  "select 'stage.apcd_elig_demo' as 'table', 'distinct count' as qa_type, count(distinct id_apcd) as qa from stage.apcd_elig_demo",
  .con = db_claims))
res2 <- dbGetQuery(conn = db_claims, glue_sql(
  "select 'stage.apcd_member_month_detail' as 'table', 'distinct count' as qa_type, count(distinct internal_member_id) as qa from stage.apcd_member_month_detail",
  .con = db_claims))
res3 <- dbGetQuery(conn = db_claims, glue_sql(
  "select 'stage.apcd_elig_demo' as 'table', 'count' as qa_type, count(id_apcd) as qa from stage.apcd_elig_demo",
  .con = db_claims))
apcd_demo_qa1 <- bind_rows(res1, res2, res3)
rm(list = ls(pattern = "^res"))

#Reminder - run line-level QA script at \\dchs-shares01\dchsdata\dchsphclaimsdata\qa_line_level\qa_stage.apcd_elig_demo.sql             



