#### MASTER CODE TO CREATE MULTI-YEAR TABLES FOR MCARE DATA ON STAGE SCHEMA
#
# Loads and QAs data on stage schema with load suffix in table name
# Changes schema of existing stage tables to archive
# Removes "load" suffix from new stage tables
# Adds clustered columnstore indexes to new stage tables
#
# Eli Kern, PHSKC (APDE)
# Adapted from Eli Kern's APCD analytic script
#
# 2019-12


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
#### Table 1: mcare_bcarrier_claims ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_bcarrier_claims.R")

### B) Create table
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_bcarrier_claims.yaml",
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_bcarrier_claims_f())

### D) Table-level QA
system.time(mcare_bcarrier_claims_qa1 <- qa_stage.mcare_bcarrier_claims_qa1_f())
rm(mcare_bcarrier_claims_qa1)

### E) Archive current table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "mcare_bcarrier_claims")

### F) Remove "load" suffix from new stage table
#alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_demo")

### G) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_bcarrier_claims on stage.mcare_bcarrier_claims")))
