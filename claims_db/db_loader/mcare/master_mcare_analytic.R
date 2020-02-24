#### MASTER CODE TO CREATE ANALYTIC TABLES FOR MCARE DATA ON STAGE SCHEMA
#
# Loads and QAs data on stage schema
# Changes schema of existing final tables to archive
# Changes schema of new stage tables to final
# Adds clustered columnstore indexes to new final tables
#
# Eli Kern, PHSKC (APDE)
# Adapted from Eli Kern's APCD analytic script
#
# 2020-01

#Note: Currently only includes code for claims analytic tables, elig tables are run from separate script


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
#### Table 1: mcare_claim_line ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_claim_line.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_claim_line.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_claim_line_f())

### D) Table-level QA (12 min)
system.time(mcare_claim_line_qa <- qa_stage.mcare_claim_line_qa_f())
rm(config_url)

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcare_claim_line")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcare_claim_line")



## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 2: mcare_claim_icdcm_header ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_claim_icdcm_header.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_claim_icdcm_header.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_claim_icdcm_header_f())

### D) Table-level QA (12 min)
system.time(mcare_claim_icdcm_header_qa <- qa_stage.mcare_claim_icdcm_header_qa_f())
rm(config_url)

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcare_claim_icdcm_header")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcare_claim_icdcm_header")


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 3: mcare_claim_procedure ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_claim_procedure.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_claim_procedure.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_claim_procedure_f())

### D) Table-level QA (14 min)
system.time(mcare_claim_procedure_qa <- qa_stage.mcare_claim_procedure_qa_f())
rm(config_url)

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcare_claim_procedure")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcare_claim_procedure")



## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 4: mcare_claim_provider ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_claim_provider.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_claim_provider.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_claim_provider_f())

### D) Table-level QA (15 min)
system.time(mcare_claim_provider_qa <- qa_stage.mcare_claim_provider_qa_f())
rm(config_url)

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcare_claim_provider")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcare_claim_provider")


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### INDEX ALL TABLES PRIOR TO CREATING CLAIM_HEADER TABLE ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_claim_line on final.mcare_claim_line")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_claim_icdcm_header on final.mcare_claim_icdcm_header")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_claim_procedure on final.mcare_claim_procedure")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_claim_provider on final.mcare_claim_provider")))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 4: mcare_claim_header ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_claim_header.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_claim_header.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_claim_header_f())

### D) Table-level QA (x min)
system.time(mcare_claim_header_qa <- qa_stage.mcare_claim_header_qa_f())
rm(config_url)

### E) Run line-level QA script

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcare_claim_header")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcare_claim_header")


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### INDEX CLAIM_HEADER TABLE ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_claim_header on final.mcare_claim_header")))
