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
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_bcarrier_claims.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_bcarrier_claims_f())

### D) Table-level QA
system.time(mcare_bcarrier_claims_qa <- qa_stage.mcare_bcarrier_claims_qa_f())
rm(config_url)
#rm(mcare_bcarrier_claims_qa)

### E) Archive current table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "mcare_bcarrier_claims")

### F) Remove "load" suffix from new stage table
dbSendQuery(conn = db_claims, glue_sql("exec sp_rename 'stage.mcare_bcarrier_claims_load', 'mcare_bcarrier_claims';"))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 2: mcare_bcarrier_line ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_bcarrier_line.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_bcarrier_line.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_bcarrier_line_f())

### D) Table-level QA
system.time(mcare_bcarrier_line_qa <- qa_stage.mcare_bcarrier_line_qa_f())
rm(config_url)
#rm(mcare_bcarrier_line_qa)

### E) Archive current table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "mcare_bcarrier_line")

### F) Remove "load" suffix from new stage table
dbSendQuery(conn = db_claims, glue_sql("exec sp_rename 'stage.mcare_bcarrier_line_load', 'mcare_bcarrier_line';"))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 3: mcare_dme_claims ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_dme_claims.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_dme_claims.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_dme_claims_f())

### D) Table-level QA
system.time(mcare_dme_claims_qa <- qa_stage.mcare_dme_claims_qa_f())
rm(config_url)
#rm(mcare_dme_claims_qa)

### E) Archive current table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "mcare_dme_claims")

### F) Remove "load" suffix from new stage table
dbSendQuery(conn = db_claims, glue_sql("exec sp_rename 'stage.mcare_dme_claims_load', 'mcare_dme_claims';"))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 4: mcare_dme_line ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_dme_line.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_dme_line.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_dme_line_f())

### D) Table-level QA
system.time(mcare_dme_line_qa <- qa_stage.mcare_dme_line_qa_f())
rm(config_url)
#rm(mcare_dme_line_qa)

### E) Archive current table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "mcare_dme_line")

### F) Remove "load" suffix from new stage table
dbSendQuery(conn = db_claims, glue_sql("exec sp_rename 'stage.mcare_dme_line_load', 'mcare_dme_line';"))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 5: mcare_hha_base_claims ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_hha_base_claims.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_hha_base_claims.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_hha_base_claims_f())

### D) Table-level QA
system.time(mcare_hha_base_claims_qa <- qa_stage.mcare_hha_base_claims_qa_f())
rm(config_url)
#rm(mcare_hha_base_claims_qa)

### E) Archive current table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "mcare_hha_base_claims")

### F) Remove "load" suffix from new stage table
dbSendQuery(conn = db_claims, glue_sql("exec sp_rename 'stage.mcare_hha_base_claims_load', 'mcare_hha_base_claims';"))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 6: mcare_hha_revenue_center ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_hha_revenue_center.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_hha_revenue_center.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_hha_revenue_center_f())

### D) Table-level QA
system.time(mcare_hha_revenue_center_qa <- qa_stage.mcare_hha_revenue_center_qa_f())
rm(config_url)
#rm(mcare_hha_revenue_center_qa)

### E) Archive current table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "mcare_hha_revenue_center")

### F) Remove "load" suffix from new stage table
dbSendQuery(conn = db_claims, glue_sql("exec sp_rename 'stage.mcare_hha_revenue_center_load', 'mcare_hha_revenue_center';"))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 7: mcare_hospice_base_claims ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_hospice_base_claims.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_hospice_base_claims.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_hospice_base_claims_f())

### D) Table-level QA
system.time(mcare_hospice_base_claims_qa <- qa_stage.mcare_hospice_base_claims_qa_f())
rm(config_url)
#rm(mcare_hospice_base_claims_qa)

### E) Archive current table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "mcare_hospice_base_claims")

### F) Remove "load" suffix from new stage table
dbSendQuery(conn = db_claims, glue_sql("exec sp_rename 'stage.mcare_hospice_base_claims_load', 'mcare_hospice_base_claims';"))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 8: mcare_hospice_revenue_center ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_hospice_revenue_center.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_hospice_revenue_center.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_hospice_revenue_center_f())

### D) Table-level QA
system.time(mcare_hospice_revenue_center_qa <- qa_stage.mcare_hospice_revenue_center_qa_f())
rm(config_url)
#rm(mcare_hospice_revenue_center_qa)

### E) Archive current table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "mcare_hospice_revenue_center")

### F) Remove "load" suffix from new stage table
dbSendQuery(conn = db_claims, glue_sql("exec sp_rename 'stage.mcare_hospice_revenue_center_load', 'mcare_hospice_revenue_center';"))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 9: mcare_inpatient_base_claims ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_inpatient_base_claims.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_inpatient_base_claims.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_inpatient_base_claims_f())

### D) Table-level QA
system.time(mcare_inpatient_base_claims_qa <- qa_stage.mcare_inpatient_base_claims_qa_f())
rm(config_url)
#rm(mcare_inpatient_base_claims_qa)

### E) Archive current table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "mcare_inpatient_base_claims")

### F) Remove "load" suffix from new stage table
dbSendQuery(conn = db_claims, glue_sql("exec sp_rename 'stage.mcare_inpatient_base_claims_load', 'mcare_inpatient_base_claims';"))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 10: mcare_inpatient_revenue_center ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_inpatient_revenue_center.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_inpatient_revenue_center.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_inpatient_revenue_center_f())

### D) Table-level QA
system.time(mcare_inpatient_revenue_center_qa <- qa_stage.mcare_inpatient_revenue_center_qa_f())
rm(config_url)
#rm(mcare_inpatient_revenue_center_qa)

### E) Archive current table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "mcare_inpatient_revenue_center")

### F) Remove "load" suffix from new stage table
dbSendQuery(conn = db_claims, glue_sql("exec sp_rename 'stage.mcare_inpatient_revenue_center_load', 'mcare_inpatient_revenue_center';"))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 11: mcare_outpatient_base_claims ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_outpatient_base_claims.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_outpatient_base_claims.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_outpatient_base_claims_f())

### D) Table-level QA
system.time(mcare_outpatient_base_claims_qa <- qa_stage.mcare_outpatient_base_claims_qa_f())
rm(config_url)
#rm(mcare_outpatient_base_claims_qa)

### E) Archive current table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "mcare_outpatient_base_claims")

### F) Remove "load" suffix from new stage table
dbSendQuery(conn = db_claims, glue_sql("exec sp_rename 'stage.mcare_outpatient_base_claims_load', 'mcare_outpatient_base_claims';"))



## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 12: mcare_outpatient_revenue_center ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_outpatient_revenue_center.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_outpatient_revenue_center.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_outpatient_revenue_center_f())

### D) Table-level QA
system.time(mcare_outpatient_revenue_center_qa <- qa_stage.mcare_outpatient_revenue_center_qa_f())
rm(config_url)
#rm(mcare_outpatient_revenue_center_qa)

### E) Archive current table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "mcare_outpatient_revenue_center")

### F) Remove "load" suffix from new stage table
dbSendQuery(conn = db_claims, glue_sql("exec sp_rename 'stage.mcare_outpatient_revenue_center_load', 'mcare_outpatient_revenue_center';"))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 13: mcare_snf_base_claims ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_snf_base_claims.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_snf_base_claims.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_snf_base_claims_f())

### D) Table-level QA
system.time(mcare_snf_base_claims_qa <- qa_stage.mcare_snf_base_claims_qa_f())
rm(config_url)
#rm(mcare_snf_base_claims_qa)

### E) Archive current table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "mcare_snf_base_claims")

### F) Remove "load" suffix from new stage table
dbSendQuery(conn = db_claims, glue_sql("exec sp_rename 'stage.mcare_snf_base_claims_load', 'mcare_snf_base_claims';"))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 14: mcare_snf_revenue_center ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_snf_revenue_center.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_snf_revenue_center.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_snf_revenue_center_f())

### D) Table-level QA
system.time(mcare_snf_revenue_center_qa <- qa_stage.mcare_snf_revenue_center_qa_f())
rm(config_url)
#rm(mcare_snf_revenue_center_qa)

### E) Archive current table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "mcare_snf_revenue_center")

### F) Remove "load" suffix from new stage table
dbSendQuery(conn = db_claims, glue_sql("exec sp_rename 'stage.mcare_snf_revenue_center_load', 'mcare_snf_revenue_center';"))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### FINAL STEP: INDEX ALL TABLES ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_bcarrier_claims on stage.mcare_bcarrier_claims")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_bcarrier_line on stage.mcare_bcarrier_line")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_dme_claims on stage.mcare_dme_claims")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_dme_line on stage.mcare_dme_line")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_hha_base_claims on stage.mcare_hha_base_claims")))
#placeholder for hha_revenue_center
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_hospice_base_claims on stage.mcare_hospice_base_claims")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_hospice_revenue_center on stage.mcare_hospice_revenue_center")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_inpatient_base_claims on stage.mcare_inpatient_base_claims")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_inpatient_revenue_center on stage.mcare_inpatient_revenue_center")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_outpatient_base_claims on stage.mcare_outpatient_base_claims")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_outpatient_revenue_center on stage.mcare_outpatient_revenue_center")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_snf_base_claims on stage.mcare_snf_base_claims")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_snf_revenue_center on stage.mcare_snf_revenue_center")))