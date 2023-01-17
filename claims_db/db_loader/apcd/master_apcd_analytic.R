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

#2022-02: switched to using APDE repo functions where Alastair has moved them over


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, glue)

db_claims <- dbConnect(odbc(), "PHClaims51")

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_table.R")
#devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/load_table_from_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/load_table_from_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/add_index.R")

devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/alter_schema.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/load_ccw.R")

## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 1: apcd_elig_demo ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

####
#Before creating this table, run following QA script to check for new ethnicities to map to race categories
#https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.apcd_ethnicity_race_map_update_check.sql
####

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_demo.R")

### B) Create table
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_demo.yaml",
               overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")

### C) Load tables
system.time(load_stage.apcd_elig_demo_f())

### D) Table-level QA
system.time(apcd_demo_qa1 <- qa_stage.apcd_elig_demo_f())
#rm(apcd_demo_qa1)

### E) Run line-level QA script at \\dchs-shares01\dchsdata\dchsphclaimsdata\qa_line_level\qa_stage.apcd_elig_demo.sql             

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_demo")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_demo")

### H) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_demo on final.apcd_elig_demo")))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 2: apcd_elig_timevar ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_timevar.R")

### B) Create table
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_timevar.yaml",
               overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")

### C) Load tables [CHANGE EXTRACT END DATE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!]
system.time(load_stage.apcd_elig_timevar_f(extract_end_date = "2022-06-30"))

### D) Table-level QA
system.time(apcd_timevar_qa1 <- qa_stage.apcd_elig_timevar_f())
#rm(apcd_timevar_qa1)

### E) Run line-level QA script at \\dchs-shares01\dchsdata\dchsphclaimsdata\qa_line_level\qa_stage.apcd_elig_timevar.sql

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_timevar")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_timevar")

### H) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_timevar on final.apcd_elig_timevar")))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 3: apcd_elig_plr_DATE ####
# Note: Eventually use claim_elig function to generate these tables
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr.R")

### B) Create table
# 2014
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2014.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")
# 2015
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2015.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")
# 2016
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2016.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")
# 2017
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2017.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")
# 2018
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2018.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")
# 2019
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2019.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")
# 2020
create_table(conn = db_claims, 
             config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2020.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")

#2020-03-01 through 2021-02-28
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_20210228.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")

#2018-07-01 through 2019-06-30
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_20190630.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")

### C) Load tables
system.time(load_stage.apcd_elig_plr_f(from_date = "2014-01-01", to_date = "2014-12-31")) #2014
system.time(load_stage.apcd_elig_plr_f(from_date = "2015-01-01", to_date = "2015-12-31")) #2015
system.time(load_stage.apcd_elig_plr_f(from_date = "2016-01-01", to_date = "2016-12-31")) #2016
system.time(load_stage.apcd_elig_plr_f(from_date = "2017-01-01", to_date = "2017-12-31")) #2017
system.time(load_stage.apcd_elig_plr_f(from_date = "2018-01-01", to_date = "2018-12-31")) #2018
system.time(load_stage.apcd_elig_plr_f(from_date = "2019-01-01", to_date = "2019-12-31")) #2019
system.time(load_stage.apcd_elig_plr_f(from_date = "2020-01-01", to_date = "2020-12-31")) #2020
system.time(load_stage.apcd_elig_plr_f(from_date = "2020-03-01", to_date = "2021-02-28", calendar_year = F, table_name = "20210228")) #2020-03-01 -> 2021-02-28, most recent 12 months
system.time(load_stage.apcd_elig_plr_f(from_date = "2018-07-01", to_date = "2019-06-30", calendar_year = F, table_name = "20190630")) #2018-07-01 -> 2019-06-30, custom TPCHD window

### D) Table-level QA
system.time(apcd_plr_2014_qa1 <- qa_stage.apcd_elig_plr_f(year = "2014"))
system.time(apcd_plr_2015_qa1 <- qa_stage.apcd_elig_plr_f(year = "2015"))
system.time(apcd_plr_2016_qa1 <- qa_stage.apcd_elig_plr_f(year = "2016"))
system.time(apcd_plr_2017_qa1 <- qa_stage.apcd_elig_plr_f(year = "2017"))
system.time(apcd_plr_2018_qa1 <- qa_stage.apcd_elig_plr_f(year = "2018"))
system.time(apcd_plr_2019_qa1 <- qa_stage.apcd_elig_plr_f(year = "2019"))
system.time(apcd_plr_2020_qa1 <- qa_stage.apcd_elig_plr_f(year = "2020"))
system.time(apcd_plr_custom1_qa <- qa_stage.apcd_elig_plr_f(year = "20210228"))
system.time(apcd_plr_custom2_qa <- qa_stage.apcd_elig_plr_f(year = "20190630"))

### E) Run line-level QA script on a single year only at \\dchs-shares01\dchsdata\dchsphclaimsdata\qa_line_level\qa_stage.apcd_elig_plr.sql

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_plr_2014")
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_plr_2015")
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_plr_2016")
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_plr_2017")
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_plr_2018")
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_plr_2019")
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_plr_2020")
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_plr_20200229") #Old table for most recent 12 months
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_plr_20190630")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_plr_2014")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_plr_2015")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_plr_2016")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_plr_2017")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_plr_2018")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_plr_2019")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_plr_2020")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_plr_20210228") #New table for most recent 12 months
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_plr_20190630")

### H) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_plr on final.apcd_elig_plr_2014")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_plr on final.apcd_elig_plr_2015")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_plr on final.apcd_elig_plr_2016")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_plr on final.apcd_elig_plr_2017")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_plr on final.apcd_elig_plr_2018")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_plr on final.apcd_elig_plr_2019")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_plr on final.apcd_elig_plr_2020")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_plr on final.apcd_elig_plr_20210228"))) #New table for most recent 12 months
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_plr on final.apcd_elig_plr_20190630")))

## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 5: apcd_claim_line ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_line.R")

### B) Create table
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_line.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")

### C) Load tables
system.time(load_stage.apcd_claim_line_f())

### D) Table-level QA
system.time(apcd_line_qa1 <- qa_stage.apcd_claim_line_f())
#rm(apcd_line_qa1)

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_claim_line")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_claim_line")

### H) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_claim_line on final.apcd_claim_line")))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 6: apcd_claim_icdcm_header ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_icdcm_header.R")

### B) Create table
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_icdcm_header.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")

### C) Load tables
system.time(load_stage.apcd_claim_icdcm_header_f())

### D) Table-level QA
system.time(apcd_icdcm_qa1 <- qa_stage.apcd_claim_icdcm_header_f())
#rm(apcd_icdcm_qa1)

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_claim_icdcm_header")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_claim_icdcm_header")

### H) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_claim_icdcm_header on final.apcd_claim_icdcm_header")))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 7: apcd_claim_procedure ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_procedure.R")

### B) Create table
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_procedure.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")

### C) Load tables
system.time(load_stage.apcd_claim_procedure_f())

### D) Table-level QA
system.time(apcd_procedure_qa1 <- qa_stage.apcd_claim_procedure_f())
#rm(apcd_procedure_qa1)

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_claim_procedure")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_claim_procedure")

### H) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_claim_procedure on final.apcd_claim_procedure")))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 8: apcd_claim_provider ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_provider.R")

### B) Create table
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_provider.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")

### C) Load tables
system.time(load_stage.apcd_claim_provider_f())

### D) Table-level QA
system.time(apcd_provider_qa1 <- qa_stage.apcd_claim_provider_f())
#rm(apcd_provider_qa1)

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_claim_provider")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_claim_provider")

### H) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_claim_provider on final.apcd_claim_provider")))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 9: ref.apcd_provider_npi ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.apcd_provider_npi.R")

### B) Archive current table
alter_schema_f(conn = db_claims, from_schema = "ref", to_schema = "archive", table_name = "apcd_provider_npi")

### C) Create table
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.apcd_provider_npi.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")

### D) Load tables
system.time(load_ref.apcd_provider_npi_f())

### E) Table-level QA
system.time(apcd_provider_npi_qa1 <- qa_ref.apcd_provider_npi_f())
#rm(apcd_provider_npi_qa1)

### F) Run line-level QA script at \\dchs-shares01\dchsdata\dchsphclaimsdata\qa_line_level\qa_ref.apcd_provider_npi.sql

### G) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_ref_apcd_provider_npi on ref.apcd_provider_npi")))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 10: ref.kc_provider_master ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.kc_provider_master.R")

### B) Archive current table
alter_schema_f(conn = db_claims, from_schema = "ref", to_schema = "archive", table_name = "kc_provider_master")

### C) Create table
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.kc_provider_master.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")

### D) Load tables
system.time(load_ref.kc_provider_master_f())

### E) Table-level QA
system.time(kc_provider_master_qa1 <- qa_ref.kc_provider_master_f())
#rm(kc_provider_master_qa1)

### F) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_ref_kc_provider_master on ref.kc_provider_master")))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 11: apcd_claim_header ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_header.R")

### B) Create table
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_header.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")

### C) Load tables
system.time(load_stage.apcd_claim_header_f())

### D) Table-level QA (90 minutes to run!)
system.time(apcd_claim_header_qa1 <- qa_stage.apcd_claim_header_f())
#rm(apcd_claim_header_qa1)

### E) Run line-level QA script at \\dchs-shares01\dchsdata\dchsphclaimsdata\qa_line_level\qa_stage.apcd_claim_header.sql             

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_claim_header")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_claim_header")

### H) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_claim_header on final.apcd_claim_header")))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 12: apcd_claim_ccw ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Create table
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_ccw.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "KCITSQLUTPDBH51")

### C) Load tables
system.time(load_ccw(server = "phclaims", conn = db_claims, source = c("apcd"),
                     config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_ccw.yaml"))

### D) Table-level QA

#all members should be in elig_demo table
apcd_claim_ccw_qa1 <- dbGetQuery(conn = db_claims, glue_sql(
  "select 'stage.apcd_claim_ccw' as 'table', '# members not in elig_demo, expect 0' as qa_type,
    count(distinct a.id_apcd) as qa
    from stage.apcd_claim_ccw as a
    left join final.apcd_elig_demo as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
  .con = db_claims))

#count conditions run
apcd_claim_ccw_qa2 <- dbGetQuery(conn = db_claims, glue_sql(
  "select 'stage.apcd_claim_ccw' as 'table', '# conditions, expect 27' as qa_type,
  count(distinct ccw_code) as qa
  from PHClaims.stage.apcd_claim_ccw;",
  .con = db_claims))
#rm(apcd_claim_ccw_qa1, apcd_claim_ccw_qa2)

### E) Run line-level QA script at \\dchs-shares01\dchsdata\dchsphclaimsdata\qa_line_level\qa_stage.apcd_claim_ccw.sql             

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_claim_ccw")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_claim_ccw")

### H) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_claim_ccw on final.apcd_claim_ccw")))