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
#### Table 1: apcd_elig_demo ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_elig_demo.R")

### B) Create table
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_elig_demo.yaml",
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.apcd_elig_demo_f())

### D) Table-level QA
system.time(apcd_demo_qa1 <- qa_stage.apcd_elig_demo_f())
rm(apcd_demo_qa1)

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
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_elig_timevar.R")

### B) Create table
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_elig_timevar.yaml",
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.apcd_elig_timevar_f(extract_end_date = "2019-03-31"))

### D) Table-level QA
system.time(apcd_timevar_qa1 <- qa_stage.apcd_elig_timevar_f())
rm(apcd_timevar_qa1)

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
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr.R")

### B) Create table
# 2014
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2014.yaml",
               overall = T, ind_yr = F, overwrite = T, test_mode = F)
# 2015
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2015.yaml",
               overall = T, ind_yr = F, overwrite = T, test_mode = F)
# 2016
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2016.yaml",
               overall = T, ind_yr = F, overwrite = T, test_mode = F)
#2017
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2017.yaml",
               overall = T, ind_yr = F, overwrite = T, test_mode = F)
# 2018
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2018.yaml",
               overall = T, ind_yr = F, overwrite = T, test_mode = F)


### C) Load tables
system.time(load_stage.apcd_elig_plr_f(from_date = "2014-01-01", to_date = "2014-12-31")) #2014
system.time(load_stage.apcd_elig_plr_f(from_date = "2015-01-01", to_date = "2015-12-31")) #2015
system.time(load_stage.apcd_elig_plr_f(from_date = "2016-01-01", to_date = "2016-12-31")) #2016
system.time(load_stage.apcd_elig_plr_f(from_date = "2017-01-01", to_date = "2017-12-31")) #2017
system.time(load_stage.apcd_elig_plr_f(from_date = "2018-01-01", to_date = "2018-12-31")) #2018


### D) Table-level QA
system.time(apcd_plr_2014_qa1 <- qa_stage.apcd_elig_plr_f(year = "2014"))
system.time(apcd_plr_2015_qa1 <- qa_stage.apcd_elig_plr_f(year = "2015"))
system.time(apcd_plr_2016_qa1 <- qa_stage.apcd_elig_plr_f(year = "2016"))
system.time(apcd_plr_2017_qa1 <- qa_stage.apcd_elig_plr_f(year = "2017"))
system.time(apcd_plr_2018_qa1 <- qa_stage.apcd_elig_plr_f(year = "2018"))

### E) Run line-level QA script on a single year only at \\dchs-shares01\dchsdata\dchsphclaimsdata\qa_line_level\qa_stage.apcd_elig_plr.sql

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_plr_2014")
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_plr_2015")
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_plr_2016")
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_plr_2017")
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_elig_plr_2018")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_plr_2014")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_plr_2015")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_plr_2016")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_plr_2017")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_elig_plr_2018")

### H) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_plr on final.apcd_elig_plr_2014")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_plr on final.apcd_elig_plr_2015")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_plr on final.apcd_elig_plr_2016")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_plr on final.apcd_elig_plr_2017")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_elig_plr on final.apcd_elig_plr_2018")))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 4: apcd_claim_line ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_claim_line.R")

### B) Create table
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_claim_line.yaml",
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.apcd_claim_line_f())

### D) Table-level QA
system.time(apcd_line_qa1 <- qa_stage.apcd_claim_line_f())
rm(apcd_line_qa1)

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_claim_line")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_claim_line")

### H) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_claim_line on final.apcd_claim_line")))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 4: apcd_claim_icdcm_header ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_claim_icdcm_header.R")

### B) Create table
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_claim_icdcm_header.yaml",
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.apcd_claim_icdcm_header_f())

### D) Table-level QA
system.time(apcd_icdcm_qa1 <- qa_stage.apcd_claim_icdcm_header_f())
rm(apcd_icdcm_qa1)

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_claim_icdcm_header")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_claim_icdcm_header")

### H) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_claim_icdcm_header on final.apcd_claim_icdcm_header")))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 5: apcd_claim_procedure ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_claim_procedure.R")

### B) Create table
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_claim_procedure.yaml",
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.apcd_claim_procedure_f())

### D) Table-level QA
system.time(apcd_procedure_qa1 <- qa_stage.apcd_claim_procedure_f())
rm(apcd_procedure_qa1)

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "apcd_claim_procedure")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "apcd_claim_procedure")

### H) Create clustered columnstore index
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_apcd_claim_procedure on final.apcd_claim_procedure")))
