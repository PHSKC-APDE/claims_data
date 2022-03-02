#### MASTER CODE TO UPDATE COMBINED MEDICAID/MEDICARE ANALYTIC TABLES
#
# Alastair Matheson, PHSKC (APDE)
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
library(RecordLinkage)


# NB. Currently Medicare data can only be loaded to on-prem servers so DO NOT USE HHSAW
server <- select.list(choices = c("phclaims", "hhsaw"))

if (server == "phclaims") {
  db_claims <- DBI::dbConnect(odbc::odbc(), "PHClaims51")
} else if (server == "hhsaw") {
  db_claims <- DBI::dbConnect(odbc::odbc(),
                              driver = "ODBC Driver 17 for SQL Server",
                              server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433",
                              database = "hhs_analytics_workspace",
                              uid = keyring::key_list("hhsaw_dev")[["username"]],
                              pwd = keyring::key_get("hhsaw_dev", keyring::key_list("hhsaw_dev")[["username"]]),
                              Encrypt = "yes",
                              TrustServerCertificate = "yes",
                              Authentication = "ActiveDirectoryPassword")
}


#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/add_index.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/alter_schema.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/claim_ccw.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_sql.R")


#### IDENTITY LINKAGE ####
# Make stage version of linkage
# stage_mcaid_elig_demo_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.xwalk_apde_mcaid_mcare_pha.yaml")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.xwalk_apde_mcaid_mcare_pha.R")

# QA stage version
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/qa_stage.xwalk_apde_mcaid_mcare_pha.R")
qa_xwalk_apde_mcaid_mcare_pha_f(conn = db_claims, load_only = F)


# Archive previous xwalk table so that we can more easily update IDs in claims tables below
alter_schema_f(conn = db_claims, 
               from_schema = "final", 
               to_schema = "archive",
               table_name = "xwalk_apde_mcaid_mcare_pha")

# Alter schema to final table (currently hard coded, use YAML eventually)
alter_schema_f(conn = db_claims, 
               from_schema = "stage", 
               to_schema = "final",
               table_name = "xwalk_apde_mcaid_mcare_pha")

# Add index
DBI::dbExecute(db_claims,
               'CREATE CLUSTERED COLUMNSTORE INDEX "idx_ccs_final_xwalk_apde_mcaid_mcare_pha" ON 
                              final.xwalk_apde_mcaid_mcare_pha')



#### CREATE ELIG ANALYTIC TABLES ------------------------------------------- ----
#### MCAID_MCARE_ELIG_DEMO ####
# Create and load stage
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_elig_demo.R")

# QA stage version
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/qa_stage.mcaid_mcare_elig_demo.R")
qa_mcaid_mcare_elig_demo_f(conn = db_claims, load_only = F)

# Alter schema to final table (currently hard coded, use YAML eventually)
alter_schema_f(conn = db_claims, 
               from_schema = "stage", 
               to_schema = "final",
               table_name = "mcaid_mcare_elig_demo")

# Add index
DBI::dbExecute(db_claims,
               'CREATE CLUSTERED COLUMNSTORE INDEX "idx_ccs_final_mcaid_mcare_elig_demo" ON 
                              final.mcaid_mcare_elig_demo')


#### MCAID_MCARE_ELIG_TIMEVAR ####
# Create and load stage
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_elig_timevar.R")

# QA stage version
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/qa_stage.mcaid_mcare_elig_timevar.R")
qa_mcaid_mcare_elig_timevar_f(conn = db_claims, load_only = F)

# Alter schema to final table (currently hard coded, use YAML eventually)
alter_schema_f(conn = db_claims, 
               from_schema = "stage", 
               to_schema = "final",
               table_name = "mcaid_mcare_elig_timevar")

# Add index
DBI::dbExecute(db_claims,
               'CREATE CLUSTERED COLUMNSTORE INDEX "idx_ccs_final_mcaid_mcare_elig_timevar" ON 
                              final.mcaid_mcare_elig_timevar')



#### CREATE CLAIMS TABLES -------------------------------------------------- ----

## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 1: mcaid_mcare_claim_line ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_claim_line.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_claim_line.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables (~50 min)
system.time(load_stage.mcaid_mcare_claim_line_f(conn = db_claims,
                                                config_url = config_url))

### D) Table-level QA (~1 min)
system.time(mcaid_mcare_claim_line_qa <- qa_stage.mcaid_mcare_claim_line_qa_f())
rm(config_url)

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcaid_mcare_claim_line")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcaid_mcare_claim_line")

rm(load_stage.mcaid_mcare_claim_line_f, qa_stage.mcaid_mcare_claim_line_qa_f, config_url)

## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 2: mcaid_mcare_claim_icdcm_header ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_claim_icdcm_header.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_claim_icdcm_header.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables (~50 min)
system.time(load_stage.mcaid_mcare_claim_icdcm_header_f())

### D) Table-level QA (~1 min)
system.time(mcaid_mcare_claim_icdcm_header_qa <- qa_stage.mcaid_mcare_claim_icdcm_header_qa_f())


### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcaid_mcare_claim_icdcm_header")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcaid_mcare_claim_icdcm_header")

rm(load_stage.mcaid_mcare_claim_icdcm_header_f, qa_stage.mcaid_mcare_claim_icdcm_header_qa_f, config_url)


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 3: mcaid_mcare_claim_procedure ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_claim_procedure.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_claim_procedure.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables (~35 min)
system.time(load_stage.mcaid_mcare_claim_procedure_f())

### D) Table-level QA (~1 min)
system.time(mcaid_mcare_claim_procedure_qa <- qa_stage.mcaid_mcare_claim_procedure_qa_f())
rm(config_url)

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcaid_mcare_claim_procedure")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcaid_mcare_claim_procedure")

rm(load_stage.mcaid_mcare_claim_procedure_f, qa_stage.mcaid_mcare_claim_procedure_qa_f, config_url)


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 4: mcaid_mcare_claim_provider ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### Place holder once mcaid_claim_provider table exists


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 5: mcaid_mcare_claim_header ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_claim_header.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_claim_header.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables (~5.3 hours)
system.time(load_stage.mcaid_mcare_claim_header_f())

### D) Table-level QA (14 min)
system.time(mcaid_mcare_claim_header_qa <- qa_stage.mcaid_mcare_claim_header_qa_f())

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcaid_mcare_claim_header")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcaid_mcare_claim_header")

rm(load_stage.mcaid_mcare_claim_header_f, qa_stage.mcaid_mcare_claim_header_qa_f, config_url)


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 6: mcaid_mcare_claim_ccw ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_claim_ccw.yaml"

### B) Create table
create_table_f(conn = db_claims, server = "phclaims",
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_ccw(conn = db_claims, server = "phclaims", 
                     source = "mcaid_mcare", config_url = config_url))

### D) Line-level QA
#Run script: qa_stage.mcaid_mcare_claim_ccw.sql

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcaid_mcare_claim_ccw")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcaid_mcare_claim_ccw")


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### INDEX ALL TABLES ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
# (~ 18 min)
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_mcaid_mcare_claim_line on final.mcaid_mcare_claim_line")))
# (~ 35 min)
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_mcaid_mcare_claim_icdcm_header on final.mcaid_mcare_claim_icdcm_header")))
# (~23 min)
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_mcaid_mcare_claim_procedure on final.mcaid_mcare_claim_procedure")))

#system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_mcaid_mcare_claim_provider on final.mcaid_mcare_claim_provider")))
# (~30 min)
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_mcaid_mcare_claim_header on final.mcaid_mcare_claim_header")))
# (~1 min)
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_mcaid_mcare_claim_ccw on final.mcaid_mcare_claim_ccw")))
