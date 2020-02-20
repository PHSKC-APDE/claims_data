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


db_claims <- dbConnect(odbc(), "PHClaims51")


#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/add_index.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/alter_schema.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/claim_ccw.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_sql.R")


#### IDENTITY LINKAGE ####
# Make stage version of linkage
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.xwalk_apde_mcaid_mcare_pha.r")

# QA stage version
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.xwalk_apde_mcaid_mcare_pha.r")
qa_xwalk_apde_mcaid_mcare_pha_f(conn = db_claims, load_only = F)

# Alter schema to final table (currently hard coded, use YAML eventually)
alter_schema_f(conn = db_claims, 
               from_schema = "stage", 
               to_schema = "final",
               table_name = "xwalk_apde_mcaid_mcare_pha")

# Add index
DBI::dbExecute(db_claims,
               'CREATE CLUSTERED COLUMNSTORE INDEX "idx_ccs_final_xwalk_apde_mcaid_mcare_pha" ON 
                              final.xwalk_apde_mcaid_mcare_pha')



#### CREATE ELIG ANALYTIC TABLES -----------------------------------------------
#### MCAID_MCARE_ELIG_DEMO ####
# Create and load stage
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_elig_demo.R")

# QA and load to final
# (no QA outside of what is in the stage code right now)
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
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_elig_timevar.R")

# QA and load to final
# (no QA outside of what is in the stage code right now)
# Alter schema to final table (currently hard coded, use YAML eventually)
alter_schema_f(conn = db_claims, 
               from_schema = "stage", 
               to_schema = "final",
               table_name = "mcaid_mcare_elig_timevar")

# Add index
DBI::dbExecute(db_claims,
               'CREATE CLUSTERED COLUMNSTORE INDEX "idx_ccs_final_mcaid_mcare_elig_timevar" ON 
                              final.mcaid_mcare_elig_timevar')



#### CREATE CLAIMS TABLES ------------------------------------------------------
#### MCAID_MCARE_CLAIM_ICDCM_HEADER ####
# Create and load stage
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_claim_icdcm_header.R")


#### MCAID_MCARE_CLAIM_HEADER ####
# Create and load stage
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_claim_header.R")


#### MCAID_MCARE_CLAIM_CCW ####
# Create and load stage (also currently loading to final)
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_claim_ccw.R")


