#### AS-NEEDED SCRIPT TO CREATE ANALYTIC TABLE SHELLS WITH CCI ON HHSAW FOR APCD DATA
#
# Eli Kern, PHSKC (APDE)
# 2024-06
#
# 2024-08-26 update: Modifying to use apde.etl package

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, glue, apde.etl)

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_table.R")

## Connect to HHSAW
interactive_auth <- FALSE
prod <- TRUE
db_claims <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)


## Create and index table shells if table does not exist
# Remember to reuse YAML config files used for Synapse, need to manually pass schema and table name to create_table function
#change overwrite to 'T' when it's an existing table
## [FILL IN BLANKS BEFORE USING]

apde.etl::create_table(
  conn = db_claims, 
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.BLANK.yaml",
  overall = T, ind_yr = F, overwrite = F, server = "hhsaw", to_schema = "claims", to_table = "BLANK")

dbExecute(
  conn = db_claims,
  glue_sql("create clustered columnstore index idx_ccs_BLANK on claims.BLANK;"))

##EXAMPLE: 
# apde.etl::create_table(
#   conn = db_claims, 
#   config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/refs/heads/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2024.yaml",
#   overall = T, ind_yr = F, overwrite = T, server = "hhsaw", to_schema = "claims", to_table = "final_apcd_elig_plr_2024")
# 
# dbExecute(
#   conn = db_claims,
#   glue_sql("create clustered columnstore index idx_ccs_BLANK on claims.final_apcd_elig_plr_2024;"))