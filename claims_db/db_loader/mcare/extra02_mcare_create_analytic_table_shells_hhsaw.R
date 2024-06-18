#### AS-NEEDED SCRIPT TO CREATE ANALYTIC TABLE SHELLS WITH CCI ON HHSAW FOR APCD DATA
#
# Eli Kern, PHSKC (APDE)
# 2024-06

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, glue)

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_table.R")

## Connect to HHSAW
interactive_auth <- FALSE
prod <- TRUE
db_claims <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)


## Create and index table shells if table does not exist
# Remember to reuse YAML config files used for Synapse, need to manually pass schema and table name to create_table function

## Update list as needed to replace list with new tables or tables that have had changes to column number/name/type
tables <- data.frame(table_name = c(
  "final_mcare_claim_bh",
  "final_mcare_claim_ccw",
  "final_mcare_claim_header",
  "final_mcare_claim_icdcm_header",
  "final_mcare_claim_line",
  "final_mcare_claim_moud",
  "final_mcare_claim_naloxone",
  "final_mcare_claim_pharm",
  "final_mcare_claim_pharm_char",
  "final_mcare_claim_procedure",
  "final_mcare_claim_provider",
  "final_mcare_elig_demo",
  "final_mcare_elig_timevar"
))

## Create and index table shells using loop
for (i in 1:nrow(tables)) {
  
  create_table(
    conn = db_claims, 
    config_url = glue("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.",
                      gsub("final_", "", tables[i,1]),
                      ".yaml"),
    overall = T, ind_yr = F, overwrite = F, server = "hhsaw", to_schema = "claims", to_table = tables[i,1])
  
  indexname <- DBI::SQL(glue("idx_ccs", tables[i,1]))
  tablename <- DBI::SQL(glue("claims.", tables[i,1]))
  dbExecute(
    conn = db_claims,
    glue_sql("create clustered columnstore index {`indexname`} on {`tablename`};", .con = db_claims))

}