#### MASTER CODE TO CREATE SQL CODE FOR KCIT TO CREATE EXTERNAL TABLES ON HHSAW
#
# Eli Kern, PHSKC (APDE)
# Adapted from Jeremy Whitehurst's script
# 2024-06

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, glue)

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/0273be9efb4f470645c1c30ddf5f6083f8b13ce6/R/external_table_check.R")

## Connect to Synapse
conn <- create_db_connection("inthealth", interactive = F, prod = T)
conn_ext <- create_db_connection("hhsaw", interactive = F, prod = T)

## Create list of tables on inthealth_edw for which I need an external table on HHSAW
## Update list as needed to replace list with new tables or tables that have had changes to column number/name/type
tables <- data.frame(table_name = c(
  "stage_mcare_claim_bh",
  "stage_mcare_claim_ccw",
  "stage_mcare_claim_header",
  "stage_mcare_claim_icdcm_header",
  "stage_mcare_claim_line",
  "stage_mcare_claim_moud",
  "stage_mcare_claim_naloxone",
  "stage_mcare_claim_pharm",
  "stage_mcare_claim_pharm_char",
  "stage_mcare_claim_procedure",
  "stage_mcare_claim_provider",
  "stage_mcare_elig_demo",
  "stage_mcare_elig_timevar"
))

## Use function to loop over table names and create SQL code
for (i in 1:nrow(tables)) {
  external_table_check_f(conn = conn,
                         db = "inthealth_edw",
                         schema = "stg_claims",
                         table = gsub("stage_", "final_", tables[i,1]),
                         db_ext = "hhs_analytics_workspace",
                         conn_ext = conn_ext,
                         schema_ext = "claims",
                         table_ext = tables[i, 1],
                         sql_display = T,
                         sql_file_path = "\\\\dphcifs/apde-cdip/mcaid-mcare/mcare_ext_tables_20240618.sql",
                         overwrite = F) 
}