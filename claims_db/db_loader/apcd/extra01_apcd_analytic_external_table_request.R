#### MASTER CODE TO CREATE SQL CODE FOR KCIT TO CREATE EXTERNAL TABLES ON HHSAW
#
# Eli Kern, PHSKC (APDE)
# Adapted from Jeremy Whitehurst's script
# 2024-05

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
  "stage_apcd_claim_ccw",
  "stage_apcd_claim_header",
  "stage_apcd_claim_icdcm_header",
  "stage_apcd_claim_line",
  "stage_apcd_claim_preg_episode",
  "stage_apcd_claim_procedure",
  "stage_apcd_claim_provider",
  "stage_apcd_elig_demo",
  "stage_apcd_elig_timevar",
  "stage_apcd_elig_plr_2014",
  "stage_apcd_elig_plr_2015",
  "stage_apcd_elig_plr_2016",
  "stage_apcd_elig_plr_2017",
  "stage_apcd_elig_plr_2018",
  "stage_apcd_elig_plr_2019",
  "stage_apcd_elig_plr_2020",
  "stage_apcd_elig_plr_2021",
  "stage_apcd_elig_plr_2022",
  "stage_apcd_elig_plr_2023"
))

## Use function to loop over table names and create SQL code
for (i in 1:nrow(tables)) {
  external_table_check_f(conn = conn,
                         db = "inthealth_edw",
                         schema = "stg_claims",
                         table = tables[i, 1],
                         db_ext = "hhs_analytics_workspace",
                         conn_ext = conn_ext,
                         schema_ext = "claims",
                         table_ext = tables[i, 1],
                         sql_display = T,
                         sql_file_path = "\\\\dphcifs/apde-cdip/apcd/apcd_ext_tables_20240524.sql",
                         overwrite = F) 
}