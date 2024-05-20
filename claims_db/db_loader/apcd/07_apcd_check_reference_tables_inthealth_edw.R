#### CODE TO COMPARE AND UPDATE AS NEEDED ETL REFERENCE TABLES
# Eli Kern, PHSKC (APDE)
#
# 2024-05

## Set up global parameters and call in libraries
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)
origin <- "1970-01-01" # Date origin
pacman::p_load(tidyverse, odbc, configr, glue, keyring, svDialogs, R.utils)

#### STEP 1: Connect to SQL DATABASES ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")
interactive_auth <- FALSE
prod <- TRUE
db_claims <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)
dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

#Load Jeremy's table duplicate function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/table_duplicate.R")

#Specific tables to be copied
table_df <- as.data.frame(
  list(
    from_schema = c("claims"),
    from_table = c("ref_apcd_claim_status", "ref_apcd_ethnicity_race_map"),
    to_schema = c("stg_claims"),
    to_table = c("ref_apcd_claim_status", "ref_apcd_ethnicity_race_map")
    )
  )

#Run command
system.time(table_duplicate_f(
  conn_from = db_claims,
  conn_to = dw_inthealth,
  server_to = "inthealth_edw_prod",
  db_to = "inthealth_edw",
  table_df = table_df
))
