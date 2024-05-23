#### CODE TO COPY NEEDED ETL REFERENCE TABLES FROM HHSAW TO SYNAPSE
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
keyring::key_list() #Confirm you have a key set for hhsaw and inthealth_edw_prod on this machine

#Load Jeremy's table duplicate function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/table_duplicate.R")


#### STEP 2: Copy tables from HHSAW claims schema ####

#Specify tables to be copied from claims schema of HHSAW
table_df_claims <- as.data.frame(
  list(
    from_schema = c("claims"),
    from_table = c("ref_apcd_claim_status",
                   "ref_apcd_ethnicity_race_map",
                   "ref_apcd_zip_group",
                   "ref_ccw_lookup",
                   "ref_geo_county_code_wa",
                   "ref_geo_kc_zip",
                   "ref_kc_claim_type_crosswalk",
                   "ref_moll_preg_endpoint",
                   "ref_moll_trimester",
                   "ref_pc_visit_oregon",
                   "ref_rolling_time_12mo_2012_2020",
                   "ref_rolling_time_24mo_2012_2020",
                   "ref_rolling_time_36mo_2012_2020",
                   "ref_date"),
    to_schema = c("stg_claims"),
    to_table = c("ref_apcd_claim_status",
                 "ref_apcd_ethnicity_race_map",
                 "ref_apcd_zip_group",
                 "ref_ccw_lookup",
                 "ref_geo_county_code_wa",
                 "ref_geo_kc_zip",
                 "ref_kc_claim_type_crosswalk",
                 "ref_moll_preg_endpoint",
                 "ref_moll_trimester",
                 "ref_pc_visit_oregon",
                 "ref_rolling_time_12mo_2012_2020",
                 "ref_rolling_time_24mo_2012_2020",
                 "ref_rolling_time_36mo_2012_2020",
                 "ref_date")
    )
  )

#Run command
system.time(table_duplicate_f(
  conn_from = db_claims,
  conn_to = dw_inthealth,
  server_to = "inthealth_edw_prod",
  db_to = "inthealth_edw",
  table_df = table_df_claims,
  confirm_tables = TRUE,
  delete_table = TRUE
))

#### Step 3: Copy tables from HHSAW ref schema ####

#Specify tables to be copied from ref schema of HHSAW
table_df_ref <- as.data.frame(
  list(
    from_schema = c("ref"),
    from_table = c("icdcm_codes"),
    to_schema = c("stg_claims"),
    to_table = c("ref_icdcm_codes")
  )
)

#Run command
system.time(table_duplicate_f(
  conn_from = db_claims,
  conn_to = dw_inthealth,
  server_to = "inthealth_edw_prod",
  db_to = "inthealth_edw",
  table_df = table_df_ref,
  confirm_tables = TRUE,
  delete_table = TRUE
))