#### CODE TO COPY NEEDED ETL REFERENCE TABLES BETWEEN INTHEALTH_EDW AND HHSAW
# Eli Kern, PHSKC (APDE)
#
# 2024-05
#10-16-2024 added keyring for inthealth
#02-05-2026 update the deprecated function
#08-14-2026 Eli updated to use apde.etl package, added section to copy Onpoint APCD ref tables from inthealth_edw to hhsaw

# clean memory ----
rm(list=ls())

## Set up global parameters and call in libraries
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)
origin <- "1970-01-01" # Date origin
#install.packages("remotes")       # if you don’t already have remotes
# use these instructions #1-2 if you need to set up a token https://github.com/PHSKC-APDE/apde.data
#remotes::install_github("PHSKC-APDE/apde.etl")
#info on package https://github.com/PHSKC-APDE/apde.etl

pacman::p_load(tidyverse, odbc, configr, glue, keyring, svDialogs, R.utils, apde.etl)

#### STEP 1: Connect to SQL DATABASES ####
#key_set("inthealth_edw_prod", username = "shernandez@kingcounty.gov") #Only run this each time password is changed
interactive_auth <- FALSE
prod <- TRUE
db_claims <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)
dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
keyring::key_list() #Confirm you have a key set for hhsaw and inthealth_edw_prod on this machine


#### STEP 2: Copy tables from HHSAW claims schema ####

#Specify tables to be copied from claims schema of HHSAW
tables_to_copy_from_hhsaw_claims <- data.frame(
    from_schema = ("claims"),
    from_table = c("ref_ccw_lookup",
                   "ref_geo_county_code_wa",
                   "ref_geo_kc_zip",
                   "ref_kc_claim_type_crosswalk",
                   "ref_moll_preg_endpoint",
                   "ref_moll_trimester",
                   "ref_pc_visit_oregon",
                   "ref_date"),
    to_schema = ("stg_claims"),
    to_table = c("ref_ccw_lookup",
                 "ref_geo_county_code_wa",
                 "ref_geo_kc_zip",
                 "ref_kc_claim_type_crosswalk",
                 "ref_moll_preg_endpoint",
                 "ref_moll_trimester",
                 "ref_pc_visit_oregon",
                 "ref_date"),
    stringsAsFactors = FALSE # prevents character columns in a data frame to be automatically converted into factors
    )

#Run command
system.time(apde.etl::table_duplicate(
  conn_from = db_claims,
  conn_to = dw_inthealth,
  server_to = "inthealth_edw_prod",
  db_to = "inthealth_edw",
  table_df = tables_to_copy_from_hhsaw_claims,
  confirm_tables = TRUE,
  delete_table = TRUE
))


#### Step 3: Copy tables from HHSAW ref schema ####

#Specify tables to be copied from ref schema of HHSAW
tables_to_copy_from_hhsaw_ref <- as.data.frame(
  list(
    from_schema = c("ref"),
    from_table = c("icdcm_codes", "rda_value_sets_apde", "ndc_codes"),
    to_schema = c("stg_claims"),
    to_table = c("ref_icdcm_codes", "ref_rda_value_sets_apde", "ref_ndc_codes")
  )
)

#Run command
system.time(apde.etl::table_duplicate(
  conn_from = db_claims,
  conn_to = dw_inthealth,
  server_to = "inthealth_edw_prod",
  db_to = "inthealth_edw",
  table_df = tables_to_copy_from_hhsaw_ref,
  confirm_tables = TRUE,
  delete_table = TRUE
))


#### Step 4: Copy Onpoint APCD ref tables from INTHEALTH_EDW to HHSAW ####

#Generate vector of all Onpoint APCD ref tables
onpoint_ref_tables <- DBI::dbListTables(dw_inthealth, schema = "stg_claims")
onpoint_ref_tables <- onpoint_ref_tables[grepl("^ref_apcd", onpoint_ref_tables)]

tables_to_copy_from_inthealth_edw <- data.frame(
  from_schema = ("stg_claims"),
  from_table = onpoint_ref_tables,
  to_schema = ("claims"),
  to_table = onpoint_ref_tables,
  stringsAsFactors = FALSE # prevents character columns in a data frame to be automatically converted into factors
)

#Run command
system.time(apde.etl::table_duplicate(
  conn_from = dw_inthealth,
  conn_to = db_claims,
  server_to = "hhsaw",
  db_to = "hhs_analytics_workspace",
  table_df = tables_to_copy_from_inthealth_edw,
  confirm_tables = TRUE,
  delete_table = TRUE
))