#### MASTER CODE TO RUN A FULL MEDICAID DATA REFRESH
#
# Eli Kern, PHSKC (APDE)
#
# 2019-06


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

library(tidyverse) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code


db_claims <- dbConnect(odbc(), "PHClaims51")

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_sql.R")


#### archive.apcd_cmsdrg_output_multi_ver ####
create_table_f(
  conn = db_claims, 
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/create_archive.apcd_cmsdrg_output_multi_ver.yaml",
  overall = T, ind_yr = F, overwrite = T)

system.time(load_table_from_sql_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/load_archive.apcd_cmsdrg_output_multi_ver.yaml", 
  truncate = T, truncate_date = F))

#### archive.apcd_dental_claim ####
create_table_f(
  conn = db_claims, 
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/create_archive.apcd_dental_claim.yaml",
  overall = T, ind_yr = F, overwrite = T)

system.time(load_table_from_sql_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/load_archive.apcd_dental_claim.yaml", 
  truncate = T, truncate_date = F))

#### archive.apcd_eligibility ####
create_table_f(
  conn = db_claims, 
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/create_archive.apcd_eligibility.yaml",
  overall = T, ind_yr = F, overwrite = T)

system.time(load_table_from_sql_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/load_archive.apcd_eligibility.yaml", 
  truncate = T, truncate_date = F))

#### archive.apcd_inpatient_stay_summary_ltd ####
create_table_f(
  conn = db_claims, 
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/create_archive.apcd_inpatient_stay_summary_ltd.yaml",
  overall = T, ind_yr = F, overwrite = T)

system.time(load_table_from_sql_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/load_archive.apcd_inpatient_stay_summary_ltd.yaml", 
  truncate = T, truncate_date = F))

#### archive.apcd_medical_claim_header ####
create_table_f(
  conn = db_claims, 
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/create_archive.apcd_medical_claim_header.yaml",
  overall = T, ind_yr = F, overwrite = T)

system.time(load_table_from_sql_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/load_archive.apcd_medical_claim_header.yaml", 
  truncate = T, truncate_date = F))

#### archive.apcd_medical_crosswalk ####
create_table_f(
  conn = db_claims, 
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/create_archive.apcd_medical_crosswalk.yaml",
  overall = T, ind_yr = F, overwrite = T)

system.time(load_table_from_sql_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/load_archive.apcd_medical_crosswalk.yaml", 
  truncate = T, truncate_date = F))

#### archive.apcd_member_month_detail ####
create_table_f(
  conn = db_claims, 
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/create_archive.apcd_member_month_detail.yaml",
  overall = T, ind_yr = F, overwrite = T)

system.time(load_table_from_sql_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/load_archive.apcd_member_month_detail.yaml", 
  truncate = T, truncate_date = F))

#### archive.apcd_pharmacy_claim ####
create_table_f(
  conn = db_claims, 
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/create_archive.apcd_pharmacy_claim.yaml",
  overall = T, ind_yr = F, overwrite = T)

system.time(load_table_from_sql_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/load_archive.apcd_pharmacy_claim.yaml", 
  truncate = T, truncate_date = F))

#### archive.apcd_provider ####
create_table_f(
  conn = db_claims, 
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/create_archive.apcd_provider.yaml",
  overall = T, ind_yr = F, overwrite = T)

system.time(load_table_from_sql_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/load_archive.apcd_provider.yaml", 
  truncate = T, truncate_date = F))

#### archive.apcd_provider_master ####
create_table_f(
  conn = db_claims, 
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/create_archive.apcd_provider_master.yaml",
  overall = T, ind_yr = F, overwrite = T)

system.time(load_table_from_sql_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/load_archive.apcd_provider_master.yaml", 
  truncate = T, truncate_date = F))

#### archive.apcd_provider_practice_roster ####
create_table_f(
  conn = db_claims, 
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/create_archive.apcd_provider_practice_roster.yaml",
  overall = T, ind_yr = F, overwrite = T)

system.time(load_table_from_sql_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/load_archive.apcd_provider_practice_roster.yaml", 
  truncate = T, truncate_date = F))

#### archive.apcd_medical_claim ####
create_table_f(
  conn = db_claims, 
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/create_archive.apcd_medical_claim.yaml",
  overall = T, ind_yr = F, overwrite = T)

system.time(load_table_from_sql_f(
  conn = db_claims,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/archive/tables/load_archive.apcd_medical_claim.yaml", 
  truncate = T, truncate_date = F))



