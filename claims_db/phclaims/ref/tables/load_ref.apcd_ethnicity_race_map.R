##Code to create and load data to ref.apcd_ethnicity_race_map
##Lookup table for mapping APCD ethnicity to race
##Reference: https://www.nap.edu/catalog/12696/race-ethnicity-and-language-data-standardization-for-health-care-quality
##Table E-1
##Eli Kern (PHSKC-APDE)
##2019-10

##2022-02-18 updates:
#updated to use new create_table and load_table_from_file functions on apde repo
#updated yaml file to match current parameter synatx
#updates ethnicity_race map Excel file to add one new ethnicity code

#2024-03-22 update: Wrote table to HHSAW

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, RCurl, configr, glue, Microsoft365R)

#SQL loading functions developed by APDE
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/load_table_from_file.R")

#Connect to HHSAW
interactive_auth <- FALSE
prod <- TRUE
db_claims <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)

##Connect to Cross-Sector Data SP site

#Clear all current tokens
AzureAuth::clean_token_directory(confirm = FALSE)

#Connect to a named MS TEAMS site
team_site <- get_team(
  team_name = "DPH-KCCross-SectorData",
  tenant = "kingcounty.gov",
  username = keyring::key_list("sharepoint")$username,
  password = keyring::key_get("sharepoint", keyring::key_list("sharepoint")$username),
  auth_type = "resource_owner")


##New approach - load data to R and write to SQL using dbWriteTable (like ref.icdcm_codes table)

##Old code to modify
create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.apcd_ethnicity_race_map.yaml",
               overall = T,
               ind_yr = F,
               overwrite = T,
               server = "KCITSQLPRPENT40")

load_table_from_file(conn = db_claims, 
                       config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.apcd_ethnicity_race_map.yaml",
                       overall = T,
                       ind_yr = F,
                       server = "KCITSQLPRPENT40",
                       drop_index = F)