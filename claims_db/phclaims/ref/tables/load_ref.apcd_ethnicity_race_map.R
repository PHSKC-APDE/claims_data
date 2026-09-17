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

#2024-03-22 updates:
#Wrote table to HHSAW using dbWriteTable instead of YAML file approach
#Added Cuban to ethnicity-race map

##2026-02 updates:
#Added key ring and revised location of the files

#8/14/26 Eli updated to use apde.etl package

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, RCurl, configr, glue, Microsoft365R, apde.etl)

# Connect to HHSAW
interactive_auth <- FALSE
prod <- TRUE
db_claims <- apde.etl::create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)

## Connect to Cross-Sector Data SP site

# Clear all current tokens
AzureAuth::clean_token_directory(confirm = FALSE)

#to set SharePoint keyring
#keyring::key_set("sharepoint", username = "shernandez@kingcounty.gov")

# Connect to a named MS TEAMS site
team_site <- get_sharepoint_site(
  site_url = "https://kc1.sharepoint.com/teams/DPH-APDE-Healthcare-Data-InternalOpsSharedChannel",
  tenant = "kingcounty.gov",
  username = keyring::key_list("sharepoint")$username,
  password = keyring::key_get("sharepoint", keyring::key_list("sharepoint")$username),
  auth_type = "resource_owner")

team_folder <- team_site$
  get_drive("Documents")$
  get_item("APCD")$
  get_item("References")

team_folder$list_items()

apcd_race_map_csv <- team_folder$get_item("apcd_ethnicity_race_mapping.csv")$load_dataframe()

## Load data to HHSAW
system.time(dbWriteTable(db_claims, name = DBI::Id(schema = "claims", table = "ref_apcd_ethnicity_race_map"), 
             value = as.data.frame(apcd_race_map_csv), 
             overwrite = T))

message("Table loaded to HHSAW")