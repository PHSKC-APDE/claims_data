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

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, RCurl, configr, glue)

db_claims <- dbConnect(odbc(), "PHClaims51")

#SQL loading functions developed by APDE
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/load_table_from_file.R")

create_table(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.apcd_ethnicity_race_map.yaml",
               overall = T,
               ind_yr = F,
               overwrite = T,
               server = "KCITSQLUTPDBH51")

load_table_from_file(conn = db_claims, 
                       config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.apcd_ethnicity_race_map.yaml",
                       overall = T,
                       ind_yr = F,
                       server = "KCITSQLUTPDBH51",
                       drop_index = F)