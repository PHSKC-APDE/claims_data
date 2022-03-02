##Code to create and load data to ref.geo_county_code_wa
##Lookup table for mapping WA county number to FIPS county codes
##Eli Kern (PHSKC-APDE)
##2019-10


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, RCurl, configr, glue)

db_claims <- dbConnect(odbc(), "PHClaims51")
git_path <- "H:/my documents/GitHub"

#SQL loading functions developed by APDE
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/load_table.R")

create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.geo_county_code_wa.yaml",
               overall = T,
               ind_yr = F,
               overwrite = T,
               test_mode = F)

load_table_from_file_f(conn = db_claims, 
                       config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.geo_county_code_wa.yaml",
                       overall = T,
                       ind_yr = F,
                       test_mode = F)
