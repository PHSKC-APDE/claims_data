#### CODE TO SET UP OR UPDATE ADDRESS_CLEAN TABLES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-04


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, scipen = 999)

library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(sf) # Read shape files

db_apde <- dbConnect(odbc(), "PH_APDEStore51")
db_claims <- dbConnect(odbc(), "PHClaims51")

geocode_path <- "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Geocoding"
source("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
source("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")


#### INITAL ADDRESS_CLEAN SETUP ####
# Load straight from stage to ref


### Create SQL table
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_clean.yaml",
               overall = T, ind_yr = F)


load_table_from_sql_f(conn = db_claims, 
                      config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.address_clean.yaml",
                      truncate = T, truncate_date = F)



#### SUBSEQUENT ADDRESS_CLEAN SETUP ####
# QA check?
# Append/union stage.address_clean to ref.address_clean
# Add metadata?

### Create SQL table
create_table_f(conn = db_claims51, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.address_clean.yaml",
               overall = T, ind_yr = F)

# MORE CODE TO COME

