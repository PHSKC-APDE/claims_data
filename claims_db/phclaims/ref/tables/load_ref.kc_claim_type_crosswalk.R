##Code to create and load data to ref.kc_claim_type_crosswalk
##Crosswalk between King County claim type variable and ProviderOne, Medicare and WA-APCD
##Eli Kern (PHSKC-APDE)
##2019-4-26


#### Load libraries, functions, and connect to servers ####
options(max.print = 350, tibble.print_max = 50, scipen = 999)
origin <- "1970-01-01" # Date origin
library(odbc) # Work with SQL server
library(devtools)
library(configr)
library(glue)
git_path <- "H:/my documents/GitHub"

#SQL loading functions developed by APDE
source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")

#Connect to PHClaims 51 Server
db.claims51 <- dbConnect(odbc(), "PHClaims51")


#### STEP 1: Create table shell ####
create_table_f(conn = db.claims51, 
               config_file = file.path(git_path, "claims_data/claims_db/phclaims/ref/tables", "create_ref.kc_claim_type_crosswalk.yaml"),
               overall = T, ind_yr = F, test_mode = F)

#### STEP 2: Load data from CSV ####
load_table_from_file_f(conn = db.claims51, 
                       config_file = file.path(git_path, "claims_data/claims_db/phclaims/ref/tables", "load_ref.kc_claim_type_crosswalk.yaml"),
                       overall = T, ind_yr = F, test_mode = F)
