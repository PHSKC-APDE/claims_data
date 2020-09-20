##Code to create and load data to ref.provider_nppes_load
##Lookup table for provider NPIs and other information
##Reference: https://download.cms.gov/nppes/NPI_Files.html
##Eli Kern (PHSKC-APDE)
##2020-09


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, RCurl, configr, glue)

db_claims <- dbConnect(odbc(), "PHClaims51")

#SQL loading functions developed by APDE
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")

#create table shell
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.provider_nppes_load.yaml",
               overall = T,
               ind_yr = F,
               overwrite = T,
               test_mode = T)

#Load data from file
load_table_from_file_f(conn = db_claims, 
                       config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.provider_nppes_load.yaml",
                       overall = T,
                       ind_yr = F,
                       test_mode = T) ##test mode

#Add last_run datetime
dbSendQuery(conn = db_claims51, 
            "alter table phclaims.tmp.ref_provider_nppes_load
            add column datetime not null default(getdate());")

