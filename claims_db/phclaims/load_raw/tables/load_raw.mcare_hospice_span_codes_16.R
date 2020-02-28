# Susan Hernandez
# adapted code from Eli Kern
# APDE, PHSKC
# 2019-9-25
#

#### Import Medicare data from csv files, --Hospice Span Codes ####

#### Step 1: clear memory; Load libraries; functions; and connect to servers #########


# clean memory ----
rm(list=ls())

# load libraries ----
options(max.print = 350, tibble.print_max = 50, scipen = 999)
origin <- "1970-01-01" # Date origin

# load libraries ----
library(pacman)
pacman::p_load(odbc, devtools, configr, glue, DBI, tidyverse, sqldf, methods, tibble, claims)
devtools::install_github("PHSKC-APDE/claims_data")


#file path for yaml code
##git_path <- "" --if using git specify here and add to the file path

#SQL loading functions developed by APDE
source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")


##Disconnect and reconnect to database--
#disconnect
db.Disconnect(sql_database_conn)

#Connect to PHClaims 51 Server
db.claims51 <- dbConnect(odbc(), "PH_APDEClaims51")


#### STEP 2: Create table shell ####
create_table_f(conn = db.claims51, 
               config_file = file.path("C:/Users/shernandez/code/claims_data/claims_db/phclaims/load_raw/tables/","create_load_raw.mcare_hospice_span_codes_16.yaml"),
               overall = T, ind_yr = F, test_mode = F)

#### STEP 3: Load data from CSV ####
load_table_from_file_f(conn = db.claims51, 
                       config_file = file.path("C:/Users/shernandez/code/claims_data/claims_db/phclaims/load_raw/tables/","load_raw.mcare_hospice_span_codes_16.yaml"),
                       overall = T, ind_yr = F, test_mode =F)

#(7 vars, 859 obs)
# 859 rows copied.
#Network packet size (bytes): 4096
#Clock Time (ms.) Total     : 15     Average : (57266.67 rows per sec.)
