# Shuva Dawadi
#adapted code from Susan H and Eli Kern
# APDE, PHSKC
# 2020-1-27
#

#### Import Medicare data from csv files, --2017 bcarrier claims AND line  ####

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
db.claims51 <- dbConnect(odbc(), "PHClaims51")

#### STEP 2: Create table shell ####
create_table_f(conn = db.claims51, 
               config_file = file.path("C:/Users/sdawadi/Desktop/Mcare_yaml","create_load_raw_mcare_bcarrier_claims_17.yaml"),
               overall = T, ind_yr = F, test_mode = F)





#### STEP 3: Load data from CSV ####
load_table_from_file_f(conn = db.claims51, 
                       config_file = file.path("C:/Users/sdawadi/Desktop/Mcare_yaml","load_raw_mcare_bcarrier_claims_17.yaml"),
                       overall = T, ind_yr = F, test_mode = F)

#16,623,411 rows copied.
#Clock Time (ms.) Total: 821,531 Average : (20234.67 rows per sec.)



#####################################
#####################################
#####################################
##bcarrier  line file##
###########################


#### STEP 2: Create table shell ####
create_table_f(conn = db.claims51, 
               config_file = file.path("C:/Users/sdawadi/Desktop/Mcare_yaml","create_load_raw_mcare_bcarrier_line_k_17.yaml"),
               overall = T, ind_yr = F, test_mode = F)





#### STEP 3: Load data from CSV ####
load_table_from_file_f(conn = db.claims51, 
                       config_file = file.path("C:/Users/sdawadi/Desktop/Mcare_yaml","load_raw_mcare_bcarrier_line_k_17.yaml"),
                       overall = T, ind_yr = F, test_mode = F)



##31,511,529 rows copied.
##Clock Time (ms.) Total: 1,848,656 Average : (17045.64 rows per sec.)
