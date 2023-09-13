# Shuva Dawadi
#adapted code from Susan H and Eli Kern
# APDE, PHSKC
# 2020-1-27
#

#### Import Medicare data from csv files, -HHA years 2014-2016 ####

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
source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/create_table.R")
source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/load_table.R")



#Connect to PHClaims 51 Server
db.claims51 <- dbConnect(odbc(), "PHClaims51")

#### STEP 2: Create table shell ####
create_table_f(conn = db.claims51, 
               config_file = file.path("C:/Users/sdawadi/Desktop/Mcare_yaml","create_load_raw_macare_hha_rev_codes_14.yaml"),
               overall = T, ind_yr = F, test_mode = F)





#### STEP 3: Load data from CSV ####
load_table_from_file_f(conn = db.claims51, 
                       config_file = file.path("C:/Users/sdawadi/Desktop/Mcare_yaml","load_raw_mcare_hha_rev_codes_14.yaml"),
                       overall = T, ind_yr = F, test_mode = F)


##1,248,504 rows copied.
##Clock Time (ms.) Total: 25047  



#########################################################
#########################################################
##uploading 2015 HHA rev center to slq##
#########################################################
#########################################################


#### STEP 2: Create table shell ####
create_table_f(conn = db.claims51, 
               config_file = file.path("C:/Users/sdawadi/Desktop/Mcare_yaml","create_load_raw_macare_hha_rev_codes_15.yaml"),
               overall = T, ind_yr = F, test_mode = F)





#### STEP 3: Load data from CSV ####
load_table_from_file_f(conn = db.claims51, 
                       config_file = file.path("C:/Users/sdawadi/Desktop/Mcare_yaml","load_raw_mcare_hha_rev_codes_15.yaml"),
                       overall = T, ind_yr = F, test_mode = F)


##1,327,028 rows copied.
##Clock Time (ms.) Total: 32875  Average : (40365.87 rows per sec.)





#########################################################
#########################################################
##uploading 2016 HHA rev center to slq##
#########################################################
#########################################################


#### STEP 2: Create table shell ####
create_table_f(conn = db.claims51, 
               config_file = file.path("C:/Users/sdawadi/Desktop/Mcare_yaml","create_load_raw_macare_hha_rev_codes_16.yaml"),
               overall = T, ind_yr = F, test_mode = F)





#### STEP 3: Load data from CSV ####
load_table_from_file_f(conn = db.claims51, 
                       config_file = file.path("C:/Users/sdawadi/Desktop/Mcare_yaml","load_raw_mcare_hha_rev_codes_16.yaml"),
                       overall = T, ind_yr = F, test_mode = F)

##1443188 rows copied.
##Clock Time (ms.) Total     : 28891  Average : (49952.86 rows per sec.)
