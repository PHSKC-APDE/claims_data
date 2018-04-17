###############################################################################
# Eli Kern
# 2018-2-27
# APDE
# Script to send a SQL query to the SQL server to return a Medicaid eligibility cohort with specified parameters
###############################################################################

##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(dplyr) # Used to manipulate data
library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(RODBC) # Used to connect to SQL server

##### Set date origin #####
origin <- "1970-01-01"

##### Connect to the servers #####
db.apde51 <- odbcConnect("PH_APDEStore51")

##### Load user-defined functions for Medicaid data #####
source("analysis/Medicaid cohort function/mcaid_cohort_function.R")

##### Request Medicaid eligibility cohort #####
#Refer to Readme file on GitHub for instructions on how to use this function#
#https://github.com/PHSKC-APDE/Medicaid/tree/master/analysis/Medicaid%20cohort%20function

ptm01 <- proc.time() # Times how long this query takes
mcaid_cohort <- sqlQuery(
  db.apde51,
  mcaid_cohort_f(from_date = "2017-01-01", to_date = "2017-06-30"),
  stringsAsFactors = FALSE
  )
proc.time() - ptm01