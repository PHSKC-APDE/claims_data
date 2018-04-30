###############################################################################
# Eli Kern
# 2018-4-30
# APDE
# Script to send a SQL query to the SQL server to return a Medicaid eligibility cohort with specified parameters
# Version 1.2

#v1.2 updates:
  #Changed over from RODBC to odbc package (for consistency with other R scripts)
  #Added new mutually exclusive race and gender variables
  #Added in date of birth so that end users can calculate age for custom date
###############################################################################

##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(dplyr) # Used to manipulate data
library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(odbc) # Used to connect to SQL server

##### Set date origin #####
origin <- "1970-01-01"

##### Connect to the servers #####
db.claims51 <- dbConnect(odbc(), "PHClaims51")

##### Load user-defined functions for Medicaid data #####
source("analysis/Medicaid cohort function/mcaid_cohort_function.R")

##### Request Medicaid eligibility cohort #####
#Refer to Readme file on GitHub for instructions on how to use this function#
#https://github.com/PHSKC-APDE/Medicaid/tree/master/analysis/Medicaid%20cohort%20function

ptm01 <- proc.time() # Times how long this query takes
# Run SQL stored procedure to select Medicaid cohort
mcaid_cohort_res <- dbSendQuery(
  db.claims51,
  mcaid_cohort_f(from_date = "2017-01-01", to_date = "2017-06-30")  
  )
mcaid_cohort <- dbFetch(mcaid_cohort_res) #Save SQL server result as R data frame
dbClearResult(mcaid_cohort_res) #Clear SQL server result
proc.time() - ptm01