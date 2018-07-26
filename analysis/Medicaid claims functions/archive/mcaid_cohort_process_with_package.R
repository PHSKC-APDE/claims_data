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
library(medicaid) #Used to build SQL query to return Medicaid member cohort
library(odbc) # Used to connect to SQL server

##### Set date origin #####
origin <- "1970-01-01"

##### Connect to the servers #####
db.claims51 <- dbConnect(odbc(), "PHClaims51")

##### Request Medicaid eligibility cohort #####
system.time(mcaid_cohort <- dbGetQuery(db.claims51,
  mcaid_cohort_f(from_date = "2017-01-01", to_date = "2017-12-31")  
))