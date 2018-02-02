###############################################################################
# Eli Kern
# 2018-1-31
# APDE
# Code to return a demographic subset of the Medicaid pop for a specific time period

###############################################################################


##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(RODBC) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(car) # used to recode variables
library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(RecordLinkage) # used to clean up duplicates in the data
library(phonics) # used to extract phonetic version of names

##### Set date origin #####
origin <- "1970-01-01"

##### Define global useful functions #####

##### Connect to the servers #####
#db.claims51 <- odbcConnect("PHClaims51")
db.apde51 <- odbcConnect("PH_APDEStore51")

##### Load user-defined functions for Medicaid data #####
source("analysis/Elig cohort function/mcaid_cohort_function.R")

##### Bring in joined elig_overall and elig_dob table using SQL stored procedure for testing #####
#Note extra "\" to escape "\" in front of "KERNELI" - this is only required in R, not in SQL
#Function mcaid_cohort_f takes the following parameters:
  #begin: begin date for medicaid coverage period, input as "YYYY-MM-DD", use quotation marks
  #end: end date for medicaid coverage period, same input guidelines as begin date
  #covmin: mininum coverage required, enter as number on percent scale (0 - 100)
  #agemin: minimum age for cohort, input as number, if left blank will be set to 0 years as default
  #agemax: maximum age for cohort, input as number, if left blank will be set to 200 years as default
  #gender and race: input as 0 (no) or 1 (yes), all vars are alone or in combination EVER
  #geographic vars: enter as comma-separated values with no spaces, e.g. "east,seattle,north"

ptm01 <- proc.time() # Times how long this query takes
mcaid_cohort <- sqlQuery(
  db.apde51,
  mcaid_cohort_f(begin = "2017-01-01", end = "2017-06-30", agemin = 18, agemax = 64, male = 1, black = 1, covmin = 50,
                 zip = "98103,98105"),
  stringsAsFactors = FALSE
  )
proc.time() - ptm01






