###############################################################################
# Eli Kern
# 2018-1-16

# Code to create a SQL table elig_gender which holds Medicaid member gender by ID and time period
# Use to select a gender-based cohort for a given data range
# Data elements: ID, gender, from date, to date 

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

##### Connect to SQL servers #####
db.claims51 <- odbcConnect("PHClaims51")

##### Bring in Medicaid eligibility data for linking addresses by time period to clean address table processing #####
#Note to bring in test subset of Medicaid data, insert "top 50000" between SELECT and z.FIRST COLUMN
ptm01 <- proc.time() # Times how long this query takes - 172 sec
elig_gender <- sqlQuery(
  db.claims51,
  " select distinct y.CLNDR_YEAR_MNTH as calmo, y.MEDICAID_RECIPIENT_ID as id, y.GENDER as gender
    FROM (
    select z.CLNDR_YEAR_MNTH, z.MEDICAID_RECIPIENT_ID, z.GENDER
      FROM [PHClaims].[dbo].[NewEligibility] as z
    ) as y",
  stringsAsFactors = FALSE
  )
proc.time() - ptm01

##### Convert calendar month to calendar start and end dates for interval overlap comparison #####
elig_gender <- elig_gender %>%
  mutate(
    calstart = ymd(paste(as.character(calmo), "01", sep = "")),
    calend = ymd(paste(as.character(calmo), days_in_month(ymd(paste(as.character(calmo), "01", sep = ""))), sep = ""))
  )

##### Set gender to UPPERCASE #####
elig_gender <- elig_gender %>%
  mutate_at(
    vars(gender),
    toupper
  )

##### Collapse contiguous time periods when gender does not change #####

#Find multiple genders by ID (just for browsing to make sure collapsing worked)
elig_gender_mult.tmp <- elig_gender %>%
  distinct(id, gender) %>%
  count(id)

#Group contiguous date ranges with same gender
elig_gender_final <- elig_gender %>%
  #sort dataset by ID, from date to set up for comparing each time window to preceding
  arrange(id, gender, calstart) %>%
  mutate(
    #use lag function to compare each row's from date to the prior row's to date, if difference is 1 day they are contiguous and crit = TRUE
    #default = 1 ignores the 1st row
    crit = calstart - lag(calend, default = 1) == 1, 
    #crit value of FALSE flags each break in contiguous time periods and advances cum sum to next integer
    gr = cumsum(crit == FALSE)) %>%
  #group rows by group #, id, and all address fields
  group_by(gr, id, gender) %>% 
  #collapse data set to contiguous time intervals for each ID - gender combo
  summarise(calstart = min(calstart), calend = max(calend)) %>%
  ungroup()

#Drop group variable
elig_gender_final <- select(elig_gender_final, id, calstart, calend, gender)

##### Save dob.gender to SQL server 51 #####
#This took 47 mins for 1,177,881 obs with 4 variables
#sqlDrop(db.claims51, "dbo.elig_gender") # Commented out because not always necessary
ptm03 <- proc.time() # Times how long this query take
sqlSave(
  db.claims51,
  elig_gender_final,
  tablename = "dbo.elig_gender",
  rownames = FALSE,
  fast = TRUE,
  varTypes = c(
    id = "Varchar(255)",
    calstart = "Date",
    calend = "Date"
  )
)
proc.time() - ptm03














