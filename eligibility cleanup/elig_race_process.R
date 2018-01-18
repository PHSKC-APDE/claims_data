###############################################################################
# Eli Kern
# 2018-1-17

# Code to create a SQL table dbo.mcaid_elig_race which holds Medicaid member race/ethnicity by ID and time period
# Use to select a race/ethnicity-based cohort for a given data range
# Data elements: ID, alone or in combination race/ethnicity flags, from date, to date 

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
#Note to bring in test subset of Medicaid data, insert "top 100000" between SELECT and z.FIRST COLUMN
ptm01 <- proc.time() # Times how long this query takes - 172 sec
elig_race <- sqlQuery(
  db.claims51,
  " select distinct y.CLNDR_YEAR_MNTH as calmo, y.MEDICAID_RECIPIENT_ID as id, y.RACE1 as race1, y.RACE2 as race2, 
      y.RACE3 as race3, y.RACE4 as race4, y.HISPANIC_ORIGIN_NAME as hispanic
    FROM (
    select top 100000 z.CLNDR_YEAR_MNTH, z.MEDICAID_RECIPIENT_ID, z.RACE1, z.RACE2, z.RACE3, z.RACE4, z.HISPANIC_ORIGIN_NAME
      FROM [PHClaims].[dbo].[NewEligibility] as z
    ) as y",
  stringsAsFactors = FALSE
  )
proc.time() - ptm01

##### Convert calendar month to calendar start and end dates for interval overlap comparison #####
elig_race <- elig_race %>%
  mutate(
    calstart = ymd(paste(as.character(calmo), "01", sep = "")),
    calend = ymd(paste(as.character(calmo), days_in_month(ymd(paste(as.character(calmo), "01", sep = ""))), sep = ""))
  )

##### Set race to UPPERCASE #####
elig_race <- elig_race %>%
  mutate_at(
    vars(race1:hispanic),
    toupper
  )

#### Set NOT PROVIDED and OTHER race to null ####
nullrace_txt <- c("NOT PROVIDED", "OTHER")

elig_race <- elig_race %>%
  mutate_at(
    vars(race1:hispanic),
    str_replace, pattern = paste(nullrace_txt, collapse = '|'), replacement = NA_character_
  )

#### Create alone or in combination race variables ####

aian_txt <- c("ALASKAN NATIVE", "AMERICAN INDIAN")
black_txt <- c("BLACK")
asian_txt <- c("ASIAN")
nhpi_txt <- c("HAWAIIAN", "PACIFIC ISLANDER")
white_txt <- c("WHITE")
latino_txt <- c("^HISPANIC$")

elig_race$aian <- rowSums(sapply(elig_race[c("race1", "race2", "race3", "race4")], function(x) str_detect(x, paste(aian_txt, collapse = '|'))), na.rm = TRUE)
elig_race$asian <- rowSums(sapply(elig_race[c("race1", "race2", "race3", "race4")], function(x) str_detect(x, asian_txt)), na.rm = TRUE)
elig_race$black <- rowSums(sapply(elig_race[c("race1", "race2", "race3", "race4")], function(x) str_detect(x, black_txt)), na.rm = TRUE)
elig_race$nhpi <- rowSums(sapply(elig_race[c("race1", "race2", "race3", "race4")], function(x) str_detect(x, paste(nhpi_txt, collapse = '|'))), na.rm = TRUE)
elig_race$white <- rowSums(sapply(elig_race[c("race1", "race2", "race3", "race4")], function(x) str_detect(x, white_txt)), na.rm = TRUE)
elig_race$latino <- rowSums(sapply(elig_race[c("hispanic")], function(x) str_detect(x, latino_txt)), na.rm = TRUE)

#As the same race can sometimes be listed more than once across the race variables, replace all sums > 1 with 1
elig_race <- elig_race %>%
  mutate_at(
    vars(aian:latino),
    funs(ifelse(.>1, 1, .))
  )

##Replace race vars with NA if race1 is NA, latino with NA if hispanic is NA

#Function to replace 1 variable with NA if a 2nd variable is NA
na_check <- function(x, y) {
    ifelse(is.na(x), NA, y)
}
#sapply(elig_race[c("aian")], function(x) na_check(elig_race$race1, x))


elig_race <- elig_race %>%
  mutate_at(
    vars(aian, asian, black, nhpi, white, other),
    funs(na_check(elig_race$race1, .))
  ) %>%
  mutate_at(
    vars(latino),
    funs(na_check(elig_race$hispanic, .))
  )

##### Collapse contiguous time periods when race does not change #####

#Find multiple races by ID (just for browsing to make sure collapsing worked)
elig_race_mult.tmp <- elig_race %>%
  distinct(id, race1) %>%
  count(id)

#Group contiguous date ranges with same racial makeup
elig_race_final <- elig_race %>%
  #sort dataset by ID, from date to set up for comparing each time window to preceding
  arrange(id, aian, asian, black, nhpi, white, latino, calstart) %>%
  mutate(
    #use lag function to compare each row's from date to the prior row's to date, if difference is 1 day they are contiguous and crit = TRUE
    #default = 1 ignores the 1st row
    crit = calstart - lag(calend, default = 1) == 1, 
    #crit value of FALSE flags each break in contiguous time periods and advances cum sum to next integer
    gr = cumsum(crit == FALSE)) %>%
  #group rows by group #, id, and all address fields
  group_by(gr, id, aian, asian, black, nhpi, white, latino) %>% 
  #collapse data set to contiguous time intervals for each ID - gender combo
  summarise(calstart = min(calstart), calend = max(calend)) %>%
  ungroup()

#Drop group variable
elig_race_final <- select(elig_race_final, id, calstart, calend, aian:latino)

##### Save dob.mcaid_elig_race to SQL server 51 #####
#This took XX mins for XX obs with 9 variables
#sqlDrop(db.claims51, "dbo.mcaid_elig_race") # Commented out because not always necessary
ptm03 <- proc.time() # Times how long this query take
sqlSave(
  db.claims51,
  elig_race_final,
  tablename = "dbo.mcaid_elig_race",
  rownames = FALSE,
  fast = TRUE,
  varTypes = c(
    id = "Varchar(255)",
    calstart = "Date",
    calend = "Date"
  )
)
proc.time() - ptm03














