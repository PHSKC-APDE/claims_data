###############################################################################
# Eli Kern
# 2018-1-17

# Code to create a SQL table elig_race which holds Medicaid member race/ethnicity by ID and time period
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
      y.RACE3 as race3, y.RACE4 as race4, y.HISPANIC_ORIGIN_NAME as latino
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
    vars(race1:latino),
    toupper
  )

#### Set NOT PROVIDED to null ####
elig_race <- elig_race %>%
  mutate_at(
    vars(race1:latino),
    str_replace, pattern = "NOT PROVIDED", replacement = NA_character_
  )

#### Create alone or in combination race variables ####

aian_txt <- c("ALASKAN NATIVE", "AMERICAN INDIAN")
black_txt <- c("BLACK")

elig_race <- elig_race %>%
  
  mutate(
    aian = ifelse(str_detect(race1, paste(aian_txt, collapse = '|')), 1, 0)
  )

elig_race <- elig_race %>%
  
  mutate (
    aian = ifelse(str_detect(race1, paste(aian_txt, collapse = '|')) | str_detect(!is.na(race2), paste(aian_txt, collapse = '|'))
      | str_detect(!is.na(race3), paste(aian_txt, collapse = '|')) | str_detect(!is.na(race4), paste(aian_txt, collapse = '|')), 1, 0),
    
    black = ifelse(str_detect(race1, black_txt) | str_detect(!is.na(race2), black_txt) | str_detect(!is.na(race3), black_txt)
      | str_detect(!is.na(race4), black_txt), 1, 0)
  )


temp <- slice(elig_race, 80:90)

temp <- temp %>%
  
  mutate(
    black = ifelse(str_detect(race1, black_txt) | str_detect(race2, black_txt) | str_detect(race3, black_txt)
                   | str_detect(race4, black_txt), 1, 0)
  ) 

# 3 ways to get similar output with apply functions
lapply(temp[8:9], function(x) max(x))
sapply(temp[8:9], function(x) max(x))
mapply(function(x) max(x), temp[8:9]) 

lapply(temp[c("race1", "race2", "race3", "race4")], function(x) str_detect(x, black_txt))
lapply(temp[c("race1", "race2", "race3", "race4")], function(x) sum(str_detect(x, black_txt), na.rm = TRUE))
z <- data.frame(sapply(temp[c("race1", "race2", "race3", "race4")], function(x) str_detect(x, black_txt)))

#this works!
x <- z %>%
  rowwise %>%
  mutate(
    test = sum(race1, race2, race3, race4, na.rm = TRUE)
  )

#try in base R (simplest)
temp$test <- rowSums(sapply(temp[c("race1", "race2", "race3", "race4")], function(x) str_detect(x, black_txt)), na.rm = TRUE)





















##### Collapse contiguous time periods when race does not change #####

#Find multiple races by ID (just for browsing to make sure collapsing worked)
elig_race_mult.tmp <- elig_race %>%
  distinct(id, race1) %>%
  count(id)

#Group contiguous date ranges with same race
elig_race_final <- elig_race %>%
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
elig_race_final <- select(elig_race_final, id, calstart, calend, gender)

##### Save dob.elig_race to SQL server 51 #####
#This took 47 mins for 1,177,881 obs with 4 variables
#sqlDrop(db.claims51, "dbo.elig_race") # Commented out because not always necessary
ptm03 <- proc.time() # Times how long this query take
sqlSave(
  db.claims51,
  elig_race_final,
  tablename = "dbo.elig_race",
  rownames = FALSE,
  fast = TRUE,
  varTypes = c(
    id = "Varchar(255)",
    calstart = "Date",
    calend = "Date"
  )
)
proc.time() - ptm03














