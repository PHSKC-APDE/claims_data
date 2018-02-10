###############################################################################
# Eli Kern
# 2018-2-2

# Code to create a SQL table dbo.mcaid_elig_dual which a Medicare-Medicaid dual eligibility flag by ID and time period
# Calendar month year values are used to create new from and to dates
# Data elements: id, from and to dates, dual flag

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
#db.claims50 <- odbcConnect("PHClaims50")
db.claims51 <- odbcConnect("PHClaims51")
#db.apde <- odbcConnect("PH_APDEStore50")
#db.apde51 <- odbcConnect("PH_APDEStore51")

##### Bring in Medicaid eligibility data  #####
#Note to bring in test subset of Medicaid data, insert "top 100000" between SELECT and z.FIRST COLUMN
ptm01 <- proc.time() # Times how long this query takes - 172 sec
elig_dual <- sqlQuery(
  db.claims51,
  " select distinct y.MEDICAID_RECIPIENT_ID as 'id', y.CLNDR_YEAR_MNTH as 'calmo', y.DUAL_ELIG as 'dual'
    FROM (
    select z.MEDICAID_RECIPIENT_ID, z.CLNDR_YEAR_MNTH, z.DUAL_ELIG
      FROM [PHClaims].[dbo].[NewEligibility] as z
    ) as y",
  stringsAsFactors = FALSE
  )
proc.time() - ptm01

##### Clean up from/to dates #####

#Convert calendar month to calendar start and end dates for interval overlap comparison
elig_dual <- elig_dual %>%
  mutate(
    calstart = ymd(paste(as.character(calmo), "01", sep = "")),
    calend = ymd(paste(as.character(calmo), days_in_month(ymd(paste(as.character(calmo), "01", sep = ""))), sep = ""))
  )

##### Collapse contiguous time periods when dual flag does not change #####

#Find duplicated dual flags by ID (just for browsing to make sure collapsing worked)
elig_dual.tmp <- elig_dual %>%
  group_by(id, dual) %>%
  mutate(
    dual_cnt = n()
  ) %>%
  ungroup()

#Group contiguous date ranges with same dual flag
elig_dual_final <- elig_dual %>%
  #sort dataset by ID, dual flag and from date to set up for comparing each time window to preceding
  arrange(id, dual, calstart) %>%
  mutate(
    #use lag function to compare each row's from date to the prior row's to date, if difference is 1 day they are contiguous and crit = TRUE
    #default = 1 ignores the 1st row
    crit = calstart - lag(calend, default = 1) == 1, 
    #crit value of FALSE flags each break in contiguous time periods and advances cum sum to next integer
    gr = cumsum(crit == FALSE)) %>%
  #group rows by group #, id, and dual flag
  group_by(gr, id, dual) %>% 
  #collapse data set to contiguous time intervals for each ID - dual combo
  summarise(from_date = min(calstart), to_date = max(calend)) %>%
  ungroup()

#Drop group variable
elig_dual_final <- select(elig_dual_final, id, from_date, to_date, dual)

##### Save dob.mcaid_elig_dual to SQL server 51 #####
#This took 47 mins for 1,189,580 obs with 4 variables
#sqlDrop(db.claims51, "dbo.mcaid_elig_dual") # Commented out because not always necessary
ptm03 <- proc.time() # Times how long this query take
sqlSave(
  db.claims51,
  elig_dual_final,
  tablename = "dbo.mcaid_elig_dual",
  rownames = FALSE,
  fast = TRUE,
  varTypes = c(
    id = "Varchar(255)",
    from_date = "Date",
    to_date = "Date"
  )
)
proc.time() - ptm03














