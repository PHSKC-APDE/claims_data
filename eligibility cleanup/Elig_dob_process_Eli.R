###############################################################################
# Eli Kern
# 2017-9-6

# Code to assign a single date of birth to Medicaid members using the eligibility data

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

#Recode variables using CAR package
recode2 <- function ( data, fields, recodes, as.factor.result = FALSE ) {
  for ( i in which(names(data) %in% fields) ) { # iterate over column indexes that are present in the passed dataframe that are also included in the fields list
    data[,i] <- car::recode( data[,i], recodes, as.factor.result = as.factor.result )
  }
  data
}

##### Connect to the servers #####
#db.claims50 <- odbcConnect("PHClaims50")
db.claims51 <- odbcConnect("PHClaims51")
#db.apde <- odbcConnect("PH_APDEStore50")
#db.apde51 <- odbcConnect("PH_APDEStore51")

##### Bring in Medicaid eligibility data for DOB processing #####
#Note to bring in test subset of Medicaid data, insert "top 50000" between SELECT and z.MEDICAID_RECIPIENT_ID

ptm01 <- proc.time() # Times how long this query takes
elig_dob <- sqlQuery(
  db.claims51,
  " select distinct y.MEDICAID_RECIPIENT_ID as id, y.SOCIAL_SECURITY_NMBR as ssn, y.BIRTH_DATE as dob, count(*) as row_cnt
	FROM (
  SELECT z.MEDICAID_RECIPIENT_ID, z.SOCIAL_SECURITY_NMBR, z.BIRTH_DATE
  FROM [PHClaims].[dbo].[NewEligibility] as z
) as y
  group by y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, y.BIRTH_DATE
  order by y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, row_cnt desc, y.BIRTH_DATE",
  stringsAsFactors = FALSE
  )
proc.time() - ptm01

#Code to find duplicated Medicaid IDs
elig_dob <- elig_dob %>%
  group_by(id) %>%
  mutate(
    id_cnt = n()
  ) %>%
  ungroup()

#Code to find different DOBs by ID-SSN sets
elig_dob <- elig_dob %>%
  group_by(id, ssn) %>%
  mutate(
    dob_cnt = n()
  ) %>%
  ungroup()

#### SSN and DOB cleanup ####
# Dealing with multiple SSNs
ssn.tmp <- elig_dob %>%
  filter(!is.na(ssn)) %>%
  select(id, ssn, row_cnt) %>%
  arrange(id, row_cnt) %>%
  distinct(id, ssn, .keep_all = TRUE) %>%
  group_by(id) %>%
  # where there is a tie, the first SSN is selected, which is an issue if the data are sorted differently
  # currently takes the most frequently used SSN
  slice(which.max(row_cnt)) %>%
  ungroup() %>%
  select(id, ssn)

# Merge back with the primary data and update SSN
elig_dob <- left_join(elig_dob, ssn.tmp, by = c("id"))
rm(ssn.tmp) # remove temp data frames to save memory

# Make new variable with cleaned up SSN
elig_dob <- mutate(elig_dob, ssnnew = ifelse(!is.na(ssn.y), ssn.y, ssn.x))
                   
#Filter to distinct
elig_dob <- distinct(elig_dob, id, ssnnew, dob, row_cnt)

# Dealing with multiple DOBs
dob.tmp <- elig_dob %>%
  filter(!is.na(dob)) %>%
  select(id, dob, row_cnt) %>%
  arrange(id, row_cnt) %>%
  distinct(id, dob, .keep_all = TRUE) %>%
  group_by(id) %>%
  # where there is a tie, the first DOB is selected, which is an issue if the data are sorted differently
  # currently takes the most frequently used DOB
  slice(which.max(row_cnt)) %>%
  ungroup() %>%
  select(id, dob)

# Merge back with the primary data and update SSN
elig_dob <- left_join(elig_dob, dob.tmp, by = c("id"))
rm(dob.tmp) # remove temp data frames to save memory

# Make new variable with cleaned up DOB
elig_dob <- mutate(elig_dob, dobnew = ymd(as.Date(ifelse(!is.na(dob.y), dob.y, dob.x))))

#Filter to distinct
elig_dob <- distinct(elig_dob, id, ssnnew, dobnew)

##### Save dob.elig_dob to SQL server 51 #####
#This took 40 min to upload, 829,000 rows x 3 variables
sqlDrop(db.claims51, "dbo.elig_dob") # Commented out because not always necessary
sqlSave(
  db.claims51,
  elig_dob,
  tablename = "dbo.elig_dob",
  rownames = FALSE,
  fast = TRUE,
  varTypes = c(
    id = "Varchar(255)",  
    ssnnew = "Varchar(255)",  
    dobnew = "Date"
  )
)
















