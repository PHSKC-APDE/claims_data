###############################################################################
# Eli Kern
# 2017-9-19

# Code to bring in eligibility data, deduplicate by SSN and ID, and count unique clients by reportable RAC or groups of RAC codes

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

##### Bring in Medicaid eligibility data for reportable RAC processing #####
ptm01 <- proc.time() # Times how long this query takes (~400 secs)
elig_rac <- sqlQuery(
  db.claims51,
  "SELECT distinct MEDICAID_RECIPIENT_ID as id, SOCIAL_SECURITY_NMBR as ssn, RAC_NAME as rac_name, RAC_CODE as rac_code, CLNDR_YEAR_MNTH as elig_month
  FROM [PHClaims].[dbo].[NewEligibility]",
  stringsAsFactors = FALSE
)
proc.time() - ptm01

#Code to find duplicated Medicaid IDs
elig_rac <- elig_rac %>%
  group_by(id) %>%
  mutate(
    id_cnt = n()
  ) %>%
  ungroup()

#### SSN and DOB cleanup ####
# Dealing with multiple SSNs
ssn.tmp <- elig_rac %>%
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
elig_rac <- left_join(elig_rac, ssn.tmp, by = c("id"))
rm(ssn.tmp) # remove temp data frames to save memory

# Make new variable with cleaned up SSN
elig_rac <- mutate(elig_rac, ssnnew = ifelse(!is.na(ssn.y), ssn.y, ssn.x))

#Filter to distinct
elig_rac <- distinct(elig_rac, id, ssnnew, rac_name, rac_code)

# Dealing with multiple DOBs
dob.tmp <- elig_dob %>%
  filter(!is.na(dob)) %>%
  select(id, dob, row_cnt) %>%
  arrange(id, row_cnt) %>%
  distinct(id, dob, .keep_all = TRUE) %>%
  group_by(id) %>%
  # where there is a tie, the first SSN is selected, which is an issue if the data are sorted differently
  # currently takes the most frequently used SSN
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

##### Create age variable using any reference date #####

#Set up local macros
refdate <- 20161231
agevar <- "age2016" ##will need to learn how to write function in R to use this, equivalent to local macro in Stata

elig_dob <- elig_dob %>%
  mutate(
    age2016 = as.integer(interval(dobnew,ymd(refdate))/years(1)),
    id = str_to_upper(id)
  )

##### Bring in elig_overall table #####
#Join to dob table in order to count # of individuals by age bands who were enrolled in 2016
ptm01 <- proc.time() # Times how long this query takes (~400 secs)
elig_overall <- sqlQuery(
  db.claims51,
  " select *
  FROM [PHClaims].[dbo].elig_overall",
  stringsAsFactors = FALSE
)
proc.time() - ptm01

##### Calculate covered days and months for any date range, need to add 1 day to include end date ##### 

#Set up local macros 
start <- 20160101
end <- 20161231
dayvar <- "cov2016_dy" ##need to use in function
movar <- "cov2016_mth" ##need to use in function
dayvar_tot <- "cov2016_dy_tot" ##need to use in function
movar_tot <- "cov2016_mth_tot" ##need to use in function

elig_overall <- elig_overall %>%
  
  mutate(
    
    #Interval
    #int_temp = lubridate::intersect(interval(ymd(20120101),ymd(20121231)),interval(ymd(startdate),ymd(enddate))),
    
    #Days
    cov2016_dy = (day(as.period(lubridate::intersect(interval(ymd(start),ymd(end)),interval(ymd(startdate),ymd(enddate))),"days"))) + 1,
    
    #Months
    cov2016_mth = round(cov2016_dy/30,digits=0)
  )

#Replace NA with 0 for covered days/months
elig_overall$cov2016_dy <- car::recode(elig_overall$cov2016_dy,"NA=0")
elig_overall$cov2016_mth <- car::recode(elig_overall$cov2016_mth,"NA=0")


##### Total coverage days and months per calendar year #####
elig_overall <- elig_overall %>%
  group_by(MEDICAID_RECIPIENT_ID) %>%
  
  mutate(
    
    #Days  
    cov2016_dy_tot = sum(cov2016_dy),
    
    #Months  
    cov2016_mth_tot = sum(cov2016_mth)
  ) %>%
  ungroup()





