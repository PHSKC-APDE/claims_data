###############################################################################
# Eli Kern
# 2018-1-8

# Code to assign a single address to each individual and time period based on elig_address_clean
# Code to assign ZIP-based regions to Medicaid members

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

##### Bring in calendar month field from SQL eligibility table to find maximum month (to use later) #####
elig_calmo <- sqlQuery(
  db.claims51,
  " select distinct CLNDR_YEAR_MNTH as calmo
      FROM [PHClaims].[dbo].[NewEligibility]",
  stringsAsFactors = FALSE
)

#Convert calendar month to calendar date for later use
elig_calmo <- elig_calmo %>%
  mutate(caldate = ymd(paste(as.character(calmo), days_in_month(ymd(paste(as.character(calmo), "01", sep = ""))), sep = "")))

##### Bring in Medicaid eligibility data for linking addresses by time period to clean address table processing #####
#Note to bring in test subset of Medicaid data, insert "top 50000" between SELECT and z.FIRST COLUMN
ptm01 <- proc.time() # Times how long this query takes - 172 sec
elig_address <- sqlQuery(
  db.claims51,
  " select distinct y.CLNDR_YEAR_MNTH as calmo, y.MEDICAID_RECIPIENT_ID as id, y.FROM_DATE as 'from', y.TO_DATE as 'to', y.RSDNTL_ADRS_LINE_1 as add1, 
      y.RSDNTL_ADRS_LINE_2 as add2, y.RSDNTL_CITY_NAME as city, y.RSDNTL_STATE_CODE as state, 
	    y.RSDNTL_POSTAL_CODE as zip, y.RSDNTL_COUNTY_CODE as cntyfips, y.RSDNTL_COUNTY_NAME as cntyname
    FROM (
    select z.CLNDR_YEAR_MNTH, z.MEDICAID_RECIPIENT_ID, z.FROM_DATE, z.TO_DATE, z.RSDNTL_ADRS_LINE_1, z.RSDNTL_ADRS_LINE_2, z.RSDNTL_CITY_NAME, 
      z.RSDNTL_STATE_CODE, z.RSDNTL_POSTAL_CODE, z.RSDNTL_COUNTY_CODE, z.RSDNTL_COUNTY_NAME
      FROM [PHClaims].[dbo].[NewEligibility] as z
    ) as y",
  stringsAsFactors = FALSE
  )
proc.time() - ptm01

##### Clean up from/to dates #####

#Convert calendar month to calendar start and end dates for interval overlap comparison
elig_address <- elig_address %>%
  mutate(
    calstart = ymd(paste(as.character(calmo), "01", sep = "")),
    calend = ymd(paste(as.character(calmo), days_in_month(ymd(paste(as.character(calmo), "01", sep = ""))), sep = ""))
  )

##Convert from and to address dates to date format and set to max elig date for ongoing coverage

#Query maximum calendar month from elig table
max <- as.Date(max(elig_calmo$caldate), origin = origin)

#Set to date equal to maximum elig calendar month if 2999
elig_address <- elig_address %>%
  
  mutate(
    from_add_tmp = as.Date(from),
    to_add_tmp = as.Date(ifelse(year(as.Date(to)) == 2999, max, as.Date(to)), origin = origin)
  )

#Create final to/from fields for address using following logic
  #from OR to address date after clndr_year_mnth start date AND before clndr_year_mnth end date -> use from/to address dates
  #from address date before clndr_year_mnth start date OR after clndr_year_mnth end date -> use clndr_year_mnth dates

elig_address <- elig_address %>%
  
  mutate(
    from_add = as.Date(ifelse(from_add_tmp > calstart & from_add_tmp < calend, from_add_tmp, calstart), origin = origin), 
    to_add = as.Date(ifelse(to_add_tmp > calstart & to_add_tmp < calend, to_add_tmp, calend), origin = origin)
  )

##### Bring in cleaned address table (created by Alastair) and join original address data to this #####
ptm02 <- proc.time() # Times how long this query takes
elig_address_clean <- sqlQuery(
  db.claims51,
  " select *
    from PHClaims.dbo.elig_address_clean",
  stringsAsFactors = FALSE
)
proc.time() - ptm02

#Strip dates and IDs to get distinct addresses in original data for linking to clean address table
elig_address_only <- distinct(select(elig_address,add1,add2,city,state,zip,cntyfips,cntyname))

#Merge original address data with clean address table
#Note that if only one match is found in right-hand table, then # of rows should rename the same
elig_address.tmp <- left_join(elig_address_only, elig_address_clean, by = c("add1","add2","city","state","zip","cntyfips","cntyname"))
#Addresses that have "NA" in add1 and add2 may find more than 1 match in clean address table, thus take distinct of old address fields to remove dups
elig_address.tmp <- distinct(elig_address.tmp, add1, add2, city, state, zip, cntyfips, cntyname, .keep_all = TRUE)

#Merge back with IDs and from and to dates
elig_address <- left_join(elig_address,elig_address.tmp, by = c("add1","add2","city","state","zip","cntyfips","cntyname"))
rm(elig_address_only,elig_address.tmp)

#Drop old address information and from/to dates
elig_address <- select(elig_address,id,from_add,to_add,add1_new,add2_new,city_new,state_new,zip_new,cntyfips_new,cntyname_new,confidential,homeless,mailbox,care_of,overridden)

##### Bring in ZIP-based region definitions #####
geo_file <- "//dchs-shares01/dchsdata/DCHSPHClaimsData/References/Geographic definitions/APDE_geographic definitions.xlsx"
regdef <- read.xlsx(xlsxFile = geo_file, sheet = "RegionZIP102017")

#Merge to address data
elig_address <- left_join(elig_address,regdef, by = c("zip_new" = "zip"))
#count(elig_address,kcreg_zip)

##### Collapse contiguous time periods when address does not change #####

#Find duplicated addresses by ID (just for browsing to make sure collapsing worked)
elig_address.tmp <- elig_address %>%
  group_by(id, add1_new, add2_new, city_new, zip_new, cntyfips_new, cntyname_new, confidential, homeless, mailbox, care_of, overridden, kcreg_zip) %>%
  mutate(
    add_cnt = n()
  ) %>%
  ungroup()

#Group contiguous date ranges with same address
elig_address_final <- elig_address %>%
  #sort dataset by ID, address fields and from date to set up for comparing each time window to preceding
  arrange(id, add1_new, add2_new, city_new, state_new, zip_new, cntyfips_new, cntyname_new, confidential,
          homeless, mailbox, care_of, overridden, kcreg_zip, from_add) %>%
  mutate(
    #use lag function to compare each row's from date to the prior row's to date, if difference is 1 day they are contiguous and crit = TRUE
    #default = 1 ignores the 1st row
    crit = from_add - lag(to_add, default = 1) == 1, 
    #crit value of FALSE flags each break in contiguous time periods and advances cum sum to next integer
    gr = cumsum(crit == FALSE)) %>%
  #group rows by group #, id, and all address fields
  group_by(gr, id, add1_new, add2_new, city_new, state_new, zip_new, cntyfips_new, cntyname_new, confidential,
           homeless, mailbox, care_of, overridden, kcreg_zip) %>% 
  #collapse data set to contiguous time intervals for each ID - address combo
  summarise(from_add = min(from_add), to_add = max(to_add)) %>%
  ungroup()


#Drop group variable
elig_address_final <- select(elig_address_final, id, from_add, to_add, add1_new, add2_new, city_new, state_new, zip_new, cntyfips_new, cntyname_new, confidential, 
                      homeless, mailbox, care_of, overridden, kcreg_zip)

##### Save dob.elig_address to SQL server 51 #####
#This took XX mins
#sqlDrop(db.claims51, "dbo.elig_address") # Commented out because not always necessary
ptm03 <- proc.time() # Times how long this query take
sqlSave(
  db.claims51,
  elig_address_final,
  tablename = "dbo.elig_address",
  rownames = FALSE,
  fast = TRUE,
  varTypes = c(
    id = "Varchar(255)",
    from_add = "Date",
    to_add = "Date"
  )
)
proc.time() - ptm03

rm(elig_address_clean, regdef)














