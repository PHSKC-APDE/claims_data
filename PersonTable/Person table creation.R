###############################################################################
# Code to create a cleaned person table from the Medicaid eligibility data
# 
# Alastair Matheson (PHSKC-APDE)
# 2016-07-21
###############################################################################


##### Notes #####
# Any manipulation in R will not carry over to the SQL tables unless uploaded to the SQL server



##### Set up global parameter and call in libraries #####
options(max.print = 700, scipen = 0)

library(RODBC) # used to connect to SQL server
library(readxl) # used to read in Excel files
library(car) # used to recode variables
library(stringr) # used to manipulate string variables
library(dplyr) # used to manipulate data


##### Connect to the servers #####
db.claims <- odbcConnect("PHClaims")
db.apde <- odbcConnect("PH_APDESTRE51")


##### Bring in all the relevant eligibility data #####
# Bring in all eligibility data
ptm01 <- proc.time() # Times how long this query takes (~240 secs)
elig <-
  sqlQuery(
    db.claims,
    "SELECT CAL_YEAR AS 'year', MEDICAID_RECIPIENT_ID AS 'id', HOH_ID AS 'hhid', SOCIAL_SECURITY_NMBR AS 'ssn',
    FIRST_NAME AS 'fname', MIDDLE_NAME AS 'mname', LAST_NAME AS 'lname', GENDER AS 'gender',
    RACE1  AS 'race1', RACE2 AS 'race2', RACE3 AS 'race3', RACE4 AS 'race4', HISPANIC_ORIGIN_NAME AS 'hispanic',
    BIRTH_DATE AS 'dob', CTZNSHP_STATUS_NAME AS 'citizenship', INS_STATUS_NAME AS 'immigration',
    SPOKEN_LNG_NAME AS 'langs', WRTN_LNG_NAME AS 'langw', FPL_PRCNTG AS 'fpl', PRGNCY_DUE_DATE AS 'duedate',
    RAC_CODE AS 'RACcode', RAC_NAME AS 'RACname', FROM_DATE AS 'fromdate', TO_DATE AS 'todate',
    covtime = DATEDIFF(dd,FROM_DATE, CASE WHEN TO_DATE > GETDATE() THEN GETDATE() ELSE TO_DATE END),
    END_REASON AS 'endreason', COVERAGE_TYPE_IND AS 'coverage', DUAL_ELIG AS 'dualelig',
    ADRS_LINE_1 AS 'add1', ADRS_LINE_2 AS 'add2', CITY_NAME AS 'city', POSTAL_CODE AS 'zip', COUNTY_CODE AS 'cntyfips',
    COUNTY_NAME AS 'cntyname'
    FROM dbo.vEligibility
    ORDER BY MEDICAID_RECIPIENT_ID, FROM_DATE DESC, TO_DATE DESC",
    stringsAsFactors = FALSE
  )
proc.time() - ptm01

# Make a copy of the dataset to avoid having to reread it
elig.bk <- elig


##### Items to resolve for deduplication #####
# 1) Multiple SSNs per Medicaid ID
# 2) Address cleaning and resolve formatting issues
# 3) Decide what to do with addresses that indicate the person is homeless
# 4) Inconsistent demographics per Med ID or SSN
# 5) Overlapping coverage periods per Med ID
# 6) Multiple addresses per Med ID during the same coverage period

# Other areas still to be addressed
# 1) RAC code and Medicaid programs
# 2) Identifying pregnant women
# 3) Filling in missing rows for FPL, citizenship/immigration status, and languages


##### Data cleaning #####
# The purpose is to ensure that each ID + SSN combo only represents one person
# Demographics can then be ascribed to each ID + SSN combo


#### SSN ####
# Goal: Find IDs with >1 SSN and figure out if they are separate people
elig <- elig %>%
  group_by(id) %>%
  mutate(ssn_tot = n_distinct(ssn, na.rm = FALSE),
         # Set up a shortened last name to account for hyphenated names that don't match
         lname_trunc = str_sub(lname, 1, 4)) %>%
  group_by(id, ssn) %>%
  mutate(ssn_cnt = n()) %>%
  ungroup()


# Dealing with multiple SSNs
# Look at names and DOB to see if there is a match. If so, take the most common SSN
ssn.tmp <- elig %>%
  filter(!is.na(ssn)) %>%
  select(id, ssn, ssn_tot, ssn_cnt, dob, fname, lname_trunc) %>%
  distinct(id, ssn, dob, fname, lname_trunc, .keep_all = TRUE) %>%
  arrange(id, ssn_cnt, dob, fname, lname_trunc) %>%
  group_by(id, dob, fname, lname_trunc) %>%
  # where there is a tie, the first SSN is selected, which is an issue if the data are sorted differently
  slice(which.max(ssn_cnt)) %>%
  ungroup() %>%
  select(id, ssn, dob, fname, lname_trunc)

# Merge back with the primary data and update SSN
elig <- left_join(elig, ssn.tmp, by = c("id", "fname", "lname_trunc", "dob"))
rm(ssn.tmp) # remove temp data frames to save memory

# Make new variable with cleaned up SSN
elig <- mutate(elig, ssnnew = ifelse(!is.na(ssn.y), ssn.y, ssn.x))



#### City ####
# Goal: Tidy up city name spellings

# Bring in city lookup table
city.lookup <- read_excel("H:/My Documents/Medicaid claims/MedicaidRProj/PersonTable/City Lookup.xlsx")

# Set up variables for writing
elig <- mutate(elig, citynew = NA, f = "")
city.lookup$numfound <- 0

# Use lookup table
for(i in 1:nrow(city.lookup)) {
  found.bools = str_detect(elig$city, city.lookup$ICU_regex[i]) #create bool array from regex
  elig$f[found.bools] = "*"
  elig$citynew[found.bools] = city.lookup$city[i]  # reassign from bool array
  city.lookup$numfound[i] = sum(found.bools) # save how many we found
}
# Report out progress
cat("Finished: Updated",
    sum(!is.na(elig$citynew)),
    "of",
    nrow(elig),
    "\n")
# Pass the unchanged cities through
elig$citynew[is.na(elig$citynew)] = elig$city[is.na(elig$citynew)]


#### Addresses (NB. a separate geocoding process may fill in some gaps later) ####
# Goal: Tidy up and standardize addresses
elig <- elig %>%
  mutate(
    # Set up homeless, PO Box, and C/O variables first to avoid capturing non-permanent residential 
    # addresses (e.g., relatives and temporary shelters)
    homeless = ifelse(
      str_detect(paste(add1, add2, city, sep = ""), "HOMELESS") == TRUE,
      1,
      0
    ),
    # Need spaces to avoid falsely capturing Boxley Pl and other similarly named roads
    pobox = ifelse(
      str_detect(paste(add1, add2, sep = ""), "[:space:]BOX[:space:]") == TRUE,
      1,
      0
    ),
    careof = ifelse(
      str_detect(paste(add1, add2, sep = ""), "C/O") == TRUE,
      1,
      0
    ),
    # Looks to see if the 2nd address field starts with a numeral.
    # If so, use 2nd address as the primary address line, else 
    # look to see if the 1st address field starts with a numeral.
    # If so, use 1st address as the primary address line, else
    # if both address fields start with non-numeric chracters, concatenate both fields into
    # the primary arress line
    street = ifelse(
      str_detect(add2, "^[:digit:]") == TRUE & !is.na(add2),
      add2,
      ifelse(
        (str_detect(add2, "^[^:digit:]") | is.na(add2)) &
          str_detect(add1, "^[:digit:]"),
        add1,
        ifelse(
          str_detect(add2, "^[^:digit:]") & str_detect(add1, "^[^:digit:]"),
          paste(add1, add2, sep = ","),
          NA
        )
      )
    ),
    # Remove temporary or relative's addresses that have been erroneously assigned
    street = replace(street, which(homeless == 1 | pobox == 1 | careof == 1), NA),
    # Strip out apartment numbers etc. (works only if they are after the street address)
    street2 = str_sub(street,
                      1,
                      ifelse(
                        is.na(str_locate(
                          street, "[:space:]*APT|#|UNIT|TRLR|STE|SUITE"
                        )[, 1]),-1,
                        str_locate(street, "[:space:]*APT|#|UNIT|TRLR|STE|SUITE")[, 1] - 1
                      )),
    # Clear white space at each end to make matches more accurate
    street2 = str_trim(street2, side = c("both")),
    # (Optional) Remove addresses with no apparent street number and those with blank addresses
    street2 = replace(street2, which(str_detect(street2, "^[:alpha:]") == TRUE | 
                                       street2 == ""), NA)
  )


#### Find most common address ####
# When coverage periods overlap, we need to select a single address to assign to a person
# The code below identifies the most common address for each person
elig <- elig %>%
  group_by(id, ssnnew, street2, city, zip) %>%
  mutate(add_cnt = n()) %>%
  ungroup()



#### Gender ####
# Goal: Find IDs with >1 recorded gender
# NB. This fails if attempting to complete all operations in one command
elig <- mutate(elig,
               gendernew = car::recode(gender, c("'Female' = 1; 'Male' = 2; 'Unknown' = NA")))
  # Count the number of genders recorded for an individual
  elig <- elig %>%
  group_by(id, ssnnew) %>%
  mutate(gender_tot = n_distinct(gendernew, na.rm = TRUE)) %>%
  ungroup() %>%
  # Replace multiple genders as 3
  mutate(gendernew = replace(gendernew, which(gender_tot > 1), 3))



#### Race ####
# Goals: Rationalize discordant race entries and fill in missing races

# Collapse initial groups
elig <- elig %>%
  mutate(race1new = ifelse(race1 %in% c("American Indian", "Alaskan Native"), "AIAN", 
                         ifelse(race1 %in% c("Hawaiian", "Pacific Islander"), "NHPI",
                                race1)),
         race2new = ifelse(race2 %in% c("American Indian", "Alaskan Native"), "AIAN", 
                         ifelse(race2 %in% c("Hawaiian", "Pacific Islander"), "NHPI",
                                race2)),
         race3new = ifelse(race3 %in% c("American Indian", "Alaskan Native"), "AIAN", 
                         ifelse(race3 %in% c("Hawaiian", "Pacific Islander"), "NHPI",
                                race3)),
         race4new = ifelse(race4 %in% c("American Indian", "Alaskan Native"), "AIAN", 
                         ifelse(race4 %in% c("Hawaiian", "Pacific Islander"), "NHPI",
                                race4))
  )

# Clean up NAs
elig <- elig %>%
  mutate(race1new = replace(race1new, which(race1new == "Not Provided"), NA),
         race2new = replace(race2new, which(race2new == "Not Provided"), NA),
         race3new = replace(race3new, which(race3new == "Not Provided"), NA),
         race4new = replace(race4new, which(race4new == "Not Provided"), NA)
         )

# Remove duplicated race fields
elig <- elig %>%
  mutate(race2new = replace(race2new, which(race2new == race1new), NA),
         race3new = replace(race3new, which(race3new == race1new), NA),
         race3new = replace(race3new, which(race3new == race2new), NA),
         race4new = replace(race4new, which(race4new == race1new), NA),
         race4new = replace(race4new, which(race4new == race2new), NA),
         race4new = replace(race4new, which(race4new == race3new), NA)
         )

# Set up AIAN and NHPI fields
elig <- elig %>%
  mutate(aian = ifelse(str_detect(
    paste(race1new, race2new, race3new, race4new, sep = ""), "AIAN"
  ) == TRUE,
  1,
  0),
  nhpi = ifelse(str_detect(
    paste(race1new, race2new, race3new, race4new, sep = ""), "NHPI"
  ) == TRUE,
  1,
  0))

# Set up single race code with Hispanic and identify multiple races in a single row
elig <- elig %>%
  mutate(
    raceh = race1new,
    raceh = replace(raceh, which(hispanic == "HISPANIC"), "Hispanic"),
    raceh = replace(raceh, which(
      !is.na(race2new) | !is.na(race3new) | !is.na(race4new)
    ), "Multiple")
  )


# Identify people with >1 race codes
# NB. This fails if attempting to complete all operations in one command
elig <- elig %>%
  mutate(racehnum = car::recode(
    raceh,
    c(
      "'AIAN' = 1; 'Asian' = 2; 'Black' = 3;
      'Hispanic' = 4; 'NHPI' = 5; 'White' = 6;
      'Multiple' = 7; 'Other' = 8"
    )
    ))


# Count the number of different race/ethnicities each person has and
# record whether at any time they were recorded as AIAN or NHPI
elig <- elig %>%
  group_by(id, ssnnew) %>%
  mutate(race_tot = n_distinct(racehnum, na.rm = TRUE),
         aian_cnt = sum(aian == 1),
         nhpi_cnt = sum(nhpi == 1)) %>%
  ungroup()


# Replace multiple races as multiple
elig <-
  mutate(elig, raceh = replace(raceh, which(race_tot > 1), "Multiple"),
         racehnum = replace(racehnum, which(race_tot > 1), "7"))


# Identify a person's race category to fill in NAs
race.tmp <- elig %>%
  filter(!is.na(raceh)) %>%
  select(id, ssnnew, raceh, racehnum, aian_cnt, nhpi_cnt) %>%
  distinct(id, ssnnew, raceh, racehnum) # This assumes that each person only has one race variable,
  # which should be the case after the above code is run
  
  
# Merge back with the primary data and rename variables
elig <- left_join(elig, race.tmp, by = c("id", "ssnnew"))
elig <- elig %>%
  mutate(aian = ifelse(aian_cnt > 0, 1, 0),
         nhpi = ifelse(nhpi_cnt > 0, 1, 0)) %>%
  select(-(raceh.x), -(racehnum.x), -(aian_cnt), -(nhpi_cnt)) %>%
  rename(raceh = raceh.y, racehnum = racehnum.y)
rm(race.tmp) # remove temp data frames to save memory

  
  

#### COVERAGE PERIOD ####
# Goal: Make one line per person per coverage period and address (ignoring RAC codes for now)
# NB. All scenarios are looking within an id + ssn combo and ignore RAC code
# Scenario 1: Rows with duplicate year + from/to dates + address = remove duplicates
# Scenario 2: From/to dates and addresses are identical across years = take most recent year
# Scenario 3: From/to dates the same but different addresses = take most common address
# Scenario 4: Year < current year and todate is 2099-12-31 = change todate to be <year>-12-31
# Scenario 5: Year = current year and todate is 2099-12-31 = change todate to be today's date
# Scenario 6: From date and address identical but different to date = take most recent to date
#             (NB. consider scenario 4 and 5 first)
# Scenario 7: From/to dates within bounds of another row's from/to date, regardless of address
#             = remove row  (NB. consider scenario 4 and 5 first) 
# Scenario 8: Different addresses but overlapping from/to dates = Adjust *to date* of address with 
#             earliest *from date* to be the *from date - 1* of the other row 
#             (e.g., Address 1, from date: 2013-06-01, to date: 2015-05-30 and
#                    Address 2, from date: 2014-02-01, to date: 2016-01-31 becomes
#                    Address 1, from date: 2013-06-01, to date: 2014-01-31)
#             (NB. consider scenario 4 and 5 first)



# Steps:
# 1)  Arrange data and remove rows with duplicate year + from/to dates + address
# 2)  When from/to dates and addresses are identical across years, take the most recent year
# 3)  Recode to dates of 2099-12-31 to be <year>-12-31
# 4)  When there are identical coverage periods, pick the most common address
# 5)  Find rows with from/to dates that sit completely within preceding row and removes them
# 6)  Find adjacent rows with the same from date + address and takes the largest to date
# 7)  Rewrite from/to dates so that continuous coverage is on a single row
# 8)  Sort from/to dates so that addresses are intermingled
# 9)  Remove rows and truncate dates so that each address occupies a single time period
#     (this give priority to the existing address)



### Step 1) Arrange data and remove rows with duplicate year + from/to dates + address
# Order by coverage dates
elig <- arrange(elig, id, ssnnew, year, fromdate, todate)

# Make from and to dates date variables
elig <- mutate(elig, fromdate = as.Date(fromdate),
               todate = as.Date(todate))

# Remove rows with duplicate year + from/to dates + address
elig <- distinct(elig, id, ssnnew, year, fromdate, todate, street2, city, zip, .keep_all = TRUE)


### Step 2) When from/to dates and addresses are identical across years, take the most recent year

# It looks like when a coverage period spans >1 year, a new record is created for each year that is all or partially covered.
# Consolidate rows that are duplicated within a single year at first
# NB. There seem to be multiple coverage types (FFS or MC) over the same period
#     so this is not being considered for now
# RAC code is also being ignored for now
elig <- elig %>%
  group_by(id, ssnnew, fromdate, todate, street2, city, zip) %>%
  slice(which.max(year)) %>%
  ungroup()


### Step 3) Recode to dates of 2999-12-31 to be <year>-12-31
elig <- elig %>%
  mutate(
    todate =
      if_else(
        todate == "2999-12-31",
        as.Date(paste(year, "-12-31", sep = ""), origin = "1970-01-01"),
        todate
      )
  )



### Step 4) When there are identical coverage periods, pick the most common address
# If there are identical coverage periods (can also add in RAC codes), select the most common address and drop other rows
elig <- elig %>%
  arrange(id, ssnnew, year, fromdate, todate, add_cnt, street2, city, zip) %>%
  group_by(id, ssnnew, year, fromdate, todate) %>%
  # where there is a tie, the first address is selected, which is an issue if the data are sorted differently
  slice(which.max(add_cnt)) %>%
  ungroup()


### Step 5) Find rows with from/to dates that sit completely within preceding row and removes them

# Order by address (ignore RACcode for now)
elig <- elig %>%
  arrange(id, ssnnew, street2, city, zip, fromdate, todate)


# The loop runs until there are no more adjacent periods like this
# (this works on smaller test data but not here, so run each iteration manually until the # of rows remains constant)
repeat {
  dfsize <-  nrow(elig)
  elig <- elig %>%
    group_by(id, ssnnew, street2, city, zip) %>%
    mutate(drop = ifelse((fromdate > lag(fromdate, 1) &
                            todate <= lag(todate, 1)) &
                           !is.na(lag(fromdate, 1)) &
                           !is.na(lag(todate, 1)),
                         1,
                         0
    )) %>%
    ungroup() %>%
    filter(drop == 0)
  dfsize2 <- nrow(elig)
  if (dfsize2 == dfsize) {
    break
  }
}


### Step 6) Find adjacent rows with the same from date + address and takes the largest to date
# The loop repeats until there are no more adjacent periods like this
# Also slow to run so maybe conduct each iteration manually
repeat {
  dfsize <-  nrow(elig)
  elig <- elig %>%
    group_by(id, ssnnew, street2, city, zip) %>%
    filter(!(fromdate == lead(fromdate, 1) &
               todate <= lead(todate, 1)) |
             is.na(lead(fromdate, 1)) | is.na(lead(todate, 1)))
  dfsize2 <- nrow(elig)
  if (dfsize2 == dfsize) {
    break
  }
}

elig <- ungroup(elig)


### Step 7) Rewrite from/to dates so that continuous coverage is on a single row
# Check to see the subsequent row's from date is <=  or immediately following the current to date
elig <- elig %>%
  arrange(id, ssnnew, street2, city, zip, fromdate, todate) %>%
  group_by(id, ssnnew, street2, city, zip) %>%
  mutate(overlap = if_else(todate + 1 >= lead(fromdate, 1) &
                             !is.na(lead(fromdate, 1)) &
                             !is.na(lead(todate, 1)),
                           1,
                           0)) %>%
  ungroup()


# Figure out how many overlapping rows to look down to find the new todate and
# identify which rows to drop
# NB. These operations do not to be done on grouped variables, which speeds things up

# This function was adapted from here: 
# http://stackoverflow.com/questions/5012516/count-how-many-consecutive-values-are-true
cumul_ones <- function(x)  {
  rl <- rle(x)
  len <- rl$lengths
  v <- rl$values
  cumLen <- cumsum(len)
  z <- x
  # replace the 0 at the end of each zero-block in z by the 
  # negative of the length of the preceding 1-block....
  iDrops <- c(0, diff(v)) < 0
  z[ cumLen[ iDrops ] ] <- -len[ c(iDrops[-1],FALSE) ]
  # ... to ensure that the cumsum below does the right thing.
  # We zap the cumsum with x so only the cumsums for the 1-blocks survive:
  x*cumsum(z)
}

elig <- elig %>%
  arrange(id, ssnnew, street2, city, zip, desc(fromdate), desc(todate)) %>%
  mutate(
    overlap_num =
      cumul_ones(overlap)) %>%
  arrange(id, ssnnew, street2, city, zip, fromdate, todate) %>%
  mutate(selector = 1:nrow(elig) + overlap_num,
         drop = if_else(selector == lag(selector, 1) &
                          !is.na(lag(selector)),
                        1,
                        0)
  )


# Replace the current row's todate with that of the futherest overlapping row
# Use the row defined by the selector variable above and drop the others
elig <- elig %>%
  mutate(todatenew = todate[selector]) %>%
  filter(drop == 0) %>%
  select(-(selector))


### Step 8) Sort from/to dates so that addresses are intermingled
elig <- elig %>%
  arrange(id, ssnnew, fromdate, todatenew, street2, city)


### Step 9) Remove rows with from/to dates newly enclosed within preceding rows and
#            truncate from dates so that each address occupies a single time period
# NB. This approach gives preference to a person's existing address

# Remove addresses with dates fully contained within another address's dates
repeat {
  dfsize <-  nrow(elig)
  elig <- elig %>%
    group_by(id, ssnnew) %>%
    mutate(drop = ifelse((fromdate > lag(fromdate, 1) &
                            todatenew <= lag(todatenew, 1)) &
                           !is.na(lag(fromdate, 1)) &
                           !is.na(lag(todatenew, 1)),
                         1,
                         0
    )) %>%
    ungroup() %>%
    filter(drop == 0)
  dfsize2 <- nrow(elig)
  if (dfsize2 == dfsize) {
    break
  }
}

# Truncate from dates so that each address occupies a single time period
elig <- elig %>%
  group_by(id, ssnnew) %>%
  mutate(fromdatenew = if_else(fromdate <= lag(todatenew, 1) &
                                 !is.na(lag(todatenew, 1)),
                               lag(todatenew, 1) + 1,
                               fromdate)) %>%
  ungroup()


#### Save cleaned person table ####
elig <- elig %>%
  select(id:dob, fromdate:cntyname, ssnnew, citynew, homeless:gendernew, race1new:racehnum,
         todatenew, fromdatenew)

ptm02 <- proc.time() # Times how long this query takes
sqlDrop(db.apde, "dbo.medicaidPerTbl1")
sqlSave(db.apde, elig, tablename = "dbo.medicaidPerTbl1", varTypes = c(fromdate = "Date", 
                                                                       fromdatenew = "Date",
                                                                       todate = "Date",
                                                                       todatenew = "Date"))
proc.time() - ptm02

