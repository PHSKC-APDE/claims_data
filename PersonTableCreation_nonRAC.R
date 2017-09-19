###############################################################################
# Code to create a cleaned person table from the Medicaid eligibility data
# 
# Alastair Matheson (PHSKC-APDE)
# 2016-06-10
# Last updated: 2017-01-24
###############################################################################
#eli was vandalizing here

##### Notes #####
# Any manipulation in R will not carry over to the SQL tables unless uploaded to the SQL server


##### Set up global parameter and call in libraries #####
options(max.print = 700, scipen = 0)

library(RODBC) # used to connect to SQL server
library(openxlsx) # used to read in Excel files
library(car) # used to recode variables
library(stringr) # used to manipulate string variables
library(lubridate) # used to manipulate date variables
library(dplyr) # used to manipulate data


##### Connect to the servers #####
db.claims <- odbcConnect("PHClaims")
db.apde <- odbcConnect("PH_APDEStore")
db.apde51 <- odbcConnect("PH_APDEStore51")


#### Bring in all eligibility data ####
ptm01 <- proc.time() # Times how long this query takes (~400 secs)
elig <-
  sqlQuery(
    db.claims,
    "SELECT MEDICAID_RECIPIENT_ID AS 'id', HOH_ID AS 'hhid', SOCIAL_SECURITY_NMBR AS 'ssn',
    FIRST_NAME AS 'fname', MIDDLE_NAME AS 'mname', LAST_NAME AS 'lname', GENDER AS 'gender',
    RACE1  AS 'race1', RACE2 AS 'race2', RACE3 AS 'race3', RACE4 AS 'race4', HISPANIC_ORIGIN_NAME AS 'hispanic',
    BIRTH_DATE AS 'dob', CTZNSHP_STATUS_NAME AS 'citizenship', INS_STATUS_NAME AS 'immigration',
    SPOKEN_LNG_NAME AS 'langs', WRTN_LNG_NAME AS 'langw', FPL_PRCNTG AS 'fpl', PRGNCY_DUE_DATE AS 'duedate',
    RAC_CODE AS 'RACcode', RAC_NAME AS 'RACname', FROM_DATE AS 'fromdate', TO_DATE AS 'todate',
    END_REASON AS 'endreason', COVERAGE_TYPE_IND AS 'coverage', DUAL_ELIG AS 'dualelig',
    ADRS_LINE_1 AS 'add1', ADRS_LINE_2 AS 'add2', CITY_NAME AS 'city', POSTAL_CODE AS 'zip', COUNTY_CODE AS 'cntyfips',
    COUNTY_NAME AS 'cntyname', ADR_FROM_DATE AS 'addfrom', ADR_TO_DATE AS 'addto', MBR_H_SID AS 'id2'
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
# 3) Filling in missing rows for HHID, FPL, citizenship/immigration status, and languages


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
# Originally used first name as part of match but too many slight variations
ssn.tmp <- elig %>%
  filter(!is.na(ssn)) %>%
  select(id, ssn, ssn_tot, ssn_cnt, dob, lname_trunc, fromdate, todate) %>%
  arrange(id, ssn_cnt, desc(fromdate), desc(todate), dob, lname_trunc) %>%
  distinct(id, ssn, dob, lname_trunc, .keep_all = TRUE) %>%
  group_by(id, dob, lname_trunc) %>%
  # where there is a tie, the first SSN is selected, which is an issue if the data are sorted differently
  # currently takes the most recently used SSN
  slice(which.max(ssn_cnt)) %>%
  ungroup() %>%
  select(id, ssn, dob, lname_trunc)

# Merge back with the primary data and update SSN
elig <- left_join(elig, ssn.tmp, by = c("id", "lname_trunc", "dob"))
rm(ssn.tmp) # remove temp data frames to save memory

# Make new variable with cleaned up SSN
elig <- mutate(elig, ssnnew = ifelse(!is.na(ssn.y), ssn.y, ssn.x))



#### City ####
# Goal: Tidy up city name spellings

# Bring in city lookup table
city.lookup <- read.xlsx("H:/My Documents/Medicaid claims/MedicaidRProj/PersonTable/City Lookup.xlsx", 
                         sheet = "regex", colNames = TRUE)

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
# Goal: Tidy up and standardize addresses so that they can be geocoded
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


#### Addresses for geocoding ####
# Pull out distinct addresses
today <- Sys.Date()
address <- distinct(elig, street2, citynew, zip, .keep_all = TRUE) %>%
  select(add1, add2, city, zip, street, street2, citynew) %>%
  arrange(street2, citynew, zip)
write.xlsx(address, file = 
             paste0("//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Geocoding/Distinct addresses_", today, ".xlsx"),
           col.names = TRUE)
# NB. If R throws an error about not having Rtools installed, run this command:
# Sys.setenv(R_ZIPCMD= "C:/Rtools/bin/zip")


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


# Determine if a person was recorded as Hispanic at any time
elig <- mutate(elig, hispnum = ifelse(hispanic == "HISPANIC", 1, 0)) %>%
  group_by(id, ssnnew) %>%
  mutate(hisp_cnt = sum(hispnum == 1)) %>%
  ungroup()

hisp.tmp <- elig %>%
  distinct(id, ssnnew, hisp_cnt)

elig <- left_join(elig, hisp.tmp, by = c("id", "ssnnew"))
elig <- mutate(elig, hispanic = ifelse(hisp_cnt.y > 0, "HISPANIC", "NOT HISPANIC"),
               hispnum = ifelse(hisp_cnt.y > 0, 1, 0)) %>%
  select(-(hisp_cnt.x), -(hisp_cnt.y))

rm(hisp.tmp) # remove temp data frames to save memory


# Set up single race codes (one with Hispanic and one without)
# and identify multiple races in a single row
elig <- elig %>%
  mutate(
    racem = race1new,
    racem = replace(racem, which(
      !is.na(race2new) | !is.na(race3new) | !is.na(race4new)
    ), "Multiple"),
    raceh = race1new,
    raceh = replace(raceh, which(hispanic == "HISPANIC"), "Hispanic"),
    raceh = replace(raceh, which(
      !is.na(race2new) | !is.na(race3new) | !is.na(race4new)
    ), "Multiple")
  )


# Identify people with >1 race codes
# NB. This fails if attempting to complete all operations in one command
elig <- elig %>%
  mutate(racemnum = car::recode(
    racem,
    c("'AIAN' = 1; 'Asian' = 2; 'Black' = 3;
      'Hispanic' = 4; 'NHPI' = 5; 'White' = 6;
      'Multiple' = 7; 'Other' = 8")
    ),
    racehnum = car::recode(
    raceh,
    c(
      "'AIAN' = 1; 'Asian' = 2; 'Black' = 3;
      'Hispanic' = 4; 'NHPI' = 5; 'White' = 6;
      'Multiple' = 7; 'Other' = 8")
    ))


# Count the number of different race/ethnicities each person has and
# record whether at any time they were recorded as AIAN, or NHPI
elig <- elig %>%
  group_by(id, ssnnew) %>%
  mutate(racem_tot = n_distinct(racemnum, na.rm = TRUE),
         raceh_tot = n_distinct(racehnum, na.rm = TRUE),
         aian_cnt = sum(aian == 1),
         nhpi_cnt = sum(nhpi == 1)) %>%
  ungroup()


# Replace multiple races as multiple
elig <-
  mutate(elig, racem = replace(racem, which(racem_tot > 1), "Multiple"),
         racemnum = replace(racemnum, which(racem_tot > 1), "7"),
         raceh = replace(raceh, which(raceh_tot > 1), "Multiple"),
         racehnum = replace(racehnum, which(raceh_tot > 1), "7"))


# Identify a person's race category to fill in NAs
race.tmp <- elig %>%
  filter(!is.na(racem)) %>%
  select(id, ssnnew, racem, racemnum, raceh, racehnum, aian_cnt, nhpi_cnt) %>%
  distinct(id, ssnnew, racem, racemnum, .keep_all = TRUE) # This assumes that each person only has one race variable,
  # which should be the case after the above code is run
  
  
# Merge back with the primary data and rename variables
elig <- left_join(elig, race.tmp, by = c("id", "ssnnew"))
elig <- elig %>%
  mutate(aian = ifelse(aian_cnt.y > 0, 1, 0),
         nhpi = ifelse(nhpi_cnt.y > 0, 1, 0)) %>%
  select(-(racem.x), -(racemnum.x), -(raceh.x), -(racehnum.x), 
         -(aian_cnt.x), -(aian_cnt.y), -(nhpi_cnt.x), -(nhpi_cnt.y),
         -(racem_tot), -(raceh_tot)) %>%
  rename(racem = racem.y, racemnum = racemnum.y, raceh = raceh.y, racehnum = racehnum.y)
rm(race.tmp) # remove temp data frames to save memory


#### Citizenship ####
# To come





#### COVERAGE PERIOD ####
# Goal: Make one line per person per coverage period and address (ignoring RAC codes for now)
# NB. All scenarios are looking within an id + ssn combo and ignore RAC code
# Scenario 1: Rows with duplicate year + from/to dates (elig and) + address = remove duplicates
# Scenario 2: From/to dates the same but different addresses = take most common address
#             (NB. Should not be an issue now address from/to dates available)
# Scenario 3: Todate is 2099-12-31 = change todate to be <year>-12-31
#             (NB. This includes the current year)
# Scenario 4: From date and address identical but different to date = take most recent to date
#             (NB. consider scenario 3 first)
# Scenario 5: From/to dates within bounds of another row's from/to date + address the same = 
#             adjust address to dates to fit on one row and remove other rows
#             (NB. consider scenario 3 frist)
# Scenario 6: Different addresses but overlapping from/to dates = Adjust *to date* of address with 
#             earliest *from date* to be the *from date - 1* of the other row 
#             (e.g., Address 1, from date: 2013-06-01, to date: 2015-05-30 and
#                    Address 2, from date: 2014-02-01, to date: 2016-01-31 becomes
#                    Address 1, from date: 2013-06-01, to date: 2014-01-31)
#             (NB. consider scenario 3 and should not be an issue now address from/to dates available)
# Scenario 7: Missing address from date (and usually missing address) = not currently addressed,
#             results in 190 individuals with overlapping final dates



#### Step 1) Arrange data, fix date errors, and remove rows with duplicate year + from/to dates + address ####
# Order by coverage dates
# NB. It looks like the address_to field is always NA so infer that a new address_from date means a change
elig <- arrange(elig, id, ssnnew, year, fromdate, todate, addfrom, addto)

# Convert date variables to date format
elig <- mutate_at(elig, vars(fromdate, todate, addfrom, addto),
                  funs(as.Date(.)))


# Clean up poor date quality
elig <- elig %>%
  mutate(
    todatenew = ifelse(todate == "2999-10-31", 
      as.Date(paste("2999-12-31", sep = ""), origin = "1970-01-01"), todate
    ),
    # Separate out date components in order to fix year typos
    todateyear = year(todate),
    todatemonth = month(todate),
    todateday = day(todate),
    # Fix year typos
    todatenew = ifelse(
      todateyear == "2105",
      as.Date(paste("2015", todatemonth, todateday, sep = "-"),
              origin = "1970-01-01"),
      todatenew
    ),
    # May need to manually check this is the likely typo (it is currently)
    todatenew = ifelse(
      todateyear == "2044",
      as.Date(paste("2014", todatemonth, todateday, sep = "-"),
              origin = "1970-01-01"),
      todatenew
    ),
    # May need to manually check this is the likely typo (it is currently)
    todatenew = ifelse(
      todateyear == "2045",
      as.Date(paste("2015", todatemonth, todateday, sep = "-"),
              origin = "1970-01-01"),
      todatenew
    ),
    # May need to manually check this is the likely typo (it is currently)
    todatenew = ifelse(
      todateyear == "2049",
      as.Date(paste("2015", todatemonth, todateday, sep = "-"),
              origin = "1970-01-01"),
      todatenew
    ),
    todatenew = as.Date(todatenew, origin = "1970-01-01")
  )


# Remove rows with duplicate year + from/to dates + address
elig <- distinct(elig, id, ssnnew, year, fromdate, todatenew, addfrom, addto, street2, citynew, .keep_all = TRUE)



#### Step 2) Recode todates of 2999-12-31 to be <year>-12-31 ####
elig <- elig %>%
  mutate(
    todatenew =
      if_else(
        todatenew == "2999-12-31",
        as.Date(paste(year, "-12-31", sep = ""), origin = "1970-01-01"),
        todatenew
      )
  )


#### Step 3) Clean up address from and to dates ####

# Update address from dates to match eligibility from date (if add.from < elig.from or NA)
elig <- elig %>%
  mutate(addfromnew = ifelse(addfrom < fromdate | is.na(addfrom), fromdate, addfrom),
         addfromnew = as.Date(addfromnew, origin = "1970-01-01"))

# Create address_to dates (all NA initially)
# Logic:
# 1) Set add. to date as the day before the next add. from date
# 2) If the next add. from date is missing (e.g., it is the last row for that person),
#    take the smallest of the current row's eligibility to date or the end of the current year
today <- Sys.Date()

elig <- elig %>%
  arrange(id, ssnnew, fromdate, todatenew, addfrom, street2) %>%
  group_by(id, ssnnew) %>%
  mutate(
    addto = ifelse(
      is.na(lead(addfromnew, 1)) | addfromnew >= lead(addfromnew, 1),
      pmin(as.numeric(as.Date(
        paste0(year(today), "-12-31"), origin = "1970-01-01"
      )),
      todatenew),
      lead(addfromnew, 1) - 1
    ),
    addto = as.Date(addto, origin = "1970-01-01")
  ) %>%
  ungroup()

# Remove the few rows where the address date range is outside the eligibility date range
# (usually because the eligibility to date had a typo)
elig <- filter(elig, !(addfromnew > todatenew) | is.na(addfromnew))


#### Step 4) Consolidate elig and address from/to dates within a single address first ####
# Need to do steps 4+5 within an address first otherwise some rows with overlapping
# dates will remain

# Find rows where the elig and address from/to dates fit completely within a previous row
# (for a given address)
# The loop runs until there are no more adjacent periods like this
# Make sure to order first
elig <- arrange(elig, id, ssnnew, street2, fromdate, todatenew, addfromnew, addto)

repeat {
  dfsize <-  nrow(elig)
  elig <- elig %>%
    mutate(drop = ifelse((fromdate > lag(fromdate, 1) &
                            todatenew <= lag(todatenew, 1)) &
                           !is.na(lag(fromdate, 1)) &
                           !is.na(lag(todatenew, 1)) &
                           addfromnew >= lag(addfromnew, 1) &
                           addto <= lag(addto, 1) &
                           !is.na(lag(addfromnew, 1)) &
                           !is.na(lag(addto, 1)) &
                           street2 == lag(street2, 1) &
                           id == lag(id, 1) &
                           ssnnew == lag(ssnnew, 1) &
                           !is.na(lag(id, 1)) &
                           !is.na(lag(ssnnew, 1)),
                         1,
                         0
    )) %>%
    filter(drop == 0)
  dfsize2 <- nrow(elig)
  if (dfsize2 == dfsize) {
    break
  }
}


#### Step 5) Rewrite from/to dates so that continuous coverage is on a single row for each address ####
# Consolidate dates within the same address
elig <- elig %>%
  arrange(id, ssnnew, street2, fromdate, todatenew, addfromnew, addto) %>%
  mutate(overlap = ifelse((is.na(street2) &
                             !is.na(lead(street2, 1))) |
                            (!is.na(street2) &
                               is.na(lead(street2, 1))),
                          0,
                          if_else(
                            todatenew + 1 >= lead(fromdate, 1) &
                              !is.na(lead(todatenew, 1)) &
                              !is.na(lead(fromdate, 1)) &
                              addto + 1 >= lead(addfromnew, 1) &
                              addfromnew < lead(addfromnew, 1) &
                              !is.na(lead(addto, 1)) &
                              !is.na(lead(addfromnew, 1)) &
                              (street2 == lead(street2, 1) |
                                 (is.na(street2) & is.na(lead(
                                   street2, 1)))) &
                              id == lead(id, 1) &
                              ssnnew == lead(ssnnew, 1) &
                              !is.na(lead(id, 1)) &
                              !is.na(lead(ssnnew, 1)),
                            1,
                            0
                          )
  ))


# Figure out how many overlapping rows to look down to find the new addr.todate and
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

# Apply function to count rows
elig <- elig %>%
  arrange(id, ssnnew, street2, desc(fromdate), desc(todatenew), desc(addfromnew), desc(addto)) %>%
  mutate(
    overlap_num =
      cumul_ones(overlap)) %>%
  arrange(id, ssnnew, street2, fromdate, todatenew, addfromnew, addto) %>%
  mutate(selector = 1:nrow(elig) + overlap_num,
         drop = if_else(selector == lag(selector, 1) &
                          !is.na(lag(selector)),
                        1,
                        0)
  )


# Replace the current row's todate with that of the futherest overlapping row
# Use the row defined by the selector variable above and drop the others
elig <- elig %>%
  mutate(todatenew = todatenew[selector],
         addtonew = addto[selector]) %>%
  filter(drop == 0) %>%
  select(-(selector), -(overlap), -(overlap_num))


#### Step 6) Repeat steps 4 and 5 ####
# Repetition is necessary as the first round of row deletions adjusts the adjacency of dates
# Consolidate elig and address from/to dates within a single address
elig <- arrange(elig, id, ssnnew, street2, fromdate, todatenew, addfromnew, addtonew)

repeat {
  dfsize <-  nrow(elig)
  elig <- elig %>%
    mutate(drop = ifelse((fromdate > lag(fromdate, 1) &
                            todatenew <= lag(todatenew, 1)) &
                           !is.na(lag(fromdate, 1)) &
                           !is.na(lag(todatenew, 1)) &
                           addfromnew >= lag(addfromnew, 1) &
                           addto <= lag(addto, 1) &
                           !is.na(lag(addfromnew, 1)) &
                           !is.na(lag(addto, 1)) &
                           street2 == lag(street2, 1) &
                           id == lag(id, 1) &
                           ssnnew == lag(ssnnew, 1) &
                           !is.na(lag(id, 1)) &
                           !is.na(lag(ssnnew, 1)),
                         1,
                         0
    )) %>%
    filter(drop == 0)
  dfsize2 <- nrow(elig)
  if (dfsize2 == dfsize) {
    break
  }
}

# Rewrite from/to dates so that continuous coverage is on a single row for each address
elig <- elig %>%
  arrange(id, ssnnew, street2, fromdate, todatenew, addfromnew, addtonew) %>%
  mutate(overlap = ifelse((is.na(street2) &
                             !is.na(lead(street2, 1))) |
                            (!is.na(street2) &
                               is.na(lead(street2, 1))),
                          0,
                          if_else(
                            todatenew + 1 >= lead(fromdate, 1) &
                              !is.na(lead(todatenew, 1)) &
                              !is.na(lead(fromdate, 1)) &
                              addtonew + 1 >= lead(addfromnew, 1) &
                              addfromnew < lead(addfromnew, 1) &
                              !is.na(lead(addtonew, 1)) &
                              !is.na(lead(addfromnew, 1)) &
                              (street2 == lead(street2, 1) |
                                 (is.na(street2) & is.na(lead(
                                   street2, 1)))) &
                              id == lead(id, 1) &
                              ssnnew == lead(ssnnew, 1) &
                              !is.na(lead(id, 1)) &
                              !is.na(lead(ssnnew, 1)),
                            1,
                            0
                          )
  ))

elig <- elig %>%
  arrange(id, ssnnew, street2, desc(fromdate), desc(todatenew), desc(addfromnew), desc(addtonew)) %>%
  mutate(
    overlap_num =
      cumul_ones(overlap)) %>%
  arrange(id, ssnnew, street2, fromdate, todatenew, addfromnew, addtonew) %>%
  mutate(selector = 1:nrow(elig) + overlap_num,
         drop = if_else(selector == lag(selector, 1) &
                          !is.na(lag(selector)),
                        1,
                        0)
  )

elig <- elig %>%
  mutate(todatenew = todatenew[selector],
         addtonew = addtonew[selector]) %>%
  filter(drop == 0) %>%
  select(-(selector), -(overlap), -(overlap_num))


<<<<<<< HEAD
#### Step 8) Repeat steps 4 and 5 again ####
# Copy of code in step 7. Need to turn this into a function to save space
# Consolidate elig and address from/to dates within a single address
elig <- arrange(elig, id, ssnnew, street2, fromdate, todatenew, addfromnew, addtonew)
=======
#### Step 8) Repeat steps 5 and 6 again ####
# Copy of code in step 7. Need to turn this into a function to save space
# Consolidate elig and address from/to dates within a single address
elig <- arrange(elig, id, ssnnew, street2, fromdate, todatenew, addfromnew, addtonew)

repeat {
  dfsize <-  nrow(elig)
  elig <- elig %>%
    mutate(drop = ifelse((fromdate > lag(fromdate, 1) &
                            todatenew <= lag(todatenew, 1)) &
                           !is.na(lag(fromdate, 1)) &
                           !is.na(lag(todatenew, 1)) &
                           addfromnew >= lag(addfromnew, 1) &
                           addto <= lag(addto, 1) &
                           !is.na(lag(addfromnew, 1)) &
                           !is.na(lag(addto, 1)) &
                           street2 == lag(street2, 1) &
                           id == lag(id, 1) &
                           ssnnew == lag(ssnnew, 1) &
                           !is.na(lag(id, 1)) &
                           !is.na(lag(ssnnew, 1)),
                         1,
                         0
    )) %>%
    filter(drop == 0)
  dfsize2 <- nrow(elig)
  if (dfsize2 == dfsize) {
    break
  }
}

# Rewrite from/to dates so that continuous coverage is on a single row for each address
elig <- elig %>%
  arrange(id, ssnnew, street2, fromdate, todatenew, addfromnew, addtonew) %>%
  mutate(overlap = ifelse((is.na(street2) &
                             !is.na(lead(street2, 1))) |
                            (!is.na(street2) &
                               is.na(lead(street2, 1))),
                          0,
                          if_else(
                            todatenew + 1 >= lead(fromdate, 1) &
                              !is.na(lead(todatenew, 1)) &
                              !is.na(lead(fromdate, 1)) &
                              addtonew + 1 >= lead(addfromnew, 1) &
                              addfromnew < lead(addfromnew, 1) &
                              !is.na(lead(addtonew, 1)) &
                              !is.na(lead(addfromnew, 1)) &
                              (street2 == lead(street2, 1) |
                                 (is.na(street2) & is.na(lead(
                                   street2, 1)))) &
                              id == lead(id, 1) &
                              ssnnew == lead(ssnnew, 1) &
                              !is.na(lead(id, 1)) &
                              !is.na(lead(ssnnew, 1)),
                            1,
                            0
                          )
  ))

elig <- elig %>%
  arrange(id, ssnnew, street2, desc(fromdate), desc(todatenew), desc(addfromnew), desc(addtonew)) %>%
  mutate(
    overlap_num =
      cumul_ones(overlap)) %>%
  arrange(id, ssnnew, street2, fromdate, todatenew, addfromnew, addtonew) %>%
  mutate(selector = 1:nrow(elig) + overlap_num,
         drop = if_else(selector == lag(selector, 1) &
                          !is.na(lag(selector)),
                        1,
                        0)
  )

elig <- elig %>%
  mutate(todatenew = todatenew[selector],
         addtonew = addtonew[selector]) %>%
  filter(drop == 0) %>%
  select(-(selector), -(overlap), -(overlap_num))
>>>>>>> 49f75d6eb8ee3403c926b737521041d434e69357

repeat {
  dfsize <-  nrow(elig)
  elig <- elig %>%
    mutate(drop = ifelse((fromdate > lag(fromdate, 1) &
                            todatenew <= lag(todatenew, 1)) &
                           !is.na(lag(fromdate, 1)) &
                           !is.na(lag(todatenew, 1)) &
                           addfromnew >= lag(addfromnew, 1) &
                           addto <= lag(addto, 1) &
                           !is.na(lag(addfromnew, 1)) &
                           !is.na(lag(addto, 1)) &
                           street2 == lag(street2, 1) &
                           id == lag(id, 1) &
                           ssnnew == lag(ssnnew, 1) &
                           !is.na(lag(id, 1)) &
                           !is.na(lag(ssnnew, 1)),
                         1,
                         0
    )) %>%
    filter(drop == 0)
  dfsize2 <- nrow(elig)
  if (dfsize2 == dfsize) {
    break
  }
}

# Rewrite from/to dates so that continuous coverage is on a single row for each address
elig <- elig %>%
  arrange(id, ssnnew, street2, fromdate, todatenew, addfromnew, addtonew) %>%
  mutate(overlap = ifelse((is.na(street2) &
                             !is.na(lead(street2, 1))) |
                            (!is.na(street2) &
                               is.na(lead(street2, 1))),
                          0,
                          if_else(
                            todatenew + 1 >= lead(fromdate, 1) &
                              !is.na(lead(todatenew, 1)) &
                              !is.na(lead(fromdate, 1)) &
                              addtonew + 1 >= lead(addfromnew, 1) &
                              addfromnew < lead(addfromnew, 1) &
                              !is.na(lead(addtonew, 1)) &
                              !is.na(lead(addfromnew, 1)) &
                              (street2 == lead(street2, 1) |
                                 (is.na(street2) & is.na(lead(
                                   street2, 1)))) &
                              id == lead(id, 1) &
                              ssnnew == lead(ssnnew, 1) &
                              !is.na(lead(id, 1)) &
                              !is.na(lead(ssnnew, 1)),
                            1,
                            0
                          )
  ))

elig <- elig %>%
  arrange(id, ssnnew, street2, desc(fromdate), desc(todatenew), desc(addfromnew), desc(addtonew)) %>%
  mutate(
    overlap_num =
      cumul_ones(overlap)) %>%
  arrange(id, ssnnew, street2, fromdate, todatenew, addfromnew, addtonew) %>%
  mutate(selector = 1:nrow(elig) + overlap_num,
         drop = if_else(selector == lag(selector, 1) &
                          !is.na(lag(selector)),
                        1,
                        0)
  )

elig <- elig %>%
  mutate(todatenew = todatenew[selector],
         addtonew = addtonew[selector]) %>%
  filter(drop == 0) %>%
  select(-(selector), -(overlap), -(overlap_num))


#### Step 8) Create final from and to dates that indicates coverage at that address ####
elig <- elig %>%
  mutate(fromfinal = ifelse(addfromnew > fromdate, addfromnew, fromdate),
         tofinal = ifelse(addtonew < todatenew, addtonew, todatenew),
         fromfinal = as.Date(fromfinal, origin = "1970-01-01"),
         tofinal = as.Date(tofinal, origin = "1970-01-01")) %>%
  arrange(id, ssnnew, fromfinal, tofinal, street2)


#### Step 9) Remove any duplicate rows based on final dates ####
elig <- distinct(elig, id, ssnnew, year, fromfinal, tofinal, street2, .keep_all = TRUE)


#### Step 10) Consolidate rows where final dates sit completely within preceding row ####
# Look within addresses for now

# The loop runs until there are no more adjacent periods like this
repeat {
  dfsize <-  nrow(elig)
  elig <- elig %>%
    mutate(drop = ifelse((fromfinal > lag(fromfinal, 1) &
                            tofinal <= lag(tofinal, 1)) &
                           !is.na(lag(fromfinal, 1)) &
                           !is.na(lag(tofinal, 1)) &
                           street2 == lag(street2, 1) &
                           id == lag(id, 1) &
                           ssnnew == lag(ssnnew, 1) &
                           !is.na(lag(id, 1)) &
                           !is.na(lag(ssnnew, 1)),
                         1,
                         0
    )) %>%
    filter(drop == 0)
  dfsize2 <- nrow(elig)
  if (dfsize2 == dfsize) {
    break
  }
}


#### Step 11) Find adjacent rows with the same final from_date, same address, and takes the largest to date ####
# The loop repeats until there are no more adjacent periods like this
elig <- arrange(elig, id, ssnnew, fromfinal, tofinal, street2)

repeat {
  dfsize <-  nrow(elig)
  elig <- elig %>%
    mutate(
      drop = ifelse(
        fromfinal == lead(fromfinal, 1) &
          tofinal <= lead(tofinal, 1) &
          !is.na(lead(fromfinal, 1)) &
          !is.na(lead(tofinal, 1)) &
          id == lead(id, 1) &
          ssnnew == lead(ssnnew, 1) &
          street2 == lead(street2, 1) &
          !is.na(lead(id, 1)) &
          !is.na(lead(ssnnew, 1)),
        1,
        0
      )
    ) %>%
    filter(drop == 0)
  dfsize2 <- nrow(elig)
  if (dfsize2 == dfsize) {
    break
  }
}


#### Step 12) Rewrite from/to dates so that continuous coverage is on a single row ####
elig <- elig %>%
  arrange(id, ssnnew, fromfinal, tofinal) %>%
  mutate(overlap = ifelse(((is.na(street2) &
                             !is.na(lead(street2, 1))) |
                            (!is.na(street2) &
                               is.na(lead(street2, 1)))) |
                            ((is.na(ssnnew) &
                                !is.na(lead(ssnnew, 1))) |
                               (!is.na(ssnnew) &
                                  is.na(lead(ssnnew, 1)))),
                          0,
                          if_else(
                            tofinal + 1 >= lead(fromfinal, 1) &
                              !is.na(lead(tofinal, 1)) &
                              !is.na(lead(fromfinal, 1)) &
                              (street2 == lead(street2, 1) |
                                 (is.na(street2) & is.na(lead(
                                   street2, 1
                                 )))) &
                              id == lead(id, 1) &
                              !is.na(lead(id, 1)) &
                              (ssnnew == lead(ssnnew, 1) |
                                 (is.na(ssnnew) & is.na(lead(
                                   ssnnew, 1
                                 )))),
                            1,
                            0
                          )
  ))


elig <- elig %>%
  arrange(id, ssnnew, desc(fromfinal), desc(tofinal), street2) %>%
  mutate(
    overlap_num =
      cumul_ones(overlap)) %>%
  arrange(id, ssnnew, fromfinal, tofinal, street2) %>%
  mutate(selector = 1:nrow(elig) + overlap_num,
         drop = if_else(selector == lag(selector, 1) &
                          !is.na(lag(selector)),
                        1,
                        0)
  )

elig <- elig %>%
  mutate(todatenew = todatenew[selector],
         addtonew = addtonew[selector]) %>%
  filter(drop == 0) %>%
  select(-(selector), -(overlap), -(overlap_num))


#### Step 13) Repeat steps 11 and 12 ####
# Find adjacent rows with the same final from_date, same address, and takes the largest to date
elig <- arrange(elig, id, ssnnew, fromfinal, tofinal, street2)

repeat {
  dfsize <-  nrow(elig)
  elig <- elig %>%
    mutate(
      drop = ifelse(
        fromfinal == lead(fromfinal, 1) &
          tofinal <= lead(tofinal, 1) &
          !is.na(lead(fromfinal, 1)) &
          !is.na(lead(tofinal, 1)) &
          id == lead(id, 1) &
          ssnnew == lead(ssnnew, 1) &
          street2 == lead(street2, 1) &
          !is.na(lead(id, 1)) &
          !is.na(lead(ssnnew, 1)),
        1,
        0
      )
    ) %>%
    filter(drop == 0)
  dfsize2 <- nrow(elig)
  if (dfsize2 == dfsize) {
    break
  }
}

# Rewrite from/to dates so that continuous coverage is on a single row
elig <- elig %>%
  arrange(id, ssnnew, fromfinal, tofinal) %>%
  mutate(overlap = ifelse(((is.na(street2) &
                              !is.na(lead(street2, 1))) |
                             (!is.na(street2) &
                                is.na(lead(street2, 1)))) |
                            ((is.na(ssnnew) &
                                !is.na(lead(ssnnew, 1))) |
                               (!is.na(ssnnew) &
                                  is.na(lead(ssnnew, 1)))),
                          0,
                          if_else(
                            tofinal + 1 >= lead(fromfinal, 1) &
                              !is.na(lead(tofinal, 1)) &
                              !is.na(lead(fromfinal, 1)) &
                              (street2 == lead(street2, 1) |
                                 (is.na(street2) & is.na(lead(
                                   street2, 1
                                 )))) &
                              id == lead(id, 1) &
                              !is.na(lead(id, 1)) &
                              (ssnnew == lead(ssnnew, 1) |
                                 (is.na(ssnnew) & is.na(lead(
                                   ssnnew, 1
                                 )))),
                            1,
                            0
                          )
  ))


elig <- elig %>%
  arrange(id, ssnnew, desc(fromfinal), desc(tofinal), street2) %>%
  mutate(
    overlap_num =
      cumul_ones(overlap)) %>%
  arrange(id, ssnnew, fromfinal, tofinal, street2) %>%
  mutate(selector = 1:nrow(elig) + overlap_num,
         drop = if_else(selector == lag(selector, 1) &
                          !is.na(lag(selector)),
                        1,
                        0)
  )

elig <- elig %>%
  mutate(todatenew = todatenew[selector],
         addtonew = addtonew[selector]) %>%
  filter(drop == 0) %>%
  select(-(selector), -(overlap), -(overlap_num))


#### Step 14) Repeat steps 11 and 12 ####
# Again, need to turn these into programs to save space
# Find adjacent rows with the same final from_date, same address, and takes the largest to date
elig <- arrange(elig, id, ssnnew, fromfinal, tofinal, street2)

repeat {
  dfsize <-  nrow(elig)
  elig <- elig %>%
    mutate(
      drop = ifelse(
        fromfinal == lead(fromfinal, 1) &
          tofinal <= lead(tofinal, 1) &
          !is.na(lead(fromfinal, 1)) &
          !is.na(lead(tofinal, 1)) &
          id == lead(id, 1) &
          ssnnew == lead(ssnnew, 1) &
          street2 == lead(street2, 1) &
          !is.na(lead(id, 1)) &
          !is.na(lead(ssnnew, 1)),
        1,
        0
      )
    ) %>%
    filter(drop == 0)
  dfsize2 <- nrow(elig)
  if (dfsize2 == dfsize) {
    break
  }
}

# Rewrite from/to dates so that continuous coverage is on a single row
elig <- elig %>%
  arrange(id, ssnnew, fromfinal, tofinal) %>%
  mutate(overlap = ifelse(((is.na(street2) &
                              !is.na(lead(street2, 1))) |
                             (!is.na(street2) &
                                is.na(lead(street2, 1)))) |
                            ((is.na(ssnnew) &
                                !is.na(lead(ssnnew, 1))) |
                               (!is.na(ssnnew) &
                                  is.na(lead(ssnnew, 1)))),
                          0,
                          if_else(
                            tofinal + 1 >= lead(fromfinal, 1) &
                              !is.na(lead(tofinal, 1)) &
                              !is.na(lead(fromfinal, 1)) &
                              (street2 == lead(street2, 1) |
                                 (is.na(street2) & is.na(lead(
                                   street2, 1
                                 )))) &
                              id == lead(id, 1) &
                              !is.na(lead(id, 1)) &
                              (ssnnew == lead(ssnnew, 1) |
                                 (is.na(ssnnew) & is.na(lead(
                                   ssnnew, 1
                                 )))),
                            1,
                            0
                          )
  ))


elig <- elig %>%
  arrange(id, ssnnew, desc(fromfinal), desc(tofinal), street2) %>%
  mutate(
    overlap_num =
      cumul_ones(overlap)) %>%
  arrange(id, ssnnew, fromfinal, tofinal, street2) %>%
  mutate(selector = 1:nrow(elig) + overlap_num,
         drop = if_else(selector == lag(selector, 1) &
                          !is.na(lag(selector)),
                        1,
                        0)
  )

elig <- elig %>%
  mutate(todatenew = todatenew[selector],
         addtonew = addtonew[selector]) %>%
  filter(drop == 0) %>%
  select(-(selector), -(overlap), -(overlap_num))



#### Save cleaned and consolidated person table ####
elig.clean <- elig %>%
  select(id, hhid, ssnnew, fname:lname, gendernew, racem, raceh, hispanic, aian, nhpi, dob, add1:city, 
         cntyfips:cntyname, street, street2, citynew, zip, homeless:careof, fromfinal, tofinal)

ptm02 <- proc.time() # Times how long this query takes
#sqlDrop(db.apde, "dbo.medicaid_elig_consolidated_noRAC")
sqlSave(
  db.apde51,
  elig.clean,
  tablename = "dbo.medicaid_elig_consolidated_noRAC",
  varTypes = c(
    fromfinal = "Date",
    tofinal = "Date",
    dob = "Date"
  )
)
proc.time() - ptm02




