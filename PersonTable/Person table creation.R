###############################################################################
# Code to create a cleaned person table from the Medicaid eligibility data
# 
# Alastair Matheson (PHSKC-APDE)
# 2016-06-10
###############################################################################


##### Notes #####
# Any manipulation in R will not carry over to the SQL tables



##### Set up global parameter and call in libraries #####
options(max.print = 700, scipen = 0)

library(RODBC) # used to connect to SQL server
library(car) # used to recode variables
library(stringr) # used to manipulate string variables
library(dplyr) # used to manipulate data


##### Connect to the server #####
db.claims <- odbcConnect("PHClaims")


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


##### Look at some basic stats (already done in SQL) #####

# Number of unique variables
count(distinct(elig, id))
count(distinct(elig, hhid))
count(distinct(elig, id, ssn))
count(distinct(elig, id, fname, lname, dob))

# Values in different address fields (will identify spelling errors and where fields have shifted (e.g., city in zip))
table(elig$city, useNA = 'always')
table(elig$zip, useNA = 'always')


##### Items to resolve for deduplication #####
# 1) Multiple Medicaid IDs per SSN
# 2) Overlapping coverage periods per Med ID
# 3) Inconsistent demographics per Med ID or SSN
# 4) Address formatting issues (e.g., city in zip field)
# 5) Multiple addresses per Med ID during the same coverage period
# 6) Decide what to do with addresses that indicate the person is homeless
# 7) Others?


##### Data cleaning #####
# The purpose is to ensure that each ID + SSN combo only represents one person
# Demographics can then be asribed to each ID + SSN combo


#### SSN ####
# Goal: Find IDs with >1 SSN and figure out if they are separate people
elig <- elig %>%
  group_by(id) %>%
  mutate(ssn_tot = n_distinct(ssn, na_rm = TRUE)) %>%
  group_by(id, ssn) %>%
  mutate(ssn_cnt = n()) %>%
  ungroup()


# Options for dealing with multiple SSNs
# 1) Look at names and DOB to see there is a match and take the most common SSN
ssn.tmp <- elig %>%
  filter(!is.na(ssn)) %>%
  distinct(id, ssn, dob, fname, lname) %>%
  arrange(id, ssn, dob, fname, lname) %>%
  group_by(id, dob, fname, lname) %>%
  # where there is a tie, the first SSN is selected, which is an issue if the data are sorted differently
  slice(which.max(ssn_cnt)) %>%
  ungroup() %>%
  select(id, ssn, dob, fname, lname)

# 1 cont) Merge back with the primary data and update SSN
elig <- left_join(elig, ssn.tmp, by = c("id", "fname", "lname", "dob"))
rm(ssn.tmp) # remove temp data frames to save memory

# Make new variable with cleaned up SSN
elig <- mutate(elig, ssnnew = ifelse(!is.na(ssn.y), ssn.y, ssn.x))



#### City ####
# Goal: Tidy up city name spellings
elig <- elig %>%
  mutate(
    citynew = city,
    citynew = replace(
      citynew,
      which(
        str_detect(city, "^AU[BD][:alnum:]*N") == TRUE |
          str_detect(city, "^ABUR") == TRUE |
          str_detect(city, "^ARB[:alnum:]*N") == TRUE |
          str_detect(city, "^AURB[:alnum:]*N") == TRUE |
          str_detect(city, "SOUTH AUBURN") == TRUE
      ),
      "AUBURN"
    ),
    citynew = replace(citynew,
                   which(str_detect(city, "[BD]ELL[EV]") == TRUE),
                   "BELLEVUE"),
    citynew = replace(citynew,
                   which(str_detect(city, "BOTHEL") == TRUE),
                   "BOTHELL"),
    citynew = replace(citynew,
                   which(
                     str_detect(city, "CARN[:alnum:]*ON") == TRUE
                   ),
                   "CARNATION"),
    citynew = replace(
      citynew,
      which(
        str_detect(city, "COVIN") == TRUE |
          str_detect(city, "CONVIN") == TRUE
      ),
      "COVINGTON"
    ),
    citynew = replace(
      citynew,
      which(
        str_detect(city, "MOIN") == TRUE |
          str_detect(city, "DES[:space:]*M") == TRUE |
          city %in% c("DEMING", "DE MONIES")
      ),
      "DES MOINES"
    ),
    citynew = replace(citynew,
                   which(
                     str_detect(city, "MCLAW") == TRUE |
                       str_detect(city, "ENU[:alnum:]*C[:alnum:]*W") == TRUE
                   ),
                   "ENUMCLAW"),
    citynew = replace(citynew,
                   which(
                     str_detect(city, "EVE[:alnum:]*[TE]") == TRUE
                   ),
                   "EVERETT"),
    citynew = replace(citynew,
                   which(
                     str_detect(city, "FALL[:alnum:]*[:space:]*CITY") == TRUE
                   ),
                   "FALL CITY"),
    citynew = replace(
      citynew,
      which(
        str_detect(city, "FED[:alnum:]*[:space:]*W") == TRUE |
          str_detect(city, "FER[:alnum:]*[:space:]*WAY") == TRUE |
          str_detect(city, "FER[:alnum:]*[:space:]*W[AY]") == TRUE |
          str_detect(city, "[DF]E[DF]ERAL") == TRUE
      ),
      "FEDERAL WAY"
    ),
    citynew = replace(citynew,
                   which(
                     str_detect(city, "[AI]S[:alnum:]*AH") == TRUE |
                       str_detect(city, "IS[:alnum:]*QUA") == TRUE
                   ),
                   "ISSAQUAH"),
    citynew = replace(citynew,
                   which(
                     str_detect(city, "^KE[:alnum:]*ORE") == TRUE |
                       city %in% c("AEMMORE", "KEN")
                   ),
                   "KENMORE"),
    citynew = replace(citynew,
                   which(
                     str_detect(city, "KE[NT][NT][:alnum:]*") == TRUE |
                       city %in% c("4ENT", "KNET", "KUNT")
                   ),
                   "KENT"),
    citynew = replace(citynew,
                   which(
                     str_detect(city, "KI[RK][:alnum:]*[LA]ND[:alnum:]*") == TRUE
                   ),
                   "KIRKLAND"),
    citynew = replace(citynew,
                   which(
                     str_detect(city, "L[AK][:alnum:]*[:space:]*FOR[:alnum:]*") == TRUE
                   ),
                   "LAKE FOREST PARK"),
    citynew = replace(
      citynew,
      which(
        str_detect(city, "MOU[NT][:alnum:]*[:space:]*AKE[:space:]*TERR") == TRUE |
          str_detect(city, "MT[:alnum:]*[:space:]*AKE[:space:]*TERR") == TRUE
      ),
      "MOUNTLAKE TERRACE"
    ),
    citynew = replace(citynew,
                   which(
                     str_detect(city, "NOR[:alnum:]*PARK") == TRUE
                   ),
                   "NORMANDY PARK"),
    citynew = replace(citynew,
                   which(
                     str_detect(city, "NO[:alnum:]*[:space:]*B[AE][ND]*") == TRUE |
                       (
                         str_detect(city, "H BEND") == TRUE &
                           str_detect(city, "SOUTH") == FALSE
                       )
                   ),
                   "NORTH BEND"),
    citynew = replace(citynew,
                   which(
                     str_detect(city, "S[AO]M[:alnum:]*SH") == TRUE
                   ),
                   "SAMMAMISH"),
    citynew = replace(citynew,
                   which(
                     str_detect(
                       citynew,
                       "S[EA][:alnum:]*[:punct:]*[:space:]*TA[CPS][:alnum:]*"
                     ) == TRUE |
                       str_detect(city, "S[EA][:alnum:]*[:punct:]*[:space:]*TC") == TRUE
                   ),
                   "SEATAC"),
    citynew = replace(
      citynew,
      which(
        str_detect(city, "ATTLE") == TRUE |
          str_detect(city, "[DS]EA[:alnum:]*[KLT]E") == TRUE |
          str_detect(city, "SEATTL") == TRUE |
          city %in% c(
            "BALLARD",
            "BEACON HILL",
            "DOWNTOWN FREMONT",
            "SEATT",
            "SEATTEL",
            "SETTLE",
            "STATTLE"
          )
      ),
      "SEATTLE"
    ),
    citynew = replace(citynew,
                   which(
                     str_detect(city, "SHO[ER][:alnum:]*[:space:]*LI[EN]*") == TRUE
                   ),
                   "SHORELINE"),
    citynew = replace(citynew,
                   which(
                     str_detect(
                       citynew,
                       "S[MN]O[:alnum:]*Q[:alnum:]*[IM][IM][AE][:space:]*[:alnum:]*"
                     ) == TRUE
                   ),
                   "SNOQUALMIE"),
    citynew = replace(citynew,
                   which(
                     str_detect(city, "TU[AKQRW][:alnum:]*[IL][KLW]A") == TRUE |
                       city %in% c("PUKWILA", "TEQUILLA")
                   ),
                   "TUKWILA")
  )


#### Addresses (NB. a separate geocoding process may fill in some gaps later) ####
# Goal: Tidy up and standardize addresses
elig <- elig %>%
  # Set up homeless and PO Box variables first to avoid capturing non-permanent residential addresses (e.g., relatives and temporary shelters)
  mutate(
    homeless = ifelse(
      str_detect(add1, "HOMELESS") == TRUE |
        str_detect(add2, "HOMELESS"),
      1,
      0
    ),
    # Need spaces to avoid falsely capturing Boxley Pl and other similarly named roads
    pobox = ifelse(
      str_detect(add1, "[:space:]BOX[:space:]") == TRUE |
        str_detect(add2, "[:space:]BOX[:space:]"),
      1,
      0
    ),
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
    street = replace(street, which(homeless == 1 | pobox == 1), NA),
    # Strip out apartment numbers etc.
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
    street2 = replace(street2, which(str_detect(
      street2, "^[:alpha:]"
    ) == TRUE | street2 == ""), NA)
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
               female = recode(gender, "'Female' = 1; 'Male' = 0; 'Unknown' = NA"))

elig <- elig %>%
  group_by(id, ssnnew) %>%
  mutate(gender_tot = n_distinct(female, na_rm = TRUE)) %>%
  ungroup()


# Replace multiple genders as missing
elig <-
  mutate(elig, female = replace(female, which(gender_tot > 1), NA))



#### Race ####
# Goal: Rationalize discordant race entries

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

# Set up single race code with Hispanic
elig <- elig %>%
  mutate(
    raceh = race1new,
    raceh = replace(raceh, which(hispanic == "HISPANIC"), "Hispanic"),
    raceh = replace(raceh, which(
      !is.na(race2new) | !is.na(race3new) | !is.na(race4new)
    ), "Multiple")
  )


# Identify people with >1 single race codes
# NB. This fails if attempting to complete all operations in one command
elig <- elig %>%
  mutate(racehnum = recode(
    raceh,
    c(
      "'AIAN' = 1; 'Asian' = 2; 'Black' = 3;
      'Hispanic' = 4; 'NHPI' = 5; 'White' = 6;
      'Multiple' = 7; 'Other' = 8"
    )
    ))

elig <- elig %>%
  group_by(id, ssnnew) %>%
  mutate(race_tot = n_distinct(racehnum, na_rm = TRUE)) %>%
  ungroup()

elig <- elig %>%
  group_by(id, ssnnew, raceh) %>%
  mutate(race_cnt = n()) %>%
  ungroup()

# Replace multiple races as multiple
elig <-
  mutate(elig, raceh = replace(raceh, which(race_tot > 1), "Multiple"),
         racehnum = replace(racehnum, which(race_tot > 1), "7"))



#### COVERAGE PERIOD ####
# Goal: Make one line per person per coverage period

# Order by coverage dates
elig <- arrange(elig, id, ssnnew, fromdate, todate)


# Make from and to dates date variables
elig <- mutate(elig, fromdate = as.Date(fromdate),
               todate = as.Date(todate))


# It looks like when a coverage period spans >1 year, # a new record is created for each year that is all or partially covered.
# Consolidate rows that are duplicated across different years
# NB. There seem to be multiple coverage types (FFS or MC) over the same period
#     so this is not being considered for now
elig <- distinct(elig, id, ssnnew, fromdate, todate, street2, city, zip, RACcode)



################ TESTING AREA ####################
elig.bk2 <- elig # make a new backup once code is run up to coverage period

# Make a test data frame
elig.tst <- elig %>%
  filter(row_number() <= 5000)


# If there are identical coverage periods and RAC codes, select the most common address and drop other rows
elig.tst <- elig.tst %>%
  group_by(id, ssnnew, fromdate, todate, RACcode) %>%
  arrange(add_cnt, street2, city, zip) %>%
  # where there is a tie, the first address is selected, which is an issue if the data are sorted differently
  slice(which.max(add_cnt)) %>%
  ungroup()


# Order by address and build up single row per continuous coverage per address (ignore RACcode for now)
elig.tst <- elig.tst %>%
  arrange(id, ssnnew, street2, city, zip, fromdate, todate) %>%
  group_by(id, ssnnew, street2, city, zip)
  
repeat {
  dfsize <-  nrow(elig.tst)
  elig.tst <- elig.tst %>%
    filter(!(fromdate > lag(fromdate, 1) &
               todate <= lag(todate, 1)) |
             is.na(lag(fromdate, 1)) | is.na(lag(todate, 1)))
    dfsize2 <- nrow(elig.tst)
  if (dfsize2 == dfsize) {
    break
  }
}
  
  # Find periods that sit completely within preceding period
  filter(!(fromdate > lag(fromdate, 1) & todate <= lag(todate, 1)) |
             is.na(lag(fromdate, 1)) | is.na(lag(todate, 1))) %>%
  # Run again to account for new order (should turn this into a loop that goes until the size of the data frame remains unchanged)
  filter(!(fromdate > lag(fromdate, 1) & todate <= lag(todate, 1)) |
         is.na(lag(fromdate, 1)) | is.na(lag(todate, 1))) %>%
  # When there is a draw in the fromdate, take the latest todate
  group_by(id, ssnnew, street2, city, zip, fromdate) %>%
  filter(row_number() == n())
  
  
  mutate(
    fromdatenew = as.numeric(ifelse(
      fromdate < lag(todate, 1),
      lag(todate, 1) + 1,
      fromdate
    )),
    fromdatenew = as.Date(fromdatenew, origin = "1970-01-01"))



# Check to see the next row's start date is immediately following the end date
# NB. The ifelse command strips the date attribute so must be wrapped with as.numeric and reformatted
elig.tst <- elig.tst %>%
  select(year, id, hhid, ssnnew, fromdate, todate, street2, city, RACcode, coverage)  %>%
  group_by(id, ssnnew) %>%
  mutate(
    todatenew = as.numeric(ifelse(
      (todate + 1 == lead(fromdate, 1) |
        todate == lead(fromdate, 1)) &
        street2 == lead(street2, 1) &
        city == lead(city, 1),
      lead(todate, 1),
      todate
    )),
    todatenew = as.Date(todatenew, origin = "1970-01-01")
  )
  


elig.tst %>% 
  select(year, id, hhid, ssnnew, fromdate, todate, street2, city, RACcode, coverage)  %>%
  group_by(id, ssnnew) %>%
  mutate(todatenew = lead(fromdate, 1) + 1,
         todatenew2 = todate + 1)


# View results
elig %>% select(year, id, hhid, ssnnew, fromdate, todate, street2, city, RACcode, coverage)



# Convert all far off dates (indicating continuous coverage) to today's date
todate = replace(todate, which(todate == "2999-12-31"), Sys.Date())


################ END TESTING AREA ####################






################ TESTING AREA ####################
# Try to replicate Lin's results

elig.test <- elig %>%
  filter(year %in% c(2012:2015))


# see how many unique users in the 2012–2015 time period (matches Lin's #)
length(unique(elig.test$id))

# see how many unique addresses (differs from Lin by ~150 due to city clean up)
distinct(elig.test, add1, add2, city, zip) %>% summarize(count = n())

# after cleaning
distinct(elig.test, street2, city, zip) %>% summarize(count = n())

distinct(elig.test, street2, city, zip) %>% arrange(street) %>% select(street2, city, zip)
distinct(elig.test, street2, city, zip) %>% arrange(desc(street)) %>% select(street2, city, zip)


#### compare with Lin's addresses ####
# Make subset to match Lin
elig.comp <- elig %>%
  filter(year %in% c(2012:2015) & !is.na(street2)) %>%
  distinct(street2, city, zip) %>% 
  arrange(street) %>% 
  select(street2, city, zip) %>%
  mutate(source = "X")

# Bring in Lin's data
library(readxl)
linadd <- read_excel("H:/My Documents/Medicaid claims/temp address file.xlsx", col_names = T)
# take out spaces around the imported addresses
linadd <- mutate(linadd, ADDRESS = str_trim(ADDRESS, side = c("both")),
       source = "Y")

# Merge and look at non-matches
addcomp <- merge(elig.comp, linadd, by.x = c("street2", "city", "zip"), by.y = c("ADDRESS", "CITY", "ZIPCODE"), all = T)
tmp <- filter(addcomp, is.na(source.x) | is.na(source.y)) %>% arrange(street2, city, zip, source.x, source.y)


################ END TESTING AREA ####################
