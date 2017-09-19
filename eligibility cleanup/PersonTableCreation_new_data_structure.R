
##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(RODBC) # used to connect to SQL server
library(openxlsx) # used to read in Excel files
library(dplyr) # used to manipulate data
library(stringr) # used to manipulate string variables


##### Connect to the servers #####
db.claims50 <- odbcConnect("PHClaims50")
db.claims51 <- odbcConnect("PHClaims51")
db.apde50 <- odbcConnect("PH_APDEStore50")
db.apde51 <- odbcConnect("PH_APDEStore51")


#### Read in elig file from SQL ####
#### Bring in all eligibility data ####
ptm01 <- proc.time() # Times how long this query takes (~400 secs)
elig <-
  sqlQuery(
    db.claims51,
    "SELECT CLNDR_YEAR_MNTH AS 'calmonth', MEDICAID_RECIPIENT_ID AS 'id', HOH_ID AS 'hh_id', SOCIAL_SECURITY_NMBR AS 'ssn',
    FIRST_NAME AS 'fname', MIDDLE_NAME AS 'mname', LAST_NAME AS 'lname', GENDER AS 'gender',
    RACE1  AS 'race1', RACE2 AS 'race2', RACE3 AS 'race3', RACE4 AS 'race4', HISPANIC_ORIGIN_NAME AS 'hispanic',
    BIRTH_DATE AS 'dob', CTZNSHP_STATUS_NAME AS 'citizenship', INS_STATUS_NAME AS 'immigration',
    SPOKEN_LNG_NAME AS 'langs', WRTN_LNG_NAME AS 'langw', FPL_PRCNTG AS 'fpl', PRGNCY_DUE_DATE AS 'duedate',
    RAC_CODE AS 'rac_code', RAC_NAME AS 'rac_name', FROM_DATE AS 'fromdate', TO_DATE AS 'todate',
    END_REASON AS 'endreason', COVERAGE_TYPE_IND AS 'coverage', DUAL_ELIG AS 'dualelig',
    RSDNTL_ADRS_LINE_1 AS 'add1', RSDNTL_ADRS_LINE_2 AS 'add2', RSDNTL_CITY_NAME AS 'city', RSDNTL_POSTAL_CODE AS 'zip', 
    RSDNTL_STATE_CODE AS 'state', RSDNTL_COUNTY_CODE AS 'cntyfips', RSDNTL_COUNTY_NAME AS 'cntyname', 
    MBR_H_SID AS 'id2'
    FROM dbo.NewEligibility
    ORDER BY MEDICAID_RECIPIENT_ID, FROM_DATE DESC, TO_DATE DESC",
    stringsAsFactors = FALSE
  )
proc.time() - ptm01


# Make a copy of the dataset to avoid having to reread it
elig.bk <- elig


elig <- read.delim(file = "M:/Latest data/New data structure test data/KC_ELIG_CY2012.txt", fill = TRUE, header = TRUE, sep = "\t", quote="",
                   stringsAsFactors = FALSE)

# Rename variables
elig <- elig %>%
  rename(
    calmonth = CLNDR_YEAR_MNTH, id = MEDICAID_RECIPIENT_ID, hhid = HOH_ID, ssn = SOCIAL_SECURITY_NMBR,
    fname = FIRST_NAME, mname = MIDDLE_NAME, lname = LAST_NAME, gender = GENDER,
    race1 = RACE1, race2 = RACE2, race3 = RACE3, race4 = RACE4, hispanic = HISPANIC_ORIGIN_NAME,
    dob = BIRTH_DATE, citizenship = CTZNSHP_STATUS_NAME, immigration = INS_STATUS_NAME,
    langs =SPOKEN_LNG_NAME, langw = WRTN_LNG_NAME, fpl = FPL_PRCNTG, duedate = PRGNCY_DUE_DATE,
    rac_code = RAC_CODE, rac_name = RAC_NAME, fromdate = FROM_DATE, todate = TO_DATE,
    endreason = END_REASON, coverage = COVERAGE_TYPE_IND, dualelig = DUAL_ELIG,
    add1 = RSDNTL_ADRS_LINE_1, add2 = RSDNTL_ADRS_LINE_2, city = RSDNTL_CITY_NAME, 
    state = RSDNTL_STATE_CODE, zip = RSDNTL_POSTAL_CODE, cntyfips = RSDNTL_COUNTY_CODE,
    cntyname = RSDNTL_COUNTY_NAME, id2 = MBR_H_SID
  )


# Make backup to avoid rereading data
elig.bk <- elig


##### Items to resolve for deduplication #####

# Look for multiple SSNs per ID
elig <- elig %>%
  group_by(id) %>%
  mutate(ssn_tot = n_distinct(ssn, na.rm = TRUE),
         ssn_tot_na = n_distinct(ssn, na.rm = FALSE)) %>%
  group_by(id, ssn) %>%
  mutate(ssn_cnt = n()) %>%
  ungroup()


# Look for multiple IDs per SSN (note mutate did not work)
temp <- elig %>%
  group_by(ssn) %>%
  summarise(id_tot = n_distinct(id)) %>%
  ungroup()
filter(temp, id_tot >= 2 & id_tot < 200)


##### Addresses #####
# Goal: Tidy up city name spellings

# Bring in city lookup table
city.lookup <- read.xlsx("H:/My Documents/Medicaid claims/R Projects/Medicaid-PersonTable/City Lookup.xlsx", 
                         sheet = "city", colNames = TRUE)

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



##### Write out for HCA to look at oddities #####
multi_ssn <- elig %>% filter(ssn_tot > 1) %>% select(-(f))
write.csv(multi_ssn, file = "M:/Alastair/Data for HCA to look at/Lists of people with unusual data_2017-04-20/Multiple SSNs per ID.csv",
          row.names = TRUE)

multi_id <- temp %>% filter(id_tot == 2) %>% left_join(., elig, by = c("ssn")) %>% select(-(f))
write.csv(multi_id, file = "M:/Alastair/Data for HCA to look at/Lists of people with unusual data_2017-04-20/Multiple IDs per SSN.csv",
            row.names = TRUE)

non_king_cities <- elig %>% filter(citynew %in% c("ABERDEEN", "BATTLE GROUND", "BONNEY LAKE", "OLYMPIA", "YAKIMA")) %>%
  select(-(f))
write.csv(non_king_cities, file = "M:/Alastair/Data for HCA to look at/Lists of people with unusual data_2017-04-20/Non-King County addresses.csv",
          row.names = TRUE)


##### Reshape to store continuous coverage at an address on one row #####

# NB. Testing for now, update var names once earlier cleaning code is run

elig <- elig %>%
  arrange(id, ssn, calmonth) %>%
  mutate(overlap = ifelse((is.na(add1) &
                             !is.na(lead(add1, 1))) |
                            (!is.na(add1) & is.na(lead(add1, 1))),
                          0,
                          if_else(
                            calmonth + 1 == lead(calmonth, 1) &
                              (add1 == lead(add1, 1) | (is.na(add1) & is.na(lead(add1, 1)))) &
                              id == lead(id, 1) &
                              ssn == lead(ssn, 1) &
                              !is.na(lead(id, 1)) &
                              !is.na(lead(ssn, 1)),
                            1,
                            0
                            )
                          )
         )

