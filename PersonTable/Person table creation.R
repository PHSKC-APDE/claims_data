###############################################################################
# Code to create a cleaned person table from the Medicaid eligibility data
# 
# Alastair Matheson (PHSKC-APDE)
# 2016-06-10
###############################################################################


##### Notes #####
# Any manipulation in R will not carry over to the SQL tables



##### Set up global parameter and call in libraries #####
options(max.print = 600, scipen = 0)

library(RODBC) # used to connect to SQL server
library(stringr) # used to manipulate string variables
library(dplyr) # used to manipulate data


##### Connect to the server #####
db.claims <- odbcConnect("PHClaims")


##### Bring in all the relevant eligibility data #####

# Bring in all eligibility data
ptm01 <- proc.time() # Times how long this query takes (~21 secs)
elig <-
  sqlQuery(
    db.claims,
    "SELECT CAL_YEAR AS 'year', MEDICAID_RECIPIENT_ID AS 'id', HOH_ID AS 'hhid', SOCIAL_SECURITY_NMBR AS 'ssn',
    FIRST_NAME AS 'fname', MIDDLE_NAME AS 'mname', LAST_NAME AS 'lname', GENDER AS 'gender',
    RACE1  AS 'race1', RACE2 AS 'race2', RACE3 AS 'race3', RACE4 AS 'race4', HISPANIC_ORIGIN_NAME AS 'hispanic',
    BIRTH_DATE AS 'DOB', CTZNSHP_STATUS_NAME AS 'citizenship', INS_STATUS_NAME AS 'immigration',
    SPOKEN_LNG_NAME AS 'langs', WRTN_LNG_NAME AS 'langw', FPL_PRCNTG AS 'FPL', PRGNCY_DUE_DATE AS 'duedate',
    RAC_CODE AS 'RACcode', RAC_NAME AS 'RACname', FROM_DATE AS 'fromdate', TO_DATE AS 'todate',
    covtime = DATEDIFF(dd,FROM_DATE, CASE WHEN TO_DATE > GETDATE() THEN GETDATE() ELSE TO_DATE END),
    END_REASON AS 'endreason', COVERAGE_TYPE_IND AS 'coverage', DUAL_ELIG AS 'dualelig',
    ADRS_LINE_1 AS 'add1', ADRS_LINE_2 AS 'add2', CITY_NAME AS 'city', POSTAL_CODE AS 'zip', COUNTY_CODE AS 'cntyfips',
    COUNTY_NAME AS 'cntyname'
    FROM dbo.vEligibility
    ORDER BY MEDICAID_RECIPIENT_ID, FROM_DATE DESC, TO_DATE DESC"
  )
proc.time() - ptm01

# Make a copy of the dataset to avoid having to reread it
elig.bk <- elig


##### Look at some basic stats (already done in SQL) #####

# Number of unique variables
count(distinct(elig, id))
count(distinct(elig, hhid))
count(distinct(elig, SSN))

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

### Addresses (NB. a separate geocoding process may fill in some gaps later)
elig <- elig %>%
  mutate(
    city = as.character(replace(
      city,
      which(
        str_detect(city, "AUB") == TRUE |
          str_detect(city, "AURB") == TRUE |
          str_detect(city, "AUD") == TRUE |
          str_detect(city, "ABUR") == TRUE
      ),
      "AUBURN"
    )),
    city = replace(
      city,
      which(
        str_detect(city, "BELLE") == TRUE |
          str_detect(city, "BELLV") == TRUE |
          city %in% c("DELLVUE")
      ),
      "BELLEVUE"
    ),
    city = replace(
      city,
      which(
        str_detect(city, "COVIN") == TRUE |
          str_detect(city, "CONVIN") == TRUE
      ),
      "COVINGTON"
    ),
    city = replace(
      city,
      which(
        str_detect(city, "MOIN") == TRUE |
          str_detect(city, "DES M") == TRUE |
          str_detect(city, "DESM") == TRUE |
          city %in% c("DEMING")
      ),
      "DES MOINES"
    ),
    city = replace(city,
                   which(
                     str_detect(city, "MCLAW") == TRUE |
                       str_detect(city, "ENU*C*W") == TRUE
                   ),
                   "ENUMCLAW"),
    city = replace(
      city,
      which(
        str_detect(city, "ATTLE") == TRUE |
          str_detect(city, "SEATTE") == TRUE |
          str_detect(city, "SEATTL") == TRUE |
          city %in% c(
            "BALLARD",
            "BEACON HILL",
            "SEAETTLE",
            "SEATLE",
            "SEATLLE",
            "SEATLTLE",
            "SEATT",
            "SEATTKE",
            "SEATTTE",
            "SEATTTLE",
            "SEAWTTLE",
            "SETTLE"
          )
      ),
      "SEATTLE"
    )
  )
                        

  
