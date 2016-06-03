##########################################################
# Generate data for U01 prediction model
# This code identifies children with an asmtha-related claim in 2014.
# It then brings in 2015 claims to look at risk factors for asmtha-related hospital or ED visit

# APDE, PHSKC
# SQL code by Lin Song, edited by Alastair Matheson to work in R and include medication
# 2016-05-26
##########################################################

options(max.print = 600)

library(RODBC) # used to connect to SQL server
library(dplyr) # used to manipulate data
library(reshape2) # used to reshape data


# DATA SETUP --------------------------------------------------------------

### Connect to the server
db.claims <- odbcConnect("PHClaims")


##### Bring in all the relevant eligibility data #####

# Bring in 2014 data for children aged 3-17 (born between 1997-2011) in 2014
ptm01 <- proc.time() # Times how long this query takes (~21 secs)
elig <-
  sqlQuery(
    db.claims,
    "SELECT CAL_YEAR AS 'Year', MEDICAID_RECIPIENT_ID AS 'ID2014', SOCIAL_SECURITY_NMBR AS 'SSN',
    GENDER AS 'Gender', RACE1  AS 'Race1', RACE2 AS 'Race2', HISPANIC_ORIGIN_NAME AS 'Hispanic',
    BIRTH_DATE AS 'DOB', CTZNSHP_STATUS_NAME AS 'Citizenship', INS_STATUS_NAME AS 'Immigration',
    SPOKEN_LNG_NAME AS 'Lang', FPL_PRCNTG AS 'FPL', RAC_CODE AS 'RACcode', RAC_NAME AS 'RACname',
    FROM_DATE AS 'FromDate', TO_DATE AS 'ToDate',
    covtime = DATEDIFF(dd,FROM_DATE, CASE WHEN TO_DATE > GETDATE() THEN GETDATE() ELSE TO_DATE END),
    END_REASON AS 'EndReason', COVERAGE_TYPE_IND AS 'Coverage', POSTAL_CODE AS 'Zip',
    ROW_NUMBER() OVER(PARTITION BY MEDICAID_RECIPIENT_ID ORDER BY MEDICAID_RECIPIENT_ID, FROM_DATE DESC, TO_DATE DESC) AS 'Row'
    FROM dbo.vEligibility
    WHERE CAL_YEAR=2014 AND BIRTH_DATE BETWEEN '1997-01-01' AND '2011-12-31'
    ORDER BY MEDICAID_RECIPIENT_ID, FROM_DATE DESC, TO_DATE DESC"
  )
proc.time() - ptm01


# Keep the last row from 2014 for each child
elig2014 <- elig %>%
  group_by(ID2014) %>%
  filter(row_number() == n())


# Select children in the following year to be matched with baseline
elig2015 <-
  sqlQuery(
    db.claims,
    "SELECT DISTINCT MEDICAID_RECIPIENT_ID AS 'ID2015'
    FROM dbo.vEligibility
    WHERE CAL_YEAR = 2015 AND BIRTH_DATE BETWEEN '1997-01-01' AND '2011-12-31'
    GROUP BY MEDICAID_RECIPIENT_ID"
  )


# Match baseline with the following year (only include children present in both years)
eligall <- merge(elig2014, elig2015, by.x = "ID2014", by.y = "ID2015")


##### Bring in all the relevant claims data ####

# Baseline (2014) hospitalizations and ED visits (any cause)
ptm02 <- proc.time() # Times how long this query takes (~90 secs)
hospED <-
  sqlQuery(
    db.claims,
    "SELECT DISTINCT MEDICAID_RECIPIENT_ID AS 'ID2014',
    SUM(CASE WHEN CAL_YEAR = 2014 AND CLM_TYPE_CID = 31 THEN 1 ELSE 0 END) AS 'Hosp',
    SUM(CASE WHEN CAL_YEAR=2014 AND REVENUE_CODE IN ('0450','0456','0459','0981') THEN 1 ELSE 0 END) AS 'ED'
    FROM dbo.vClaims
    GROUP BY MEDICAID_RECIPIENT_ID"
  )
proc.time() - ptm02


# 2014 and 2015 claims for patients with asthma
ptm03 <- proc.time() # Times how long this query takes (~90 secs)
asthma <-
  sqlQuery(
    db.claims,
    "SELECT MEDICAID_RECIPIENT_ID AS 'ID2014', *
    FROM dbo.vClaims
    WHERE CAL_YEAR IN (2014, 2015)
    AND (PRIMARY_DIAGNOSIS_CODE LIKE '493%' OR PRIMARY_DIAGNOSIS_CODE LIKE 'J45%'
    OR DIAGNOSIS_CODE_2 LIKE '493%' OR DIAGNOSIS_CODE_2 LIKE 'J45%'
    OR DIAGNOSIS_CODE_3 LIKE '493%' OR DIAGNOSIS_CODE_3 LIKE 'J45%'
    OR DIAGNOSIS_CODE_4 LIKE '493%' OR DIAGNOSIS_CODE_4 LIKE 'J45%'
    OR DIAGNOSIS_CODE_5 LIKE '493%' OR DIAGNOSIS_CODE_5 LIKE 'J45%')"
  )
proc.time() - ptm03


##### Merge all eligible children with asthma claims
asthmachild <- merge(eligall, asthma, by = "ID2014")

# Count up number of baseline (2014) predictors for each child
asthmarisk <- asthmachild %>%
  group_by(ID2014) %>%
  mutate(
    hospcnt14 = sum(ifelse(CAL_YEAR == 2014 &
                             CLM_TYPE_CID == 31, 1, 0)),
    # hospitalizations for asthma, any diagnosis
    hospcntprim14 = sum(ifelse(
      CAL_YEAR == 2014 & CLM_TYPE_CID == 31 &
        (
          substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "493" |
            substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "J45"
        ),
      1,
      0
    )),
    # hospitalizations for asthma, primary diagnosis
    EDcnt14 = sum(ifelse(
      CAL_YEAR == 2014 & REVENUE_CODE %in% c(0450, 0456, 0459, 0981), 1, 0
    )),
    # ED visits for asthma, any diagnosis
    EDcntprim14 = sum(ifelse(
      CAL_YEAR == 2014 & REVENUE_CODE %in% c(0450, 0456, 0459, 0981) &
        (
          substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "493" |
            substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "J45"
        ),
      1,
      0
    )),
    # ED visits for asthma, primary diagnosis
    wellcnt14 = sum(ifelse(CAL_YEAR == 2014 &
                             CLM_TYPE_CID == 27, 1, 0)),
    # well-child checks for asthma, any diagnosis
    wellcntprim14 = sum(ifelse(
      CAL_YEAR == 2014 & CLM_TYPE_CID == 27 &
        (
          substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "493" |
            substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "J45"
        ),
      1,
      0
    )),
    # well-child checks for asthma, primary diagnosis
    asthmacnt14 = sum(ifelse(CAL_YEAR == 2014, 1, 0)),
    asmthacntprim14 = sum(ifelse(
      CAL_YEAR == 2014 &
        (
          substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "493" |
            substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "J45"
        ),
      1,
      0
    )),
    # Count up number of outcome (2015) measures for each child
    hospcnt15 = sum(ifelse(CAL_YEAR == 2015 &
                             CLM_TYPE_CID == 31, 1, 0)),
    # hospitalizations for asthma, any diagnosis
    hospcntprim15 = sum(ifelse(
      CAL_YEAR == 2015 & CLM_TYPE_CID == 31 &
        (
          substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "493" |
            substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "J45"
        ),
      1,
      0
    )),
    # hospitalizations for asthma, primary diagnosis
    EDcnt15 = sum(ifelse(
      CAL_YEAR == 2015 & REVENUE_CODE %in% c(0450, 0456, 0459, 0981), 1, 0
    )),
    # ED visits for asthma, any diagnosis
    EDcntprim15 = sum(ifelse(
      CAL_YEAR == 2015 & REVENUE_CODE %in% c(0450, 0456, 0459, 0981) &
        (
          substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "493" |
            substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "J45"
        ),
      1,
      0
    )),
    # ED visits for asthma, primary diagnosis
    wellcnt15 = sum(ifelse(CAL_YEAR == 2015 &
                             CLM_TYPE_CID == 27, 1, 0))
  )



# ANALYSIS ----------------------------------------------------------------


