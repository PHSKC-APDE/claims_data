###############################################################################
# Code the create a cleaned person table from the Medicaid eligibility data
# 
# Alastair Matheson (PHSKC-APDE)
# 2016-06-10
###############################################################################


##### Notes #####

# Any manipulation in R will not carry over to the SQL tables




options(max.print = 400, scipen = 0)


library(RODBC) # used to connect to SQL server


### Connect to the server
db.claims <- odbcConnect("PHClaims")


##### Bring in all the relevant eligibility data #####

# Bring in all eligibility data
elig <-
  sqlQuery(
    db.claims,
    "SELECT CAL_YEAR AS 'year', MEDICAID_RECIPIENT_ID AS 'ID', HOH_ID AS 'hhid', SOCIAL_SECURITY_NMBR AS 'SSN',
    FIRST_NAME AS 'fname', MIDDLE_NAME AS 'mname', LAST_NAME AS 'lname', GENDER AS 'Gender',
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



