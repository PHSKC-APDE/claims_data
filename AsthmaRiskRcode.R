##########################################################
# Generate data for U01 prediction model
# This code identifies children with an asmtha-related claim in 2014.
# It then brings in 2015 claims to look at risk factors for asmtha-related hospital or ED visit

# APDE, PHSKC
# SQL code by Lin Song, edited by Alastair Matheson to work in R and include medication
# 2016-05-26
##########################################################

options(max.print = 400)

library(RODBC) # used to connect to SQL server
library(dplyr) # used to manipulate data
library(reshape2) # used to reshape data
library(car) # used to recode variables
library(haven) # used to read in Stata files
library(lmtest) # used to conduct likelihood ratio tests
library(rms) # used for other model diagnostics


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

# Drop data frames to free up memory
rm("elig2014", "elig2015")


##### Bring in all the relevant claims data #####

# Baseline (2014) hospitalizations and ED visits (any cause)
ptm02 <- proc.time() # Times how long this query takes (~90 secs)
hospED <-
  sqlQuery(
    db.claims,
    "SELECT DISTINCT MEDICAID_RECIPIENT_ID AS 'ID2014',
    SUM(CASE WHEN CAL_YEAR = 2014 AND CLM_TYPE_CID = 31 THEN 1 ELSE 0 END) AS 'hosp',
    SUM(CASE WHEN CAL_YEAR=2014 AND REVENUE_CODE IN ('0450','0456','0459','0981') THEN 1 ELSE 0 END) AS 'ED'
    FROM dbo.vClaims
    GROUP BY MEDICAID_RECIPIENT_ID"
  )
proc.time() - ptm02


# Bring in all pharmacy claims from 2014 and 2015
ptm.temp <- proc.time() # Times how long this query takes (~150-250 secs)
pharmall <-
  sqlQuery(
    db.claims,
    "SELECT MEDICAID_RECIPIENT_ID AS 'ID2014', CAL_YEAR, NDC AS 'ndc', NDC_DESC AS 'ndcdesc',
    PRSCRPTN_FILLED_DATE AS 'rxdate', DRUG_DOSAGE AS 'dose'
    FROM dbo.vClaims
    WHERE CAL_YEAR IN (2014, 2015) AND CLM_TYPE_CID = 24
    ORDER BY MEDICAID_RECIPIENT_ID"
  )
proc.time() - ptm.temp


# Limit pharmacy records to only those included children
pharmchild <- semi_join(pharmall, distinct(eligall, ID2014), by = "ID2014") %>%
  mutate(ID2014 = as.factor(ID2014))
# Remove the total pharm data to free up memory
rm("pharmall")


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


##### Bring in other relevant data #####
# Bring in asthma med list
meds <- read.csv("H:/my documents/Medicaid claims/Asthma/NDC493.csv")
# Set up type of medication
meds <- meds %>%
  mutate(controller = ifelse(category %in% c("antiasthmatic combinations", "antibody inhibitor", "inhaled corticosteroids",
                                             "inhaled steroid combinations", "leukotriene modifiers", "mast cell stablizers",
                                             "methylxanthines"), 1, 0),
         reliever = ifelse(category %in% c("short-acting inhaled beta-2 agonists"), 1, 0))



##### Merge all eligible children with asthma claims and total hospital/ED visits #####
asthmachild <- merge(eligall, asthma, by = "ID2014") %>%
  arrange(ID2014, FROM_SRVC_DATE)


# Append all pharma records for children with 1+ asthma claims
pharmasthma <- semi_join(pharmchild, distinct(asthmachild, ID2014), by = "ID2014") %>%
  mutate(ID2014 = as.factor(ID2014)) %>%
  arrange(ID2014, rxdate) # This keeps pharmacy records only for children with asthma claims
asthmachild <- bind_rows(asthmachild, pharmasthma) %>%
  mutate(ID2014 = as.factor(ID2014))
  

# Merge with asthma meds
asthmachild <- mutate(asthmachild, ndc = ifelse(is.na(ndc), NDC, ndc)) # fill in any blank ndc records
asthmachild <- merge(asthmachild, meds, by = "ndc", all.x = TRUE)



################### TESTING AREA ###############################

# Figure out how many children received asthma rx but had no asthma dx
tmp.asthmamed <- merge(meds, pharmchild, by = 'ndc')
tmp.asthmamed <- tmp.asthmamed %>%
  filter(!is.na(controller) | !is.na(reliever)) %>%
  distinct(ID2014) %>%
  select(ID2014, ndc, category, controller, reliever) # keeps a list of unique children who had an asthma rx

tmp.noasthmadx <- anti_join(tmp.asthmamed, asthmachild, by = "ID2014") %>%
  mutate(Id2014 = as.factor(ID2014))

# Count number of unique children with asthma dx
length(unique(asthmachild$ID2014)) # should be 10,455

# Count number of children with asthma dx and asthma rx
tmp.rx <- asthmachild %>%
  group_by(ID2014) %>%
  filter(!is.na(controller) | !is.na(reliever)) %>%
  summarise(n = n(), control = sum(controller), reliever = sum(reliever))


################## END TESTING AREA ############################


# Count up number of baseline (2014) predictors for each child
asthmachild <- asthmachild %>%
  group_by(ID2014) %>%
  mutate(
    # hospitalizations for asthma, any diagnosis
    hospcnt14 = sum(ifelse(CAL_YEAR == 2014 &
                             CLM_TYPE_CID == 31, 1, 0), na.rm = TRUE),
    # hospitalizations for asthma, primary diagnosis
    hospcntprim14 = sum(ifelse(
      CAL_YEAR == 2014 & CLM_TYPE_CID == 31 &
        (
          substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "493" |
            substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "J45"
        ),
      1,
      0
    ), na.rm = TRUE),
    # ED visits for asthma, any diagnosis
    EDcnt14 = sum(ifelse(
      CAL_YEAR == 2014 & REVENUE_CODE %in% c(0450, 0456, 0459, 0981), 1, 0
    ), na.rm = TRUE),
    # ED visits for asthma, primary diagnosis
    EDcntprim14 = sum(ifelse(
      CAL_YEAR == 2014 & REVENUE_CODE %in% c(0450, 0456, 0459, 0981) &
        (
          substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "493" |
            substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "J45"
        ),
      1,
      0
    ), na.rm = TRUE),
    # well-child checks for asthma, any diagnosis
    wellcnt14 = sum(ifelse(CAL_YEAR == 2014 &
                             CLM_TYPE_CID == 27, 1, 0), na.rm = TRUE),
    # well-child checks for asthma, primary diagnosis
    wellcntprim14 = sum(ifelse(
      CAL_YEAR == 2014 & CLM_TYPE_CID == 27 &
        (
          substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "493" |
            substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "J45"
        ),
      1,
      0
    ), na.rm = TRUE),
    # total number of asthma claims, primary diagnosis
    asthmacnt14 = sum(ifelse(CAL_YEAR == 2014, 1, 0), na.rm = TRUE),
    asmthacntprim14 = sum(ifelse(
      CAL_YEAR == 2014 &
        (
          substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "493" |
            substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "J45"
        ),
      1,
      0
    ), na.rm = TRUE),
    # Count up number of outcome (2015) measures for each child
    # hospitalizations for asthma, any diagnosis
    hospcnt15 = sum(ifelse(CAL_YEAR == 2015 &
                             CLM_TYPE_CID == 31, 1, 0), na.rm = TRUE),
    # hospitalizations for asthma, primary diagnosis
    hospcntprim15 = sum(ifelse(
      CAL_YEAR == 2015 & CLM_TYPE_CID == 31 &
        (
          substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "493" |
            substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "J45"
        ),
      1,
      0
    ), na.rm = TRUE),
    # ED visits for asthma, any diagnosis
    EDcnt15 = sum(ifelse(
      CAL_YEAR == 2015 & REVENUE_CODE %in% c(0450, 0456, 0459, 0981), 1, 0
    ), na.rm = TRUE),
    # ED visits for asthma, primary diagnosis
    EDcntprim15 = sum(ifelse(
      CAL_YEAR == 2015 & REVENUE_CODE %in% c(0450, 0456, 0459, 0981) &
        (
          substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "493" |
            substr(PRIMARY_DIAGNOSIS_CODE, 1, 3) == "J45"
        ),
      1,
      0
    ), na.rm = TRUE)
  ) %>%
arrange(ID2014, CAL_YEAR)


# Calculate asthma medication ratio
  tmp.meds <- asthmachild %>%
    group_by(ID2014) %>%
    filter(CAL_YEAR == "2014" &
             (!is.na(controller) | !is.na(reliever))) %>%
    mutate(
      controltot = sum(ifelse(
        CAL_YEAR == 2014 &
          !is.na(CAL_YEAR) & controller == 1 & !is.na(controller),
        1,
        0
      ), na.rm = TRUE),
      relievertot = sum(ifelse(
        CAL_YEAR == 2014 &
          !is.na(CAL_YEAR) & reliever == 1 & !is.na(reliever),
        1,
        0
      ), na.rm = TRUE),
      amr14 = controltot / (controltot + relievertot),
      amr14risk = ifelse(amr14 < 0.5, 1, ifelse(amr14 >= 0.5, 0, NA))
    ) %>%
    distinct(ID2014, amr14) %>%
    select(ID2014, controltot, relievertot, amr14, amr14risk)

  
# Collapse claims data and merge with medication data
asthmarisk <- asthmachild %>%
  distinct(ID2014) %>%
  # Keep columns of interest
  select(ID2014, hospcnt14:EDcntprim15)
  

# Merge with total hospitalizations, demogs from eligibility, and meds
asthmarisk <- merge(asthmarisk, hospED, by = "ID2014", all.x = TRUE)
asthmarisk <- merge(asthmarisk, eligall, by = "ID2014", all.x = TRUE)
asthmarisk <- merge(asthmarisk, tmp.meds, by = "ID2014", all.x = TRUE)
  



# Create and recode other variables for analysis
arbwght <- 3 # sets up the arbitrary weight for hospitalizations (compared with ED visits)

asthmarisk <- asthmarisk %>%
  mutate(
    # count of non-asthma-related hospitalizations
    hospnonasth = hosp - hospcnt14,
    # count of non-asthma-related ED visits
    EDnonasth = ED - EDcnt14,
    # weighted count of 2014 (baseline) asthma-related hospital/ED encounters, any diagnosis
    asthmaenc14 = (hospcnt14 * arbwght) + EDcnt14,
    # weighted count of 2015 (outcome) asthma-related hospital/ED encounters, any diagnosis
    asthmaenc15 = (hospcnt15 * arbwght) + EDcnt15,
    # weighted count of 2015 (outcome) asthma-related hospital/ED encounters, primary diagnosis
    asthmaencprim15 = (hospcntprim15 * arbwght) + EDcntprim15,
    # recoded count of 2015 (outcome) hospital-related asthma visits
    hospcnt15.r = recode(hospcnt15, "0 = 0; 1:2 = 1; 3:hi = 2"),
    # recode Hispanic variable
    hisp = recode(Hispanic, "'NOT HISPANIC' = 0; 'HISPANIC' = 1", as.factor.result = FALSE), # the last part ensures the new variable codes as numeric
    # recode gender variable
    female = recode(Gender, "'Female' = 1; 'Male' = 0", as.factor.result = FALSE),
    # recode race variable
    race = recode(Race1, "c('Alaskan Native','American Indian') = 1; 'Asian' = 2; 
                          'Black' = 3; c('Hawaiian', 'Pacific Islander') = 5; 'White' = 6; else = 7",
                              as.factor.result = FALSE),
    # makes Hispanic race category from those with no defined race
    race = replace(race, which(race == 7 & hisp == 1), 4),
    # recodes those with an Asian language and no race to Asian race
    race = replace(race, which(race == 7 & Lang %in% c("Burmese","Chinese","Korean","Vietnamese","Tagalog")), 2),
    # recodes those with Somali language and no race to black race
    race = replace(race, which(race == 7 & Lang == "Somali"), 2),  
    # recodes those with Russian language and no race to white race
    race = replace(race, which(race == 7 & Lang == "Russian"), 2),   
    # recodes those with Spanish language and no race to Hispanic race
    race = replace(race, which(race == 7 & Lang == "Spanish; Castillian"), 2),
    # extract age from birth year and recode into age groups
    age = 2014 - as.numeric(substr(DOB,1,4)),
    agegrp = cut(age, breaks = c(3, 5, 11, 18), right = FALSE, labels = c(1:3)),
    # recode Federal poverty level into groups
    fplgrp = cut(FPL, breaks = c(1, 133, 199, max(FPL[!is.na(FPL)])), right = FALSE, labels = c(1:3)),
    fplgrp = replace(fplgrp, which(fplgrp == 1 & RACcode == 1203), 1),
    # make outcomes binary
    outcome = ifelse(hospcnt15 > 0 | EDcnt15 > 0, 1, 0),
    outcomeprim = ifelse(hospcntprim15 > 0 | EDcntprim15 > 0, 1, 0)
  )


# Bring in zipcode file and recode
zipgps <- read_dta("H:/my documents/Medicaid claims/Asthma/zipgps.dta")
zipgps <- zipgps %>%
  mutate(region = recode(hpa, "c('Bellevue', 'Bothell/Woodinville', 'Issaquah/Sammamish', 
                          'Kirkland', 'Mercer Isl/Point Cities', 'Redmond/Union Hill') = 1;
                          c('Auburn', 'Burien/Des Moines', 'Federal Way', 'Kent', 'Lower Valley & Upper Sno',
                          'Southeast King County', 'Tukwila/SeaTac', 'Vashon Island') = 2;
                          else = 3", as.factor.result = FALSE),
         hizip = ifelse(zipcode %in% c(98001, 98002, 98022, 98023, 98030, 98042, 98047, 98052, 98057,
                                       98065, 98092, 98112, 98118, 98122, 98144, 98146, 98155, 98178, 98188), 1, 0))
  
asthmarisk <- merge(asthmarisk, zipgps, by.x = "Zip", by.y = "zipcode", all.x = TRUE)
  

# ANALYSIS ----------------------------------------------------------------

# Predictive model of future asthma-related hospital and ED visits
m1 <- glm(outcome ~ asthmacnt14 + ED + hospcnt14 + EDcnt14 + wellcnt14 + agegrp + female + race + fplgrp + hizip,
          family = "binomial", data = asthmarisk)

summary(m1)
p1 <- predict.glm(m1, type = "response")

# Predictive model including asthma medication ratio
# Need to make version of m1 limited to where amr14risk is filled in for LR test to work
m2 <- glm(outcome ~ asthmacnt14 + ED + hospcnt14 + EDcnt14 + wellcnt14 + agegrp + female + race + fplgrp + hizip,
          family = "binomial", data = asthmarisk, subset = !is.na(amr14risk))

m3 <- glm(outcome ~ asthmacnt14 + ED + hospcnt14 + EDcnt14 + wellcnt14 + agegrp + female + race + fplgrp + hizip + amr14risk,
          family = "binomial", data = asthmarisk)

summary(m3)
p3 <- predict.glm(m3, type = "response")

# Look at LR test
lrtest(m2, m3)

# Look at correlation coefficient
cor(p3, subset(asthmarisk, !is.na(asthmacnt14) & !is.na(ED) & !is.na(hospcnt14) & !is.na(EDcnt14) & !is.na(wellcnt14) 
               & !is.na(agegrp) & !is.na(female) & !is.na(race) & !is.na(fplgrp) & !is.na(hizip) & !is.na(amr14risk),
               select = c("outcome")))


# Using rms package
m4 <- lrm(outcome ~ asthmacnt14 + ED + hospcnt14 + EDcnt14 + wellcnt14 + agegrp + female + race + fplgrp + hizip + amr14risk,
          data = asthmarisk, x = TRUE, y = TRUE)
residuals