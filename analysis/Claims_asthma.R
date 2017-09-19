###############################################################################
# Eli Kern
# 2017-9-6

# Code to identify patients with asthma using Chronic Conditions Warehouse algorithm
#https://www.ccwdata.org/web/guest/condition-categories

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

##### Bring in IDs for members with asthma diagnosis in any diagnosis field #####
#Per CCW algorithm, this includes only members with 1 inpatient, SNF, home health agency OR 2 hospital outpatient or carrier claims

##members with inpatient, SNF, or home health asthma claims
ptm01 <- proc.time() # Times how long this query takes (~400 secs)
ast_ipt1 <- sqlQuery(
  db.claims51,
  "SELECT distinct id, tcn
FROM
  (SELECT MEDICAID_RECIPIENT_ID AS id, tcn, PRIMARY_DIAGNOSIS_CODE AS dx1,
  DIAGNOSIS_CODE_2 AS dx2, DIAGNOSIS_CODE_3 AS dx3,DIAGNOSIS_CODE_4 AS dx4,DIAGNOSIS_CODE_5 AS dx5,
  DIAGNOSIS_CODE_6 as dx6, DIAGNOSIS_CODE_7 as dx7, DIAGNOSIS_CODE_8 as dx8, DIAGNOSIS_CODE_9 as dx9,
  DIAGNOSIS_CODE_10 as dx10, DIAGNOSIS_CODE_11 as dx11, DIAGNOSIS_CODE_12 as dx12
  FROM PHClaims.dbo.NewClaims
  
  WHERE FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31' --claims from 2016, 1-year ref period per CCW
  AND CLM_TYPE_CID in (31, 12, 23) --inpatient, snf, hha claims
  )as a
  --CCW ICD9 and ICD10 codes for asthma
  unpivot(value for col in(dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12)) as x
  where (x.value LIKE '493%' OR x.value LIKE 'J45%' OR x.value LIKE 'J44%')",
  stringsAsFactors = FALSE
)
proc.time() - ptm01

##members with inpatient, SNF, home health, or crossover inpatient asthma claims
ptm01 <- proc.time() # Times how long this query takes (~400 secs)
ast_ipt2 <- sqlQuery(
  db.claims51,
  "SELECT distinct id, tcn
FROM
  (SELECT MEDICAID_RECIPIENT_ID AS id, tcn, PRIMARY_DIAGNOSIS_CODE AS dx1,
  DIAGNOSIS_CODE_2 AS dx2, DIAGNOSIS_CODE_3 AS dx3,DIAGNOSIS_CODE_4 AS dx4,DIAGNOSIS_CODE_5 AS dx5,
  DIAGNOSIS_CODE_6 as dx6, DIAGNOSIS_CODE_7 as dx7, DIAGNOSIS_CODE_8 as dx8, DIAGNOSIS_CODE_9 as dx9,
  DIAGNOSIS_CODE_10 as dx10, DIAGNOSIS_CODE_11 as dx11, DIAGNOSIS_CODE_12 as dx12
  FROM PHClaims.dbo.NewClaims
  
  WHERE FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31' --claims from 2016, 1-year ref period per CCW
  AND CLM_TYPE_CID in (31, 12, 23, 33) --inpatient, snf, hha claims, part A XO inpatient
  )as a
  --CCW ICD9 and ICD10 codes for asthma
  unpivot(value for col in(dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12)) as x
  where (x.value LIKE '493%' OR x.value LIKE 'J45%' OR x.value LIKE 'J44%')",
  stringsAsFactors = FALSE
)
proc.time() - ptm01

##members with outpatient or professional asthma claims
ptm01 <- proc.time() # Times how long this query takes (~400 secs)
ast_opt1 <- sqlQuery(
  db.claims51,
  "SELECT distinct id
FROM
  (SELECT DISTINCT id, COUNT(DISTINCT fr_sdt) AS clm_cnt
  FROM
  (SELECT MEDICAID_RECIPIENT_ID AS id, tcn, FROM_SRVC_DATE AS fr_sdt, PRIMARY_DIAGNOSIS_CODE AS dx1,
  DIAGNOSIS_CODE_2 AS dx2, DIAGNOSIS_CODE_3 AS dx3,DIAGNOSIS_CODE_4 AS dx4,DIAGNOSIS_CODE_5 AS dx5,
  DIAGNOSIS_CODE_6 as dx6, DIAGNOSIS_CODE_7 as dx7, DIAGNOSIS_CODE_8 as dx8, DIAGNOSIS_CODE_9 as dx9,
  DIAGNOSIS_CODE_10 as dx10, DIAGNOSIS_CODE_11 as dx11, DIAGNOSIS_CODE_12 as dx12
  FROM PHClaims.dbo.NewClaims
  WHERE (FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31' --claims from 2016
  AND (CLM_TYPE_CID in (3, 26, 1))) --outpatient & professional claims
  )as a
  --CCW ICD9 and ICD10 codes for asthma
  unpivot(value for col in(dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12)) as x
  where (x.value LIKE '493%' OR x.value LIKE 'J45%' OR x.value LIKE 'J44%')
  GROUP BY id) b
  WHERE clm_cnt>1",
  stringsAsFactors = FALSE
)
proc.time() - ptm01

##outpatient or professional asthma claims (to intersect with IDs with >1 claims in R)
ptm01 <- proc.time() # Times how long this query takes (~400 secs)
ast_opt1clm <- sqlQuery(
  db.claims51,
  "SELECT distinct id, tcn
FROM
  (SELECT MEDICAID_RECIPIENT_ID AS id, tcn, PRIMARY_DIAGNOSIS_CODE AS dx1,
  DIAGNOSIS_CODE_2 AS dx2, DIAGNOSIS_CODE_3 AS dx3,DIAGNOSIS_CODE_4 AS dx4,DIAGNOSIS_CODE_5 AS dx5,
  DIAGNOSIS_CODE_6 as dx6, DIAGNOSIS_CODE_7 as dx7, DIAGNOSIS_CODE_8 as dx8, DIAGNOSIS_CODE_9 as dx9,
  DIAGNOSIS_CODE_10 as dx10, DIAGNOSIS_CODE_11 as dx11, DIAGNOSIS_CODE_12 as dx12
  FROM PHClaims.dbo.NewClaims
  
  WHERE FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31' --claims from 2016, 1-year ref period per CCW
  AND CLM_TYPE_CID in (3, 26, 1) --outpatient & professional claims
  )as a
  --CCW ICD9 and ICD10 codes for asthma
  unpivot(value for col in(dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12)) as x
  where (x.value LIKE '493%' OR x.value LIKE 'J45%' OR x.value LIKE 'J44%')",
  stringsAsFactors = FALSE
)
proc.time() - ptm01

##members with outpatient, professional, part A XO outpatient, part B XO, epsdt, kidney center, or ambulatory surgery asthma claims
ptm01 <- proc.time() # Times how long this query takes (~400 secs)
ast_opt2 <- sqlQuery(
  db.claims51,
  "SELECT distinct id
FROM
	(SELECT DISTINCT id, COUNT(DISTINCT fr_sdt) AS clm_cnt
	FROM
		(SELECT MEDICAID_RECIPIENT_ID AS id, tcn, FROM_SRVC_DATE AS fr_sdt, PRIMARY_DIAGNOSIS_CODE AS dx1,
		DIAGNOSIS_CODE_2 AS dx2, DIAGNOSIS_CODE_3 AS dx3,DIAGNOSIS_CODE_4 AS dx4,DIAGNOSIS_CODE_5 AS dx5,
		DIAGNOSIS_CODE_6 as dx6, DIAGNOSIS_CODE_7 as dx7, DIAGNOSIS_CODE_8 as dx8, DIAGNOSIS_CODE_9 as dx9,
		DIAGNOSIS_CODE_10 as dx10, DIAGNOSIS_CODE_11 as dx11, DIAGNOSIS_CODE_12 as dx12
		FROM PHClaims.dbo.NewClaims
		WHERE (FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31' --claims from 2016
			AND (CLM_TYPE_CID in (3, 26, 1, 34, 28, 27, 25, 19))) --claim type
		)as a
	--CCW ICD9 and ICD10 codes for asthma
	unpivot(value for col in(dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12)) as x
	where (x.value LIKE '493%' OR x.value LIKE 'J45%' OR x.value LIKE 'J44%')
	GROUP BY id) b
WHERE clm_cnt>1",
  stringsAsFactors = FALSE
)
proc.time() - ptm01

##outpatient, professional, part A XO outpatient, part B XO, epsdt, kidney center, or ambulatory surgery asthma claims (to intersect with IDs with >1 claims in R)
ptm01 <- proc.time() # Times how long this query takes (~400 secs)
ast_opt2clm <- sqlQuery(
  db.claims51,
  "SELECT distinct id, tcn
FROM
  (SELECT MEDICAID_RECIPIENT_ID AS id, tcn, PRIMARY_DIAGNOSIS_CODE AS dx1,
  DIAGNOSIS_CODE_2 AS dx2, DIAGNOSIS_CODE_3 AS dx3,DIAGNOSIS_CODE_4 AS dx4,DIAGNOSIS_CODE_5 AS dx5,
  DIAGNOSIS_CODE_6 as dx6, DIAGNOSIS_CODE_7 as dx7, DIAGNOSIS_CODE_8 as dx8, DIAGNOSIS_CODE_9 as dx9,
  DIAGNOSIS_CODE_10 as dx10, DIAGNOSIS_CODE_11 as dx11, DIAGNOSIS_CODE_12 as dx12
  FROM PHClaims.dbo.NewClaims
  
  WHERE FROM_SRVC_DATE BETWEEN '2016-01-01' AND '2016-12-31' --claims from 2016, 1-year ref period per CCW
  AND CLM_TYPE_CID in (3, 26, 1, 34, 28, 27, 25, 19) --claim type
  )as a
  --CCW ICD9 and ICD10 codes for asthma
  unpivot(value for col in(dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12)) as x
  where (x.value LIKE '493%' OR x.value LIKE 'J45%' OR x.value LIKE 'J44%')",
  stringsAsFactors = FALSE
)
proc.time() - ptm01

##### Append and deduplicate (i.e. union) inpatient/outpatient data extracts #####

#Subset outpatient/prof claims to those that meet claim count criteria
ast_opt1_mrg <- left_join(ast_opt1, ast_opt1clm, by = c("id"), suffix = c("x", "y"))
ast_opt2_mrg <- left_join(ast_opt2, ast_opt2clm, by = c("id"), suffix = c("x", "y"))

#Combine inpatietn and outpatient claims (duplicates removed)
ast1 <- union(ast_ipt1, ast_opt1_mrg)
ast2 <- union(ast_ipt2, ast_opt2_mrg)

#Make sure IDs are all uppercase
ast1 <- mutate(ast1, id = str_to_upper(id))
ast2 <- mutate(ast2, id = str_to_upper(id))

#drop temp files
rm(ast_ipt1,ast_ipt2,ast_opt1,ast_opt2,ast_opt1clm,ast_opt2clm,ast_opt1_mrg,ast_opt2_mrg)

##### Count members and claims, method 1 #####

#count distinct asthmatic members per algorithm
summarise(ast1,dplyr::n_distinct(id))
summarise(ast2,dplyr::n_distinct(id))

##### Merge with eligibility data to set minimum coverage thresholds #####
#NOTE: this assumes elig_dob_process_Eli script has been run with local macros set to desired date range

#Filter elig_overall to those with XX enrollment in 2016
elig_2016 <- filter(elig_overall, cov2016_mth_tot >= 1)

#Merge with DOB information
elig_2016 <- rename(elig_2016, id = MEDICAID_RECIPIENT_ID)
elig_2016 <- left_join(elig_2016, elig_dob, by = c("id"))

#Keep only needed columns, drop duplicate rows as now all information about members and 2016 coverage is on same row
elig_2016 <- distinct(select(elig_2016, id, cov2016_dy_tot, cov2016_mth_tot, ssnnew, dobnew, age2016))

#drop temp files
rm(elig_dob,elig_overall)

##### SCENARIO 1 - VARY COVERAGE THRESHOLD #####

#Make data subsets corresponding to 1, 3, 6, 9 and 12 months coverage in 2016
elig_2016_1 <- elig_2016
elig_2016_3 <- filter(elig_2016, cov2016_mth_tot >= 3)
elig_2016_6 <- filter(elig_2016, cov2016_mth_tot >= 6)
elig_2016_9 <- filter(elig_2016, cov2016_mth_tot >= 9)
elig_2016_12 <- filter(elig_2016, cov2016_mth_tot >= 12)

#Filter asthmatic members/claims data by coverage month thresholds
ast1_1 <- inner_join(ast1, elig_2016_1, by = c("id"))

test <- setdiff(ast1, select(ast1_1,id,tcn))
test <- mutate(test, id = str_to_upper(id))                     
