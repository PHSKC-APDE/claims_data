
# This code creates table ([stage].[mcaid_claim_icdcm_header]) to hold DISTINCT 
# procedure codes in long format for Medicaid claims data
# 
# SQL script created by: Eli Kern, APDE, PHSKC, 2018-03-21
# R functions created by: Alastair Matheson, PHSKC (APDE), 2019-05
# Modified by: Philip Sylling, 2019-06-11
# 
# Data Pull Run time: 17.36 min
# Create Index Run Time: 9.44 min
# 
# Returns
# [stage].[mcaid_claim_icdcm_header]
#  [id_mcaid]
# ,[claim_header_id]
# ,[first_service_date]
# ,[last_service_date]
# ,[icdcm_raw]
# ,[icdcm_norm]
# ,[icdcm_version]
# ,[icdcm_number]
# ,[last_run]

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170)

library(configr) # Read in YAML files
library(DBI)
library(dbplyr)
library(devtools)
library(dplyr)
library(glue)
library(janitor)
library(lubridate)
library(medicaid)
library(odbc)
library(openxlsx)
library(RCurl) # Read files from Github
library(tidyr)
library(tidyverse) # Manipulate data

db_claims <- dbConnect(odbc(), "PHClaims")

print("Creating stage.mcaid_claim_icdcm_header")

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")

step1_sql <- glue::glue_sql("
if object_id('[stage].[mcaid_claim_icdcm_header]', 'U') is not null
drop table [stage].[mcaid_claim_icdcm_header];
create table [stage].[mcaid_claim_icdcm_header]
([id_mcaid] varchar(255)
,[claim_header_id] bigint
,[first_service_date] date
,[last_service_date] date
,[icdcm_raw] varchar(255)
,[icdcm_norm] varchar(255)
,[icdcm_version] tinyint
,[icdcm_number] varchar(5)
,[last_run] datetime)
on [PRIMARY];
", .con = conn)
odbc::dbGetQuery(conn = db_claims, step1_sql)
dbDisconnect(db_claims)

#### CREATE TABLE ####
# create_table_f(conn = db_claims, 
#                config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.mcaid_claim_icdcm_header.yaml",
#                overall = T, ind_yr = F)

db_claims <- dbConnect(odbc(), "PHClaims")
step2_sql <- glue::glue_sql("
insert into [stage].[mcaid_claim_icdcm_header] with (tablock)
([id_mcaid]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[icdcm_raw]
,[icdcm_norm]
,[icdcm_version]
,[icdcm_number]
,[last_run])

select distinct
 id_mcaid
,claim_header_id
,first_service_date
,last_service_date
--original diagnosis codes without zero right-padding
,cast(diagnoses as varchar(200)) as icdcm_raw

,	
	cast(
		case
		    -- right-zero-pad ICD-9 diagnoses
			when (diagnoses like '[0-9]%' and len(diagnoses) = 3) then diagnoses + '00'
			when (diagnoses like '[0-9]%' and len(diagnoses) = 4) then diagnoses + '0'
			-- Both ICD-9 and ICD-10 codes have 'V' and 'E' prefixes
			-- Diagnoses prior to 2015-10-01 are ICD-9
			when (diagnoses like 'V%' and last_service_date < '2015-10-01' and len(diagnoses) = 3) then diagnoses + '00'
			when (diagnoses like 'V%' and last_service_date < '2015-10-01' and len(diagnoses) = 4) then diagnoses + '0'
			when (diagnoses like 'E%' and last_service_date < '2015-10-01' and len(diagnoses) = 3) then diagnoses + '00'
			when (diagnoses like 'E%' and last_service_date < '2015-10-01' and len(diagnoses) = 4) then diagnoses + '0'
			else diagnoses 
		end 
	as varchar(200)) as icdcm_norm

,
	cast(
		case
			when (diagnoses like '[0-9]%') then 9
			when (diagnoses like 'V%' and last_service_date < '2015-10-01') then 9
			when (diagnoses like 'E%' and last_service_date < '2015-10-01') then 9
			else 10 
		end 
	as tinyint) as icdcm_version

,cast(dx_number as varchar(5)) as icdcm_number
,getdate() as last_run

from 
(
select 
 MEDICAID_RECIPIENT_ID as id_mcaid
,TCN as claim_header_id
--,CLM_LINE_TCN
,FROM_SRVC_DATE as first_service_date
,TO_SRVC_DATE as last_service_date
,PRIMARY_DIAGNOSIS_CODE as [01]
,DIAGNOSIS_CODE_2 as [02]
,DIAGNOSIS_CODE_3 as [03]
,DIAGNOSIS_CODE_4 as [04]
,DIAGNOSIS_CODE_5 as [05]
,DIAGNOSIS_CODE_6 as [06]
,DIAGNOSIS_CODE_7 as [07]
,DIAGNOSIS_CODE_8 as [08]
,DIAGNOSIS_CODE_9 as [09]
,DIAGNOSIS_CODE_10 as [10]
,DIAGNOSIS_CODE_11 as [11]
,DIAGNOSIS_CODE_12 as [12]
,ADMTNG_DIAGNOSIS_CODE as [admit]

from stage.mcaid_claim
) as a

unpivot(diagnoses for dx_number IN ([01], [02], [03], [04], [05], [06], [07], [08], [09], [10], [11], [12], [admit])) as diagnoses;
", .con = conn)

print("Running step 2: Load to [stage].[mcaid_claim_icdcm_header]")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step2_sql)
time_end <- Sys.time()
print(paste0("Step 2 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))
dbDisconnect(db_claims)

db_claims <- dbConnect(odbc(), "PHClaims")
step3_sql <- glue::glue_sql("
create clustered index [idx_cl_mcaid_claim_icdcm_header_claim_header_id_icdcm_number]
on [stage].[mcaid_claim_icdcm_header]([claim_header_id], [icdcm_number]);
create nonclustered index [idx_nc_mcaid_claim_icdcm_header_icdcm_version_icdcm_norm] 
on [stage].[mcaid_claim_icdcm_header]([icdcm_version], [icdcm_norm]);
create nonclustered index [idx_nc_mcaid_claim_icdcm_header_first_service_date] 
on [stage].[mcaid_claim_icdcm_header]([first_service_date]);
", .con = conn)

print("Running step 3: Create Indexes")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step3_sql)
time_end <- Sys.time()
print(paste0("Step 3 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))
dbDisconnect(db_claims)

