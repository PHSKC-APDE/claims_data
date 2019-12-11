
# This code creates table ([stage].[mcaid_claim_icdcm_header]) to hold DISTINCT 
# procedure codes in long format for Medicaid claims data
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_analytic.R
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


#### SET UP FUNCTIONS, ETC. ####
if (!exists("db_claims")) {
  db_claims <- dbConnect(odbc(), "PHClaims")  
}

if (!exists("create_table_f")) {
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
}

if (!exists("add_index")) {
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/add_index.R")
}

table_config_claim_icdcm_header <- yaml::yaml.load(
  RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_icdcm_header.yaml"))


#### STEP 1: CREATE TABLE ####
message("Running step 1: create table shell")
create_table_f(conn = db_claims,
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_icdcm_header.yaml",
               overall = T, ind_yr = F)


#### step 2: INSERT INTO TABLE ####
step2_sql <- glue::glue_sql("
insert into [stage].[mcaid_claim_icdcm_header] with (tablock)
({`names(table_config_claim_icdcm_header$vars)`*})

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
", .con = db_claims)

message("Running step 2: Load to [stage].[mcaid_claim_icdcm_header]")
time_start <- Sys.time()
DBI::dbExecute(conn = db_claims, step2_sql)
time_end <- Sys.time()
message(glue::glue("Step 2 took {round(difftime(time_end, time_start, units = 'secs'), 2)} ",
                   " secs ({round(difftime(time_end, time_start, units = 'mins'), 2)} mins)"))


#### STEP 3: ADD INDEX ####
message("Running step 3: create index")
time_start <- Sys.time()
add_index_f(db_claims, table_config = table_config_claim_icdcm_header)
time_end <- Sys.time()
message(glue::glue("Index creation took {round(difftime(time_end, time_start, units = 'secs'), 2)} ",
                   " secs ({round(difftime(time_end, time_start, units = 'mins'), 2)} mins)"))

