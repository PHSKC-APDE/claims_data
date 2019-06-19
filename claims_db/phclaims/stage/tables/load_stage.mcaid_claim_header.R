
# This code creates table ([tmp].[mcaid_claim_header]) to hold DISTINCT 
# header-level claim information in long format for Medicaid claims data
# 
# SQL script created by: Eli Kern, APDE, PHSKC, 2018-03-21
# R functions created by: Alastair Matheson, PHSKC (APDE), 2019-05
# Modified by: Philip Sylling, 2019-06-13
# 
# Data Pull Run time: XX min
# Create Index Run Time: XX min
# 
# Returns
# [stage].[mcaid_claim_header]
# 
# /* Header-level columns from [stage].[mcaid_claim] */
#  [id_mcaid]
# ,[claim_header_id]
# ,[clm_type_mcaid_id]
# ,[claim_type_id]
# ,[first_service_date]
# ,[last_service_date]
# ,[patient_status]
# ,[admsn_source]
# ,[admsn_date]
# ,[admsn_time]
# ,[dschrg_date]
# ,[place_of_service_code]
# ,[type_of_bill_code]
# ,[clm_status_code]
# ,[billing_provider_npi]
# ,[drvd_drg_code]
# ,[insrnc_cvrg_code]
# ,[last_pymnt_date]
# ,[bill_date]
# ,[system_in_date]
# ,[claim_header_id_date]
# 
# /* Derived claim event flag columns (formerly columns from [mcaid_claim_summary]) */
#   
# ,[primary_diagnosis]
# ,[icdcm_version]
# ,[primary_diagnosis_poa]
# ,[mental_dx1]
# ,[mental_dxany]
# ,[mental_dx_rda_any]
# ,[sud_dx_rda_any]
# ,[maternal_dx1]
# ,[maternal_broad_dx1]
# ,[newborn_dx1]
# ,[ed]
# ,[ed_nohosp]
# ,[ed_bh]
# ,[ed_avoid_ca]
# ,[ed_avoid_ca_nohosp]
# ,[ed_ne_nyu]
# ,[ed_pct_nyu]
# ,[ed_pa_nyu]
# ,[ed_npa_nyu]
# ,[ed_mh_nyu]
# ,[ed_sud_nyu]
# ,[ed_alc_nyu]
# ,[ed_injury_nyu]
# ,[ed_unclass_nyu]
# ,[ed_emergent_nyu]
# ,[ed_nonemergent_nyu]
# ,[ed_intermediate_nyu]
# ,[inpatient]
# ,[ipt_medsurg]
# ,[ipt_bh]
# ,[intent]
# ,[mechanism]
# ,[sdoh_any]
# ,[ed_sdoh]
# ,[ipt_sdoh]
# ,[ccs]
# ,[ccs_description]
# ,[ccs_description_plain_lang]
# ,[ccs_mult1]
# ,[ccs_mult1_description]
# ,[ccs_mult2]
# ,[ccs_mult2_description]
# ,[ccs_mult2_plain_lang]
# ,[ccs_final_description]
# ,[ccs_final_plain_lang]
# 
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

print("Creating stage.mcaid_claim_header")

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")

step1_sql <- glue::glue_sql("
if object_id('[tmp].[mcaid_claim_header]', 'U') is not null
drop table [tmp].[mcaid_claim_header];
if object_id('[stage].[mcaid_claim_header]', 'U') is not null
drop table [stage].[mcaid_claim_header];
", .con = conn)
odbc::dbGetQuery(conn = db_claims, step1_sql)

#### CREATE TABLE ####
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.mcaid_claim_header.yaml",
               overall = T, ind_yr = F)

step2_sql <- glue::glue_sql("
select distinct 
 cast([MEDICAID_RECIPIENT_ID] as varchar(255)) as id_mcaid
,cast([TCN] as bigint) as claim_header_id
,cast([CLM_TYPE_CID] as varchar(20)) as clm_type_mcaid_id
,cast(ref.[kc_clm_type_id] as tinyint) as claim_type_id
,cast([FROM_SRVC_DATE] as date) as first_service_date
,cast([TO_SRVC_DATE] as date) as last_service_date
,cast([PATIENT_STATUS_LKPCD] as varchar(255)) as patient_status
,cast([ADMSN_SOURCE_LKPCD] as varchar(255)) as admsn_source
,cast([ADMSN_DATE] as date) as admsn_date
,cast(timefromparts([ADMSN_HOUR] / 100, [ADMSN_HOUR] % 100, 0, 0, 0) as time(0)) as admsn_time
,cast([DSCHRG_DATE] as date) as dschrg_date
,cast([FCLTY_TYPE_CODE] as varchar(255)) as place_of_service_code
,cast([TYPE_OF_BILL] as varchar(255)) as type_of_bill_code
,cast([CLAIM_STATUS] as tinyint) as clm_status_code
,cast(case when [CLAIM_STATUS] = 71 then [BLNG_NATIONAL_PRVDR_IDNTFR] 
           when ([CLAIM_STATUS] = 83 and [NPI] is not null) then [NPI] 
		   when ([CLAIM_STATUS] = 83 and [NPI] is null) then [BLNG_NATIONAL_PRVDR_IDNTFR] 
 end as bigint) as billing_provider_npi
,cast([DRVD_DRG_CODE] as varchar(255)) as drvd_drg_code
,cast([PRIMARY_DIAGNOSIS_POA_LKPCD] as varchar(255)) as primary_diagnosis_poa
,cast([INSRNC_CVRG_CODE] as varchar(255)) as insrnc_cvrg_code
,cast([LAST_PYMNT_DATE] as date) as last_pymnt_date
,cast([BILL_DATE] as date) as bill_date
,cast([SYSTEM_IN_DATE] as date) as system_in_date
,cast([TCN_DATE] as date) as claim_header_id_date

into [tmp].[mcaid_claim_header]
from [stage].[mcaid_claim] as clm
left join [ref].[kc_claim_type_crosswalk] as ref
on cast(clm.[CLM_TYPE_CID] as varchar(20)) = ref.[source_clm_type_id];
", .con = conn)

print("Running step 2...")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step2_sql)
time_end <- Sys.time()
print(paste0("Step 2 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))

try(odbc::dbRemoveTable(db_claims, "##dx_lookup", temporary = T))

step3_sql <- glue::glue_sql("
with cte as
(
SELECT *
	  ,ROW_NUMBER() OVER(PARTITION BY [dx_ver], [dx] ORDER BY [dx_ver], [dx]) AS [row_num]
FROM [PHClaims].[ref].[dx_lookup]
)
SELECT *
into ##dx_lookup
from cte
where [row_num] = 1;
create unique clustered index idx_cl_dx_lookup on #dx_lookup(dx_ver, dx);
", .con = conn)

print("Running step 3...")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step3_sql)
time_end <- Sys.time()
print(paste0("Step 3 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))
			 
try(odbc::dbRemoveTable(db_claims, "##", temporary = T))

step3_sql <- glue::glue_sql("
with cte as
(
SELECT *
	  ,ROW_NUMBER() OVER(PARTITION BY [dx_ver], [dx] ORDER BY [dx_ver], [dx]) AS [row_num]
FROM [PHClaims].[ref].[dx_lookup]
)
SELECT *
into ##dx_lookup
from cte
where [row_num] = 1;
create unique clustered index idx_cl_dx_lookup on #dx_lookup(dx_ver, dx);
", .con = conn)

print("Running step 3...")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step3_sql)
time_end <- Sys.time()
print(paste0("Step 3 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))

