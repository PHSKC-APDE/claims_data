
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
#   [id_mcaid]
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
#   ,[primary_diagnosis]
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
EXEC [stage].[sp_mcaid_claim_header];
", .con = conn)
odbc::dbGetQuery(conn = db_claims, step1_sql)
dbDisconnect(db_claims)

db_claims <- dbConnect(odbc(), "PHClaims")
step2_sql <- glue::glue_sql("
create clustered index [idx_cl_mcaid_claim_header_claim_header_id] 
on [stage].[mcaid_claim_header]([claim_header_id]);
create nonclustered index [idx_nc_mcaid_claim_header_type_of_bill_code] 
on [stage].[mcaid_claim_header]([type_of_bill_code]);
create nonclustered index [idx_nc_mcaid_claim_header_clm_type_mcaid_id] 
on [stage].[mcaid_claim_header]([clm_type_mcaid_id]);
create nonclustered index [idx_nc_mcaid_claim_header_drvd_drg_code] 
on [stage].[mcaid_claim_header]([drvd_drg_code]);
create nonclustered index [idx_nc_mcaid_claim_header_first_service_date] 
on [stage].[mcaid_claim_header]([first_service_date]);
create nonclustered index [idx_nc_mcaid_claim_header_id_mcaid] 
on [stage].[mcaid_claim_header]([id_mcaid]);
create nonclustered index [idx_nc_mcaid_claim_header_place_of_service_code] 
on [stage].[mcaid_claim_header]([place_of_service_code]);
", .con = conn)

print("Running step 2: Create Indexes")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step2_sql)
time_end <- Sys.time()
print(paste0("Step 2 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))
dbDisconnect(db_claims)

