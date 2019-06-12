
# This code creates table ([stage].[mcaid_claim_procedure]) to hold DISTINCT 
# procedure codes in long format for Medicaid claims data
# 
# SQL script created by: Eli Kern, APDE, PHSKC, 2018-03-21
# R functions created by: Alastair Matheson, PHSKC (APDE), 2019-05
# Modified by: Philip Sylling, 2019-06-11
# 
# Data Pull Run time: 10 min
# Create Index Run Time: 4 min
# 
# Returns
#  [stage].[mcaid_claim_procedure]
#  [id_mcaid]
# ,[claim_header_id]
# ,[procedure_code]
# ,[procedure_code_number]
# ,[modifier_1]
# ,[modifier_2]
# ,[modifier_3]
# ,[modifier_4]
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

print("Creating stage.mcaid_claim_procedure")

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")

step1_sql <- glue::glue_sql("
if object_id('[stage].[mcaid_claim_procedure]', 'U') is not null
drop table [stage].[mcaid_claim_procedure];
", .con = conn)
odbc::dbGetQuery(conn = db_claims, step1_sql)

#### CREATE TABLE ####
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.mcaid_claim_procedure.yaml",
               overall = T, ind_yr = F)

step2_sql <- glue::glue_sql("
insert into [stage].[mcaid_claim_procedure] with(tablock)
([id_mcaid]
,[claim_header_id]
,[procedure_code]
,[procedure_code_number]
,[modifier_1]
,[modifier_2]
,[modifier_3]
,[modifier_4]
,[last_run])

select distinct 
 id_mcaid
,claim_header_id
,procedure_code
,cast(procedure_code_number as varchar(4)) as procedure_code_number
,modifier_1
,modifier_2
,modifier_3
,modifier_4
,getdate() as last_run

from 
(
select
--top(100)
 MEDICAID_RECIPIENT_ID as id_mcaid
,TCN as claim_header_id
,PRCDR_CODE_1 as [01]
,PRCDR_CODE_2 as [02]
,PRCDR_CODE_3 as [03]
,PRCDR_CODE_4 as [04]
,PRCDR_CODE_5 as [05]
,PRCDR_CODE_6 as [06]
,PRCDR_CODE_7 as [07]
,PRCDR_CODE_8 as [08]
,PRCDR_CODE_9 as [09]
,PRCDR_CODE_10 as [10]
,PRCDR_CODE_11 as [11]
,PRCDR_CODE_12 as [12]
,LINE_PRCDR_CODE as [line]
,MDFR_CODE1 as [modifier_1]
,MDFR_CODE2 as [modifier_2]
,MDFR_CODE3 as [modifier_3]
,MDFR_CODE4 as [modifier_4]
from [stage].[mcaid_claim]
) as a

unpivot(procedure_code for procedure_code_number in ([01],[02],[03],[04],[05],[06],[07],[08],[09],[10],[11],[12],[line])) as procedure_code;
", .con = conn)

print("Running step 2: Load to [stage].[mcaid_claim_procedure]")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step2_sql)
time_end <- Sys.time()
print(paste0("Step 2 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))

step3_sql <- glue::glue_sql("
create clustered index [idx_cl_stage_mcaid_claim_procedure_claim_header_id] 
on [stage].[mcaid_claim_procedure]([claim_header_id]);
create nonclustered index [idx_nc_stage_mcaid_claim_procedure_procedure_code] 
on [stage].[mcaid_claim_procedure]([procedure_code]);
", .con = conn)

print("Running step 3: Create Indexes")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step3_sql)
time_end <- Sys.time()
print(paste0("Step 3 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))

