
# This code creates table ([stage].[mcaid_claim_pharm]) to hold DISTINCT 
# pharmacy information
# 
# SQL script created by: Eli Kern, APDE, PHSKC, 2018-03-21
# R functions created by: Alastair Matheson, PHSKC (APDE), 2019-05
# Modified by: Philip Sylling, 2019-06-11
# 
# Data Pull Run time: 5.58 min
# Create Index Run Time: 2.17 min
# 
# Returns
# [stage].[mcaid_claim_pharm]
#  [id_mcaid]
# ,[claim_header_id]
# ,[ndc]
# ,[rx_days_supply]
# ,[rx_quantity]
# ,[rx_fill_date]
# ,[pharmacy_npi]
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
library(odbc)
library(openxlsx)
library(RCurl) # Read files from Github
library(tidyr)
library(tidyverse) # Manipulate data

db_claims <- dbConnect(odbc(), "PHClaims")
print("Creating stage.mcaid_claim_pharm")

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")

step1_sql <- glue::glue_sql("
if object_id('[stage].[mcaid_claim_pharm]', 'U') is not null
drop table [stage].[mcaid_claim_pharm];
create table [stage].[mcaid_claim_pharm]
([id_mcaid] varchar(255)
,[claim_header_id] bigint
,[ndc] varchar(255)
,[rx_days_supply] smallint
,[rx_quantity] numeric(19,3)
,[rx_fill_date] date
,[prescriber_id_format] varchar(10)
,[prescriber_id] varchar(255)
,[pharmacy_npi] bigint
,[last_run] datetime)
on [PRIMARY];
", .con = conn)
odbc::dbGetQuery(conn = db_claims, step1_sql)
dbDisconnect(db_claims)

#### CREATE TABLE ####
# create_table_f(conn = db_claims, 
#                config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.mcaid_claim_pharm.yaml",
#                overall = T, ind_yr = F)

db_claims <- dbConnect(odbc(), "PHClaims")
step2_sql <- glue::glue_sql("
insert into [stage].[mcaid_claim_pharm] with (tablock)
([id_mcaid]
,[claim_header_id]
,[ndc]
,[rx_days_supply]
,[rx_quantity]
,[rx_fill_date]
,[prescriber_id_format]
,[prescriber_id]
,[pharmacy_npi]
,[last_run])

select distinct 
 cast(MEDICAID_RECIPIENT_ID as varchar(255)) as id_mcaid
,cast(TCN as bigint) as claim_header_id
,cast(NDC as varchar(255)) as ndc
,cast(DAYS_SUPPLY as smallint) as rx_days_supply
,cast(SBMTD_DISPENSED_QUANTITY as numeric(19,3)) as rx_quantity
,cast(PRSCRPTN_FILLED_DATE as date) as rx_fill_date

,cast(case when (len([PRSCRBR_ID]) = 10 and isnumeric([PRSCRBR_ID]) = 1 and left([PRSCRBR_ID], 1) in (1,2)) then 'NPI'
           when (len([PRSCRBR_ID]) = 9 and isnumeric(substring([PRSCRBR_ID], 1, 2)) = 0 and isnumeric(substring([PRSCRBR_ID], 3, 7)) = 1) then 'DEA'
           when (len([PRSCRBR_ID]) = 6 and isnumeric(substring([PRSCRBR_ID], 1, 1)) = 0 and isnumeric(substring([PRSCRBR_ID], 2, 5)) = 1) then 'UPIN'
	       when [PRSCRBR_ID] = '5123456787' then 'WA HCA'
		   when [PRSCRBR_ID] is not null then 'UNKNOWN' end as varchar(10)) as prescriber_id_format

,cast(case when (len([PRSCRBR_ID]) <> 10 or isnumeric([PRSCRBR_ID]) = 0 or left([PRSCRBR_ID], 1) not in (1,2)) then [PRSCRBR_ID] end as varchar(255)) as prescriber_id

,cast(case when (len([PRSCRBR_ID]) = 10 and isnumeric([PRSCRBR_ID]) = 1 and left([PRSCRBR_ID], 1) in (1,2)) then [PRSCRBR_ID] end as bigint) as pharmacy_npi

,getdate() as last_run

from [stage].[mcaid_claim]
where ndc is not null;
", .con = conn)

print("Running step 2: Load to [stage].[mcaid_claim_pharm]")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step2_sql)
time_end <- Sys.time()
print(paste0("Step 2 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))
dbDisconnect(db_claims)

db_claims <- dbConnect(odbc(), "PHClaims")
step3_sql <- glue::glue_sql("
create clustered index [idx_cl_mcaid_claim_pharm_claim_header_id] 
on [stage].[mcaid_claim_pharm]([claim_header_id]);
create nonclustered index [idx_nc_mcaid_claim_pharm_ndc] 
on [stage].[mcaid_claim_pharm]([ndc]);
create nonclustered index [idx_nc_mcaid_claim_pharm_rx_fill_date] 
on [stage].[mcaid_claim_pharm]([rx_fill_date]);
", .con = conn)

print("Running step 3: Create Indexes")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step3_sql)
time_end <- Sys.time()
print(paste0("Step 3 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))
dbDisconnect(db_claims)

