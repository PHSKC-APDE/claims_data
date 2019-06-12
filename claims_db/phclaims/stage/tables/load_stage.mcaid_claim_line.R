

# This code creates table ([stage].[mcaid_claim_line]) to hold DISTINCT 
# line-level claim information
# 
# Created by: Eli Kern, APDE, PHSKC, 2018-03-21
# R functions created by Alastair Matheson, PHSKC (APDE), 2019-05
# Modified by: Philip Sylling, 2019-06-11
# 
# Data Pull Run time: XX min
# Create Index Run Time: XX min
# 
# Table 'mcaid_claim'. Scan count 3, logical reads 8955218, physical reads 0, read-ahead reads 8926013, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
# Table 'Worktable'. Scan count 0, logical reads 0, physical reads 0, read-ahead reads 621302, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
# 
# Returns
# [stage].[mcaid_claim_line]
#  [id_mcaid]
# ,[claim_header_id]
# ,[claim_line_id]
# ,[rev_code]
# ,[rac_code_line]
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

print("Creating stage.mcaid_claim_line")

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")

step1_sql <- glue::glue_sql("
if object_id('[stage].[mcaid_claim_line]', 'U') is not null
drop table [stage].[mcaid_claim_line];
", .con = conn)
odbc::dbGetQuery(conn = db_claims, step1_sql)

#### CREATE TABLE ####
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.mcaid_claim_line.yaml",
               overall = T, ind_yr = F)

step2_sql <- glue::glue_sql("
insert into [stage].[mcaid_claim_line] with (tablock)
(id_mcaid
,claim_header_id
,claim_line_id
,rev_code
,rac_code_line
,last_run)

select 
distinct
--top(100)
 MEDICAID_RECIPIENT_ID as id_mcaid
,TCN as claim_header_id
,CLM_LINE_TCN as claim_line_id
,REVENUE_CODE as rev_code
,RAC_CODE_L as rac_code_line
,getdate() as last_run

from [stage].[mcaid_claim];
", .con = conn)

print("Running step 2: Load to [stage].[mcaid_claim_line]")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step2_sql)
time_end <- Sys.time()
print(paste0("Step 2 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))

step3_sql <- glue::glue_sql("
CREATE CLUSTERED INDEX [idx_cl_stage_mcaid_claim_line_claim_header_id] 
ON [stage].[mcaid_claim_line]([claim_header_id]);
CREATE NONCLUSTERED INDEX [idx_nc_stage_mcaid_claim_line_rev_code] 
ON [stage].[mcaid_claim_line]([rev_code]);
", .con = conn)

print("Running step 3: Create Indexes")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step3_sql)
time_end <- Sys.time()
print(paste0("Step 3 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))

