# This code creates table ([stage].[mcaid_claim_procedure]) to hold DISTINCT 
# procedure codes in long format for Medicaid claims data
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# SQL script created by: Eli Kern, APDE, PHSKC, 2018-03-21
# R functions created by: Alastair Matheson, PHSKC (APDE), 2019-05 and 2019-12
# Modified by: Philip Sylling, 2019-06-11
# 
# Data Pull Run time: 9.66 min
# Create Index Run Time: 5.75 min
# 


#### PULL IN CONFIG FILE ####
config_url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_procedure.yaml"

load_mcaid_claim_procedure_config <- yaml::yaml.load(RCurl::getURL(config_url))


#### CREATE TABLE ####
create_table_f(conn = db_claims, config_url = config_url, overall = T, ind_yr = F)

#### LOAD TABLE ####
# NB: Changes in table structure need to altered here and the YAML file
insert_sql <- glue::glue_sql("
insert into [stage].[mcaid_claim_procedure] with (tablock)
([id_mcaid]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
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
,first_service_date
,last_service_date
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
,FROM_SRVC_DATE as first_service_date
,TO_SRVC_DATE as last_service_date
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

unpivot(procedure_code for procedure_code_number in 
([01],[02],[03],[04],[05],[06],[07],[08],[09],[10],[11],[12],[line])) as procedure_code;", 
.con = conn)

message(glue::glue("Loading to {load_mcaid_claim_procedure_config$to_schema}.{load_mcaid_claim_procedure_config$to_table}"))
time_start <- Sys.time()
DBI::dbExecute(conn = db_claims, insert_sql)
time_end <- Sys.time()
print(paste0("Loading took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))


#### ADD INDEX ####
add_index_f(db_claims, table_config = load_mcaid_claim_procedure_config)


#### CLEAN  UP ####
rm(config_url, load_mcaid_claim_procedure_config)
rm(insert_sql)
rm(time_start, time_end)
