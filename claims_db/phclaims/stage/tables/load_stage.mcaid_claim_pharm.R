# This code creates table ([stage].[mcaid_claim_pharm]) to hold DISTINCT 
# pharmacy information
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# SQL script created by: Eli Kern, APDE, PHSKC, 2018-03-21
# R functions created by: Alastair Matheson, PHSKC (APDE), 2019-05 and 2019-12
# Modified by: Philip Sylling, 2019-06-11
# 
# Data Pull Run time: 5.58 min
# Create Index Run Time: 2.17 min


#### PULL IN CONFIG FILE ####
config_url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_pharm.yaml"

load_mcaid_claim_pharm_config <- yaml::yaml.load(RCurl::getURL(config_url))


#### DROP EXISTING TABLE TO USE SELECT INTO ####
try(DBI::dbRemoveTable(db_Claims, DBI::Id(schema = load_mcaid_claim_pharm_config$to_schema,
                                          table = load_mcaid_claim_pharm_config$to_Table)))

#### LOAD TABLE ####
# NB: Changes in table structure need to altered here and the YAML file
insert_sql <- glue::glue_sql("
SELECT DISTINCT
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
INTO {`load_mcaid_claim_pharm_config$to_schema`}.{`load_mcaid_claim_pharm_config$to_table`}
FROM {`load_mcaid_claim_pharm_config$from_schema`}.{`load_mcaid_claim_pharm_config$from_table`}
where ndc is not null;
", .con = db_claims)

message(glue::glue("Loading to {load_mcaid_claim_pharm_config$to_schema}.{load_mcaid_claim_pharm_config$to_table}"))
time_start <- Sys.time()
DBI::dbExecute(conn = db_claims, insert_sql)
time_end <- Sys.time()
print(paste0("Loading took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))


#### ADD INDEX ####
add_index_f(db_claims, table_config = load_mcaid_claim_pharm_config)


#### CLEAN  UP ####
rm(config_url, load_mcaid_claim_pharm_config)
rm(insert_sql)
rm(time_start, time_end)
