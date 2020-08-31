# This code creates table ([stage].[mcaid_claim_line]) to hold DISTINCT 
# line-level claim information
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/azure_migration/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# Created by: Eli Kern, APDE, PHSKC, 2018-03-21
# R functions created by Alastair Matheson, PHSKC (APDE), 2019-05 and 2019-12
# Modified by: Philip Sylling, 2019-06-28
# 
# Data Pull Run time: 7.68 min
# Create Index Run Time: 7.2 min

#### PULL IN CONFIG FILE ####
config_url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/azure_migration/claims_db/phclaims/stage/tables/load_stage.mcaid_claim_line.yaml"

load_mcaid_claim_line_config <- yaml::yaml.load(RCurl::getURL(config_url))

#### DROP EXISTING TABLE TO USE SELECT INTO ####
try(DBI::dbRemoveTable(db_Claims, DBI::Id(schema = load_mcaid_claim_line_config$to_schema,
                                          table = load_mcaid_claim_line_config$to_Table)))

#### LOAD TABLE ####
# NB: Changes in table structure need to altered here and the YAML file
insert_sql <- glue::glue_sql("
INSERT INTO {`load_mcaid_claim_line_config$to_schema`}.{`load_mcaid_claim_line_config$to_table`} with (tablock)
(id_mcaid
,claim_header_id
,claim_line_id
,first_service_date
,last_service_date
,rev_code
,rac_code_line
,last_run)

SELECT DISTINCT
 MEDICAID_RECIPIENT_ID as id_mcaid
,TCN as claim_header_id
,CLM_LINE_TCN as claim_line_id
,FROM_SRVC_DATE as first_service_date
,TO_SRVC_DATE as last_service_date
,REVENUE_CODE as rev_code
,RAC_CODE_L as rac_code_line
,getdate() as last_run

from {`load_mcaid_claim_line_config$from_schema`}.{`load_mcaid_claim_line_config$from_table`};
", .con = db_claims)

message(glue::glue("Loading to {load_mcaid_claim_line_config$to_schema}.{load_mcaid_claim_line_config$to_table}"))
time_start <- Sys.time()
DBI::dbExecute(conn = db_claims, insert_sql)
time_end <- Sys.time()
print(paste0("Loading took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))


#### ADD INDEX ####
add_index_f(db_claims, table_config = load_mcaid_claim_line_config)


#### CLEAN  UP ####
rm(config_url, load_mcaid_claim_line_config)
rm(insert_sql)
rm(time_start, time_end)
