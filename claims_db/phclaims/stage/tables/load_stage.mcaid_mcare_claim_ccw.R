#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCAID_MCARE_CLAIM_CCW
# Eli Kern, PHSKC (APDE), 2019-10
# Alastair Mathesonm PHSKC (APDE), 2020-01
#
# This code is designed to be run as part of the master Medicaid/Medicare script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_mcare_analytic.R
#
# Run time: 113 min

if (!exists("load_ccw")) {
  devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/claim_ccw.R")
}


#### Load script ####
system.time(load_ccw(conn = db_claims, source = "mcaid_mcare"))


### Run QA
# Adapt script at https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/phclaims/stage/tables/qa_tmp.mcare_claim_ccw.sql


#### Archive current table ####
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcaid_mcare_claim_ccw")


#### Alter schema ####
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcaid_mcare_claim_ccw")


#### Create clustered columnstore index ####
# Run time: X min
system.time(dbSendQuery(conn = db_claims, glue_sql(
  "create clustered columnstore index idx_ccs_final_mcaid_mcare_claim_ccw on final.mcaid_mcare_claim_ccw")))
