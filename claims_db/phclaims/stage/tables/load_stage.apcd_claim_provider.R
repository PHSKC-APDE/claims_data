#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_PROVIDER
# Eli Kern, PHSKC (APDE)
#
# 2019-10

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_claim_provider_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "
    ------------------
    --STEP 1: Extract header-level provider variables, reshape, and insert into table shell
    -------------------
    insert into PHClaims.stage.apcd_claim_provider with (tablock)
    select internal_member_id as id_apcd, medical_claim_header_id as claim_header_id, first_service_dt as first_service_date, 
    last_service_dt as last_service_date, provider_id_apcd, provider_id_raw_apcd, provider_type, getdate() as last_run
    from PHClaims.stage.apcd_claim_provider_raw;",
    .con = db_claims))
}