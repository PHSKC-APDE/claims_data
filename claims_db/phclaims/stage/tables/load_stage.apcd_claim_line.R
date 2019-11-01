#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_LINE
# Eli Kern, PHSKC (APDE)
#
# 2019-10

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_claim_line_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "
    ------------------
    --STEP 1: Select (distinct) desired columns from claim line table
    --Exclude all denied/orphaned claim lines
    -------------------
    insert into PHClaims.stage.apcd_claim_line with (tablock)
    select distinct
    internal_member_id as id_apcd,
    medical_claim_header_id as claim_header_id,
    medical_claim_service_line_id as claim_line_id,
    line_counter,
    first_service_dt as first_service_date,
    last_service_dt as last_service_date,
    charge_amt,
    revenue_code,
    place_of_service_code,
    getdate() as last_run
    from PHClaims.stage.apcd_medical_claim
    --exclude denined/orphaned claims
    where denied_claim_flag = 'N' and orphaned_adjustment_flag = 'N';",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.apcd_claim_line_f <- function() {
  
}