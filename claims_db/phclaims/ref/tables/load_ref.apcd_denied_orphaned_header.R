#### CODE TO LOAD & TABLE-LEVEL QA REF.APCD_DENIED_ORPHANED_HEADER
# Eli Kern, PHSKC (APDE)
#
# 2019-11

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_ref.apcd_denied_orphaned_header_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "
    insert into PHClaims.ref.apcd_denied_orphaned_header with (tablock)
    select medical_claim_header_id as claim_header_id,
      min(case when denied_claim_flag = 'Y' then 1 else 0 end) as denied_min,
      max(case when denied_claim_flag = 'Y' then 1 else 0 end) as denied_max,
      min(case when orphaned_adjustment_flag = 'Y' then 1 else 0 end) as orphaned_min,
      max(case when orphaned_adjustment_flag = 'Y' then 1 else 0 end) as orphaned_max
    from PHClaims.stage.apcd_medical_claim
    group by medical_claim_header_id;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_ref.apcd_denied_orphaned_header_f <- function() {
  
    res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}