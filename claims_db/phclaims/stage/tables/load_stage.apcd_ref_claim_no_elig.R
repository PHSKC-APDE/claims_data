#### Code to create a list of members with claims data but no enrollment data
# Eli Kern, PHSKC (APDE)
#
# 2026-08-19

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_ref_claim_no_elig_f <- function() {
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "
    --select distinct member IDs from member_month_detail table
    with temp1 as (
      select distinct internal_member_id
      from stg_claims.apcd_member_month_detail
    ),
    --select distinct member IDs in medical, pharmacy, and dental claim tables
    temp2 as (
      select distinct internal_member_id
      from stg_claims.apcd_medical_claim
      union
      select distinct internal_member_id
      from stg_claims.apcd_pharmacy_claim
      union
      select distinct internal_member_id
      from stg_claims.apcd_dental_claim
    )
    --select IDs in claim table but not in member_month table
    select internal_member_id as id_apcd, getdate() as last_run 
    into stg_claims.apcd_ref_claim_no_elig
    from temp2
    except
    select internal_member_id as id_apcd, getdate() as last_run from temp1;",
    .con = dw_inthealth))
}