#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_claim_pharm_char
# Eli Kern, PHSKC (APDE)
#
# 2024-05

### Run from 02_master_mcare_claims_analytic.R script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/02_master_mcare_claims_analytic.R

#### Load script ####
load_stage.mcare_claim_pharm_char_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(inthealth, glue::glue_sql(
    "insert into stg_claims.stage_mcare_claim_pharm_char
    select
    ncpdp_id as pharmacy_id,
    physical_location_state_code,
    physical_location_open_date,
    physical_location_close_date,
    dispenser_class,
    primary_dispenser_type,
    primary_taxonomy_code,
    secondary_dispenser_type,
    secondary_taxonomy_code,
    tertiary_dispenser_type,
    tertiary_taxonomy_code,
    relationship_id,
    relationship_from_dt,
    relationship_thru_dt,
    relationship_type,
    prnt_org_id,
    eprscrb_srvc_ind,
    eprscrb_srvc_cd,
    dme_srvc_ind,
    dme_srvc_cd,
    walkin_clinic_ind,
    walkin_clinic_cd,
    immunizations_ind,
    immunizations_cd,
    status_340b_ind,
    status_340b_cd,
    getdate() as last_run
    from stg_claims.mcare_pharm_char;",
        .con = inthealth))
}