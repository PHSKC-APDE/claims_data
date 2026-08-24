#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_LINE
# Eli Kern, PHSKC (APDE)
#
# 2019-10
#
# 2023-07-28 update: Corrected nonsensical discharge dates
# 2024-04-29 update: Modified for HHSAW migration
# 2026-08-24 update: Modified under migration of ETL from Enclave -> KC
  # Added code to exclude i) non-WA residents, ii) people with no claims but no enrollment data, iii) set charge_amt to NULL for now

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/07_apcd_create_analytic_tables.R

#### Load script ####
load_stage.apcd_claim_line_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "
    insert into stg_claims.stage_apcd_claim_line
    select distinct
    a.internal_member_id as id_apcd,
    a.medical_claim_header_id as claim_header_id,
    a.medical_claim_service_line_id as claim_line_id,
    a.line_counter,
    b.first_service_dt as first_service_date,
    b.last_service_dt as last_service_date,
    NULL as charge_amt, --interim placeholder until cost data allowed
    a.revenue_code,
    a.place_of_service_code,
    a.admission_dt as admission_date,
    
    case
      when a.discharge_dt < a.admission_dt then a.last_service_dt
      when a.admission_dt is null and a.discharge_dt < a.first_service_dt then a.last_service_dt
      else a.discharge_dt
    end as discharge_date,   
    
    a.discharge_status_code,
    a.admission_point_of_origin_code,
    a.admission_type,
    getdate() as last_run
    from stg_claims.apcd_medical_claim as a
    
    left join stg_claims.apcd_medical_claim_header as b
    on a.medical_claim_header_id = b.medical_claim_header_id
    left join stg_claims.apcd_ref_nonresident_id as y
    on a.internal_member_id = y.id_apcd
    left join stg_claims.apcd_ref_claim_no_elig as z
    on a.internal_member_id = z.id_apcd
    
    --exclude denined/orphaned claims
    where (b.denied_header_flag = 'N' and b.orphaned_header_flag = 'N')
    --exclude members with no WA residency OR no elig data
    and (y.id_apcd is null and z.id_apcd is null);",
    .con = dw_inthealth))
}

#### Table-level QA script ####
qa_stage.apcd_claim_line_f <- function() {
  
  #make sure everyone is in elig_demo
  res1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_line' as 'table', '# members not in elig_demo, expect 0' as qa_type,
    count(a.id_apcd) as qa
    from stg_claims.stage_apcd_claim_line as a
    left join stg_claims.stage_apcd_elig_demo as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = dw_inthealth))
  
  #make sure everyone is in elig_month
  res2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_line' as 'table', '# members not in elig_month, expect 0' as qa_type,
    count(a.id_apcd) as qa
    from stg_claims.stage_apcd_claim_line as a
    left join stg_claims.stage_apcd_elig_month as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = dw_inthealth))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}