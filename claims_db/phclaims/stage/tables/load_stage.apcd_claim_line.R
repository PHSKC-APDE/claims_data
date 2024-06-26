#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_LINE
# Eli Kern, PHSKC (APDE)
#
# 2019-10
#
# 2023-07-28 update: Corrected nonsensical discharge dates
# 2024-04-29 update: Modified for HHSAW migration

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/07_apcd_create_analytic_tables.R

#### Load script ####
load_stage.apcd_claim_line_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "
    ------------------
    --STEP 1: Select (distinct) desired columns from claim line table
    --Exclude all denied/orphaned claim lines
    -------------------
    insert into stg_claims.stage_apcd_claim_line
    select distinct
    a.id_apcd,
    a.claim_header_id,
    a.claim_line_id,
    a.line_counter,
    a.first_service_dt as first_service_date,
    a.last_service_dt as last_service_date,
    a.charge_amt,
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
    from stg_claims.apcd_claim_line_raw as a
    --exclude denined/orphaned claims
    left join stg_claims.apcd_medical_claim_header as b
    on a.claim_header_id = b.medical_claim_header_id
    where b.denied_header_flag = 'N' and b.orphaned_header_flag = 'N';",
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
  
  #make sure everyone is in elig_timevar
  res2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_line' as 'table', '# members not in elig_timevar, expect 0' as qa_type,
    count(a.id_apcd) as qa
    from stg_claims.stage_apcd_claim_line as a
    left join stg_claims.stage_apcd_elig_timevar as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = dw_inthealth))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}