#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_PROCEDURE
# Eli Kern, PHSKC (APDE)
#
# 2019-10
#
# 2024-04-29 update: Modified for HHSAW migration

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/07_apcd_create_analytic_tables.R

#### Load script ####
load_stage.apcd_claim_procedure_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "
    ------------------
    --STEP 1: Create procedure code table with exclusions applied
    --Exclude all denied and orphaned claim lines
    -------------------
    insert into stg_claims.stage_apcd_claim_procedure
    select a.internal_member_id as id_apcd, a.medical_claim_header_id as claim_header_id,
    	a.first_service_dt as first_service_date, a.last_service_dt as last_service_date, a.procedure_code, a.procedure_code_number,
    	a.procedure_modifier_code_1 as modifier_1, a.procedure_modifier_code_2 as modifier_2, a.procedure_modifier_code_3 as modifier_3,
    	a.procedure_modifier_code_4 as modifier_4, getdate() as last_run
    from stg_claims.apcd_claim_procedure_raw as a
    --exclude denined/orphaned claims
    left join stg_claims.apcd_medical_claim_header as b
    on a.medical_claim_header_id = b.medical_claim_header_id
    where b.denied_header_flag = 'N' and b.orphaned_header_flag = 'N';",
    .con = dw_inthealth))
}


#### Table-level QA script ####
qa_stage.apcd_claim_procedure_f <- function() {
  
  #all members should be in elig_demo and elig_timevar tables
  res1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_procedure' as 'table', '# members not in elig_demo, expect 0' as qa_type,
    count(distinct a.id_apcd) as qa
    from stg_claims.stage_apcd_claim_procedure as a
    left join stg_claims.stage_apcd_elig_demo as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = dw_inthealth))
  
  res2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_procedure' as 'table', '# members not in elig_timevar, expect 0' as qa_type,
    count(distinct a.id_apcd) as qa
    from stg_claims.stage_apcd_claim_procedure as a
    left join stg_claims.stage_apcd_elig_timevar as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = dw_inthealth))
  
  #no null procedure codes
  res3 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_procedure' as 'table', '# of null procedure codes, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_apcd_claim_procedure
    where procedure_code is null;",
    .con = dw_inthealth))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}