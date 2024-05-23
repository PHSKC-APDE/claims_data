#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_ICDCM_HEADER
# Eli Kern, PHSKC (APDE)
#
# 2019-10
#
# 2024-04-29 update: Modified for HHSAW migration

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/07_apcd_create_analytic_tables.R

#### Load script ####
load_stage.apcd_claim_icdcm_header_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "
    ------------------
    --STEP 1: Assemble final table and insert into table shell 
    --Exclude all denied and orphaned claim lines
    -------------------
    insert into stg_claims.stage_apcd_claim_icdcm_header
    select a.internal_member_id as id_apcd, a.medical_claim_header_id as 'claim_header_id', a.first_service_dt as first_service_date, 
      a.last_service_dt as last_service_date, a.icdcm_raw, a.icdcm_norm, a.icdcm_version, a.icdcm_number, getdate() as last_run
    from stg_claims.apcd_claim_icdcm_raw as a
    --exclude denined/orphaned claims
    left join stg_claims.apcd_medical_claim_header as b
    on a.medical_claim_header_id = b.medical_claim_header_id
    where b.denied_header_flag = 'N' and b.orphaned_header_flag = 'N';",
    .con = dw_inthealth))
}

#### Table-level QA script ####
qa_stage.apcd_claim_icdcm_header_f <- function() {
  
  #all members should be in elig_demo and elig_timevar tables
  res1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_icdcm_header' as 'table', '# members not in elig_demo, expect 0' as qa_type,
    count(distinct a.id_apcd) as qa
    from stg_claims.stage_apcd_claim_icdcm_header as a
    left join stg_claims.stage_apcd_elig_demo as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = dw_inthealth))
  
  res2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_icdcm_header' as 'table', '# members not in elig_timevar, expect 0' as qa_type,
    count(distinct a.id_apcd) as qa
    from stg_claims.stage_apcd_claim_icdcm_header as a
    left join stg_claims.stage_apcd_elig_timevar as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = dw_inthealth))
  
  #length of all/most ICD-9-CM is 5
  res3 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_icdcm_header' as 'table', 'minimum length of ICD-9-CM, expect 5' as qa_type,
    min(len(icdcm_norm)) as qa
    from stg_claims.stage_apcd_claim_icdcm_header
    where icdcm_version = 9;",
    .con = dw_inthealth))
  
  res4 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_icdcm_header' as 'table', 'maximum length of ICD-9-CM, expect 5' as qa_type,
    max(len(icdcm_norm)) as qa
    from stg_claims.stage_apcd_claim_icdcm_header
    where icdcm_version = 9;",
    .con = dw_inthealth))
  
  #no null diagnoses
  res5 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_icdcm_header' as 'table', '# of null diagnoses, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_apcd_claim_icdcm_header
    where icdcm_raw is null;",
    .con = dw_inthealth))
  
  #count distinct ICD-CM codes that do not join to icdcm lookup table
  res8 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_icdcm_header' as 'table', '# of diagnoses not joining, expect <100' as qa_type,
    count (distinct a.icdcm_norm) as qa
    from stg_claims.stage_apcd_claim_icdcm_header as a
    left join stg_claims.ref_icdcm_codes as b
    on a.icdcm_norm = b.icdcm and a.icdcm_version = b.icdcm_version
    where b.icdcm is null;",
    .con = dw_inthealth))
  
  #length of ICD-10-CM between 3 and 7
  res9 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_icdcm_header' as 'table', 'minimum length of ICD-10-CM, expect >=3' as qa_type,
    min(len(icdcm_norm)) as qa
    from stg_claims.stage_apcd_claim_icdcm_header
    where icdcm_version = 10;",
    .con = dw_inthealth))
  
  res10 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_icdcm_header' as 'table', 'maximum length of ICD-10-CM, expect <=7' as qa_type,
    max(len(icdcm_norm)) as qa
    from stg_claims.stage_apcd_claim_icdcm_header
    where icdcm_version = 10;",
    .con = dw_inthealth))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}