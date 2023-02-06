#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_ICDCM_HEADER
# Eli Kern, PHSKC (APDE)
#
# 2019-10

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_claim_icdcm_header_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "
    ------------------
    --STEP 1: Prepare reference table to correct errors in ICD-CM codes with trailing zeroes
    -------------------
    
    --Pull out distinct ICD-CM codes that do not join to ICD-CM reference table
    if object_id('tempdb..#icdcm_fix_step0') is not null drop table #icdcm_fix_step0;
    select distinct a.icdcm_norm, a.icdcm_version
    into #icdcm_fix_step0
    from PHClaims.stage.apcd_claim_icdcm_raw as a
    left join PHClaims.ref.dx_lookup as b
    on a.icdcm_norm = b.dx and a.icdcm_version = b.dx_ver
    where b.dx is null;
    
    --Create corrected ICD-CM code by removing rightmost trailing zero (1st pass)
    if object_id('tempdb..#icdcm_fix_step1') is not null drop table #icdcm_fix_step1;
    select *,
    case
      when right(icdcm_norm, 1) = '0' then isnull(left(icdcm_norm, len(icdcm_norm) - 1), null)
      else icdcm_norm
    end as icdcm_corrected
    into #icdcm_fix_step1
    from icdcm_fix_step0;
      
    --Join corrected ICD-CM code to ICD-CM reference table (1st pass)
    if object_id('tempdb..#icdcm_fix_step2') is not null drop table #icdcm_fix_step2;
    select a.*,
    case when b.dx is not null then 1 else 0 end as icdcm_fix_flag
    into #icdcm_fix_step2
    from #icdcm_fix_step1 as a
    left join PHClaims.ref.dx_lookup as b
    on (a.icdcm_corrected = b.dx) and (a.icdcm_version = b.dx_ver);

    --Create corrected ICD-CM code by removing rightmost trailing zero (2nd pass)
    if object_id('tempdb..#icdcm_fix_step3') is not null drop table #icdcm_fix_step3;
    select icdcm_norm, icdcm_version, icdcm_fix_flag,
    case
    	when right(icdcm_corrected, 1) = '0' and icdcm_fix_flag = 0 then
    	  isnull(left(icdcm_corrected, len(icdcm_corrected) - 1), null)
    	else icdcm_corrected
    end as icdcm_corrected
    into #icdcm_fix_step3
    from #icdcm_fix_step2;
   
    --Join corrected ICD-CM code to ICD-CM reference table (2nd pass)
    if object_id('tempdb..#icdcm_fix_step4') is not null drop table #icdcm_fix_step4;
    select a.icdcm_norm, a.icdcm_version, a.icdcm_corrected,
    case when b.dx is not null then 1 else a.icdcm_fix_flag end as icdcm_fix_flag
    into #icdcm_fix_step4
    from #icdcm_fix_step3 as a
    left join PHClaims.ref.dx_lookup as b
    on (a.icdcm_corrected = b.dx) and (a.icdcm_version = b.dx_ver);
    
    --Create reference table with corrected ICD-CM code
    if object_id('tempdb..#icdcm_fix_ref_table') is not null drop table #icdcm_fix_ref_table;
    select icdcm_norm, icdcm_version, icdcm_corrected
    into #icdcm_fix_ref_table
    from #icdcm_fix_step4
    where icdcm_fix_flag = 1;
    
    
    ------------------
    --STEP 2: Assemble final table and insert into table shell 
    --Exclude all denied and orphaned claim lines
    --Correct ICD-CM codes using reference table created in above step
    -------------------
    insert into PHClaims.stage.apcd_claim_icdcm_header with (tablock)
    select a.internal_member_id as id_apcd, a.medical_claim_header_id as 'claim_header_id', a.first_service_dt as first_service_date, 
      a.last_service_dt as last_service_date, a.icdcm_raw,
      case when c.icdcm_corrected is not null then c.icdcm_corrected else a.icdcm_norm end as icdcm_norm, 
      a.icdcm_version, a.icdcm_number, getdate() as last_run
    from PHClaims.stage.apcd_claim_icdcm_raw as a
    --exclude denined/orphaned claims
    left join PHClaims.stage.apcd_medical_claim_header as b
    on a.medical_claim_header_id = b.medical_claim_header_id
    left join #icdcm_fix_ref_table as c
    on (a.icdcm_norm = c.icdcm_norm) and (a.icdcm_version = c.icdcm_version)
    where b.denied_header_flag = 'N' and b.orphaned_header_flag = 'N';",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.apcd_claim_icdcm_header_f <- function() {
  
  #all members should be in elig_demo and elig_timevar tables
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_icdcm_header' as 'table', '# members not in elig_demo, expect 0' as qa_type,
    count(distinct a.id_apcd) as qa
    from stage.apcd_claim_icdcm_header as a
    left join final.apcd_elig_demo as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = db_claims))
  
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_icdcm_header' as 'table', '# members not in elig_timevar, expect 0' as qa_type,
    count(distinct a.id_apcd) as qa
    from stage.apcd_claim_icdcm_header as a
    left join final.apcd_elig_timevar as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = db_claims))
  
  #length of all/most ICD-9-CM is 5
  res3 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_icdcm_header' as 'table', 'minimum length of ICD-9-CM, expect 5' as qa_type,
    min(len(icdcm_norm)) as qa
    from stage.apcd_claim_icdcm_header
    where icdcm_version = 9;",
    .con = db_claims))
  
  res4 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_icdcm_header' as 'table', 'maximum length of ICD-9-CM, expect 5' as qa_type,
    max(len(icdcm_norm)) as qa
    from stage.apcd_claim_icdcm_header
    where icdcm_version = 9;",
    .con = db_claims))
  
  #no null diagnoses
  res5 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_icdcm_header' as 'table', '# of null diagnoses, expect 0' as qa_type,
    count(*) as qa
    from stage.apcd_claim_icdcm_header
    where icdcm_raw is null;",
    .con = db_claims))
  
  #count distinct ICD-CM codes that do not join to dx lookup table
  res8 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_icdcm_header' as 'table', '# of diagnoses not joining, expect <100' as qa_type,
    count (distinct a.icdcm_norm) as qa
    from stage.apcd_claim_icdcm_header as a
    left join ref.dx_lookup as b
    on a.icdcm_norm = b.dx and a.icdcm_version = b.dx_ver
    where b.dx is null;",
    .con = db_claims))
  
  #length of ICD-10-CM between 3 and 7
  res9 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_icdcm_header' as 'table', 'minimum length of ICD-10-CM, expect >=3' as qa_type,
    min(len(icdcm_norm)) as qa
    from stage.apcd_claim_icdcm_header
    where icdcm_version = 10;",
    .con = db_claims))
  
  res10 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_icdcm_header' as 'table', 'maximum length of ICD-10-CM, expect <=7' as qa_type,
    max(len(icdcm_norm)) as qa
    from stage.apcd_claim_icdcm_header
    where icdcm_version = 10;",
    .con = db_claims))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}