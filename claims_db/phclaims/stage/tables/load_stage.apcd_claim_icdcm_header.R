#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_ICDCM_HEADER
# Eli Kern, PHSKC (APDE)
#
# 2019-10

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_claim_icdcm_header_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "
    ------------------
    --STEP 1: Create temp header-level table
    --Exclude all denied and orphaned claim lines
    -------------------
    if object_id('tempdb..#temp1') is not null drop table #temp1;
    select distinct 
    	internal_member_id as 'id_apcd', medical_claim_header_id, 
    	min(first_service_dt) over(partition by medical_claim_header_id) as first_service_date,
    	max(last_service_dt) over(partition by medical_claim_header_id) as last_service_date,
    	icd_version_ind,
    	admitting_diagnosis_code as dxadmit, principal_diagnosis_code as dx01, diagnosis_code_other_1 as dx02,
    	diagnosis_code_other_2 as dx03,  diagnosis_code_other_3 as dx04,
    	diagnosis_code_other_4 as dx05, diagnosis_code_other_5 as dx06, diagnosis_code_other_6 as dx07,
    	diagnosis_code_other_7 as dx08, diagnosis_code_other_8 as dx09, diagnosis_code_other_9 as dx10,
    	diagnosis_code_other_10 as dx11, diagnosis_code_other_11 as dx12, diagnosis_code_other_12 as dx13,
        diagnosis_code_other_13 as dx14, diagnosis_code_other_14 as dx15, diagnosis_code_other_15 as dx16,
        diagnosis_code_other_16 as dx17, diagnosis_code_other_17 as dx18, diagnosis_code_other_18 as dx19,
        diagnosis_code_other_19 as dx20, diagnosis_code_other_20 as dx21, diagnosis_code_other_21 as dx22,
        diagnosis_code_other_22 as dx23, diagnosis_code_other_23 as dx24, diagnosis_code_other_24 as dx25,
    	eci_diagnosis as dxecode
    into #temp1
    from PHClaims.stage.apcd_medical_claim as a
    --exclude denined/orphaned claims
    left join PHClaims.ref.apcd_denied_orphaned_header as b
    on a.medical_claim_header_id = b.claim_header_id
    where b.denied_header_min = 0 and b.orphaned_header_min = 0;
    
    
    ------------------
    --STEP 2: Reshape diagnosis codes from wide to long, normalize ICD-9-CM to 5 digits
    -------------------
    if object_id('tempdb..#temp2') is not null drop table #temp2;
    select distinct id_apcd, medical_claim_header_id, first_service_date, last_service_date,
    	--raw diagnosis codes
    	cast(diagnoses as varchar(200)) as 'icdcm_raw',
    	--normalized diagnosis codes
    	cast(
    		case
    			when (icd_version_ind = '9' and len(diagnoses) = 3) then diagnoses + '00'
    			when (icd_version_ind = '9' and len(diagnoses) = 4) then diagnoses + '0'
    			else diagnoses 
    		end 
    	as varchar(200)) as 'icdcm_norm',
    	--convert ICD-CM version to integer
    	cast(case when icd_version_ind = '9' then 9 when icd_version_ind = '0' then 10 end as tinyint) as 'icdcm_version',
    	--ICD-CM number
    	cast(substring(icdcm_number, 3,10) as varchar(200)) as 'icdcm_number'
    into #temp2
    from #temp1 as a
    unpivot(diagnoses for icdcm_number in(dxadmit, dx01, dx02, dx03, dx04, dx05, dx06, dx07, dx08, dx09, dx10, dx11, dx12, dx13,
    	dx14, dx15, dx16, dx17, dx18, dx19, dx20, dx21, dx22, dx23, dx24, dx25, dxecode)) as diagnoses
    --exclude all diagnoses that are empty
    where diagnoses is not null;
    
    --drop 1st temp table to free memory
    drop table #temp1;
    
    ------------------
    --STEP 3: Assemble final table and insert into table shell 
    -------------------
    insert into PHClaims.stage.apcd_claim_icdcm_header with (tablock)
    select distinct id_apcd, medical_claim_header_id as 'claim_header_id', first_service_date, last_service_date,
    	icdcm_raw, icdcm_norm, icdcm_version, icdcm_number, getdate() as last_run
    from #temp2;",
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
  
  #count distinct claim header IDs that have a 25th diagnosis code
  res6 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_icdcm_header' as 'table', '# of claims with 25th diagnosis' as qa_type,
    count(distinct claim_header_id) as qa
    from stage.apcd_claim_icdcm_header
    where icdcm_number = '25';",
    .con = db_claims))
  
  res7 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_medical_claim' as 'table', '# of claims with 25th diagnosis' as qa_type,
    count(distinct medical_claim_header_id) as qa
    from PHClaims.stage.apcd_medical_claim as a
    --exclude denined/orphaned claims
    left join PHClaims.ref.apcd_denied_orphaned_header as b
    on a.medical_claim_header_id = b.claim_header_id
    where a.diagnosis_code_other_24 is not null
      and b.denied_header_min = 0 and b.orphaned_header_min = 0;",
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