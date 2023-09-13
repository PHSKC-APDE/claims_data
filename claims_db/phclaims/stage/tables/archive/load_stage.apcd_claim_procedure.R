#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_PROCEDURE
# Eli Kern, PHSKC (APDE)
#
# 2019-10

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_claim_procedure_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "
    ------------------
    --STEP 1: Create temp claim header-level table with exclusions applied
    --Exclude all denied and orphaned claim lines
    -------------------
    if object_id('tempdb..#temp1') is not null drop table #temp1;
    select distinct internal_member_id as 'id_apcd', medical_claim_header_id,
    	min(first_service_dt) over(partition by medical_claim_header_id) as first_service_date,
    	max(last_service_dt) over(partition by medical_claim_header_id) as last_service_date,
    	cast(procedure_code as varchar(20)) as pcline, cast(principal_icd_procedure_code as varchar(20)) as pc01, cast(icd_procedure_code_1 as varchar(20)) as pc02,
    	cast(icd_procedure_code_2 as varchar(20)) as pc03,  cast(icd_procedure_code_3 as varchar(20)) as pc04,
    	cast(icd_procedure_code_4 as varchar(20)) as pc05, cast(icd_procedure_code_5 as varchar(20)) as pc06, cast(icd_procedure_code_6 as varchar(20)) as pc07,
    	cast(icd_procedure_code_7 as varchar(20)) as pc08, cast(icd_procedure_code_8 as varchar(20)) as pc09, cast(icd_procedure_code_9 as varchar(20)) as pc10,
    	cast(icd_procedure_code_10 as varchar(20)) as pc11, cast(icd_procedure_code_11 as varchar(20)) as pc12, cast(icd_procedure_code_12 as varchar(20)) as pc13,
        cast(icd_procedure_code_13 as varchar(20)) as pc14, cast(icd_procedure_code_14 as varchar(20)) as pc15, cast(icd_procedure_code_15 as varchar(20)) as pc16,
        cast(icd_procedure_code_16 as varchar(20)) as pc17, cast(icd_procedure_code_17 as varchar(20)) as pc18, cast(icd_procedure_code_18 as varchar(20)) as pc19,
        cast(icd_procedure_code_19 as varchar(20)) as pc20, cast(icd_procedure_code_20 as varchar(20)) as pc21, cast(icd_procedure_code_21 as varchar(20)) as pc22,
        cast(icd_procedure_code_22 as varchar(20)) as pc23, cast(icd_procedure_code_23 as varchar(20)) as pc24, cast(icd_procedure_code_24 as varchar(20)) as pc25,
    	procedure_modifier_code_1 as modifier_1, procedure_modifier_code_2 as modifier_2,
    	procedure_modifier_code_3 as modifier_3, procedure_modifier_code_4 as modifier_4
    into #temp1
    from PHClaims.stage.apcd_medical_claim as a
    --exclude denined/orphaned claims
    left join PHClaims.ref.apcd_denied_orphaned_header as b
    on a.medical_claim_header_id = b.claim_header_id
    where b.denied_header_min = 0 and b.orphaned_header_min = 0;
    
    
    ------------------
    --STEP 2: Reshape diagnosis codes from wide to long
    --Exclude all missing procedure codes
    -------------------
    if object_id('tempdb..#temp2') is not null drop table #temp2;
    select distinct id_apcd, medical_claim_header_id, first_service_date, last_service_date, cast(pcodes as varchar(255)) as procedure_code,
    	cast(substring(procedure_code_number, 3,10) as varchar(200)) as procedure_code_number, modifier_1, modifier_2,
    	modifier_3, modifier_4
    into #temp2
    from #temp1 as a
    unpivot(pcodes for procedure_code_number in(pcline, pc01, pc02, pc03, pc04, pc05, pc06, pc07, pc08, pc09, pc10, pc11, pc12, pc13,
    	pc14, pc15, pc16, pc17, pc18, pc19, pc20, pc21, pc22, pc23, pc24, pc25)) as pcodes
    --exclude all procedure codes that are empty
    where pcodes is not null;
    
    --drop 1st temp table to free memory
    drop table #temp1;
    
    ------------------
    --STEP 3: Assemble final table and insert into table shell 
    -------------------
    insert into PHClaims.stage.apcd_claim_procedure with (tablock)
    select distinct id_apcd, medical_claim_header_id as claim_header_id,
    	first_service_date, last_service_date, procedure_code, procedure_code_number,
    	modifier_1, modifier_2, modifier_3, modifier_4, getdate() as last_run
    from #temp2;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.apcd_claim_procedure_f <- function() {
  
  #all members should be in elig_demo and elig_timevar tables
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_procedure' as 'table', '# members not in elig_demo, expect 0' as qa_type,
    count(distinct a.id_apcd) as qa
    from stage.apcd_claim_procedure as a
    left join final.apcd_elig_demo as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = db_claims))
  
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_procedure' as 'table', '# members not in elig_timevar, expect 0' as qa_type,
    count(distinct a.id_apcd) as qa
    from stage.apcd_claim_procedure as a
    left join final.apcd_elig_timevar as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = db_claims))
  
  #no null diagnoses
  res3 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_procedure' as 'table', '# of null procedure codes, expect 0' as qa_type,
    count(*) as qa
    from stage.apcd_claim_procedure
    where procedure_code is null;",
    .con = db_claims))
  
  #count distinct claim header IDs that have a 25th diagnosis code
  res4 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_procedure' as 'table', '# of claims with 25th procedure code' as qa_type,
    count(distinct claim_header_id) as qa
    from stage.apcd_claim_procedure
    where procedure_code_number = '25';",
    .con = db_claims))
  
  res5 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_medical_claim' as 'table', '# of claims with 25th procedure code' as qa_type,
    count(distinct medical_claim_header_id) as qa
    from PHClaims.stage.apcd_medical_claim as a
    --exclude denined/orphaned claims
    left join PHClaims.ref.apcd_denied_orphaned_header as b
    on a.medical_claim_header_id = b.claim_header_id
    where a.icd_procedure_code_24 is not null and 
      b.denied_header_min = 0 and b.orphaned_header_min = 0;",
    .con = db_claims))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}