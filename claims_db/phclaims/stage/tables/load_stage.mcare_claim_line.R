#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_claim_line
# Eli Kern, PHSKC (APDE)
#
# 2019-12

### Run from master_mcare_full_union script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/master_mcare_full_union.R

#### Load script ####
load_stage.mcare_claim_line_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "--Code to load data to stage.mcare_claim_line table
    --Distinct line-level claim variables that do not have a dedicated table (e.g. revenue code). In other words elements for which there is only one distinct 
    --value per claim line.
    --Eli Kern (PHSKC-APDE)
    --2020-01
    --Run time: XX min
    
    ------------------
    --STEP 1: Select (distinct) desired columns from multi-year claim tables on stage schema
    --Exclude all denied claims using proposed approach per ResDAC 01-2020 consult
    -------------------
    insert into PHClaims.stage.mcare_claim_line with (tablock)
    
    --bcarrier
    select
    --top 100
    rtrim(a.id_mcare) as id_mcare,
    rtrim(a.claim_header_id) as claim_header_id,
    a.claim_line_id,
    b.first_service_date,
    b.last_service_date,
    revenue_code = null,
    a.place_of_service_code,
    a.type_of_service,
    'carrier' as filetype_mcare,
    getdate() as last_run
    from PHClaims.stage.mcare_bcarrier_line as a
    left join PHClaims.stage.mcare_bcarrier_claims as b
    on a.claim_header_id = b.claim_header_id
    left join PHClaims.final.mcare_elig_demo as c
    on a.id_mcare = c.id_mcare
    --exclude denined claims using carrier/dme claim method
    where b.denial_code in ('1','2','3','4','5','6','7','8','9')
    --exclude claims among people who have eligibility data
    and c.id_mcare is not null
    
    --dme
    union
    select
    --top 100
    rtrim(a.id_mcare) as id_mcare,
    rtrim(a.claim_header_id) as claim_header_id,
    a.claim_line_id,
    b.first_service_date,
    b.last_service_date,
    revenue_code = null,
    a.place_of_service_code,
    a.type_of_service,
    'dme' as filetype_mcare,
    getdate() as last_run
    from PHClaims.stage.mcare_dme_line as a
    left join PHClaims.stage.mcare_dme_claims as b
    on a.claim_header_id = b.claim_header_id
    left join PHClaims.final.mcare_elig_demo as c
    on a.id_mcare = c.id_mcare
    --exclude denined claims using carrier/dme claim method
    where b.denial_code in ('1','2','3','4','5','6','7','8','9')
    --exclude claims among people who have eligibility data
    and c.id_mcare is not null
    
    --hha
    union
    select
    --top 100
    rtrim(a.id_mcare) as id_mcare,
    rtrim(a.claim_header_id) as claim_header_id,
    a.claim_line_id,
    b.first_service_date,
    b.last_service_date,
    a.revenue_code,
    place_of_service_code = null,
    type_of_service = null,
    'hha' as filetype_mcare,
    getdate() as last_run
    from PHClaims.stage.mcare_hha_revenue_center as a
    left join PHClaims.stage.mcare_hha_base_claims as b
    on a.claim_header_id = b.claim_header_id
    left join PHClaims.final.mcare_elig_demo as c
    on a.id_mcare = c.id_mcare
    --exclude denined claims using carrier/dme claim method
    where (b.denial_code_facility = '' or b.denial_code_facility is null)
    --exclude claims among people who have eligibility data
    and c.id_mcare is not null
    
    --hospice
    union
    select
    --top 100
    rtrim(a.id_mcare) as id_mcare,
    rtrim(a.claim_header_id) as claim_header_id,
    a.claim_line_id,
    b.first_service_date,
    b.last_service_date,
    a.revenue_code,
    place_of_service_code = null,
    type_of_service = null,
    'hospice' as filetype_mcare,
    getdate() as last_run
    from PHClaims.stage.mcare_hospice_revenue_center as a
    left join PHClaims.stage.mcare_hospice_base_claims as b
    on a.claim_header_id = b.claim_header_id
    left join PHClaims.final.mcare_elig_demo as c
    on a.id_mcare = c.id_mcare
    --exclude denined claims using carrier/dme claim method
    where (b.denial_code_facility = '' or b.denial_code_facility is null)
    --exclude claims among people who have eligibility data
    and c.id_mcare is not null
    
    --inpatient
    union
    select
    --top 100
    rtrim(a.id_mcare) as id_mcare,
    rtrim(a.claim_header_id) as claim_header_id,
    a.claim_line_id,
    b.first_service_date,
    b.last_service_date,
    a.revenue_code,
    place_of_service_code = null,
    type_of_service = null,
    'inpatient' as filetype_mcare,
    getdate() as last_run
    from PHClaims.stage.mcare_inpatient_revenue_center as a
    left join PHClaims.stage.mcare_inpatient_base_claims as b
    on a.claim_header_id = b.claim_header_id
    left join PHClaims.final.mcare_elig_demo as c
    on a.id_mcare = c.id_mcare
    --exclude denined claims using carrier/dme claim method
    where (b.denial_code_facility = '' or b.denial_code_facility is null)
    --exclude claims among people who have eligibility data
    and c.id_mcare is not null
    
    --outpatient
    union
    select
    --top 100
    rtrim(a.id_mcare) as id_mcare,
    rtrim(a.claim_header_id) as claim_header_id,
    a.claim_line_id,
    b.first_service_date,
    b.last_service_date,
    a.revenue_code,
    place_of_service_code = null,
    type_of_service = null,
    'outpatient' as filetype_mcare,
    getdate() as last_run
    from PHClaims.stage.mcare_outpatient_revenue_center as a
    left join PHClaims.stage.mcare_outpatient_base_claims as b
    on a.claim_header_id = b.claim_header_id
    left join PHClaims.final.mcare_elig_demo as c
    on a.id_mcare = c.id_mcare
    --exclude denined claims using carrier/dme claim method
    where (b.denial_code_facility = '' or b.denial_code_facility is null)
    --exclude claims among people who have eligibility data
    and c.id_mcare is not null
    
    --snf
    union
    select
    --top 100
    rtrim(a.id_mcare) as id_mcare,
    rtrim(a.claim_header_id) as claim_header_id,
    a.claim_line_id,
    b.first_service_date,
    b.last_service_date,
    a.revenue_code,
    place_of_service_code = null,
    type_of_service = null,
    'snf' as filetype_mcare,
    getdate() as last_run
    from PHClaims.stage.mcare_snf_revenue_center as a
    left join PHClaims.stage.mcare_snf_base_claims as b
    on a.claim_header_id = b.claim_header_id
    left join PHClaims.final.mcare_elig_demo as c
    on a.id_mcare = c.id_mcare
    --exclude denined claims using carrier/dme claim method
    where (b.denial_code_facility = '' or b.denial_code_facility is null)
    --exclude claims among people who have eligibility data
    and c.id_mcare is not null;",
        .con = db_claims))
    }

#### Table-level QA script ####
qa_stage.mcare_claim_line_qa_f <- function() {
  
  #confirm that claim line counts match for a specific revenue code
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
  "select 'stage.mcare_claim_line' as 'table', 'row count revenue code, expect match with outpatient table' as qa_type,
  count(*) as qa
  from stage.mcare_claim_line
  where filetype_mcare = 'outpatient' and revenue_code = '0450';",
  .con = db_claims))
  
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
  "select 'stage.mcare_outpatient_revenue_center' as 'table', 'row count revenue code, expect match with claim_line table' as qa_type,
  count(*) as qa
  from (
    select distinct a.id_mcare, a.claim_header_id, a.claim_line_id, b.first_service_date,
    b.last_service_date, a.revenue_code
    from stage.mcare_outpatient_revenue_center as a
    left join stage.mcare_outpatient_base_claims as b
    on a.claim_header_id = b.claim_header_id
    left join PHClaims.final.mcare_elig_demo as c
    on a.id_mcare = c.id_mcare
    --exclude denined claims using carrier/dme claim method
    where (b.denial_code_facility = '' or b.denial_code_facility is null)
    --exclude claims among people who have eligibility data
    and c.id_mcare is not null
    --specific revenue code
    and a.revenue_code = '0450'
  ) as x;",
  .con = db_claims))
  
  #confirm that claim line counts match for a specific place of service code
  res3 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcare_claim_line' as 'table', 'row count, expect match with carrier table' as qa_type,
    count(*) as qa
    from stage.mcare_claim_line
    where filetype_mcare = 'carrier' and place_of_service_code = '81';",
    .con = db_claims))
  
  res4 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcare_bcarrier_line' as 'table', 'row count, expect match with claim_line table' as qa_type,
    count(*) as qa
    from (
    	select distinct a.id_mcare, a.claim_header_id, a.claim_line_id, b.first_service_date,
    	b.last_service_date, a.place_of_service_code, a.type_of_service
    	from stage.mcare_bcarrier_line as a
    	left join stage.mcare_bcarrier_claims as b
    	on a.claim_header_id = b.claim_header_id
    	left join PHClaims.final.mcare_elig_demo as c
    	on a.id_mcare = c.id_mcare
    	--exclude denined claims using carrier/dme claim method
    	where b.denial_code in ('1','2','3','4','5','6','7','8','9')
    	--exclude claims among people who have eligibility data
    	and c.id_mcare is not null
    	--specific place of service code
    	and a.place_of_service_code = '81'
    ) as x;",
    .con = db_claims))

  #make sure everyone is in elig_demo
  res5 <- dbGetQuery(conn = db_claims, glue_sql(
  "select 'stage.mcare_claim_line' as 'table', '# members not in elig_demo, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stage.mcare_claim_line as a
    left join final.mcare_elig_demo as b
    on a.id_mcare = b.id_mcare
    where b.id_mcare is null;",
  .con = db_claims))
  
  #make sure everyone is in elig_timevar
  res6 <- dbGetQuery(conn = db_claims, glue_sql(
  "select 'stage.mcare_claim_line' as 'table', '# members not in elig_timevar, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stage.mcare_claim_line as a
    left join final.mcare_elig_timevar as b
    on a.id_mcare = b.id_mcare
    where b.id_mcare is null;",
  .con = db_claims))

res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}