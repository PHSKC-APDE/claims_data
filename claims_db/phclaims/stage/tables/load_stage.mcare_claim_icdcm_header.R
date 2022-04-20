#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_claim_icdcm_header
# Eli Kern, PHSKC (APDE)
#
# 2019-12

### Run from master_mcare_full_union script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/master_mcare_full_union.R

#### Load script ####
load_stage.mcare_claim_icdcm_header_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "--Code to load data to stage.mcare_claim_icdcm_header table
    --ICD-CM codes reshaped to long
    --Eli Kern (PHSKC-APDE)
    --2020-01
    --Run time: XX min
    
    ------------------
    --STEP 1: Select and union desired columns from multi-year claim tables on stage schema
    --Exclude all denied claims using proposed approach per ResDAC 01-2020 consult
    --Unpivot and insert into table shell
    -------------------
    insert into PHClaims.stage.mcare_claim_icdcm_header with (tablock)
    
    select distinct
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    
    	--original diagnosis code
    	diagnoses as 'icdcm_raw',
        
    	--normalized diagnosis code
    	case
    		when (diagnoses like '[0-9]%' and len(diagnoses) = 3) then diagnoses + '00'
    		when (diagnoses like '[0-9]%' and len(diagnoses) = 4) then diagnoses + '0'
    		when (diagnoses like 'V%' and first_service_date < '2015-10-01' and len(diagnoses) = 3) then diagnoses + '00'
    		when (diagnoses like 'V%' and first_service_date < '2015-10-01' and len(diagnoses) = 4) then diagnoses + '0'
    		when (diagnoses like 'E%' and first_service_date < '2015-10-01' and len(diagnoses) = 3) then diagnoses + '00'
    		when (diagnoses like 'E%' and first_service_date < '2015-10-01' and len(diagnoses) = 4) then diagnoses + '0'
    		else diagnoses 
    	end as 'icdcm_norm',
    
    	--version of diagnosis code
    	cast(case
    			when (diagnoses like '[0-9]%') then 9
    			when (diagnoses like 'V%' and first_service_date < '2015-10-01') then 9
    			when (diagnoses like 'E%' and first_service_date < '2015-10-01') then 9
    			else 10 
    	end 
    	as tinyint) as 'icdcm_version',
    	
    	--diagnosis code number
    	cast(substring(icdcm_number, 3,10) as varchar(200)) as 'icdcm_number',
    	filetype_mcare,
    	getdate() as last_run
    
    from (
    	--bcarrier
    	select
    	--top 100
    	rtrim(a.id_mcare) as id_mcare,
    	rtrim(a.claim_header_id) as claim_header_id,
    	a.first_service_date,
    	a.last_service_date,
    	'carrier' as filetype_mcare,
    	dxadmit = null,
    	a.dx01,
    	a.dx02,
    	a.dx03,
    	a.dx04,
    	a.dx05,
    	a.dx06,
    	a.dx07,
    	a.dx08,
    	a.dx09,
    	a.dx10,
    	a.dx11,
    	a.dx12,
    	a.dx13,
    	dx14 = null,
    	dx15 = null,
    	dx16 = null,
    	dx17 = null,
    	dx18 = null,
    	dx19 = null,
    	dx20 = null,
    	dx21 = null,
    	dx22 = null,
    	dx23 = null,
    	dx24 = null,
    	dx25 = null,
    	dx26 = null,
    	dxecode_1 = null,
    	dxecode_2 = null,
    	dxecode_3 = null,
    	dxecode_4 = null,
    	dxecode_5 = null,
    	dxecode_6 = null,
    	dxecode_7 = null,
    	dxecode_8 = null,
    	dxecode_9 = null,
    	dxecode_10 = null,
    	dxecode_11 = null,
    	dxecode_12 = null,
    	dxecode_13 = null
    	from PHClaims.stage.mcare_bcarrier_claims as a
    	left join PHClaims.final.mcare_elig_demo as b
    	on a.id_mcare = b.id_mcare
    	--exclude denined claims using carrier/dme claim method
    	where a.denial_code in ('1','2','3','4','5','6','7','8','9')
    	--exclude claims among people who have no eligibility data
    	and b.id_mcare is not null
    
    	--dme
    	union
    	select
    	--top 100
    	rtrim(a.id_mcare) as id_mcare,
    	rtrim(a.claim_header_id) as claim_header_id,
    	a.first_service_date,
    	a.last_service_date,
    	'dme' as filetype_mcare,
    	dxadmit = null,
    	a.dx01,
    	a.dx02,
    	a.dx03,
    	a.dx04,
    	a.dx05,
    	a.dx06,
    	a.dx07,
    	a.dx08,
    	a.dx09,
    	a.dx10,
    	a.dx11,
    	a.dx12,
    	a.dx13,
    	dx14 = null,
    	dx15 = null,
    	dx16 = null,
    	dx17 = null,
    	dx18 = null,
    	dx19 = null,
    	dx20 = null,
    	dx21 = null,
    	dx22 = null,
    	dx23 = null,
    	dx24 = null,
    	dx25 = null,
    	dx26 = null,
    	dxecode_1 = null,
    	dxecode_2 = null,
    	dxecode_3 = null,
    	dxecode_4 = null,
    	dxecode_5 = null,
    	dxecode_6 = null,
    	dxecode_7 = null,
    	dxecode_8 = null,
    	dxecode_9 = null,
    	dxecode_10 = null,
    	dxecode_11 = null,
    	dxecode_12 = null,
    	dxecode_13 = null
    	from PHClaims.stage.mcare_dme_claims as a
    	left join PHClaims.final.mcare_elig_demo as b
    	on a.id_mcare = b.id_mcare
    	--exclude denined claims using carrier/dme claim method
    	where a.denial_code in ('1','2','3','4','5','6','7','8','9')
    	--exclude claims among people who have no eligibility data
    	and b.id_mcare is not null
    
    	--hha
    	union
    	select
    	--top 100
    	rtrim(a.id_mcare) as id_mcare,
    	rtrim(a.claim_header_id) as claim_header_id,
    	a.first_service_date,
    	a.last_service_date,
    	'hha' as filetype_mcare,
    	dxadmit = null,
    	a.dx01,
    	a.dx02,
    	a.dx03,
    	a.dx04,
    	a.dx05,
    	a.dx06,
    	a.dx07,
    	a.dx08,
    	a.dx09,
    	a.dx10,
    	a.dx11,
    	a.dx12,
    	a.dx13,
    	a.dx14,
    	a.dx15,
    	a.dx16,
    	a.dx17,
    	a.dx18,
    	a.dx19,
    	a.dx20,
    	a.dx21,
    	a.dx22,
    	a.dx23,
    	a.dx24,
    	a.dx25,
    	a.dx26,
    	a.dxecode_1,
    	a.dxecode_2,
    	a.dxecode_3,
    	a.dxecode_4,
    	a.dxecode_5,
    	a.dxecode_6,
    	a.dxecode_7,
    	a.dxecode_8,
    	a.dxecode_9,
    	a.dxecode_10,
    	a.dxecode_11,
    	a.dxecode_12,
    	a.dxecode_13
    	from PHClaims.stage.mcare_hha_base_claims as a
    	left join PHClaims.final.mcare_elig_demo as b
    	on a.id_mcare = b.id_mcare
    	--exclude denined claims using carrier/dme claim method
    	where (a.denial_code_facility = '' or a.denial_code_facility is null)
    	--exclude claims among people who have no eligibility data
    	and b.id_mcare is not null
    
    	--hospice
    	union
    	select
    	--top 100
    	rtrim(a.id_mcare) as id_mcare,
    	rtrim(a.claim_header_id) as claim_header_id,
    	a.first_service_date,
    	a.last_service_date,
    	'hospice' as filetype_mcare,
    	dxadmit = null,
    	a.dx01,
    	a.dx02,
    	a.dx03,
    	a.dx04,
    	a.dx05,
    	a.dx06,
    	a.dx07,
    	a.dx08,
    	a.dx09,
    	a.dx10,
    	a.dx11,
    	a.dx12,
    	a.dx13,
    	a.dx14,
    	a.dx15,
    	a.dx16,
    	a.dx17,
    	a.dx18,
    	a.dx19,
    	a.dx20,
    	a.dx21,
    	a.dx22,
    	a.dx23,
    	a.dx24,
    	a.dx25,
    	a.dx26,
    	a.dxecode_1,
    	a.dxecode_2,
    	a.dxecode_3,
    	a.dxecode_4,
    	a.dxecode_5,
    	a.dxecode_6,
    	a.dxecode_7,
    	a.dxecode_8,
    	a.dxecode_9,
    	a.dxecode_10,
    	a.dxecode_11,
    	a.dxecode_12,
    	a.dxecode_13
    	from PHClaims.stage.mcare_hospice_base_claims as a
    	left join PHClaims.final.mcare_elig_demo as b
    	on a.id_mcare = b.id_mcare
    	--exclude denined claims using carrier/dme claim method
    	where (a.denial_code_facility = '' or a.denial_code_facility is null)
    	--exclude claims among people who have no eligibility data
    	and b.id_mcare is not null
    
    	--inpatient
    	union
    	select
    	--top 100
    	rtrim(a.id_mcare) as id_mcare,
    	rtrim(a.claim_header_id) as claim_header_id,
    	a.first_service_date,
    	a.last_service_date,
    	'inpatient' as filetype_mcare,
    	a.dxadmit,
    	a.dx01,
    	a.dx02,
    	a.dx03,
    	a.dx04,
    	a.dx05,
    	a.dx06,
    	a.dx07,
    	a.dx08,
    	a.dx09,
    	a.dx10,
    	a.dx11,
    	a.dx12,
    	a.dx13,
    	a.dx14,
    	a.dx15,
    	a.dx16,
    	a.dx17,
    	a.dx18,
    	a.dx19,
    	a.dx20,
    	a.dx21,
    	a.dx22,
    	a.dx23,
    	a.dx24,
    	a.dx25,
    	a.dx26,
    	a.dxecode_1,
    	a.dxecode_2,
    	a.dxecode_3,
    	a.dxecode_4,
    	a.dxecode_5,
    	a.dxecode_6,
    	a.dxecode_7,
    	a.dxecode_8,
    	a.dxecode_9,
    	a.dxecode_10,
    	a.dxecode_11,
    	a.dxecode_12,
    	a.dxecode_13
    	from PHClaims.stage.mcare_inpatient_base_claims as a
    	left join PHClaims.final.mcare_elig_demo as b
    	on a.id_mcare = b.id_mcare
    	--exclude denined claims using carrier/dme claim method
    	where (a.denial_code_facility = '' or a.denial_code_facility is null)
    	--exclude claims among people who have no eligibility data
    	and b.id_mcare is not null
    
    	--outpatient
    	union
    	select
    	--top 100
    	rtrim(a.id_mcare) as id_mcare,
    	rtrim(a.claim_header_id) as claim_header_id,
    	a.first_service_date,
    	a.last_service_date,
    	'outpatient' as filetype_mcare,
    	dxadmit = null,
    	a.dx01,
    	a.dx02,
    	a.dx03,
    	a.dx04,
    	a.dx05,
    	a.dx06,
    	a.dx07,
    	a.dx08,
    	a.dx09,
    	a.dx10,
    	a.dx11,
    	a.dx12,
    	a.dx13,
    	a.dx14,
    	a.dx15,
    	a.dx16,
    	a.dx17,
    	a.dx18,
    	a.dx19,
    	a.dx20,
    	a.dx21,
    	a.dx22,
    	a.dx23,
    	a.dx24,
    	a.dx25,
    	a.dx26,
    	a.dxecode_1,
    	a.dxecode_2,
    	a.dxecode_3,
    	a.dxecode_4,
    	a.dxecode_5,
    	a.dxecode_6,
    	a.dxecode_7,
    	a.dxecode_8,
    	a.dxecode_9,
    	a.dxecode_10,
    	a.dxecode_11,
    	a.dxecode_12,
    	a.dxecode_13
    	from PHClaims.stage.mcare_outpatient_base_claims as a
    	left join PHClaims.final.mcare_elig_demo as b
    	on a.id_mcare = b.id_mcare
    	--exclude denined claims using carrier/dme claim method
    	where (a.denial_code_facility = '' or a.denial_code_facility is null)
    	--exclude claims among people who have no eligibility data
    	and b.id_mcare is not null
    
    	--snf
    	union
    	select
    	--top 100
    	rtrim(a.id_mcare) as id_mcare,
    	rtrim(a.claim_header_id) as claim_header_id,
    	a.first_service_date,
    	a.last_service_date,
    	'snf' as filetype_mcare,
    	a.dxadmit,
    	a.dx01,
    	a.dx02,
    	a.dx03,
    	a.dx04,
    	a.dx05,
    	a.dx06,
    	a.dx07,
    	a.dx08,
    	a.dx09,
    	a.dx10,
    	a.dx11,
    	a.dx12,
    	a.dx13,
    	a.dx14,
    	a.dx15,
    	a.dx16,
    	a.dx17,
    	a.dx18,
    	a.dx19,
    	a.dx20,
    	a.dx21,
    	a.dx22,
    	a.dx23,
    	a.dx24,
    	a.dx25,
    	a.dx26,
    	a.dxecode_1,
    	a.dxecode_2,
    	a.dxecode_3,
    	a.dxecode_4,
    	a.dxecode_5,
    	a.dxecode_6,
    	a.dxecode_7,
    	a.dxecode_8,
    	a.dxecode_9,
    	a.dxecode_10,
    	a.dxecode_11,
    	a.dxecode_12,
    	a.dxecode_13
    	from PHClaims.stage.mcare_snf_base_claims as a
    	left join PHClaims.final.mcare_elig_demo as b
    	on a.id_mcare = b.id_mcare
    	--exclude denined claims using carrier/dme claim method
    	where (a.denial_code_facility = '' or a.denial_code_facility is null)
    	--exclude claims among people who have no eligibility data
    	and b.id_mcare is not null
    	
    ) as a
    --reshape from wide to long
    unpivot(diagnoses for icdcm_number in (
    	dxadmit,
    	dx01,
    	dx02,
    	dx03,
    	dx04,
    	dx05,
    	dx06,
    	dx07,
    	dx08,
    	dx09,
    	dx10,
    	dx11,
    	dx12,
    	dx13,
    	dx14,
    	dx15,
    	dx16,
    	dx17,
    	dx18,
    	dx19,
    	dx20,
    	dx21,
    	dx22,
    	dx23,
    	dx24,
    	dx25,
    	dx26,
    	dxecode_1,
    	dxecode_2,
    	dxecode_3,
    	dxecode_4,
    	dxecode_5,
    	dxecode_6,
    	dxecode_7,
    	dxecode_8,
    	dxecode_9,
    	dxecode_10,
    	dxecode_11,
    	dxecode_12,
    	dxecode_13)
    ) as diagnoses
    where diagnoses is not null AND  diagnoses!=' ';",
        .con = db_claims))
    }

#### Table-level QA script ####
qa_stage.mcare_claim_icdcm_header_qa_f <- function() {
  
  #confirm that claim counts match for a specific diagnosis code field: facility-level claims
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcare_claim_icdcm_header' as 'table', 'row count, expect match with inpatient table' as qa_type,
    count(*) as qa
    from stage.mcare_claim_icdcm_header
    where filetype_mcare = 'inpatient' and icdcm_number = 'ecode_2';",
    .con = db_claims))
  
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcare_inpatient_base_claims' as 'table', 'row count, expect match with claim_icdcm_header table' as qa_type,
    count(*) as qa
    from (
      select distinct a.id_mcare, a.claim_header_id, a.first_service_date,
      a.last_service_date, a.dxecode_2
    	from PHClaims.stage.mcare_inpatient_base_claims as a
    	left join PHClaims.final.mcare_elig_demo as b
    	on a.id_mcare = b.id_mcare
    	--exclude denined claims using facility method
    	where (a.denial_code_facility = '' or a.denial_code_facility is null)
    	--exclude claims among people who have no eligibility data
    	and b.id_mcare is not null
      --exclude null/blank diagnosis code rows
      and a.dxecode_2 is not null and a.dxecode_2 != ' '
    ) as x;",
    .con = db_claims))
  
  #confirm that claim counts match for a specific diagnosis code field: carrier claims
  res3 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcare_claim_icdcm_header' as 'table', 'row count, expect match with carrier table' as qa_type,
    count(*) as qa
    from stage.mcare_claim_icdcm_header
    where filetype_mcare = 'carrier' and icdcm_number = '12';",
    .con = db_claims))
  
  res4 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcare_bcarrier_claims' as 'table', 'row count, expect match with claim_icdcm_header table' as qa_type,
    count(*) as qa
    from (
      select distinct a.id_mcare, a.claim_header_id, a.first_service_date,
      a.last_service_date, a.dx12
    	from PHClaims.stage.mcare_bcarrier_claims as a
    	left join PHClaims.final.mcare_elig_demo as b
    	on a.id_mcare = b.id_mcare
    	--exclude denined claims using carrier/dme claim method
    	where a.denial_code in ('1','2','3','4','5','6','7','8','9')
    	--exclude claims among people who have no eligibility data
    	and b.id_mcare is not null
      --exclude null/blank diagnosis code rows
      and a.dx12 is not null and a.dx12 != ' '
    ) as x;",
    .con = db_claims))
  
  #make sure everyone is in elig_demo
  res5 <- dbGetQuery(conn = db_claims, glue_sql(
  "select 'stage.mcare_claim_icdcm_header' as 'table', '# members not in elig_demo, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stage.mcare_claim_icdcm_header as a
    left join final.mcare_elig_demo as b
    on a.id_mcare = b.id_mcare
    where b.id_mcare is null;",
  .con = db_claims))
  
  #make sure everyone is in elig_timevar
  res6 <- dbGetQuery(conn = db_claims, glue_sql(
  "select 'stage.mcare_claim_icdcm_header' as 'table', '# members not in elig_timevar, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stage.mcare_claim_icdcm_header as a
    left join final.mcare_elig_timevar as b
    on a.id_mcare = b.id_mcare
    where b.id_mcare is null;",
  .con = db_claims))

res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}