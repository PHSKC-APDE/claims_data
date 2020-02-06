#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_claim_procedure
# Eli Kern, PHSKC (APDE)
#
# 2019-12

### Run from master_mcare_full_union script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcare/master_mcare_full_union.R

#### Load script ####
load_stage.mcare_claim_procedure_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "--Code to load data to stage.mcare_claim_procedure table
    --Procedure codes reshaped to long
    --Eli Kern (PHSKC-APDE)
    --2020-01
    --Run time: XX min
    
    ------------------
    --STEP 1: Select and union desired columns from multi-year claim tables on stage schema
    --Exclude all denied claims using proposed approach per ResDAC 01-2020 consult
    --Unpivot and insert into table shell
    -------------------
    insert into PHClaims.stage.mcare_claim_procedure with (tablock)
    
    select z.id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	procedure_code_number,
    	modifier_1,
    	modifier_2,
    	modifier_3,
    	modifier_4,
    	filetype_mcare,
    	getdate() as last_run
    
    from (
    	--bcarrier
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	--original diagnosis code
    	procedure_codes as 'procedure_code',
    	--procedure code number/type
    	cast(substring(procedure_code_number, 3,10) as varchar(200)) as 'procedure_code_number',
    	--modifier codes
    	--set missing/blank/null equal to null
    	case when (procedure_code_hcps_modifier_1 is null or procedure_code_hcps_modifier_1 = ' ') then null else procedure_code_hcps_modifier_1 end as modifier_1,
    	case when (procedure_code_hcps_modifier_2 is null or procedure_code_hcps_modifier_2 = ' ') then null else procedure_code_hcps_modifier_2 end as modifier_2,
    	case when (procedure_code_hcps_modifier_3 is null or procedure_code_hcps_modifier_3 = ' ') then null else procedure_code_hcps_modifier_3 end as modifier_3,
    	case when (procedure_code_hcps_modifier_4 is null or procedure_code_hcps_modifier_4 = ' ') then null else procedure_code_hcps_modifier_4 end as modifier_4,
    	filetype_mcare,
    	getdate() as last_run
    
    	from (
    		select
    		--top 100
    		rtrim(a.id_mcare) as id_mcare,
    		rtrim(a.claim_header_id) as claim_header_id,
    		a.first_service_date,
    		a.last_service_date,
    		'carrier' as filetype_mcare,
    		b.procedure_code_hcpcs as pchcpcs,
    		b.procedure_code_hcps_modifier_1,
    		b.procedure_code_hcps_modifier_2,
    		procedure_code_hcps_modifier_3 = null,
    		procedure_code_hcps_modifier_4 = null,
    		b.procedure_code_betos as pcbetos
    		from PHClaims.stage.mcare_bcarrier_claims as a
    		left join PHClaims.stage.mcare_bcarrier_line as b
    		on a.claim_header_id = b.claim_header_id
    		--exclude denined claims using carrier/dme claim method
    		where a.denial_code in ('1','2','3','4','5','6','7','8','9')
    	) as x1
    
    	--reshape from wide to long
    	unpivot(procedure_codes for procedure_code_number in (
    		pchcpcs,
    		pcbetos)
    	) as procedure_codes
    	where procedure_codes is not null AND procedure_codes!=' ' 
    	   
    	--dme
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	--original diagnosis code
    	procedure_codes as 'procedure_code',
    	--procedure code number/type
    	cast(substring(procedure_code_number, 3,10) as varchar(200)) as 'procedure_code_number',
    	--modifier codes
    	--set missing/blank/null equal to null
    	case when (procedure_code_hcps_modifier_1 is null or procedure_code_hcps_modifier_1 = ' ') then null else procedure_code_hcps_modifier_1 end as modifier_1,
    	case when (procedure_code_hcps_modifier_2 is null or procedure_code_hcps_modifier_2 = ' ') then null else procedure_code_hcps_modifier_2 end as modifier_2,
    	case when (procedure_code_hcps_modifier_3 is null or procedure_code_hcps_modifier_3 = ' ') then null else procedure_code_hcps_modifier_3 end as modifier_3,
    	case when (procedure_code_hcps_modifier_4 is null or procedure_code_hcps_modifier_4 = ' ') then null else procedure_code_hcps_modifier_4 end as modifier_4,
    	filetype_mcare,
    	getdate() as last_run
    
    	from (
    		select
    		--top 100
    		rtrim(a.id_mcare) as id_mcare,
    		rtrim(a.claim_header_id) as claim_header_id,
    		a.first_service_date,
    		a.last_service_date,
    		'dme' as filetype_mcare,
    		b.procedure_code_hcpcs as pchcpcs,
    		b.procedure_code_hcps_modifier_1,
    		b.procedure_code_hcps_modifier_2,
    		b.procedure_code_hcps_modifier_3,
    		b.procedure_code_hcps_modifier_4,
    		b.procedure_code_betos as pcbetos
    		from PHClaims.stage.mcare_dme_claims as a
    		left join PHClaims.stage.mcare_dme_line as b
    		on a.claim_header_id = b.claim_header_id
    		--exclude denined claims using carrier/dme claim method
    		where a.denial_code in ('1','2','3','4','5','6','7','8','9')
    	) as x2
    
    	--reshape from wide to long
    	unpivot(procedure_codes for procedure_code_number in (
    		pchcpcs,
    		pcbetos)
    	) as procedure_codes
    	where procedure_codes is not null AND procedure_codes!=' ' 
    
    	--hha
    	--only one procedure code field thus no unpivot necessary
    	union
    	select
    	--top 100
    	rtrim(a.id_mcare) as id_mcare,
    	rtrim(a.claim_header_id) as claim_header_id,
    	a.first_service_date,
    	a.last_service_date,
    	b.procedure_code_hcpcs as procedure_code,
    	'hcpcs' as procedure_code_number,
    	case when (b.procedure_code_hcps_modifier_1 is null or b.procedure_code_hcps_modifier_1 = ' ') then null else b.procedure_code_hcps_modifier_1 end as modifier_1,
    	case when (b.procedure_code_hcps_modifier_2 is null or b.procedure_code_hcps_modifier_2 = ' ') then null else b.procedure_code_hcps_modifier_2 end as modifier_2,
    	case when (b.procedure_code_hcps_modifier_3 is null or b.procedure_code_hcps_modifier_3 = ' ') then null else b.procedure_code_hcps_modifier_3 end as modifier_3,
    	modifier_4 = null,
    	'hha' as filetype_mcare,
    	getdate() as last_run
    	from PHClaims.stage.mcare_hha_base_claims as a
    	left join PHClaims.stage.mcare_hha_revenue_center as b
    	on a.claim_header_id = b.claim_header_id
    	--exclude denined claims using carrier/dme claim method
    	where (a.denial_code_facility = '' or a.denial_code_facility is null)
    
    	--hospice
    	--only one procedure code field thus no unpivot necessary
    	union
    	select
    	--top 100
    	rtrim(a.id_mcare) as id_mcare,
    	rtrim(a.claim_header_id) as claim_header_id,
    	a.first_service_date,
    	a.last_service_date,
    	b.procedure_code_hcpcs as procedure_code,
    	'hcpcs' as procedure_code_number,
    	case when (b.procedure_code_hcps_modifier_1 is null or b.procedure_code_hcps_modifier_1 = ' ') then null else b.procedure_code_hcps_modifier_1 end as modifier_1,
    	case when (b.procedure_code_hcps_modifier_2 is null or b.procedure_code_hcps_modifier_2 = ' ') then null else b.procedure_code_hcps_modifier_2 end as modifier_2,
    	case when (b.procedure_code_hcps_modifier_3 is null or b.procedure_code_hcps_modifier_3 = ' ') then null else b.procedure_code_hcps_modifier_3 end as modifier_3,
    	modifier_4 = null,
    	'hospice' as filetype_mcare,
    	getdate() as last_run
    	from PHClaims.stage.mcare_hospice_base_claims as a
    	left join PHClaims.stage.mcare_hospice_revenue_center as b
    	on a.claim_header_id = b.claim_header_id
    	--exclude denined claims using carrier/dme claim method
    	where (a.denial_code_facility = '' or a.denial_code_facility is null)
    	   	  
    	--inpatient
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	--original diagnosis code
    	procedure_codes as 'procedure_code',
    	--procedure code number/type
    	cast(substring(procedure_code_number, 3,10) as varchar(200)) as 'procedure_code_number',
    	--modifier codes
    	--set missing/blank/null equal to null
    	case when (procedure_code_hcps_modifier_1 is null or procedure_code_hcps_modifier_1 = ' ') then null else procedure_code_hcps_modifier_1 end as modifier_1,
    	case when (procedure_code_hcps_modifier_2 is null or procedure_code_hcps_modifier_2 = ' ') then null else procedure_code_hcps_modifier_2 end as modifier_2,
    	case when (procedure_code_hcps_modifier_3 is null or procedure_code_hcps_modifier_3 = ' ') then null else procedure_code_hcps_modifier_3 end as modifier_3,
    	case when (procedure_code_hcps_modifier_4 is null or procedure_code_hcps_modifier_4 = ' ') then null else procedure_code_hcps_modifier_4 end as modifier_4,
    	filetype_mcare,
    	getdate() as last_run
    
    	from (
    		select
    		--top 100
    		rtrim(a.id_mcare) as id_mcare,
    		rtrim(a.claim_header_id) as claim_header_id,
    		a.first_service_date,
    		a.last_service_date,
    		'inpatient' as filetype_mcare,
    		b.procedure_code_hcpcs as pchcpcs,
    		b.procedure_code_hcps_modifier_1,
    		b.procedure_code_hcps_modifier_2,
    		procedure_code_hcps_modifier_3 = null,
    		procedure_code_hcps_modifier_4 = null,
    		a.pc01,
    		a.pc02,
    		a.pc03,
    		a.pc04,
    		a.pc05,
    		a.pc06,
    		a.pc07,
    		a.pc08,
    		a.pc09,
    		a.pc10,
    		a.pc11,
    		a.pc12,
    		a.pc13,
    		a.pc14,
    		a.pc15,
    		a.pc16,
    		a.pc17,
    		a.pc18,
    		a.pc19,
    		a.pc20,
    		a.pc21,
    		a.pc22,
    		a.pc23,
    		a.pc24,
    		a.pc25
    		from PHClaims.stage.mcare_inpatient_base_claims as a
    		left join PHClaims.stage.mcare_inpatient_revenue_center as b
    		on a.claim_header_id = b.claim_header_id
    		--exclude denined claims using carrier/dme claim method
    		where (a.denial_code_facility = '' or a.denial_code_facility is null)
    	) as x3
    
    	--reshape from wide to long
    	unpivot(procedure_codes for procedure_code_number in (
    		pchcpcs,
    		pc01,
    		pc02,
    		pc03,
    		pc04,
    		pc05,
    		pc06,
    		pc07,
    		pc08,
    		pc09,
    		pc10,
    		pc11,
    		pc12,
    		pc13,
    		pc14,
    		pc15,
    		pc16,
    		pc17,
    		pc18,
    		pc19,
    		pc20,
    		pc21,
    		pc22,
    		pc23,
    		pc24,
    		pc25)
    	) as procedure_codes
    	where procedure_codes is not null AND procedure_codes!=' '
    
    	--outpatient
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	--original diagnosis code
    	procedure_codes as 'procedure_code',
    	--procedure code number/type
    	cast(substring(procedure_code_number, 3,10) as varchar(200)) as 'procedure_code_number',
    	--modifier codes
    	--set missing/blank/null equal to null
    	case when (procedure_code_hcps_modifier_1 is null or procedure_code_hcps_modifier_1 = ' ') then null else procedure_code_hcps_modifier_1 end as modifier_1,
    	case when (procedure_code_hcps_modifier_2 is null or procedure_code_hcps_modifier_2 = ' ') then null else procedure_code_hcps_modifier_2 end as modifier_2,
    	case when (procedure_code_hcps_modifier_3 is null or procedure_code_hcps_modifier_3 = ' ') then null else procedure_code_hcps_modifier_3 end as modifier_3,
    	case when (procedure_code_hcps_modifier_4 is null or procedure_code_hcps_modifier_4 = ' ') then null else procedure_code_hcps_modifier_4 end as modifier_4,
    	filetype_mcare,
    	getdate() as last_run
    
    	from (
    		select
    		--top 100
    		rtrim(a.id_mcare) as id_mcare,
    		rtrim(a.claim_header_id) as claim_header_id,
    		a.first_service_date,
    		a.last_service_date,
    		'outpatient' as filetype_mcare,
    		b.procedure_code_hcpcs as pchcpcs,
    		b.procedure_code_hcps_modifier_1,
    		b.procedure_code_hcps_modifier_2,
    		procedure_code_hcps_modifier_3 = null,
    		procedure_code_hcps_modifier_4 = null,
    		a.pc01,
    		a.pc02,
    		a.pc03,
    		a.pc04,
    		a.pc05,
    		a.pc06,
    		a.pc07,
    		a.pc08,
    		a.pc09,
    		a.pc10,
    		a.pc11,
    		a.pc12,
    		a.pc13,
    		a.pc14,
    		a.pc15,
    		a.pc16,
    		a.pc17,
    		a.pc18,
    		a.pc19,
    		a.pc20,
    		a.pc21,
    		a.pc22,
    		a.pc23,
    		a.pc24,
    		a.pc25
    		from PHClaims.stage.mcare_outpatient_base_claims as a
    		left join PHClaims.stage.mcare_outpatient_revenue_center as b
    		on a.claim_header_id = b.claim_header_id
    		--exclude denined claims using carrier/dme claim method
    		where (a.denial_code_facility = '' or a.denial_code_facility is null)
    	) as x4
    
    	--reshape from wide to long
    	unpivot(procedure_codes for procedure_code_number in (
    		pchcpcs,
    		pc01,
    		pc02,
    		pc03,
    		pc04,
    		pc05,
    		pc06,
    		pc07,
    		pc08,
    		pc09,
    		pc10,
    		pc11,
    		pc12,
    		pc13,
    		pc14,
    		pc15,
    		pc16,
    		pc17,
    		pc18,
    		pc19,
    		pc20,
    		pc21,
    		pc22,
    		pc23,
    		pc24,
    		pc25)
    	) as procedure_codes
    	where procedure_codes is not null AND procedure_codes!=' '
    
    	--snf
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	--original diagnosis code
    	procedure_codes as 'procedure_code',
    	--procedure code number/type
    	cast(substring(procedure_code_number, 3,10) as varchar(200)) as 'procedure_code_number',
    	--modifier codes
    	--set missing/blank/null equal to null
    	case when (procedure_code_hcps_modifier_1 is null or procedure_code_hcps_modifier_1 = ' ') then null else procedure_code_hcps_modifier_1 end as modifier_1,
    	case when (procedure_code_hcps_modifier_2 is null or procedure_code_hcps_modifier_2 = ' ') then null else procedure_code_hcps_modifier_2 end as modifier_2,
    	case when (procedure_code_hcps_modifier_3 is null or procedure_code_hcps_modifier_3 = ' ') then null else procedure_code_hcps_modifier_3 end as modifier_3,
    	case when (procedure_code_hcps_modifier_4 is null or procedure_code_hcps_modifier_4 = ' ') then null else procedure_code_hcps_modifier_4 end as modifier_4,
    	filetype_mcare,
    	getdate() as last_run
    
    	from (
    		select
    		--top 100
    		rtrim(a.id_mcare) as id_mcare,
    		rtrim(a.claim_header_id) as claim_header_id,
    		a.first_service_date,
    		a.last_service_date,
    		'snf' as filetype_mcare,
    		b.procedure_code_hcpcs as pchcpcs,
    		b.procedure_code_hcps_modifier_1,
    		b.procedure_code_hcps_modifier_2,
    		b.procedure_code_hcps_modifier_3,
    		procedure_code_hcps_modifier_4 = null,
    		a.pc01,
    		a.pc02,
    		a.pc03,
    		a.pc04,
    		a.pc05,
    		a.pc06,
    		a.pc07,
    		a.pc08,
    		a.pc09,
    		a.pc10,
    		a.pc11,
    		a.pc12,
    		a.pc13,
    		a.pc14,
    		a.pc15,
    		a.pc16,
    		a.pc17,
    		a.pc18,
    		a.pc19,
    		a.pc20,
    		a.pc21,
    		a.pc22,
    		a.pc23,
    		a.pc24,
    		a.pc25
    		from PHClaims.stage.mcare_snf_base_claims as a
    		left join PHClaims.stage.mcare_snf_revenue_center as b
    		on a.claim_header_id = b.claim_header_id
    		--exclude denined claims using carrier/dme claim method
    		where (a.denial_code_facility = '' or a.denial_code_facility is null)
    	) as x5
    
    	--reshape from wide to long
    	unpivot(procedure_codes for procedure_code_number in (
    		pchcpcs,
    		pc01,
    		pc02,
    		pc03,
    		pc04,
    		pc05,
    		pc06,
    		pc07,
    		pc08,
    		pc09,
    		pc10,
    		pc11,
    		pc12,
    		pc13,
    		pc14,
    		pc15,
    		pc16,
    		pc17,
    		pc18,
    		pc19,
    		pc20,
    		pc21,
    		pc22,
    		pc23,
    		pc24,
    		pc25)
    	) as procedure_codes
    	where procedure_codes is not null AND procedure_codes!=' '
    	
    ) as z
    --exclude claims among people who have no eligibility data
    left join PHClaims.final.mcare_elig_demo as w
    on z.id_mcare = w.id_mcare
    where w.id_mcare is not null;",
        .con = db_claims))
    }

#### Table-level QA script ####
qa_stage.mcare_claim_procedure_qa_f <- function() {
  
  #confirm that claim line counts match for a specific ICD-PCS code
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
  "select 'stage.mcare_claim_procedure' as 'table', 'row count, expect match with inpatient table' as qa_type,
  count(*) as qa
  from stage.mcare_claim_procedure
  where filetype_mcare = 'inpatient' and procedure_code = '3995' and procedure_code_number = '02';",
  .con = db_claims))
  
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
  "select 'stage.mcare_inpatient_base_claims' as 'table', 'row count, expect match with claim_procedure table' as qa_type,
  count(*) as qa
  from (
    select distinct a.id_mcare, a.claim_header_id, b.first_service_date,
    b.last_service_date, b.pc02
    from stage.mcare_inpatient_revenue_center as a
    left join stage.mcare_inpatient_base_claims as b
    on a.claim_header_id = b.claim_header_id
    left join PHClaims.final.mcare_elig_demo as c
    on a.id_mcare = c.id_mcare
    --exclude denined claims using carrier/dme claim method
    where (b.denial_code_facility = '' or b.denial_code_facility is null)
    --exclude claims among people who have eligibility data
    and c.id_mcare is not null
    --specific revenue code
    and b.pc02 = '3995'
  ) as x;",
  .con = db_claims))
  
  #confirm that claim line counts match for a specific HCPCS code
  res3 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcare_claim_procedure' as 'table', 'row count, expect match with carrier table' as qa_type,
    count(*) as qa
    from stage.mcare_claim_procedure
    where filetype_mcare = 'carrier' and procedure_code = '99214';",
    .con = db_claims))
  
  res4 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcare_bcarrier_line' as 'table', 'row count, expect match with claim_procedure table' as qa_type,
    count(*) as qa
    from (
    	select distinct a.id_mcare, a.claim_header_id, b.first_service_date,
    	b.last_service_date, a.procedure_code_hcpcs
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
    	and a.procedure_code_hcpcs = '99214'
    ) as x;",
    .con = db_claims))

  #make sure everyone is in elig_demo
  res5 <- dbGetQuery(conn = db_claims, glue_sql(
  "select 'stage.mcare_claim_procedure' as 'table', '# members not in elig_demo, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stage.mcare_claim_procedure as a
    left join final.mcare_elig_demo as b
    on a.id_mcare = b.id_mcare
    where b.id_mcare is null;",
  .con = db_claims))
  
  #make sure everyone is in elig_timevar
  res6 <- dbGetQuery(conn = db_claims, glue_sql(
  "select 'stage.mcare_claim_procedure' as 'table', '# members not in elig_timevar, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stage.mcare_claim_procedure as a
    left join final.mcare_elig_timevar as b
    on a.id_mcare = b.id_mcare
    where b.id_mcare is null;",
  .con = db_claims))

res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}