#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_ICDCM_HEADER
# Eli Kern, PHSKC (APDE)
#
# 2019-10
#
# 2024-04-29 update: Modified for HHSAW migration
# 2026-08-24 update: Modified under migration of ETL from Enclave -> KC
  # Added code to exclude i) non-WA residents, ii) people with no claims but no enrollment data, iii) added icdcm_hash column

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/07_apcd_create_analytic_tables.R

#### Load script ####
load_stage.apcd_claim_icdcm_header_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "
	--Pull diagnosis codes from medical_claim_diagnosis table, collapse to header, apply exclusions, fix ICD-CM version
	with temp1 as (
		distinct
		a.internal_member_id as id_apcd,
		b.medical_claim_header_id as claim_header_id,
		c.first_service_dt as first_service_date,
		c.last_service_dt as last_service_date,
		a.diagnosis_code as icdcm_raw,

		--fix ICD-CM version when it it set to 9 and should be 10, convert from text to number
		case
			when a.icd_version_ind = '9' and a.last_service_dt >= '2015-10-01' and a.diagnosis_code like '[A-Z]%' then 10
			when a.icd_version_ind = '9' then 9
			when a.icd_version_ind = '0' then 10
		end as icdcm_version,

		--assign diagnosis number
		case
			when a.diagnosis_type_code = 'A' then 'admit'
			when a.diagnosis_type_code = 'E' then 'ecode'
			else 'all'
		end as icdcm_number

		from stg_claims.apcd_medical_claim_diagnosis as a
		left join stg_claims.apcd_medical_claim as b
		on a.medical_claim_service_line_id = b.medical_claim_service_line_id
		left join stg_claims.apcd_medical_claim_header as c
		on b.medical_claim_header_id = c.medical_claim_header_id
		left join stg_claims.apcd_ref_nonresident_id as y
		on a.internal_member_id = y.id_apcd
		left join stg_claims.apcd_ref_claim_no_elig as z
		on a.internal_member_id = z.id_apcd
		--exclude null/invalid ICD-CM codes
		where (a.diagnosis_code not in ('-1','-2') and a.diagnosis_type_code not in ('S'))
		--exclude denied and orphaned headers
		and (c.denied_header_flag = 'N' and c.orphaned_header_flag = 'N')
		--exclude members with no WA residency OR no elig data
		and (y.id_apcd is null and z.id_apcd is null)
	),
	--Pull primary diagnosis from medical_claim_header table, apply exclusions
	--Note that ICD-CM version in this table does not need correcting (all first digit alpha codes > 2015-10-01 are V and E codes)
	temp2 as (
		select
		a.internal_member_id as id_apcd,
		a.medical_claim_header_id as claim_header_id,
		a.first_service_dt as first_service_date,
		a.last_service_dt as last_service_date,
		a.diagnosis_code as icdcm_raw,
		case when icd_version_ind = '9' then 9 when icd_version_ind = '0' then 10 end as icdcm_version,
		'01' as icdcm_number
		from stg_claims.apcd_medical_claim_header as a
		left join stg_claims.apcd_ref_nonresident_id as y
		on a.internal_member_id = y.id_apcd
		left join stg_claims.apcd_ref_claim_no_elig as z
		on a.internal_member_id = z.id_apcd
		--exclude null/invalid ICD-CM codes
		where (a.diagnosis_code not in ('-1','-2'))
		--exclude denied and orphaned headers
		and (a.denied_header_flag = 'N' and a.orphaned_header_flag = 'N')
		--exclude members with no WA residency OR no elig data
		and (y.id_apcd is null and z.id_apcd is null)
	),
	--Normalize ICD-CM codes and union tables
	temp3 as (
		select
		internal_member_id,
		medical_claim_header_id,
		first_service_date,
		last_service_date,
		icdcm_raw,
		case
			when (icdcm_version = 10 and len(icdcm_raw) < 3) then null -- set to null ICD-10-CM codes that are too short
			when (icdcm_version = 9 and len(icdcm_raw) = 3) then icdcm_raw + '00' -- pad ICD-9-CM codes to 5 digits
			when (icdcm_version = 9 and len(icdcm_raw) = 4) then icdcm_raw + '0' -- pad ICD-9-CM codes to 5 digits
			else icdcm_raw 
		end as icdcm_norm,
		icdcm_version,
		icdcm_number
		from temp1

		union select
		internal_member_id,
		medical_claim_header_id,
		first_service_date,
		last_service_date,
		icdcm_raw,
		case
			when (icdcm_version = 10 and len(icdcm_raw) < 3) then null -- set to null ICD-10-CM codes that are too short
			when (icdcm_version = 9 and len(icdcm_raw) = 3) then icdcm_raw + '00' -- pad ICD-9-CM codes to 5 digits
			when (icdcm_version = 9 and len(icdcm_raw) = 4) then icdcm_raw + '0' -- pad ICD-9-CM codes to 5 digits
			else icdcm_raw 
		end as icdcm_norm,
		icdcm_version,
		icdcm_number
		from temp2
	)
	--Create icdcm_hash column and insert to table shell
	insert into stg_claims.stage_apcd_claim_icdcm_header
	select
	internal_member_id,
	medical_claim_header_id,
	first_service_date,
	last_service_date,
	icdcm_raw,
	icdcm_norm,
	icdcm_version,
	icdcm_number,
	checksum(icdcm_norm, icdcm_version) as icdcm_hash,
	getdate() as last_run
	from temp4;",
    .con = dw_inthealth))
}

#### Table-level QA script ####
qa_stage.apcd_claim_icdcm_header_f <- function() {
  
  #all members should be in elig_demo and elig_month tables
  res1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_icdcm_header' as 'table', '# members not in elig_demo, expect 0' as qa_type,
    count(distinct a.id_apcd) as qa
    from stg_claims.stage_apcd_claim_icdcm_header as a
    left join stg_claims.stage_apcd_elig_demo as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = dw_inthealth))
  
  res2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_icdcm_header' as 'table', '# members not in elig_month, expect 0' as qa_type,
    count(distinct a.id_apcd) as qa
    from stg_claims.stage_apcd_claim_icdcm_header as a
    left join stg_claims.stage_apcd_elig_month as b
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