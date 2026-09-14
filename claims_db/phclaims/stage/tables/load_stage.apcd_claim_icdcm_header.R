#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_ICDCM_HEADER
# Eli Kern, PHSKC (APDE)
#
# 2019-10
#
# 2024-04-29 update: Modified for HHSAW migration
# 2026-08-24 update: Modified under migration of ETL from Enclave -> KC
  # Added code to exclude i) non-WA residents, ii) people with no claims but no enrollment data, iii) added icdcm_hash column
# 2026-09-04 update: Switched CTEs to CTA statements, changed UNION to UNION ALL, and changed final INSERT INTO to CTA statement, all for faster performance

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/07_apcd_create_analytic_tables.R

#### Load script ####
load_stage.apcd_claim_icdcm_header_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "
	--STEP 0: Cleanup old temp tables
	IF OBJECT_ID('stg_claims.tmp_apcd_icdcm_d1_raw', 'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_icdcm_d1_raw;
	IF OBJECT_ID('stg_claims.tmp_apcd_icdcm_d1_dedup', 'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_icdcm_d1_dedup;
	IF OBJECT_ID('stg_claims.tmp_apcd_icdcm_header_raw', 'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_icdcm_header_raw;
	IF OBJECT_ID('stg_claims.tmp_apcd_icdcm_norm', 'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_icdcm_norm;
	
	--STEP 1: Extract line-level diagnosis codes without collapsing yet
	CREATE TABLE stg_claims.tmp_apcd_icdcm_d1_raw
	WITH
	(
	DISTRIBUTION = HASH(claim_header_id),
	CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT
	a.internal_member_id AS id_apcd,
	b.medical_claim_header_id AS claim_header_id,
	c.first_service_dt AS first_service_date,
	c.last_service_dt AS last_service_date,
	a.diagnosis_code AS icdcm_raw,
	--fix ICD-CM version when it it set to 9 and should be 10, convert from text to number
	CASE
		WHEN a.icd_version_ind = '9'
			 AND a.last_service_dt >= '2015-10-01'
			 AND a.diagnosis_code LIKE '[A-Z]%'
		THEN 10
		WHEN a.icd_version_ind = '9' THEN 9
		WHEN a.icd_version_ind = '0' THEN 10
	END AS icdcm_version,
	--assign diagnosis number
	CASE
		WHEN a.diagnosis_type_code = 'A' THEN 'admit'
		WHEN a.diagnosis_type_code = 'E' THEN 'ecode'
		ELSE 'all'
	END AS icdcm_number
	FROM stg_claims.apcd_medical_claim_diagnosis AS a
	LEFT JOIN stg_claims.apcd_medical_claim AS b
	ON a.medical_claim_service_line_id = b.medical_claim_service_line_id
	LEFT JOIN stg_claims.apcd_medical_claim_header AS c
	ON b.medical_claim_header_id = c.medical_claim_header_id
	LEFT JOIN stg_claims.apcd_ref_member_exclude AS y
	ON a.internal_member_id = y.id_apcd
	--exclude null/invalid ICD-CM codes
	WHERE a.diagnosis_code NOT IN ('-1', '-2')
	AND a.diagnosis_type_code NOT IN ('S')
	--exclude denied and orphaned headers
	AND c.denied_header_flag = 'N'
	AND c.orphaned_header_flag = 'N'
	--exclude members with no WA residency OR no elig data
	AND y.id_apcd IS NULL;

	--STEP 2: Deduplicate line-level diagnosis codes (faster than using DISTINCT in first step)
	CREATE TABLE stg_claims.tmp_apcd_icdcm_d1_dedup
	WITH
	(
	DISTRIBUTION = HASH(claim_header_id),
	CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT *
	FROM
	(
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY id_apcd, claim_header_id, icdcm_raw ORDER BY id_apcd) AS rn
	FROM stg_claims.tmp_apcd_icdcm_d1_raw
	) x
	WHERE rn = 1;
	IF OBJECT_ID('stg_claims.tmp_apcd_icdcm_d1_raw', 'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_icdcm_d1_raw;

	--STEP 3: Extract primary diagnsosis codes from medical claim header table, applying exclusions
	--Note that ICD-CM version in this table does not need correcting (all first digit alpha codes > 2015-10-01 are V and E codes)
	CREATE TABLE stg_claims.tmp_apcd_icdcm_header_raw
	WITH
	(
	DISTRIBUTION = HASH(claim_header_id),
	CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT
	a.internal_member_id AS id_apcd,
	a.medical_claim_header_id AS claim_header_id,
	a.first_service_dt AS first_service_date,
	a.last_service_dt AS last_service_date,
	a.diagnosis_code AS icdcm_raw,
	CASE WHEN icd_version_ind = '9' THEN 9 WHEN icd_version_ind = '0' THEN 10 END AS icdcm_version,
	'01' AS icdcm_number
	FROM stg_claims.apcd_medical_claim_header AS a
	LEFT JOIN stg_claims.apcd_ref_member_exclude AS y
	ON a.internal_member_id = y.id_apcd
	--exclude null/invalid ICD-CM codes
	WHERE a.diagnosis_code NOT IN ('-1','-2')
	--exclude denied and orphaned headers
	AND a.denied_header_flag = 'N'
	AND a.orphaned_header_flag = 'N'
	--exclude members with no WA residency OR no elig data
	AND y.id_apcd IS NULL;

	--STEP 4: Normalize ICD-CM codes and union tables
	CREATE TABLE stg_claims.tmp_apcd_icdcm_norm
	WITH
	(
	DISTRIBUTION = HASH(claim_header_id),
	CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT
	id_apcd,
	claim_header_id,
	first_service_date,
	last_service_date,
	icdcm_raw,
	CASE
	WHEN icdcm_version = 10 AND LEN(icdcm_raw) < 3 THEN NULL -- set to null ICD-10-CM codes that are too short
	WHEN icdcm_version = 9 AND LEN(icdcm_raw) = 3 THEN icdcm_raw + '00' -- pad ICD-9-CM codes to 5 digits
	WHEN icdcm_version = 9 AND LEN(icdcm_raw) = 4 THEN icdcm_raw + '0' -- pad ICD-9-CM codes to 5 digits
	ELSE icdcm_raw
	END AS icdcm_norm,
	icdcm_version,
	icdcm_number
	FROM stg_claims.tmp_apcd_icdcm_d1_dedup
	UNION ALL
	SELECT
	id_apcd,
	claim_header_id,
	first_service_date,
	last_service_date,
	icdcm_raw,
	CASE
	WHEN icdcm_version = 10 AND LEN(icdcm_raw) < 3 THEN NULL -- set to null ICD-10-CM codes that are too short
	WHEN icdcm_version = 9 AND LEN(icdcm_raw) = 3 THEN icdcm_raw + '00' -- pad ICD-9-CM codes to 5 digits
	WHEN icdcm_version = 9 AND LEN(icdcm_raw) = 4 THEN icdcm_raw + '0' -- pad ICD-9-CM codes to 5 digits
	ELSE icdcm_raw
	END AS icdcm_norm,
	icdcm_version,
	icdcm_number
	FROM stg_claims.tmp_apcd_icdcm_header_raw;
	IF OBJECT_ID('stg_claims.tmp_apcd_icdcm_d1_dedup', 'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_icdcm_d1_dedup;
	IF OBJECT_ID('stg_claims.tmp_apcd_icdcm_header_raw', 'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_icdcm_header_raw;
	

	--STEP 5: Create icdcm_hash column and create final table (CTA is faster than INSERT INTO in Synapse)
	IF OBJECT_ID('stg_claims.stage_apcd_claim_icdcm_header', 'U') IS NOT NULL DROP TABLE stg_claims.stage_apcd_claim_icdcm_header;
	CREATE TABLE stg_claims.stage_apcd_claim_icdcm_header
	WITH
	(
	DISTRIBUTION = HASH(claim_header_id),
	CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT
	id_apcd,
	claim_header_id,
	first_service_date,
	last_service_date,
	icdcm_raw,
	icdcm_norm,
	icdcm_version,
	icdcm_number,
	CHECKSUM(icdcm_norm, icdcm_version) AS icdcm_hash,
	GETDATE() AS last_run
	FROM stg_claims.tmp_apcd_icdcm_norm;
	IF OBJECT_ID('stg_claims.tmp_apcd_icdcm_norm', 'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_icdcm_norm;",
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