#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_HEADER
# Eli Kern, PHSKC (APDE)
#
# 2019-10
#
# 2024-04-29 update: Modified for HHSAW migration
# 2024-05-30 update: Modified to optimize query for efficient SQL pool workload in Synapse - insert into heap tables with label for all intermediate tables
# 2025-02-25 update: Adding CCS, BH and injury columns
# 2026-08-25 updates:
	#Use hash fact tables and replicate reference tables to reduce run time
	#Expanded ED pophealth definition to include Medicare Type B ED visit G-codes and 99292 (critical care add-on)
	#Applies fix to DENSE_RANK-generated values for inpatient_id, pc_visit_id, ed_perform_id
	#Add new code to extract Onpoint service type line-level flags from medical_claim table
	#Add code to exclude non WA residents and members with claims but no elig data

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/07_apcd_create_analytic_tables.R

#### Load script ####
load_stage.apcd_claim_header_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "
	------------------------------------------------------------
	--STEP 1: Do all line-level transformations that don't require ICD-CM, procedure, or provider information
	--Exclude all denied and orphaned claim headers
	--Acute inpatient stay defined through Susan Hernandez's work and dialogue with OnPoint
	--Max of discharge dt grouped by claim header will take latest discharge date when >1 discharge dt
	------------------------------------------------------------

	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_temp1', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_temp1;

	CREATE TABLE stg_claims.tmp_apcd_claim_header_temp1
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT 
		a.internal_member_id AS id_apcd, 
		a.medical_claim_header_id AS claim_header_id,
		CASE WHEN a.product_code_id IN (-1,-2) THEN NULL ELSE a.product_code_id END AS product_code_id,
		a.first_service_dt AS first_service_date,
		a.last_service_dt AS last_service_date,
		a.first_paid_dt AS first_paid_date,
		a.last_paid_dt AS last_paid_date,
		--a.charge_amt, --exclude until cost information is available
		c.claim_status_id,
		CASE WHEN a.type_of_bill_code IN (-1,-2) THEN NULL ELSE a.type_of_bill_code END AS type_of_bill_code,

		-- concatenate claim type variables
		CAST(CONVERT(varchar(100), a.claim_type_id)
			 + '.' + CONVERT(varchar(100), a.type_of_setting_id)
			 + '.' + CONVERT(varchar(100), CASE WHEN a.place_of_setting_id IN (-1,-2) THEN NULL ELSE a.place_of_setting_id END)
			 AS varchar(100)) AS claim_type_apcd_id,

		-- ED performance temp flags (RDA measure)
		CAST(CASE WHEN a.emergency_room_flag = 'Y' THEN 1 ELSE 0 END AS tinyint) AS ed_perform_temp,

		-- ED population health temp flags (Yale measure)
		b.ed_pos_temp,
		b.ed_revenue_code_temp,

		-- inpatient visit
		CASE 
			WHEN a.claim_type_id = '1' 
				 AND a.type_of_setting_id = '1' 
				 AND a.place_of_setting_id = '1'
				 AND c.claim_status_id IN (-1, -2, 1, 5, 2, 6) -- only include primary and secondary claims
				 AND b.discharge_date IS NOT NULL
			THEN 1 ELSE 0 
		END AS ipt_flag,
		b.discharge_date

	FROM stg_claims.apcd_medical_claim_header AS a

	--join to claim_line table to grab place of service and revenue code for ED visit, and discharge date for IPT
	LEFT JOIN
	(
		SELECT
			claim_header_id,
			MAX(discharge_date) AS discharge_date,
			MAX(CASE WHEN place_of_service_code = '23' THEN 1 ELSE 0 END) AS ed_pos_temp,
			MAX(CASE WHEN revenue_code LIKE '045[01269]' OR revenue_code = '0981' THEN 1 ELSE 0 END) AS ed_revenue_code_temp
		FROM stg_claims.stage_apcd_claim_line
		GROUP BY claim_header_id
	) AS b
		ON a.medical_claim_header_id = b.claim_header_id

	--left join to claim status reference table to use numeric codes rather than varchar header_status variable
	LEFT JOIN stg_claims.ref_apcd_claim_status_rep AS c
		ON a.header_status = c.claim_status_code
		
	LEFT JOIN stg_claims.apcd_ref_member_exclude AS y
		ON a.internal_member_id = y.id_apcd

	--exclude denined/orphaned claims
	WHERE a.denied_header_flag = 'N'
	  AND a.orphaned_header_flag = 'N'
	--exclude members with no WA residency OR no elig data
	  AND y.id_apcd IS NULL
	OPTION (LABEL = 'apcd_claim_header_temp1');


	------------------------------------------------------------
	--STEP 1b: Generate claim header-level flags for line-level service type flags
	------------------------------------------------------------
	IF OBJECT_ID(N'stg_claims.tmp_apcd_service_type_flags', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_service_type_flags;

	CREATE TABLE stg_claims.tmp_apcd_service_type_flags
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT medical_claim_header_id AS claim_header_id,
		MAX(CASE WHEN cardiac_imaging_and_tests_flag = 'Y' THEN 1 ELSE 0 END) AS cardiac_imaging_and_tests_flag,
		MAX(CASE WHEN chiropractic_flag = 'Y' THEN 1 ELSE 0 END) AS chiropractic_flag,
		MAX(CASE WHEN consultations_flag = 'Y' THEN 1 ELSE 0 END) AS consultations_flag,
		MAX(CASE WHEN covid19_flag = 'Y' THEN 1 ELSE 0 END) AS covid19_flag,
		MAX(CASE WHEN dialysis_flag = 'Y' THEN 1 ELSE 0 END) AS dialysis_flag,
		MAX(CASE WHEN durable_medical_equip_flag = 'Y' THEN 1 ELSE 0 END) AS durable_medical_equip_flag,
		MAX(CASE WHEN echography_flag = 'Y' THEN 1 ELSE 0 END) AS echography_flag,
		MAX(CASE WHEN endoscopic_procedure_flag = 'Y' THEN 1 ELSE 0 END) AS endoscopic_procedure_flag,
		MAX(CASE WHEN evaluation_and_management_flag = 'Y' THEN 1 ELSE 0 END) AS evaluation_and_management_flag,
		MAX(CASE WHEN health_home_utilization_flag = 'Y' THEN 1 ELSE 0 END) AS health_home_utilization_flag,
		MAX(CASE WHEN hospice_utilization_flag = 'Y' THEN 1 ELSE 0 END) AS hospice_utilization_flag,
		MAX(CASE WHEN imaging_advanced_flag = 'Y' THEN 1 ELSE 0 END) AS imaging_advanced_flag,
		MAX(CASE WHEN imaging_standard_flag = 'Y' THEN 1 ELSE 0 END) AS imaging_standard_flag,
		MAX(CASE WHEN inpatient_acute_flag = 'Y' THEN 1 ELSE 0 END) AS inpatient_acute_flag,
		MAX(CASE WHEN inpatient_nonacute_flag = 'Y' THEN 1 ELSE 0 END) AS inpatient_nonacute_flag,
		MAX(CASE WHEN lab_and_pathology_flag = 'Y' THEN 1 ELSE 0 END) AS lab_and_pathology_flag,
		MAX(CASE WHEN oncology_and_chemotherapy_flag = 'Y' THEN 1 ELSE 0 END) AS oncology_and_chemotherapy_flag,
		MAX(CASE WHEN physical_therapy_rehab_flag = 'Y' THEN 1 ELSE 0 END) AS physical_therapy_rehab_flag,
		MAX(CASE WHEN preventive_screenings_flag = 'Y' THEN 1 ELSE 0 END) AS preventive_screenings_flag,
		MAX(CASE WHEN preventive_vaccinations_flag = 'Y' THEN 1 ELSE 0 END) AS preventive_vaccinations_flag,
		MAX(CASE WHEN preventive_visits_flag = 'Y' THEN 1 ELSE 0 END) AS preventive_visits_flag,
		MAX(CASE WHEN psychiatric_visits_flag = 'Y' THEN 1 ELSE 0 END) AS psychiatric_visits_flag,
		MAX(CASE WHEN surgery_and_anesthesia_flag = 'Y' THEN 1 ELSE 0 END) AS surgery_and_anesthesia_flag,
		MAX(CASE WHEN telehealth_flag = 'Y' THEN 1 ELSE 0 END) AS telehealth_flag
	FROM stg_claims.apcd_medical_claim as a
	LEFT JOIN stg_claims.apcd_ref_member_exclude AS y
		ON a.internal_member_id = y.id_apcd
	--exclude members with no WA residency OR no elig data
	WHERE y.id_apcd IS NULL
	GROUP BY medical_claim_header_id
	OPTION (LABEL = 'apcd_service_type_flags');


	------------------------------------------------------------
	--STEP 2: Procedure code query for ED visits
	--Subset to relevant claims as last step to minimize temp table size
	------------------------------------------------------------

	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_ed_procedure_code', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_ed_procedure_code;

	CREATE TABLE stg_claims.tmp_apcd_claim_header_ed_procedure_code
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT x.medical_claim_header_id AS claim_header_id,
		   x.ed_procedure_code_temp
	FROM
	(
		SELECT 
			a.medical_claim_header_id,
			MAX(CASE WHEN b.procedure_code LIKE '9928[12345]' OR b.procedure_code in ('99291', '99292') OR b.procedure_code LIKE 'G038[01234]' THEN 1 ELSE 0 END) AS ed_procedure_code_temp
		FROM stg_claims.apcd_medical_claim_header AS a
		--procedure code table
		LEFT JOIN stg_claims.stage_apcd_claim_procedure AS b
			ON a.medical_claim_header_id = b.claim_header_id
		LEFT JOIN stg_claims.apcd_ref_member_exclude AS y
			ON a.internal_member_id = y.id_apcd
		
		--exclude denined/orphaned claims
		WHERE a.denied_header_flag = 'N'
		  AND a.orphaned_header_flag = 'N'
		--exclude members with no WA residency OR no elig data
		  AND y.id_apcd IS NULL
		
		--cluster to claim header
		GROUP BY a.medical_claim_header_id
	) AS x
	WHERE x.ed_procedure_code_temp = 1
	OPTION (LABEL = 'apcd_claim_header_ed_procedure_code');


	------------------------------------------------------------
	-- STEP 3: Primary care visit query
	------------------------------------------------------------

	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_pc_visit', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_pc_visit;

	CREATE TABLE stg_claims.tmp_apcd_claim_header_pc_visit
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT x.medical_claim_header_id AS claim_header_id,
		   x.pc_procedure_temp,
		   x.pc_zcode_temp,
		   x.pc_taxonomy_temp
	FROM
	(
		SELECT 
			a.medical_claim_header_id,
			--primary care visit temp flags
			MAX(CASE WHEN b.code IS NOT NULL THEN 1 ELSE 0 END) AS pc_procedure_temp,
			MAX(CASE WHEN c.code IS NOT NULL THEN 1 ELSE 0 END) AS pc_zcode_temp,
			MAX(CASE WHEN d.code IS NOT NULL THEN 1 ELSE 0 END) AS pc_taxonomy_temp
		FROM stg_claims.apcd_medical_claim_header AS a

		--procedure codes
		LEFT JOIN
		(
			SELECT b1.claim_header_id, b2.code
			--procedure code table
			FROM stg_claims.stage_apcd_claim_procedure AS b1
			--primary care-relevant procedure codes
			INNER JOIN
			(
				SELECT code
				FROM stg_claims.ref_pc_visit_oregon_rep
				WHERE code_system IN ('cpt', 'hcpcs')
			) AS b2
				ON b1.procedure_code = b2.code
		) AS b
			ON a.medical_claim_header_id = b.claim_header_id

		--ICD-CM codes
		LEFT JOIN
		(
			SELECT c1.claim_header_id, c2.code
			--ICD-CM table
			FROM stg_claims.stage_apcd_claim_icdcm_header AS c1
			--primary care-relevant ICD-10-CM codes
			INNER JOIN
			(
				SELECT code
				FROM stg_claims.ref_pc_visit_oregon_rep
				WHERE code_system = 'icd10cm'
			) AS c2
				ON (c1.icdcm_norm = c2.code) AND (c1.icdcm_version = 10)
		) AS c
			ON a.medical_claim_header_id = c.claim_header_id

		--provider taxonomies
		LEFT JOIN
		(
			SELECT d1.claim_header_id, d4.code
			FROM
			--rendering and attending providers
			(
				SELECT *
				FROM stg_claims.stage_apcd_claim_provider
				WHERE provider_type IN ('rendering', 'attending')
			) AS d1
			--NPIs for each provider
			INNER JOIN stg_claims.ref_apcd_provider_npi_rep AS d2
				ON d1.provider_id_apcd = d2.provider_id_apcd
			--taxonomy codes for rendering and attending providers
			INNER JOIN stg_claims.ref_kc_provider_master_rep AS d3
				ON d2.npi = d3.npi
			--primary care-relevant provider taxonomy codes
			INNER JOIN
			(
				SELECT code
				FROM stg_claims.ref_pc_visit_oregon_rep
				WHERE code_system = 'provider_taxonomy'
			) AS d4
				ON (d3.primary_taxonomy = d4.code) OR (d3.secondary_taxonomy = d4.code)
		) AS d
			ON a.medical_claim_header_id = d.claim_header_id
			
		LEFT JOIN stg_claims.apcd_ref_member_exclude AS y
			ON a.internal_member_id = y.id_apcd

		--exclude denined/orphaned claims
		WHERE a.denied_header_flag = 'N'
		  AND a.orphaned_header_flag = 'N'
		--exclude members with no WA residency OR no elig data
		  AND y.id_apcd IS NULL

		--cluster to claim header
		GROUP BY a.medical_claim_header_id
	) AS x
	WHERE (x.pc_procedure_temp = 1 OR x.pc_zcode_temp = 1)
	  AND x.pc_taxonomy_temp = 1
	OPTION (LABEL = 'apcd_claim_header_pc_visit');


	------------------------------------------------------------
	--STEP 4: Extract primary diagnosis, take first ordered ICD-CM code when >1 primary per header
	------------------------------------------------------------

	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_icd1', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_icd1;

	CREATE TABLE stg_claims.tmp_apcd_claim_header_icd1
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT
		claim_header_id,
		MIN(icdcm_norm) AS primary_diagnosis,
		MIN(icdcm_version) AS icdcm_version
	FROM stg_claims.stage_apcd_claim_icdcm_header
	WHERE icdcm_number = '01'
	GROUP BY claim_header_id
	OPTION (LABEL = 'apcd_claim_header_icd1');


	------------------------------------------------------------
	-- STEP 5: Join intermediate tables and create icdcm_hash for later joining
	------------------------------------------------------------

	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_temp1b', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_temp1b;

	CREATE TABLE stg_claims.tmp_apcd_claim_header_temp1b
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT
		a.claim_header_id,
		b.primary_diagnosis,
		b.icdcm_version,
		CHECKSUM(b.primary_diagnosis, b.icdcm_version) as icdcm_hash,
		c.ed_procedure_code_temp,
		d.pc_procedure_temp,
		d.pc_taxonomy_temp,
		d.pc_zcode_temp
	FROM
	(
		SELECT DISTINCT claim_header_id
		FROM stg_claims.tmp_apcd_claim_header_temp1
	) AS a
	LEFT JOIN stg_claims.tmp_apcd_claim_header_icd1 AS b
		ON a.claim_header_id = b.claim_header_id
	LEFT JOIN stg_claims.tmp_apcd_claim_header_ed_procedure_code AS c
		ON a.claim_header_id = c.claim_header_id
	LEFT JOIN stg_claims.tmp_apcd_claim_header_pc_visit AS d
		ON a.claim_header_id = d.claim_header_id
	OPTION (LABEL = 'apcd_claim_header_temp1b');

	--drop intermediate persistent tables no longer needed
	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_icd1', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_icd1;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_ed_procedure_code', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_ed_procedure_code;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_pc_visit', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_pc_visit;


	------------------------------------------------------------
	--STEP 6: Prepare header-level concepts using analytic claim tables
	--Add in CCS columns for primary diagnosis
	------------------------------------------------------------

	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_temp2', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_temp2;

	CREATE TABLE stg_claims.tmp_apcd_claim_header_temp2
	WITH
	(
		DISTRIBUTION = HASH(id_apcd),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT DISTINCT
		a.id_apcd,
		a.claim_header_id,
		a.product_code_id,
		a.first_service_date,
		a.last_service_date,
		a.first_paid_date,
		a.last_paid_date,
		--a.charge_amt, --exclude until cost information is available
		c.primary_diagnosis,
		c.icdcm_version,
		c.icdcm_hash,
		a.claim_status_id,
		a.claim_type_apcd_id,
		b.kc_clm_type_id AS claim_type_id,
		a.type_of_bill_code,
		d.ccs_superlevel_desc,
		d.ccs_broad_desc,
		d.ccs_broad_code,
		d.ccs_midlevel_desc,
		d.ccs_detail_desc,
		d.ccs_detail_code,
		e.cardiac_imaging_and_tests_flag,
		e.chiropractic_flag,
		e.consultations_flag,
		e.covid19_flag,
		e.dialysis_flag,
		e.durable_medical_equip_flag,
		e.echography_flag,
		e.endoscopic_procedure_flag,
		e.evaluation_and_management_flag,
		e.health_home_utilization_flag,
		e.hospice_utilization_flag,
		e.imaging_advanced_flag,
		e.imaging_standard_flag,
		e.inpatient_acute_flag,
		e.inpatient_nonacute_flag,
		e.lab_and_pathology_flag,
		e.oncology_and_chemotherapy_flag,
		e.physical_therapy_rehab_flag,
		e.preventive_screenings_flag,
		e.preventive_vaccinations_flag,
		e.preventive_visits_flag,
		e.psychiatric_visits_flag,
		e.surgery_and_anesthesia_flag,
		e.telehealth_flag,

		-- ED performance (RDA)
		CASE WHEN a.ed_perform_temp = 1 AND b.kc_clm_type_id = 4 THEN 1 ELSE 0 END AS ed_perform,

		-- ED population health (Yale)
		CASE WHEN b.kc_clm_type_id = 5
				  AND ((c.ed_procedure_code_temp = 1 AND a.ed_pos_temp = 1) OR a.ed_revenue_code_temp = 1)
			 THEN 1 ELSE 0 END AS ed_yale_carrier,
		CASE WHEN b.kc_clm_type_id = 4
				  AND (a.ed_revenue_code_temp = 1 OR a.ed_pos_temp = 1 OR c.ed_procedure_code_temp = 1)
			 THEN 1 ELSE 0 END AS ed_yale_opt,
		CASE WHEN b.kc_clm_type_id = 1
				  AND (a.ed_revenue_code_temp = 1 OR a.ed_pos_temp = 1 OR c.ed_procedure_code_temp = 1)
			 THEN 1 ELSE 0 END AS ed_yale_ipt,

		-- Inpatient visit
		ipt_flag AS inpatient,
		discharge_date,

		-- Primary care visit (Oregon)
		CASE 
			WHEN (c.pc_procedure_temp = 1 OR c.pc_zcode_temp = 1)
				 AND c.pc_taxonomy_temp = 1
				 AND a.claim_type_apcd_id NOT IN ('1.1.1', '1.1.14', '1.1.2', '2.3.8', '2.3.2', '1.2.8') --exclude inpatient, swing bed, free-standing ambulatory
				 AND a.claim_status_id IN (-1, -2, 1, 5, 2, 6) --only include primary and secondary claim headers
			THEN 1 ELSE 0
		END AS pc_visit

	FROM stg_claims.tmp_apcd_claim_header_temp1 AS a
	LEFT JOIN
	(
		SELECT *
		FROM stg_claims.ref_kc_claim_type_crosswalk_rep
		WHERE source_desc = 'apcd'
	) AS b
		ON a.claim_type_apcd_id = b.source_clm_type_id
	LEFT JOIN stg_claims.tmp_apcd_claim_header_temp1b AS c
		ON a.claim_header_id = c.claim_header_id
	LEFT JOIN stg_claims.ref_icdcm_codes_rep AS d
		ON (c.icdcm_hash = d.icdcm_hash)
	LEFT JOIN stg_claims.tmp_apcd_service_type_flags AS e
	ON a.claim_header_id = e.claim_header_id
	OPTION (LABEL = 'apcd_claim_header_temp2');

	--drop intermediate persistent tables no longer needed
	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_temp1', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_temp1;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_temp1b', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_temp1b;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_service_type_flags', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_service_type_flags;


	------------------------------------------------------------
	--STEP 7: Assign unique ID to healthcare utilization concepts that are grouped by person, service date
	--Note that HASH distribution prevents DENSE_RANK from working as desired, hence the longer approach with distinct pairs, ranks, and join to claim IDs
	------------------------------------------------------------

	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_temp3', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_temp3;

	CREATE TABLE stg_claims.tmp_apcd_claim_header_temp3
	WITH
	(
		DISTRIBUTION = HASH(id_apcd),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	WITH
	-- 1. Distinct pairs for each visit type
	pc_pairs AS (
		SELECT DISTINCT id_apcd, first_service_date
		FROM stg_claims.tmp_apcd_claim_header_temp2
		WHERE pc_visit = 1
	),
	inpatient_pairs AS (
		SELECT DISTINCT id_apcd, first_service_date
		FROM stg_claims.tmp_apcd_claim_header_temp2
		WHERE inpatient = 1
	),
	ed_pairs AS (
		SELECT DISTINCT id_apcd, first_service_date
		FROM stg_claims.tmp_apcd_claim_header_temp2
		WHERE ed_perform = 1
	),

	-- 2. Rank each set of pairs
	pc_ranked AS (
		SELECT id_apcd, first_service_date,
			   ROW_NUMBER() OVER (ORDER BY id_apcd, first_service_date) AS pc_visit_id
		FROM pc_pairs
	),
	inpatient_ranked AS (
		SELECT id_apcd, first_service_date,
			   ROW_NUMBER() OVER (ORDER BY id_apcd, first_service_date) AS inpatient_id
		FROM inpatient_pairs
	),
	ed_ranked AS (
		SELECT id_apcd, first_service_date,
			   ROW_NUMBER() OVER (ORDER BY id_apcd, first_service_date) AS ed_perform_id
		FROM ed_pairs
	),

	-- 3. Map ranked IDs back to the claim_header_id subset
	pc_claims AS (
		SELECT t.claim_header_id, r.pc_visit_id
		FROM stg_claims.tmp_apcd_claim_header_temp2 t
		JOIN pc_ranked r
		  ON t.id_apcd = r.id_apcd
		 AND t.first_service_date = r.first_service_date
		WHERE t.pc_visit = 1
	),
	inpatient_claims AS (
		SELECT t.claim_header_id, r.inpatient_id
		FROM stg_claims.tmp_apcd_claim_header_temp2 t
		JOIN inpatient_ranked r
		  ON t.id_apcd = r.id_apcd
		 AND t.first_service_date = r.first_service_date
		WHERE t.inpatient = 1
	),
	ed_claims AS (
		SELECT t.claim_header_id, r.ed_perform_id
		FROM stg_claims.tmp_apcd_claim_header_temp2 t
		JOIN ed_ranked r
		  ON t.id_apcd = r.id_apcd
		 AND t.first_service_date = r.first_service_date
		WHERE t.ed_perform = 1
	)

	-- 4. Final assembly
	SELECT 
		a.*,
		pc.pc_visit_id,
		ip.inpatient_id,
		ed.ed_perform_id
	FROM stg_claims.tmp_apcd_claim_header_temp2 a
	LEFT JOIN pc_claims pc
		ON a.claim_header_id = pc.claim_header_id
	LEFT JOIN inpatient_claims ip
		ON a.claim_header_id = ip.claim_header_id
	LEFT JOIN ed_claims ed
		ON a.claim_header_id = ed.claim_header_id
	OPTION (LABEL = 'apcd_claim_header_temp3');

	--drop intermediate persistent tables no longer needed
	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_temp2', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_temp2;


	------------------------------------------------------------
	--STEP 8: RDA behavioral health diagnosis flags
	------------------------------------------------------------

	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_bh', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_bh;

	CREATE TABLE stg_claims.tmp_apcd_claim_header_bh
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT 
		b.claim_header_id,
		MAX(CASE WHEN b.icdcm_number = '01' AND a.mh_any = 1 THEN 1 ELSE 0 END) AS mh_primary,
		MAX(CASE WHEN a.mh_any = 1 THEN 1 ELSE 0 END) AS mh_any,
		MAX(CASE WHEN b.icdcm_number = '01' AND a.sud_any = 1 THEN 1 ELSE 0 END) AS sud_primary,
		MAX(CASE WHEN a.sud_any = 1 THEN 1 ELSE 0 END) AS sud_any
	FROM stg_claims.ref_icdcm_codes_rep AS a
	INNER JOIN stg_claims.stage_apcd_claim_icdcm_header AS b
		ON (a.icdcm_hash = b.icdcm_hash)
	GROUP BY b.claim_header_id
	OPTION (LABEL = 'tmp_apcd_claim_header_bh');


	------------------------------------------------------------
	--STEP 9: Injury cause and nature per CDC guidance
	------------------------------------------------------------

	--Step 9a: Create table of distinct icdcm codes

	IF OBJECT_ID(N'stg_claims.tmp_apcd_icdcm_distinct', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_icdcm_distinct;

	CREATE TABLE stg_claims.tmp_apcd_icdcm_distinct
	WITH
	(
		DISTRIBUTION = REPLICATE,
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT DISTINCT icdcm_norm, icdcm_version, icdcm_hash
	FROM stg_claims.stage_apcd_claim_icdcm_header


	--Step 9b: Flag nature of injury codes per CDC injury hospitalization surveillance definition for ICD-9-CM and ICD-10-CM
	--Refer to 7/5/19 NHSR report for ICD-9-CM and ICD-10-CM surveillance case definition for injury hospitalizations
	--ICD-9-CM definition is in 2nd paragraph of introduction
	--ICD-10-CM definition is in Table C (note this is same as Table B in 2020 NHSR update to nature of injury body region classification)
	--Tip - For using SQL between operator, the second parameter must be the last value in the list we want to include or it will miss values (e.g. 9949 not 994)

	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_nature_ref', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_nature_ref;

	CREATE TABLE stg_claims.tmp_apcd_injury_nature_ref
	WITH
	(
		DISTRIBUTION = REPLICATE,
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT DISTINCT *
	FROM stg_claims.tmp_apcd_icdcm_distinct
	--Apply CDC surveillance definition for ICD-9-CM codes
	WHERE
		(icdcm_version = 9 AND
			(icdcm_norm BETWEEN '800%' AND '9949%' OR icdcm_norm LIKE '9955%' OR icdcm_norm BETWEEN '99580%' AND '99585%') --inclusion
			AND icdcm_norm NOT LIKE '9093%' --exclusion
			AND icdcm_norm NOT LIKE '9095%' --exclusion
		)
		OR
		--Apply CDC surveillance definition for ICD-10-CM codes
		(icdcm_version = 10 AND
			(
				(icdcm_norm LIKE 'S%' AND SUBSTRING(icdcm_norm,7,1) IN ('A','B','C','')) --inclusion
				OR (icdcm_norm BETWEEN 'T07%' AND 'T3499XS' AND SUBSTRING(icdcm_norm,7,1) IN ('A','B','C','')) --inclusion
				OR (icdcm_norm BETWEEN 'T36%' AND 'T50996S' AND SUBSTRING(icdcm_norm,6,1) IN ('1','2','3','4') AND SUBSTRING(icdcm_norm,7,1) IN ('A','B','C','')) --inclusion
				OR (icdcm_norm LIKE 'T3[679]9%' AND SUBSTRING(icdcm_norm,5,1) IN ('1','2','3','4') AND SUBSTRING(icdcm_norm,7,1) IN ('A','B','C','')) --inclusion
				OR (icdcm_norm LIKE 'T414%' AND SUBSTRING(icdcm_norm,5,1) IN ('1','2','3','4') AND SUBSTRING(icdcm_norm,7,1) IN ('A','B','C','')) --inclusion
				OR (icdcm_norm LIKE 'T427%' AND SUBSTRING(icdcm_norm,5,1) IN ('1','2','3','4') AND SUBSTRING(icdcm_norm,7,1) IN ('A','B','C','')) --inclusion
				OR (icdcm_norm LIKE 'T4[3579]9%' AND SUBSTRING(icdcm_norm,5,1) IN ('1','2','3','4') AND SUBSTRING(icdcm_norm,7,1) IN ('A','B','C','')) --inclusion
				OR (icdcm_norm BETWEEN 'T51%' AND 'T6594XS' AND SUBSTRING(icdcm_norm,7,1) IN ('A','B','C','')) --inclusion
				OR (icdcm_norm BETWEEN 'T66%' AND 'T7692XS' AND SUBSTRING(icdcm_norm,7,1) IN ('A','B','C','')) --inclusion
				OR (icdcm_norm LIKE 'T79%' AND SUBSTRING(icdcm_norm,7,1) IN ('A','B','C','')) --inclusion
				OR (icdcm_norm BETWEEN 'O9A2%' AND 'O9A53' AND SUBSTRING(icdcm_norm,7,1) IN ('A','B','C','')) --inclusion
				OR (icdcm_norm LIKE 'T8404%' AND SUBSTRING(icdcm_norm,7,1) IN ('A','B','C','')) --inclusion
				OR (icdcm_norm LIKE 'M97%' AND SUBSTRING(icdcm_norm,7,1) IN ('A','B','C','')) --inclusion
			)
		);


	--Step 9c: Create flags for broad and narrrow injury surveillance definitions

	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_nature', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_nature;

	CREATE TABLE stg_claims.tmp_apcd_injury_nature
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT 
		a.claim_header_id,
		a.icdcm_norm,
		a.icdcm_version,
		a.icdcm_number,
		a.icdcm_hash,
		CASE WHEN a.icdcm_number = '01' THEN 1 ELSE 0 END AS injury_narrow,
		1 AS injury_broad
	FROM stg_claims.stage_apcd_claim_icdcm_header AS a
	INNER JOIN stg_claims.tmp_apcd_injury_nature_ref AS b
		ON (a.icdcm_hash = b.icdcm_hash)


	--Step 9d: Identify external cause-of-injury codes for intent and mechanism

	--LIKE join distinct ICD-10-CM codes to ICD-10-CM external cause-of-injury code reference table

	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_icd10cm_ref', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_icd10cm_ref;

	CREATE TABLE stg_claims.tmp_apcd_injury_cause_icd10cm_ref
	WITH
	(
		DISTRIBUTION = REPLICATE,
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT DISTINCT a.icdcm_norm,
		   a.icdcm_version,
		   b.intent,
		   b.mechanism
	FROM
	(
		SELECT *
		FROM stg_claims.tmp_apcd_icdcm_distinct
		WHERE icdcm_version = 10
	) AS a
	INNER JOIN
	(
		SELECT icdcm,
			   icdcm + '%' AS icdcm_like,
			   icdcm_version,
			   intent,
			   mechanism
		FROM stg_claims.ref_icdcm_codes_rep
		WHERE icdcm_version = 10
		  AND intent IS NOT NULL
	) AS b
		ON (a.icdcm_norm LIKE b.icdcm_like) AND (a.icdcm_version = b.icdcm_version);

	--LIKE join distinct ICD-9-CM codes to ICD-9-CM external cause-of-injury code reference table

	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_icd9cm_ref', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_icd9cm_ref;

	CREATE TABLE stg_claims.tmp_apcd_injury_cause_icd9cm_ref
	WITH
	(
		DISTRIBUTION = REPLICATE,
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT DISTINCT a.icdcm_norm,
		   a.icdcm_version,
		   b.intent,
		   b.mechanism
	FROM
	(
		SELECT *
		FROM stg_claims.tmp_apcd_icdcm_distinct
		WHERE icdcm_version = 9
	) AS a
	INNER JOIN
	(
		SELECT icdcm,
			   icdcm + '%' AS icdcm_like,
			   icdcm_version,
			   intent,
			   mechanism
		FROM stg_claims.ref_icdcm_codes_rep
		WHERE icdcm_version = 9
		  AND intent IS NOT NULL
	) AS b
		ON (a.icdcm_norm LIKE b.icdcm_like) AND (a.icdcm_version = b.icdcm_version);

	--UNION ICD-10-CM and ICD-9-CM reference tables

	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_ref', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_ref;

	CREATE TABLE stg_claims.tmp_apcd_injury_cause_ref
	WITH
	(
		DISTRIBUTION = REPLICATE,
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT icdcm_norm, icdcm_version, intent, mechanism, CHECKSUM(icdcm_norm, icdcm_version) as icdcm_hash
	FROM stg_claims.tmp_apcd_injury_cause_icd9cm_ref
	UNION
	SELECT icdcm_norm, icdcm_version, intent, mechanism, CHECKSUM(icdcm_norm, icdcm_version) as icdcm_hash
	FROM stg_claims.tmp_apcd_injury_cause_icd10cm_ref;

	--EXACT join of above table to claims data with injury flags

	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause;

	CREATE TABLE stg_claims.tmp_apcd_injury_cause
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT DISTINCT
		a.claim_header_id,
		a.icdcm_norm,
		a.icdcm_version,
		a.icdcm_number,
		b.intent,
		b.mechanism,
		1 AS ecode_flag
	FROM stg_claims.stage_apcd_claim_icdcm_header AS a
	INNER JOIN stg_claims.tmp_apcd_injury_cause_ref AS b
		ON (a.icdcm_hash = b.icdcm_hash)


	--Create rank variables for valid nature-of-injury codes

	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_nature_ranks', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_nature_ranks;

	CREATE TABLE stg_claims.tmp_apcd_injury_nature_ranks
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT *,
		   ROW_NUMBER() OVER (PARTITION BY claim_header_id, injury_broad ORDER BY icdcm_number) AS injury_nature_rank
	FROM stg_claims.tmp_apcd_injury_nature;

	--Create rank variables for valid external cause-of-injury codes

	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_ranks', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_ranks;

	CREATE TABLE stg_claims.tmp_apcd_injury_cause_ranks
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT *,
		   ROW_NUMBER() OVER (PARTITION BY claim_header_id, ecode_flag ORDER BY icdcm_number) AS ecode_rank
	FROM stg_claims.tmp_apcd_injury_cause;

	--Step 9e: Aggregate to claim header level

	--Create some aggregated fields

	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_header_level_tmp;

	CREATE TABLE stg_claims.tmp_apcd_injury_cause_header_level_tmp
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT
		claim_header_id,
		icdcm_norm,
		MAX(injury_narrow) OVER (PARTITION BY claim_header_id) AS injury_narrow,
		MAX(injury_broad) OVER (PARTITION BY claim_header_id) AS injury_broad
	FROM stg_claims.tmp_apcd_injury_nature_ranks;


	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp2', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_header_level_tmp2;

	CREATE TABLE stg_claims.tmp_apcd_injury_cause_header_level_tmp2
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT
		claim_header_id,
		icdcm_norm,
		intent,
		mechanism,
		MAX(ecode_flag) OVER (PARTITION BY claim_header_id) AS ecode_flag_max,
		ecode_rank
	FROM stg_claims.tmp_apcd_injury_cause_ranks;

	--Collapse to claim header level

	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp3', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_header_level_tmp3;

	CREATE TABLE stg_claims.tmp_apcd_injury_cause_header_level_tmp3
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT DISTINCT
		a.claim_header_id,
		CASE WHEN b.ecode_rank = 1 THEN b.icdcm_norm ELSE NULL END AS ecode,
		a.injury_narrow,
		a.injury_broad,
		b.intent,
		b.mechanism
	FROM stg_claims.tmp_apcd_injury_cause_header_level_tmp AS a
	LEFT JOIN
	(
		SELECT *
		FROM stg_claims.tmp_apcd_injury_cause_header_level_tmp2
		WHERE ecode_flag_max = 1
		  AND ecode_rank = 1
	) AS b
		ON a.claim_header_id = b.claim_header_id;

	--Add back first-ranked diagnosis with a nature-of-injury code

	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp4', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_header_level_tmp4;

	CREATE TABLE stg_claims.tmp_apcd_injury_cause_header_level_tmp4
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT a.*,
		   b.icdcm_norm AS icdcm_injury_nature,
		   b.icdcm_version AS icdcm_injury_nature_version
	FROM stg_claims.tmp_apcd_injury_cause_header_level_tmp3 AS a
	LEFT JOIN
	(
		SELECT *
		FROM stg_claims.tmp_apcd_injury_nature_ranks
		WHERE injury_nature_rank = 1
	) AS b
		ON a.claim_header_id = b.claim_header_id;


	--Step 9f: Create reference table to categorize type of nature of injury

	--First join to ref.icdcm_codes to grab CCS detail description, removing [initial encounter] phrase

	IF OBJECT_ID(N'stg_claims.tmp_apcd_distinct_injury_nature_icdcm_tmp1', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_distinct_injury_nature_icdcm_tmp1;

	CREATE TABLE stg_claims.tmp_apcd_distinct_injury_nature_icdcm_tmp1
	WITH
	(
		DISTRIBUTION = REPLICATE,
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT DISTINCT
		icdcm_injury_nature,
		icdcm_injury_nature_version,
		CASE
			WHEN b.ccs_detail_desc LIKE '%; initial encounter%' THEN REPLACE(b.ccs_detail_desc, '; initial encounter', '')
			WHEN b.ccs_detail_desc LIKE '%, initial encounter%' THEN REPLACE(b.ccs_detail_desc, ', initial encounter', '')
			ELSE b.ccs_detail_desc
		END AS ccs_detail_desc
	FROM stg_claims.tmp_apcd_injury_cause_header_level_tmp4 AS a
	LEFT JOIN stg_claims.ref_icdcm_codes_rep AS b
		ON (a.icdcm_injury_nature = b.icdcm) AND (a.icdcm_injury_nature_version = b.icdcm_version)
	WHERE a.icdcm_injury_nature IS NOT NULL;

	--Normalize type of injury categories

	IF OBJECT_ID(N'stg_claims.tmp_apcd_distinct_injury_nature_icdcm_final', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_distinct_injury_nature_icdcm_final;

	CREATE TABLE stg_claims.tmp_apcd_distinct_injury_nature_icdcm_final
	WITH
	(
		DISTRIBUTION = REPLICATE,
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT
		icdcm_injury_nature,
		icdcm_injury_nature_version,
		CASE
			WHEN ccs_detail_desc IN ('Other specified injury', 'Other unspecified injury') THEN 'Other injuries'
			WHEN ccs_detail_desc IN ('Spinal cord injury (SCI)') THEN 'Spinal cord injury'
			WHEN ccs_detail_desc IN ('Effect of other external causes',
									 'External cause codes: other specified, classifiable and NEC',
									 'External cause codes: unspecified mechanism',
									 'Other injuries and conditions due to external causes')
				 THEN 'Other injuries and conditions due to external causes'
			WHEN ccs_detail_desc IN ('Crushing injury', 'Crushing injury or internal injury') THEN 'Crushing injury or internal injury'
			WHEN ccs_detail_desc IN ('Burns', 'Burn and corrosion') THEN 'Burn and corrosion'
			ELSE ccs_detail_desc
		END AS ccs_detail_desc
	FROM stg_claims.tmp_apcd_distinct_injury_nature_icdcm_tmp1;


	--Step 9g: Add broad type categories to nature of injury ICD-CM codes

	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_injury', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_injury;

	CREATE TABLE stg_claims.tmp_apcd_claim_header_injury
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT
		a.claim_header_id,
		a.ecode,
		a.injury_narrow,
		a.injury_broad,
		a.intent,
		a.mechanism,
		a.icdcm_injury_nature,
		a.icdcm_injury_nature_version,
		b.ccs_detail_desc AS icdcm_injury_nature_type
	FROM stg_claims.tmp_apcd_injury_cause_header_level_tmp4 AS a
	LEFT JOIN stg_claims.tmp_apcd_distinct_injury_nature_icdcm_final AS b
		ON (a.icdcm_injury_nature = b.icdcm_injury_nature)
	   AND (a.icdcm_injury_nature_version = b.icdcm_injury_nature_version)
	OPTION (LABEL = 'tmp_apcd_claim_header_injury');


	--drop intermediate persistent tables no longer needed
	IF OBJECT_ID(N'stg_claims.tmp_apcd_icdcm_distinct', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_icdcm_distinct;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_nature_ref', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_nature_ref;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_nature', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_nature;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_icd10cm_ref', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_icd10cm_ref;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_icd9cm_ref', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_icd9cm_ref;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_ref', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_ref;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_nature_ranks', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_nature_ranks;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_ranks', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_ranks;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_header_level_tmp;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp2', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_header_level_tmp2;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp3', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_header_level_tmp3;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp4', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_injury_cause_header_level_tmp4;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_distinct_injury_nature_icdcm_tmp1', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_distinct_injury_nature_icdcm_tmp1;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_distinct_injury_nature_icdcm_final', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_distinct_injury_nature_icdcm_final;


	------------------------------------------------------------
	--STEP 10: Conduct overlap and clustering for ED population health measure (Yale measure)
	--Adaptation of Philip Sylling's Medicaid code, which is adaptation of Eli Kern's original code
	------------------------------------------------------------

	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_ed_pophealth', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_ed_pophealth;

	CREATE TABLE stg_claims.tmp_apcd_claim_header_ed_pophealth
	WITH
	(
		DISTRIBUTION = HASH(claim_header_id),
		CLUSTERED COLUMNSTORE INDEX
	)
	AS
	WITH increment_stays_by_person AS
	(
		SELECT
			id_apcd,
			claim_header_id,
			first_service_date,
			last_service_date,
			--create chronological (0, 1) indicator column.
			--if 0, it is the first ED visit for the person OR the ED visit appears to be a duplicate (overlapping service dates) of the prior visit.
			--if 1, the prior ED visit appears to be distinct from the following stay.
			--this indicator column will be summed to create an episode_id.
			CASE
				WHEN ROW_NUMBER() OVER (PARTITION BY id_apcd ORDER BY first_service_date, last_service_date, claim_header_id) = 1 THEN 0
				WHEN DATEDIFF(DAY,
							  LAG(first_service_date) OVER (PARTITION BY id_apcd ORDER BY first_service_date, last_service_date, claim_header_id),
							  first_service_date) <= 1 THEN 0
				WHEN DATEDIFF(DAY,
							  LAG(first_service_date) OVER (PARTITION BY id_apcd ORDER BY first_service_date, last_service_date, claim_header_id),
							  first_service_date) > 1 THEN 1
			END AS increment
		FROM stg_claims.tmp_apcd_claim_header_temp3
		WHERE ed_yale_carrier = 1
		   OR ed_yale_opt = 1
		   OR ed_yale_ipt = 1
	),

	--Sum [increment] column (Cumulative Sum) within person to create a stay_id that combines duplicate/overlapping ED visits.
	create_within_person_stay_id AS
	(
		SELECT
			id_apcd,
			claim_header_id,
			SUM(increment) OVER (PARTITION BY id_apcd ORDER BY first_service_date, last_service_date, claim_header_id ROWS UNBOUNDED PRECEDING) + 1 AS within_person_stay_id
		FROM increment_stays_by_person
	)
	SELECT
		claim_header_id,
		DENSE_RANK() OVER (ORDER BY id_apcd, within_person_stay_id) AS ed_pophealth_id
	FROM create_within_person_stay_id
	OPTION (LABEL = 'apcd_claim_header_ed_pophealth');


	------------------------------------------------------------
	--STEP 11: Final step to join ed_pophealth, BH, and injury tables to header table on claim header ID
	------------------------------------------------------------

	INSERT INTO stg_claims.stage_apcd_claim_header
	(
		id_apcd,
		claim_header_id,
		product_code_id,
		first_service_date,
		last_service_date,
		first_paid_date,
		last_paid_date,
		charge_amt,
		primary_diagnosis,
		icdcm_version,
		claim_status_id,
		claim_type_apcd_id,
		claim_type_id,
		type_of_bill_code,
		ccs_superlevel_desc,
		ccs_broad_desc,
		ccs_broad_code,
		ccs_midlevel_desc,
		ccs_detail_desc,
		ccs_detail_code,
		mh_primary,
		mh_any,
		sud_primary,
		sud_any,
		injury_nature_narrow,
		injury_nature_broad,
		injury_nature_type,
		injury_nature_icdcm,
		injury_ecode,
		injury_intent,
		injury_mechanism,
		cardiac_imaging_and_tests_flag,
		chiropractic_flag,
		consultations_flag,
		covid19_flag,
		dialysis_flag,
		durable_medical_equip_flag,
		echography_flag,
		endoscopic_procedure_flag,
		evaluation_and_management_flag,
		health_home_utilization_flag,
		hospice_utilization_flag,
		imaging_advanced_flag,
		imaging_standard_flag,
		inpatient_acute_flag,
		inpatient_nonacute_flag,
		lab_and_pathology_flag,
		oncology_and_chemotherapy_flag,
		physical_therapy_rehab_flag,
		preventive_screenings_flag,
		preventive_vaccinations_flag,
		preventive_visits_flag,
		psychiatric_visits_flag,
		surgery_and_anesthesia_flag,
		telehealth_flag,
		ed_perform_id,
		ed_pophealth_id,
		inpatient_id,
		discharge_date,
		pc_visit_id,
		last_run
	)
	SELECT DISTINCT
		a.id_apcd,
		a.claim_header_id,
		a.product_code_id,
		a.first_service_date,
		a.last_service_date,
		a.first_paid_date,
		a.last_paid_date,
		NULL as charge_amt, --placeholder until cost information is available
		a.primary_diagnosis,
		a.icdcm_version,
		a.claim_status_id,
		a.claim_type_apcd_id,
		a.claim_type_id,
		a.type_of_bill_code,
		a.ccs_superlevel_desc,
		a.ccs_broad_desc,
		a.ccs_broad_code,
		a.ccs_midlevel_desc,
		a.ccs_detail_desc,
		a.ccs_detail_code,
		CASE WHEN c.mh_primary = 1 THEN 1 ELSE 0 END AS mh_primary,
		CASE WHEN c.mh_any = 1 THEN 1 ELSE 0 END AS mh_any,
		CASE WHEN c.sud_primary = 1 THEN 1 ELSE 0 END AS sud_primary,
		CASE WHEN c.sud_any = 1 THEN 1 ELSE 0 END AS sud_any,
		CASE WHEN d.injury_narrow = 1 THEN 1 ELSE 0 END AS injury_nature_narrow,
		CASE WHEN d.injury_broad = 1 THEN 1 ELSE 0 END AS injury_nature_broad,
		d.icdcm_injury_nature_type AS injury_nature_type,
		d.icdcm_injury_nature AS injury_nature_icdcm,
		d.ecode AS injury_ecode,
		d.intent AS injury_intent,
		d.mechanism AS injury_mechanism,
		a.cardiac_imaging_and_tests_flag,
		a.chiropractic_flag,
		a.consultations_flag,
		a.covid19_flag,
		a.dialysis_flag,
		a.durable_medical_equip_flag,
		a.echography_flag,
		a.endoscopic_procedure_flag,
		a.evaluation_and_management_flag,
		a.health_home_utilization_flag,
		a.hospice_utilization_flag,
		a.imaging_advanced_flag,
		a.imaging_standard_flag,
		a.inpatient_acute_flag,
		a.inpatient_nonacute_flag,
		a.lab_and_pathology_flag,
		a.oncology_and_chemotherapy_flag,
		a.physical_therapy_rehab_flag,
		a.preventive_screenings_flag,
		a.preventive_vaccinations_flag,
		a.preventive_visits_flag,
		a.psychiatric_visits_flag,
		a.surgery_and_anesthesia_flag,
		a.telehealth_flag,
		a.ed_perform_id,
		b.ed_pophealth_id,
		a.inpatient_id,
		a.discharge_date,
		a.pc_visit_id,
		GETDATE() AS last_run
	FROM stg_claims.tmp_apcd_claim_header_temp3 AS a
	LEFT JOIN stg_claims.tmp_apcd_claim_header_ed_pophealth AS b
		ON a.claim_header_id = b.claim_header_id
	LEFT JOIN stg_claims.tmp_apcd_claim_header_bh AS c
		ON a.claim_header_id = c.claim_header_id
	LEFT JOIN stg_claims.tmp_apcd_claim_header_injury AS d
		ON a.claim_header_id = d.claim_header_id
	OPTION (LABEL = 'stage_apcd_claim_header');

	--drop intermediate persistent tables no longer needed
	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_temp3', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_temp3;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_ed_pophealth', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_ed_pophealth;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_bh', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_bh;
	IF OBJECT_ID(N'stg_claims.tmp_apcd_claim_header_injury', N'U') IS NOT NULL DROP TABLE stg_claims.tmp_apcd_claim_header_injury;
",
    .con = dw_inthealth))
}


#### Table-level QA script ####
qa_stage.apcd_claim_header_f <- function() {
  
  #confirm that claim header is distinct
  res1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of headers' as qa_type,
    count(*) as qa
    from stg_claims.stage_apcd_claim_header;",
    .con = dw_inthealth))
  
  res2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of distinct headers' as qa_type,
    count(distinct claim_header_id) as qa
    from stg_claims.stage_apcd_claim_header;",
    .con = dw_inthealth))
  
  #compare claim header counts with raw data
  res3 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.apcd_medical_claim_header' as 'table', '# of headers in raw table' as qa_type,
    count(*) as qa
    from stg_claims.apcd_medical_claim_header as a
	LEFT JOIN stg_claims.apcd_ref_member_exclude AS y
	ON a.internal_member_id = y.id_apcd
    --exclude denined/orphaned claims
    where denied_header_flag = 'N' and orphaned_header_flag = 'N'
	--exclude members with no WA residency OR no elig data
	AND y.id_apcd IS NULL;",
    .con = dw_inthealth))
  
  #all members should be in elig_demo table
  res4 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of members not in elig_demo, expect 0' as qa_type,
    count(a.id_apcd) as qa
    from stg_claims.stage_apcd_claim_header as a
    left join stg_claims.stage_apcd_elig_demo as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = dw_inthealth))
  
  #all members should be in elig_timevar table
  res5 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of members not in elig_timevar, expect 0' as qa_type,
    count(a.id_apcd) as qa
    from stg_claims.stage_apcd_claim_header as a
    left join stg_claims.stage_apcd_elig_timevar as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = dw_inthealth))
  
  #count unmatched claim types
  res6 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of claims with unmatched claim type, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_apcd_claim_header
    where claim_type_id is null or claim_type_apcd_id is null;",
    .con = dw_inthealth))
  
  #verify that all inpatient stays have discharge date
  res7 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of ipt stays with no discharge date, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_apcd_claim_header
    where inpatient_id is not null and discharge_date is null;",
    .con = dw_inthealth))
  
  #verify that no ed_pophealth_id value is used for more than one person
  res8a <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of ed_pophealth_id values used for >1 person, expect 0' as qa_type,
    count(a.ed_pophealth_id) as qa
    from (
      select ed_pophealth_id, count(distinct id_apcd) as id_dcount
      from stg_claims.stage_apcd_claim_header
      group by ed_pophealth_id
    ) as a
    where a.id_dcount > 1;",
    .con = dw_inthealth))
  
  #verify that no inpatient_id value is used for more than one person
  res8b <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of inpatient_id values used for >1 person, expect 0' as qa_type,
    count(a.inpatient_id) as qa
    from (
      select inpatient_id, count(distinct id_apcd) as id_dcount
      from stg_claims.stage_apcd_claim_header
      group by inpatient_id
    ) as a
    where a.id_dcount > 1;",
    .con = dw_inthealth))
  
  #verify that no ed_perform_id value is used for more than one person
  res8c <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of ed_perform_id values used for >1 person, expect 0' as qa_type,
    count(a.ed_perform_id) as qa
    from (
      select ed_perform_id, count(distinct id_apcd) as id_dcount
      from stg_claims.stage_apcd_claim_header
      group by ed_perform_id
    ) as a
    where a.id_dcount > 1;",
    .con = dw_inthealth))
  
  #verify that no pc_visit_id value is used for more than one person
  res8d <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of pc_visit_id values used for >1 person, expect 0' as qa_type,
    count(a.pc_visit_id) as qa
    from (
      select pc_visit_id, count(distinct id_apcd) as id_dcount
      from stg_claims.stage_apcd_claim_header
      group by pc_visit_id
    ) as a
    where a.id_dcount > 1;",
    .con = dw_inthealth))
  
  #verify that ed_pophealth_id does not skip any values
  res9a <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of distinct ed_pophealth_id values' as qa_type,
    count(distinct ed_pophealth_id) as qa
    from stg_claims.stage_apcd_claim_header;",
    .con = dw_inthealth))
  
  res9b <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', 'max ed_pophealth_id - min + 1' as qa_type,
    cast(max(ed_pophealth_id) - min(ed_pophealth_id) + 1 as int) as qa
    from stg_claims.stage_apcd_claim_header;",
    .con = dw_inthealth))
  
  #verify that 1-day overlap window was implemented correctly with ed_pophealth_id
  res10 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "with cte as
    (
    select * 
    ,lag(ed_pophealth_id) over(partition by id_apcd, ed_pophealth_id order by first_service_date) as lag_ed_pophealth_id
    ,lag(first_service_date) over(partition by id_apcd, ed_pophealth_id order by first_service_date) as lag_first_service_date
    from stg_claims.stage_apcd_claim_header
    where [ed_pophealth_id] is not null
    )
    select 'stg_claims.stage_apcd_claim_header' as 'table', '# of ed_pophealth visits where the overlap date is greater than 1 day, expect 0' as 'qa_type',
      count(*) as qa
    from stg_claims.stage_apcd_claim_header
    where [ed_pophealth_id] in (select ed_pophealth_id from cte where abs(datediff(day, lag_first_service_date, first_service_date)) > 1);",
    .con = dw_inthealth))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}