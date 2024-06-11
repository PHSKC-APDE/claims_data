#### CODE TO LOAD & TABLE-LEVEL QA stg_claims.stage_mcare_claim_header
# Eli Kern, PHSKC (APDE)
#
# 2019-12
#
#2024-05-15 Eli update: Data from HCA, ETL in inthealth_edw

### Run from 02_master_mcare_claims_analytic.R script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/02_master_mcare_claims_analytic.R

#### Load script ####
load_stage.mcare_claim_header_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "--Code to create stg_claims.stage_mcare_claim_header table
    --Distinct header-level claim variables (e.g. claim type). In other words elements for which there is only one distinct value per claim header.
    --Eli Kern (PHSKC-APDE)
    --2024-06
    
    ------------------
    --STEP 1: Union all claim types to grab header-level concepts not currently in other analytic tables
    --Exclude all denied claims and claims among people with no enrollment data
    --Acute inpatient stay defined as NCH claim type 60
    --Max of discharge date, min of admission and hospice_from_date
    -------------------
    
    --create table shell
    if object_id(N'stg_claims.tmp_mcare_claim_header_temp1',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_temp1;
    create table stg_claims.tmp_mcare_claim_header_temp1 (
    id_mcare varchar(255) collate SQL_Latin1_General_Cp1_CS_AS,
    claim_header_id varchar(255) collate SQL_Latin1_General_Cp1_CS_AS,
    first_service_date date,
    last_service_date date,
    claim_type_mcare_id varchar(255),
    claim_type_id tinyint,
    filetype_mcare varchar(255),
    facility_type_code tinyint,
    service_type_code tinyint,
    patient_status varchar(255),
    patient_status_code varchar(255),
    inpatient_flag tinyint,
    admission_date date,
    discharge_date date,
    ipt_admission_type tinyint,
    ipt_admission_source varchar(255),
    drg_code varchar(255),
    hospice_from_date date,
    submitted_charges numeric(19,2),
    total_paid_mcare numeric(19,2),
    total_paid_insurance numeric(19,2),
    total_paid_bene numeric(19,2),
    total_cost_of_care numeric(19,2)
    )
    with (heap);
    
    --insert data after union of all claim types
    insert into stg_claims.tmp_mcare_claim_header_temp1
    select
    a.id_mcare,
    a.claim_header_id,
    a.first_service_date,
    a.last_service_date,
    a.claim_type_mcare_id,
    b.kc_clm_type_id as claim_type_id,
    a.filetype_mcare,
    a.facility_type_code,
    a.service_type_code,
    a.patient_status,
    a.patient_status_code,
    case when a.claim_type_mcare_id = '60' and discharge_date is not null then 1 else 0 end as inpatient_flag,
    cast(min(a.admission_date) over(partition by a.claim_header_id) as date) as admission_date,
    cast(max(a.discharge_date) over(partition by a.claim_header_id) as date) as discharge_date,
    a.ipt_admission_type,
    a.ipt_admission_source,
    a.drg_code,
    cast(min(a.hospice_from_date) over(partition by a.claim_header_id) as date) as hospice_from_date,
    a.submitted_charges,
    a.total_paid_mcare,
    a.total_paid_insurance,
    a.total_paid_bene,
    a.total_cost_of_care
    
    --union all claim types
    from (
    	--bcarrier
    	select
    	--top 100 --testing code
    	trim(bene_id) as id_mcare,
    	trim(clm_id) as claim_header_id,
    	cast(clm_from_dt as date) as first_service_date,
    	cast(clm_thru_dt as date) as last_service_date,
    	nch_clm_type_cd as claim_type_mcare_id,
    	'carrier' as filetype_mcare,
    	facility_type_code = null,
    	service_type_code = null,
    	patient_status = null,
    	patient_status_code = null,
    	admission_date = null,
    	discharge_date = null,
    	ipt_admission_type = null,
    	ipt_admission_source = null,
    	drg_code = null,
    	hospice_from_date = null,
    	cast(nch_carr_clm_sbmtd_chrg_amt as numeric(19,2)) as submitted_charges,
    	cast(clm_pmt_amt as numeric(19,2)) as total_paid_mcare,
    	cast(clm_pmt_amt as numeric(19,2))
    		+ cast(carr_clm_prmry_pyr_pd_amt as numeric(19,2))
    	as total_paid_insurance,
    	cast(clm_bene_pd_amt as numeric(19,2))
    		- cast(nch_clm_bene_pmt_amt as numeric(19,2))
    		+ cast(carr_clm_cash_ddctbl_apld_amt as numeric(19,2))
    	as total_paid_bene,
    	cast(clm_pmt_amt as numeric(19,2))
    		+ cast(carr_clm_prmry_pyr_pd_amt as numeric(19,2))
    		+ cast(carr_clm_cash_ddctbl_apld_amt as numeric(19,2))
    		+ cast(clm_bene_pd_amt as numeric(19,2))
    		- cast(nch_clm_bene_pmt_amt as numeric(19,2))
    	as total_cost_of_care
    	from stg_claims.mcare_bcarrier_claims
    	where carr_clm_pmt_dnl_cd in ('1','2','3','4','5','6','7','8','9')
    
    	--dme
    	union select
    	--top 100 --testing code
    	trim(bene_id) as id_mcare,
    	trim(clm_id) as claim_header_id,
    	cast(clm_from_dt as date) as first_service_date,
    	cast(clm_thru_dt as date) as last_service_date,
    	nch_clm_type_cd as claim_type_mcare_id,
    	'dme' as filetype_mcare,
    	facility_type_code = null,
    	service_type_code = null,
    	patient_status = null,
    	patient_status_code = null,
    	admission_date = null,
    	discharge_date = null,
    	ipt_admission_type = null,
    	ipt_admission_source = null,
    	drg_code = null,
    	hospice_from_date = null,
    	cast(nch_carr_clm_sbmtd_chrg_amt as numeric(19,2)) as submitted_charges,
    	cast(clm_pmt_amt as numeric(19,2)) as total_paid_mcare,
    	cast(clm_pmt_amt as numeric(19,2))
    		+ cast(carr_clm_prmry_pyr_pd_amt as numeric(19,2))
    	as total_paid_insurance,
    	cast(clm_bene_pd_amt as numeric(19,2))
    		- cast(nch_clm_bene_pmt_amt as numeric(19,2))
    		+ cast(carr_clm_cash_ddctbl_apld_amt as numeric(19,2))
    	as total_paid_bene,
    	cast(clm_pmt_amt as numeric(19,2))
    		+ cast(carr_clm_prmry_pyr_pd_amt as numeric(19,2))
    		+ cast(carr_clm_cash_ddctbl_apld_amt as numeric(19,2))
    		+ cast(clm_bene_pd_amt as numeric(19,2))
    		- cast(nch_clm_bene_pmt_amt as numeric(19,2))
    	as total_cost_of_care
    	from stg_claims.mcare_dme_claims
    	where carr_clm_pmt_dnl_cd in ('1','2','3','4','5','6','7','8','9')
    
    	--hha
    	union select
    	--top 100 --testing code
    	trim(bene_id) as id_mcare,
    	trim(clm_id) as claim_header_id,
    	cast(clm_from_dt as date) as first_service_date,
    	cast(clm_thru_dt as date) as last_service_date,
    	nch_clm_type_cd as claim_type_mcare_id,
    	'hha' as filetype_mcare,
    	clm_fac_type_cd as facility_type_code,
    	clm_srvc_clsfctn_type_cd as service_type_code,
    	patient_status = null,
    	ptnt_dschrg_stus_cd  as patient_status_code,
    	clm_admsn_dt as admission_date,
    	nch_bene_dschrg_dt as discharge_date,
    	ipt_admission_type = null,
    	ipt_admission_source = null,
    	drg_code = null,
    	hospice_from_date = null,
    	cast(clm_tot_chrg_amt as numeric(19,2)) as submitted_charges,
    	cast(clm_pmt_amt as numeric(19,2)) as total_paid_mcare,
    	cast(clm_pmt_amt as numeric(19,2))
    		+ cast(nch_prmry_pyr_clm_pd_amt as numeric(19,2))
    	as total_paid_insurance,
    	cast(0 as numeric(19,2)) as total_paid_bene,
    	cast(clm_pmt_amt as numeric(19,2))
    		+ cast(nch_prmry_pyr_clm_pd_amt as numeric(19,2))
    	as total_cost_of_care
    	from stg_claims.mcare_hha_base_claims
    	where (clm_mdcr_non_pmt_rsn_cd = '' or clm_mdcr_non_pmt_rsn_cd is null)
    
    	--hospice
    	union select
    	--top 100 --testing code
    	trim(bene_id) as id_mcare,
    	trim(clm_id) as claim_header_id,
    	cast(clm_from_dt as date) as first_service_date,
    	cast(clm_thru_dt as date) as last_service_date,
    	nch_clm_type_cd as claim_type_mcare_id,
    	'hospice' as filetype_mcare,
    	clm_fac_type_cd as facility_type_code,
    	clm_srvc_clsfctn_type_cd as service_type_code,
    	nch_ptnt_status_ind_cd as patient_status,
    	ptnt_dschrg_stus_cd  as patient_status_code,
    	admission_date = null,
    	nch_bene_dschrg_dt as discharge_date,
    	ipt_admission_type = null,
    	ipt_admission_source = null,
    	drg_code = null,
    	clm_hospc_start_dt_id as hospice_from_date,
    	cast(clm_tot_chrg_amt as numeric(19,2)) as submitted_charges,
    	cast(clm_pmt_amt as numeric(19,2)) as total_paid_mcare,
    	cast(clm_pmt_amt as numeric(19,2))
    		+ cast(nch_prmry_pyr_clm_pd_amt as numeric(19,2))
    	as total_paid_insurance,
    	cast(0 as numeric(19,2)) as total_paid_bene,
    	cast(clm_pmt_amt as numeric(19,2))
    		+ cast(nch_prmry_pyr_clm_pd_amt as numeric(19,2))
    	as total_cost_of_care
    	from stg_claims.mcare_hospice_base_claims
    	where (clm_mdcr_non_pmt_rsn_cd = '' or clm_mdcr_non_pmt_rsn_cd is null)
    
    	--inpatient
    	union select
    	--top 100 --testing code
    	trim(bene_id) as id_mcare,
    	trim(clm_id) as claim_header_id,
    	cast(clm_from_dt as date) as first_service_date,
    	cast(clm_thru_dt as date) as last_service_date,
    	nch_clm_type_cd as claim_type_mcare_id,
    	'inpatient' as filetype_mcare,
    	clm_fac_type_cd as facility_type_code,
    	clm_srvc_clsfctn_type_cd as service_type_code,
    	nch_ptnt_status_ind_cd as patient_status,
    	ptnt_dschrg_stus_cd  as patient_status_code,
    	clm_admsn_dt as admission_date,
    	nch_bene_dschrg_dt as discharge_date,
    	clm_ip_admsn_type_cd as ipt_admission_type ,
    	clm_src_ip_admsn_cd as ipt_admission_source,
    	clm_drg_cd as drg_code,
    	hospice_from_date = null,
    	cast(clm_tot_chrg_amt as numeric(19,2)) as submitted_charges,
    	cast(cast(clm_pmt_amt as numeric(19,2))
    		+ (cast(clm_pass_thru_per_diem_amt as numeric(19,2)) * cast(clm_utlztn_day_cnt as numeric(19,2)))
    		as numeric(19,2))
    	as total_paid_mcare,
    	cast(cast(clm_pmt_amt as numeric(19,2))
    		+ (cast(clm_pass_thru_per_diem_amt as numeric(19,2)) * cast(clm_utlztn_day_cnt as numeric(19,2)))
    		+ cast(nch_prmry_pyr_clm_pd_amt as numeric(19,2))
    		as numeric(19,2))
    	as total_paid_insurance,
    	cast(nch_ip_tot_ddctn_amt as numeric(19,2)) as total_paid_bene,
    	cast(cast(clm_pmt_amt as numeric(19,2))
    		+ (cast(clm_pass_thru_per_diem_amt as numeric(19,2)) * cast(clm_utlztn_day_cnt as numeric(19,2)))
    		+ cast(nch_prmry_pyr_clm_pd_amt as numeric(19,2))
    		+ cast(nch_ip_tot_ddctn_amt as numeric(19,2))
    		as numeric(19,2))
    	as total_cost_of_care
    	from stg_claims.mcare_inpatient_base_claims
    	where (clm_mdcr_non_pmt_rsn_cd = '' or clm_mdcr_non_pmt_rsn_cd is null)
    
    	--inpatient data structure j
    	union select
    	--top 100 --testing code
    	trim(bene_id) as id_mcare,
    	trim(clm_id) as claim_header_id,
    	cast(clm_from_dt as date) as first_service_date,
    	cast(clm_thru_dt as date) as last_service_date,
    	nch_clm_type_cd as claim_type_mcare_id,
    	'inpatient' as filetype_mcare,
    	clm_fac_type_cd as facility_type_code,
    	clm_srvc_clsfctn_type_cd as service_type_code,
    	nch_ptnt_status_ind_cd as patient_status,
    	ptnt_dschrg_stus_cd  as patient_status_code,
    	clm_admsn_dt as admission_date,
    	nch_bene_dschrg_dt as discharge_date,
    	clm_ip_admsn_type_cd as ipt_admission_type ,
    	clm_src_ip_admsn_cd as ipt_admission_source,
    	clm_drg_cd as drg_code,
    	hospice_from_date = null,
    	cast(clm_tot_chrg_amt as numeric(19,2)) as submitted_charges,
    	cast(cast(clm_pmt_amt as numeric(19,2))
    		+ (cast(clm_pass_thru_per_diem_amt as numeric(19,2)) * cast(clm_utlztn_day_cnt as numeric(19,2)))
    		as numeric(19,2))
    	as total_paid_mcare,
    	cast(cast(clm_pmt_amt as numeric(19,2))
    		+ (cast(clm_pass_thru_per_diem_amt as numeric(19,2)) * cast(clm_utlztn_day_cnt as numeric(19,2)))
    		+ cast(nch_prmry_pyr_clm_pd_amt as numeric(19,2))
    		as numeric(19,2))
    	as total_paid_insurance,
    	cast(nch_ip_tot_ddctn_amt as numeric(19,2)) as total_paid_bene,
    	cast(cast(clm_pmt_amt as numeric(19,2))
    		+ (cast(clm_pass_thru_per_diem_amt as numeric(19,2)) * cast(clm_utlztn_day_cnt as numeric(19,2)))
    		+ cast(nch_prmry_pyr_clm_pd_amt as numeric(19,2))
    		+ cast(nch_ip_tot_ddctn_amt as numeric(19,2))
    		as numeric(19,2))
    	as total_cost_of_care
    	from stg_claims.mcare_inpatient_base_claims_j
    	where (clm_mdcr_non_pmt_rsn_cd = '' or clm_mdcr_non_pmt_rsn_cd is null)
    
    	--outpatient
    	union select
    	--top 100 --testing code
    	trim(bene_id) as id_mcare,
    	trim(clm_id) as claim_header_id,
    	cast(clm_from_dt as date) as first_service_date,
    	cast(clm_thru_dt as date) as last_service_date,
    	nch_clm_type_cd as claim_type_mcare_id,
    	'outpatient' as filetype_mcare,
    	clm_fac_type_cd as facility_type_code,
    	clm_srvc_clsfctn_type_cd as service_type_code,
    	patient_status = null,
    	patient_status_code = null,
    	admission_date = null,
    	discharge_date = null,
    	ipt_admission_type = null,
    	ipt_admission_source = null,
    	drg_code = null,
    	hospice_from_date = null,
    	cast(clm_tot_chrg_amt as numeric(19,2)) as submitted_charges,
    	cast(clm_pmt_amt as numeric(19,2)) as total_paid_mcare,
    	cast(clm_pmt_amt as numeric(19,2))
    		+ cast(nch_prmry_pyr_clm_pd_amt as numeric(19,2))
    	as total_paid_insurance,
    	cast(nch_bene_ptb_ddctbl_amt as numeric(19,2))
    		+ cast(nch_bene_ptb_coinsrnc_amt as numeric(19,2))
    		+ cast(nch_bene_blood_ddctbl_lblty_am as numeric(19,2))
    	as total_paid_bene,
    	cast(clm_pmt_amt as numeric(19,2))
    		+ cast(nch_prmry_pyr_clm_pd_amt as numeric(19,2))
    		+ cast(nch_bene_ptb_ddctbl_amt as numeric(19,2))
    		+ cast(nch_bene_ptb_coinsrnc_amt as numeric(19,2))
    		+ cast(nch_bene_blood_ddctbl_lblty_am as numeric(19,2))
    	as total_cost_of_care
    	from stg_claims.mcare_outpatient_base_claims
    	where (clm_mdcr_non_pmt_rsn_cd = '' or clm_mdcr_non_pmt_rsn_cd is null)
    
    	--outpatient data structure j
    	union select
    	--top 100 --testing code
    	trim(bene_id) as id_mcare,
    	trim(clm_id) as claim_header_id,
    	cast(clm_from_dt as date) as first_service_date,
    	cast(clm_thru_dt as date) as last_service_date,
    	nch_clm_type_cd as claim_type_mcare_id,
    	'outpatient' as filetype_mcare,
    	clm_fac_type_cd as facility_type_code,
    	clm_srvc_clsfctn_type_cd as service_type_code,
    	patient_status = null,
    	patient_status_code = null,
    	admission_date = null,
    	discharge_date = null,
    	ipt_admission_type = null,
    	ipt_admission_source = null,
    	drg_code = null,
    	hospice_from_date = null,
    	cast(clm_tot_chrg_amt as numeric(19,2)) as submitted_charges,
    	cast(clm_pmt_amt as numeric(19,2)) as total_paid_mcare,
    	cast(clm_pmt_amt as numeric(19,2))
    		+ cast(nch_prmry_pyr_clm_pd_amt as numeric(19,2))
    	as total_paid_insurance,
    	cast(nch_bene_ptb_ddctbl_amt as numeric(19,2))
    		+ cast(nch_bene_ptb_coinsrnc_amt as numeric(19,2))
    		+ cast(nch_bene_blood_ddctbl_lblty_am as numeric(19,2))
    	as total_paid_bene,
    	cast(clm_pmt_amt as numeric(19,2))
    		+ cast(nch_prmry_pyr_clm_pd_amt as numeric(19,2))
    		+ cast(nch_bene_ptb_ddctbl_amt as numeric(19,2))
    		+ cast(nch_bene_ptb_coinsrnc_amt as numeric(19,2))
    		+ cast(nch_bene_blood_ddctbl_lblty_am as numeric(19,2))
    	as total_cost_of_care
    	from stg_claims.mcare_outpatient_base_claims_j
    	where (clm_mdcr_non_pmt_rsn_cd = '' or clm_mdcr_non_pmt_rsn_cd is null)
    
    	--snf
    	union select
    	--top 100 --testing code
    	trim(bene_id) as id_mcare,
    	trim(clm_id) as claim_header_id,
    	cast(clm_from_dt as date) as first_service_date,
    	cast(clm_thru_dt as date) as last_service_date,
    	nch_clm_type_cd as claim_type_mcare_id,
    	'snf' as filetype_mcare,
    	clm_fac_type_cd as facility_type_code,
    	clm_srvc_clsfctn_type_cd as service_type_code,
    	nch_ptnt_status_ind_cd as patient_status,
    	ptnt_dschrg_stus_cd  as patient_status_code,
    	clm_admsn_dt as admission_date,
    	nch_bene_dschrg_dt as discharge_date,
    	clm_ip_admsn_type_cd as ipt_admission_type ,
    	clm_src_ip_admsn_cd as ipt_admission_source,
    	clm_drg_cd as drg_code,
    	hospice_from_date = null,
    	cast(clm_tot_chrg_amt as numeric(19,2)) as submitted_charges,
    	cast(clm_pmt_amt as numeric(19,2)) as total_paid_mcare,
    	cast(clm_pmt_amt as numeric(19,2))
    		+ cast(nch_prmry_pyr_clm_pd_amt as numeric(19,2))
    	as total_paid_insurance,
    	cast(nch_ip_tot_ddctn_amt as numeric(19,2)) as total_paid_bene,
    	cast(clm_pmt_amt as numeric(19,2))
    		+ cast(nch_prmry_pyr_clm_pd_amt as numeric(19,2))
    		+ cast(nch_ip_tot_ddctn_amt as numeric(19,2))
    	as total_cost_of_care
    	from stg_claims.mcare_snf_base_claims
    	where (clm_mdcr_non_pmt_rsn_cd = '' or clm_mdcr_non_pmt_rsn_cd is null)
    ) as a
    
    --add in KC claim type
    left join (select * from stg_claims.ref_kc_claim_type_crosswalk where source_desc = 'mcare') as b
    on a.claim_type_mcare_id = b.source_clm_type_id
    
    --exclude claims among people who have no eligibility data
    left join stg_claims.final_mcare_elig_demo as c
    on a.id_mcare = c.id_mcare
    where c.id_mcare is not null
    option (label = 'mcare_claim_header_temp1');
    
    
    ------------------
    --STEP 2: Do all line-level transformations
    -------------------
    
    --create table shell
    if object_id(N'stg_claims.tmp_mcare_claim_header_line',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_line;
    create table stg_claims.tmp_mcare_claim_header_line (
    claim_header_id varchar(255) collate SQL_Latin1_General_Cp1_CS_AS,
    ed_pos tinyint,
    ed_rev_code_perform tinyint,
    ed_rev_code_pophealth tinyint
    )
    with (heap);
    
    --insert data
    insert into stg_claims.tmp_mcare_claim_header_line
    select
    --top 100 --testing code
    claim_header_id,
    --ED place of service flag
    max(case when place_of_service_code = '23' then 1 else 0 end) as ed_pos,
    --ED performance temp flags (RDA measure)
    max(case when revenue_code like '045[01269]' then 1 else 0 end) as ed_rev_code_perform,
    --ED population health temp flags (Yale measure)
    max(case when revenue_code like '045[01269]' or revenue_code = '0981' then 1 else 0 end) as ed_rev_code_pophealth
    from stg_claims.final_mcare_claim_line
    --grouping statement for consolidation to claim header level
    group by claim_header_id
    option (label = 'tmp_mcare_claim_header_line');
    
    
    ------------------
    --STEP 3: Procedure code query for ED visits
    --Subset to relevant claims as last step to minimize table size
    -------------------
    --create table shell
    if object_id(N'stg_claims.tmp_mcare_claim_header_pcode',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_pcode;
    create table stg_claims.tmp_mcare_claim_header_pcode (
    claim_header_id varchar(255) collate SQL_Latin1_General_Cp1_CS_AS,
    ed_procedure_code_perform tinyint,
    ed_procedure_code_pophealth tinyint
    )
    with (heap);
    
    --insert data
    insert into stg_claims.tmp_mcare_claim_header_pcode
    select a.claim_header_id,
        a.ed_procedure_code_perform, a.ed_procedure_code_pophealth
    from (
    select
    --top 100 --testing code
    claim_header_id,
    max(case when procedure_code like '9928[123458]' then 1 else 0 end) as ed_procedure_code_perform,
    max(case when procedure_code like '9928[12345]' or procedure_code = '99291' then 1 else 0 end) as ed_procedure_code_pophealth
    from stg_claims.final_mcare_claim_procedure
    group by claim_header_id
    ) as a
    where a.ed_procedure_code_perform = 1 or a.ed_procedure_code_pophealth = 1
    option (label = 'tmp_mcare_claim_header_pcode');
    
    
    ------------------
    --STEP 4: Primary care visit query
    -------------------
    
    --create table shell
    if object_id(N'stg_claims.tmp_mcare_claim_header_pc_visit',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_pc_visit;
    create table stg_claims.tmp_mcare_claim_header_pc_visit (
    claim_header_id varchar(255) collate SQL_Latin1_General_Cp1_CS_AS,
    pc_procedure_temp tinyint,
    pc_taxonomy_temp tinyint,
    pc_zcode_temp tinyint
    )
    with (heap);
    
    --insert data
    insert into stg_claims.tmp_mcare_claim_header_pc_visit
    select
    x.claim_header_id,
    x.pc_procedure_temp,
    x.pc_taxonomy_temp,
    x.pc_zcode_temp
    from (
    	select
    	--top 100 --testing code
    	a.claim_header_id,
    	--primary care visit temp flags
    	max(case when a.code is not null then 1 else 0 end) as pc_procedure_temp,
    	max(case when b.code is not null then 1 else 0 end) as pc_zcode_temp,
    	max(case when c.code is not null then 1 else 0 end) as pc_taxonomy_temp
        
    	--procedure codes
    	from (
    		select a1.id_mcare, a1.claim_header_id, a2.code
    		--procedure code table
    		from stg_claims.final_mcare_claim_procedure as a1
    		--primary care-relevant procedure codes
    		inner join (select code from stg_claims.ref_pc_visit_oregon where code_system in ('cpt', 'hcpcs')) as a2
    		on a1.procedure_code = a2.code
    	) as a
        
    	--ICD-CM codes
    	left join (
    		select b1.claim_header_id, b2.code
    		--ICD-CM table
    		from stg_claims.final_mcare_claim_icdcm_header as b1
    		--primary care-relevant ICD-10-CM codes
    		inner join (select code from stg_claims.ref_pc_visit_oregon where code_system = 'icd10cm') as b2
    		on (b1.icdcm_norm = b2.code) and (b1.icdcm_version = 10)
    	) as b
    	on a.claim_header_id = b.claim_header_id
        
    	--provider taxonomies
    	left join (
    		select c1.claim_header_id, c3.code
    		--rendering and attending providers
    		from (select * from stg_claims.final_mcare_claim_provider where provider_type in ('rendering', 'attending')) as c1
    		--taxonomy codes for rendering and attending providers
    		inner join stg_claims.ref_kc_provider_master as c2
    		on c1.provider_npi = c2.npi
    		--primary care-relevant provider taxonomy codes
    		inner join (select code from stg_claims.ref_pc_visit_oregon where code_system = 'provider_taxonomy') as c3
    		on (c2.primary_taxonomy = c3.code) or (c2.secondary_taxonomy = c3.code)
    	) as c
    	on a.claim_header_id = c.claim_header_id
    	--cluster to claim header
    	group by a.claim_header_id
    ) as x
    where (x.pc_procedure_temp = 1 or x.pc_zcode_temp = 1) and x.pc_taxonomy_temp = 1
    option (label = 'tmp_mcare_claim_header_pc_visit');
    
    
    ------------------
    --STEP 5: Extract primary diagnosis, take first ordered ICD-CM code when >1 primary per header
    ------------------
    
    --create table shell
    if object_id(N'stg_claims.tmp_mcare_claim_header_icd1',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_icd1;
    create table stg_claims.tmp_mcare_claim_header_icd1 (
    claim_header_id varchar(255) collate SQL_Latin1_General_Cp1_CS_AS,
    primary_diagnosis varchar(255),
    icdcm_version tinyint
    )
    with (heap);
    
    --insert data
    insert into stg_claims.tmp_mcare_claim_header_icd1
    select
    --top 100 --testing code
    claim_header_id,
    min(icdcm_norm) as primary_diagnosis,
    min(icdcm_version) as icdcm_version
    from stg_claims.final_mcare_claim_icdcm_header
    where icdcm_number = '01'
    group by claim_header_id
    option (label = 'tmp_mcare_claim_header_icd1');
    
    
    ------------------
    --STEP 6: Prepare header-level concepts using analytic claim tables
    --Add in principal diagnosis
    -------------------
    
    --create table shell
    if object_id(N'stg_claims.tmp_mcare_claim_header_temp2',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_temp2;
    create table stg_claims.tmp_mcare_claim_header_temp2 (
    id_mcare varchar(255) collate SQL_Latin1_General_Cp1_CS_AS,
    claim_header_id varchar(255) collate SQL_Latin1_General_Cp1_CS_AS,
    first_service_date date,
    last_service_date date,
    primary_diagnosis varchar(255),
    icdcm_version tinyint,
    claim_type_mcare_id varchar(255),
    claim_type_id tinyint,
    filetype_mcare varchar(255),
    facility_type_code tinyint,
    service_type_code tinyint,
    patient_status varchar(255),
    patient_status_code varchar(255),
    inpatient_flag tinyint,
    admission_date date,
    discharge_date date,
    ipt_admission_type tinyint,
    ipt_admission_source varchar(255),
    drg_code varchar(255),
    hospice_from_date date,
    submitted_charges numeric(19,2),
    total_paid_mcare numeric(19,2),
    total_paid_insurance numeric(19,2),
    total_paid_bene numeric(19,2),
    total_cost_of_care numeric(19,2),
    ed_perform tinyint,
    ed_yale_carrier tinyint,
    ed_yale_opt tinyint,
    ed_yale_ipt tinyint,
    pc_visit tinyint
    )
    with (heap);
    
    --insert data
    insert into stg_claims.tmp_mcare_claim_header_temp2
    
    select distinct a.id_mcare, 
    a.claim_header_id,
    a.first_service_date,
    a.last_service_date,
    b.primary_diagnosis,
    b.icdcm_version,
    a.claim_type_mcare_id,
    a.claim_type_id,
    a.filetype_mcare,
    a.facility_type_code,
    a.service_type_code,
    a.patient_status,
    a.patient_status_code,
    a.inpatient_flag,
    a.admission_date,
    a.discharge_date,
    a.ipt_admission_type,
    a.ipt_admission_source,
    a.drg_code,
    a.hospice_from_date,
    a.submitted_charges,
    a.total_paid_mcare,
    a.total_paid_insurance,
    a.total_paid_bene,
    a.total_cost_of_care,
    
    --ED performance (RDA measure)
    case when a.claim_type_id = 4 and
        (d.ed_rev_code_perform = 1 or e.ed_procedure_code_perform = 1 or d.ed_pos = 1)
    then 1 else 0 end as ed_perform,
        
    --ED population health (Yale measure)
    case when a.claim_type_id = 5 and 
        ((e.ed_procedure_code_pophealth = 1 and d.ed_pos = 1) or d.ed_rev_code_pophealth = 1)
        then 1 else 0 end as ed_yale_carrier,
    case when a.claim_type_id = 4 and 
        (d.ed_rev_code_pophealth = 1 or d.ed_pos = 1 or e.ed_procedure_code_pophealth = 1)
        then 1 else 0 end as ed_yale_opt,
    case when a.claim_type_id = 1 and
        (d.ed_rev_code_pophealth = 1 or d.ed_pos = 1 or e.ed_procedure_code_pophealth = 1)
        then 1 else 0 end as ed_yale_ipt,
        
    --Primary care visit (Oregon)
    case when (f.pc_procedure_temp = 1 or f.pc_zcode_temp = 1) and f.pc_taxonomy_temp = 1
        and a.claim_type_mcare_id not in ('60', '30') --exclude inpatient, swing bed SNF
        then 1 else 0
    end as pc_visit
        
    from stg_claims.tmp_mcare_claim_header_temp1 as a
    left join stg_claims.tmp_mcare_claim_header_icd1 as b
    on a.claim_header_id = b.claim_header_id
    left join stg_claims.tmp_mcare_claim_header_line as d
    on a.claim_header_id = d.claim_header_id
    left join stg_claims.tmp_mcare_claim_header_pcode as e
    on a.claim_header_id = e.claim_header_id
    left join stg_claims.tmp_mcare_claim_header_pc_visit as f
    on a.claim_header_id = f.claim_header_id
    option (label = 'tmp_mcare_claim_header_temp2');
    
    --drop intermediate tables to make space
    if object_id(N'stg_claims.tmp_mcare_claim_header_temp1',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_temp1;
    if object_id(N'stg_claims.tmp_mcare_claim_header_line',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_line;
    if object_id(N'stg_claims.tmp_mcare_claim_header_icd1',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_icd1;
    if object_id(N'stg_claims.tmp_mcare_claim_header_pcode',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_pcode;
    if object_id(N'stg_claims.tmp_mcare_claim_header_pc_visit',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_pc_visit;
    
    
    ------------------
    --STEP 7: Assign unique ID to healthcare utilization concepts that are grouped by person, service date
    -------------------
    
    --create table shell
    if object_id(N'stg_claims.tmp_mcare_claim_header_temp3',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_temp3;
    create table stg_claims.tmp_mcare_claim_header_temp3 (
    id_mcare varchar(255) collate SQL_Latin1_General_Cp1_CS_AS,
    claim_header_id varchar(255) collate SQL_Latin1_General_Cp1_CS_AS,
    first_service_date date,
    last_service_date date,
    primary_diagnosis varchar(255),
    icdcm_version tinyint,
    claim_type_mcare_id varchar(255),
    claim_type_id tinyint,
    filetype_mcare varchar(255),
    facility_type_code tinyint,
    service_type_code tinyint,
    patient_status varchar(255),
    patient_status_code varchar(255),
    admission_date date,
    discharge_date date,
    ipt_admission_type tinyint,
    ipt_admission_source varchar(255),
    drg_code varchar(255),
    hospice_from_date date,
    submitted_charges numeric(19,2),
    total_paid_mcare numeric(19,2),
    total_paid_insurance numeric(19,2),
    total_paid_bene numeric(19,2),
    total_cost_of_care numeric(19,2),
    ed_yale_carrier tinyint,
    ed_yale_opt tinyint,
    ed_yale_ipt tinyint,
    pc_visit tinyint,
    pc_visit_id bigint,
    inpatient tinyint,
    inpatient_id bigint,
    ed_perform tinyint,
    ed_perform_id bigint
    )
    with (heap);
    
    --insert data
    insert into stg_claims.tmp_mcare_claim_header_temp3
    select 
    id_mcare, 
    claim_header_id,
    first_service_date,
    last_service_date,
    primary_diagnosis,
    icdcm_version,
    claim_type_mcare_id,
    claim_type_id,
    filetype_mcare,
    facility_type_code,
    service_type_code,
    patient_status,
    patient_status_code,
    admission_date,
    discharge_date,
    ipt_admission_type,
    ipt_admission_source,
    drg_code,
    hospice_from_date,
    submitted_charges,
    total_paid_mcare,
    total_paid_insurance,
    total_paid_bene,
    total_cost_of_care,
    ed_yale_carrier,
    ed_yale_opt,
    ed_yale_ipt,
        
    --primary care visits
    pc_visit,
    case when pc_visit = 0 then null
    else dense_rank() over
        (order by case when pc_visit = 0 then 2 else 1 end, --sorts non-relevant claims to bottom
        id_mcare, first_service_date)
    end as pc_visit_id,
        
    --inpatient stays
    inpatient_flag as inpatient,
    case when inpatient_flag = 0 then null
    else dense_rank() over
        (order by case when inpatient_flag = 0 then 2 else 1 end, --sorts non-relevant claims to bottom
        id_mcare, discharge_date)
    end as inpatient_id,
        
    --ED performance (RDA measure)
    ed_perform,
    case when ed_perform = 0 then null
    else dense_rank() over
        (order by case when ed_perform = 0 then 2 else 1 end, --sorts non-relevant claims to bottom
        id_mcare, first_service_date)
    end as ed_perform_id
    from stg_claims.tmp_mcare_claim_header_temp2
    option (label = 'tmp_mcare_claim_header_temp3');
        
    --drop other temp tables to make space
    if object_id(N'stg_claims.tmp_mcare_claim_header_temp2',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_temp2;
    
    
    ------------------
    --STEP 8: Conduct overlap and clustering for ED population health measure (Yale measure)
    --Adaptation of Philip's Medicaid code, which is adaptation of Eli's original code
    -------------------
    
    --create table shell
    if object_id(N'stg_claims.tmp_mcare_claim_header_ed_pophealth',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_ed_pophealth;
    create table stg_claims.tmp_mcare_claim_header_ed_pophealth (
	    claim_header_id varchar(255) collate SQL_Latin1_General_Cp1_CS_AS,
    	ed_pophealth_id bigint
    )
    with (heap);
    
    --Set date of service matching window
    declare @match_window int;
    set @match_window = 1;
        
    --insert data
    with increment_stays_by_person as
    (
      select
      id_mcare,
      claim_header_id,
      first_service_date,
      last_service_date,
      --create chronological (0, 1) indicator column.
      --if 0, it is the first ED visit for the person OR the ED visit appears to be a duplicate (overlapping service dates) of the prior visit.
      --if 1, the prior ED visit appears to be distinct from the following stay.
      --this indicator column will be summed to create an episode_id.
      case
        when row_number() over(partition by id_mcare order by first_service_date, last_service_date, claim_header_id) = 1 then 0
        when datediff(day, lag(first_service_date) over(partition by id_mcare
          order by first_service_date, last_service_date, claim_header_id), first_service_date) <= @match_window then 0
        when datediff(day, lag(first_service_date) over(partition by id_mcare
          order by first_service_date, last_service_date, claim_header_id), first_service_date) > @match_window then 1
      end as increment
      from stg_claims.tmp_mcare_claim_header_temp3
      where ed_yale_carrier = 1 or ed_yale_opt = 1 or ed_yale_ipt = 1
    ),
        
    --Sum [increment] column (Cumulative Sum) within person to create an stay_id that combines duplicate/overlapping ED visits.
    create_within_person_stay_id AS
    (
      select
      id_mcare,
      claim_header_id,
      sum(increment) over(partition by id_mcare order by first_service_date, last_service_date, claim_header_id rows unbounded preceding) + 1 as within_person_stay_id
      from increment_stays_by_person
    )
    
    insert into stg_claims.tmp_mcare_claim_header_ed_pophealth
    select
    claim_header_id,
    dense_rank() over(order by id_mcare, within_person_stay_id) as ed_pophealth_id
    from create_within_person_stay_id
    option (label = 'tmp_mcare_claim_header_ed_pophealth');
    
    
    ------------------
    --STEP 9: Join back ed_pophealth table with header table on claim header ID
    -------------------
    insert into stg_claims.stage_mcare_claim_header
    select distinct
    a.id_mcare,
    a.claim_header_id,
    a.first_service_date,
    a.last_service_date,
    a.primary_diagnosis,
    a.icdcm_version,
    a.claim_type_mcare_id,
    a.claim_type_id,
    a.filetype_mcare,
    a.facility_type_code,
    a.service_type_code,
    a.patient_status,
    a.patient_status_code,
    a.ed_perform,
    a.ed_perform_id,
    case when b.ed_pophealth_id is not null then 1 else 0 end as ed_pophealth,
    b.ed_pophealth_id,
    a.inpatient,
    a.inpatient_id,
    a.admission_date,
    a.discharge_date,
    a.ipt_admission_type,
    a.ipt_admission_source,
    a.drg_code,
    a.hospice_from_date,
    a.pc_visit,
    a.pc_visit_id,
    a.submitted_charges,
    a.total_paid_mcare,
    a.total_paid_insurance,
    a.total_paid_bene,
    a.total_cost_of_care,
    getdate() as last_run
    from stg_claims.tmp_mcare_claim_header_temp3 as a
    left join stg_claims.tmp_mcare_claim_header_ed_pophealth as b
    on a.claim_header_id = b.claim_header_id;
    
    --drop final temp tables
    if object_id(N'stg_claims.tmp_mcare_claim_header_temp3',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_temp3;
    if object_id(N'stg_claims.tmp_mcare_claim_header_ed_pophealth',N'U') is not null drop table stg_claims.tmp_mcare_claim_header_ed_pophealth;",
        .con = dw_inthealth))
    }

#### Table-level QA script ####
qa_stage.mcare_claim_header_qa_f <- function() {
  
  #confirm that claim header is distinct
  res1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_header' as 'table', '# of headers' as qa_type,
    count(*) as qa
    from stg_claims.stage_mcare_claim_header;",
    .con = dw_inthealth))
  
  res2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_header' as 'table', '# of distinct headers' as qa_type,
    count(distinct claim_header_id) as qa
    from stg_claims.stage_mcare_claim_header;",
    .con = dw_inthealth))
  
  #make sure everyone is in elig_demo
  res3 <- dbGetQuery(conn = dw_inthealth, glue_sql(
  "select 'stg_claims.stage_mcare_claim_header' as 'table', '# members not in elig_demo, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stg_claims.stage_mcare_claim_header as a
    left join stg_claims.final_mcare_elig_demo as b
    on a.id_mcare = b.id_mcare
    where b.id_mcare is null;",
  .con = dw_inthealth))
  
  #make sure everyone is in elig_timevar
  res4 <- dbGetQuery(conn = dw_inthealth, glue_sql(
  "select 'stg_claims.stage_mcare_claim_header' as 'table', '# members not in elig_timevar, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stg_claims.stage_mcare_claim_header as a
    left join stg_claims.final_mcare_elig_timevar as b
    on a.id_mcare = b.id_mcare
    where b.id_mcare is null;",
  .con = dw_inthealth))
  
  #count unmatched claim types
  res5 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_header' as 'table', '# of claims with unmatched claim type, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_mcare_claim_header
    where claim_type_id is null or claim_type_mcare_id is null;",
    .con = dw_inthealth))
  
  #verify that all inpatient stays have discharge date
  res6 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_header' as 'table', '# of ipt stays with no discharge date, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_mcare_claim_header
    where inpatient_id is not null and discharge_date is null;",
    .con = dw_inthealth))
  
  #verify that no ed_pophealth_id value is used for more than one person
  res7 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_header' as 'table', '# of ed_pophealth_id values used for >1 person, expect 0' as qa_type,
    count(a.ed_pophealth_id) as qa
    from (
      select ed_pophealth_id, count(distinct id_mcare) as id_dcount
      from stg_claims.stage_mcare_claim_header
      group by ed_pophealth_id
    ) as a
    where a.id_dcount > 1;",
    .con = dw_inthealth))
  
  #verify that ed_pophealth_id does not skip any values
  res8a <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_header' as 'table', '# of distinct ed_pophealth_id values' as qa_type,
    count(distinct ed_pophealth_id) as qa
    from stg_claims.stage_mcare_claim_header;",
    .con = dw_inthealth))
  
  res8b <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_header' as 'table', 'max ed_pophealth_id - min + 1' as qa_type,
    cast(max(ed_pophealth_id) - min(ed_pophealth_id) + 1 as int) as qa
    from stg_claims.stage_mcare_claim_header;",
    .con = dw_inthealth))
  
  #verify that there are no rows with ed_perform_id without ed_pophealth_id
  res9 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_header' as 'table', '# of ed_perform rows with no ed_pophealth, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_mcare_claim_header
    where ed_perform_id is not null and ed_pophealth_id is null;",
    .con = dw_inthealth))
  
  #verify that 1-day overlap window was implemented correctly with ed_pophealth_id
  res10 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "with cte as
    (
    select * 
    ,lag(ed_pophealth_id) over(partition by id_mcare, ed_pophealth_id order by first_service_date) as lag_ed_pophealth_id
    ,lag(first_service_date) over(partition by id_mcare, ed_pophealth_id order by first_service_date) as lag_first_service_date
    from stg_claims.stage_mcare_claim_header
    where ed_pophealth_id is not null
    )
    select 'stg_claims.stage_mcare_claim_header' as 'table', '# of ed_pophealth visits where the overlap date is greater than 1 day, expect 0' as 'qa_type',
      count(*) as qa
    from stg_claims.stage_mcare_claim_header
    where ed_pophealth_id in (select ed_pophealth_id from cte where abs(datediff(day, lag_first_service_date, first_service_date)) > 1);",
    .con = dw_inthealth))
  
  #verify that total cost of care is calculated correctly
  res11 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_header' as 'table', '# of rows where total cost of care does not sum as expected, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_mcare_claim_header
    where total_cost_of_care != total_paid_insurance + total_paid_bene;",
    .con = dw_inthealth))

res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}