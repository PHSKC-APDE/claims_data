#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_claim_procedure
# Eli Kern, PHSKC (APDE)
#
# 2019-12
#
#2024-05-15 Eli update: Data from HCA, ETL in inthealth_edw

### Run from 03_master_mcare_claims_analytic.R script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/03_master_mcare_claims_analytic.R

#### Load script ####
load_stage.mcare_claim_procedure_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "--Code to load data to stage.mcare_claim_procedure table
    --Procedure codes reshaped to long
    --Eli Kern (PHSKC-APDE)
    --2024-05
        
    ------------------
    --STEP 1: Select and union desired columns from multi-year claim tables on stage schema
    --Exclude all denied claims using proposed approach per ResDAC 01-2020 consult
    --Unpivot and insert into table shell
    -------------------
    insert into stg_claims.stage_mcare_claim_procedure
        
    select distinct
    z.id_mcare,
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
        procedure_codes as 'procedure_code',
        cast(substring(procedure_code_number, 3,10) as varchar(200)) as 'procedure_code_number',
        case when (procedure_code_hcps_modifier_1 is null or procedure_code_hcps_modifier_1 = ' ') then null else procedure_code_hcps_modifier_1 end as modifier_1,
        case when (procedure_code_hcps_modifier_2 is null or procedure_code_hcps_modifier_2 = ' ') then null else procedure_code_hcps_modifier_2 end as modifier_2,
        case when (procedure_code_hcps_modifier_3 is null or procedure_code_hcps_modifier_3 = ' ') then null else procedure_code_hcps_modifier_3 end as modifier_3,
        case when (procedure_code_hcps_modifier_4 is null or procedure_code_hcps_modifier_4 = ' ') then null else procedure_code_hcps_modifier_4 end as modifier_4,
        filetype_mcare,
        getdate() as last_run
        
        from (
        	select
        	--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'carrier' as filetype_mcare,
        	b.hcpcs_cd as pchcpcs,
        	b.hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1,
        	b.hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2,
        	procedure_code_hcps_modifier_3 = null,
        	procedure_code_hcps_modifier_4 = null,
        	b.betos_cd as pcbetos
        	from stg_claims.mcare_bcarrier_claims as a
        	left join stg_claims.mcare_bcarrier_line as b
        	on a.clm_id = b.clm_id
        	--exclude denined claims using carrier/dme claim method
        	where a.carr_clm_pmt_dnl_cd in ('1','2','3','4','5','6','7','8','9')
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
        procedure_codes as 'procedure_code',
        cast(substring(procedure_code_number, 3,10) as varchar(200)) as 'procedure_code_number',
        case when (procedure_code_hcps_modifier_1 is null or procedure_code_hcps_modifier_1 = ' ') then null else procedure_code_hcps_modifier_1 end as modifier_1,
        case when (procedure_code_hcps_modifier_2 is null or procedure_code_hcps_modifier_2 = ' ') then null else procedure_code_hcps_modifier_2 end as modifier_2,
        case when (procedure_code_hcps_modifier_3 is null or procedure_code_hcps_modifier_3 = ' ') then null else procedure_code_hcps_modifier_3 end as modifier_3,
        case when (procedure_code_hcps_modifier_4 is null or procedure_code_hcps_modifier_4 = ' ') then null else procedure_code_hcps_modifier_4 end as modifier_4,
        filetype_mcare,
        getdate() as last_run
        
        from (
        	select
        	--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'dme' as filetype_mcare,
        	b.hcpcs_cd as pchcpcs,
        	b.hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1,
        	b.hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2,
        	b.hcpcs_3rd_mdfr_cd as procedure_code_hcps_modifier_3,
        	b.hcpcs_4th_mdfr_cd as procedure_code_hcps_modifier_4,
        	b.betos_cd as pcbetos
        	from stg_claims.mcare_dme_claims as a
        	left join stg_claims.mcare_dme_line as b
        	on a.clm_id = b.clm_id
        	--exclude denined claims using carrier/dme claim method
        	where a.carr_clm_pmt_dnl_cd in ('1','2','3','4','5','6','7','8','9')
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
      	trim(a.bene_id) as id_mcare,
      	trim(a.clm_id) as claim_header_id,
      	cast(a.clm_from_dt as date) as first_service_date,
      	cast(a.clm_thru_dt as date) as last_service_date,
        b.hcpcs_cd as procedure_code,
        'hcpcs' as procedure_code_number,
        case when (b.hcpcs_1st_mdfr_cd is null or b.hcpcs_1st_mdfr_cd = ' ') then null else b.hcpcs_1st_mdfr_cd end as modifier_1,
        case when (b.hcpcs_2nd_mdfr_cd is null or b.hcpcs_2nd_mdfr_cd = ' ') then null else b.hcpcs_2nd_mdfr_cd end as modifier_2,
        case when (b.hcpcs_3rd_mdfr_cd is null or b.hcpcs_3rd_mdfr_cd = ' ') then null else b.hcpcs_3rd_mdfr_cd end as modifier_3,
        modifier_4 = null,
        'hha' as filetype_mcare,
        getdate() as last_run
        from stg_claims.mcare_hha_base_claims as a
        left join stg_claims.mcare_hha_revenue_center as b
        on a.clm_id = b.clm_id
        --exclude denined claims using facility claim method
        where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
        
        --hospice
        --only one procedure code field thus no unpivot necessary
        union
        select
        --top 100
      	trim(a.bene_id) as id_mcare,
      	trim(a.clm_id) as claim_header_id,
      	cast(a.clm_from_dt as date) as first_service_date,
      	cast(a.clm_thru_dt as date) as last_service_date,
        b.hcpcs_cd as procedure_code,
        'hcpcs' as procedure_code_number,
        case when (b.hcpcs_1st_mdfr_cd is null or b.hcpcs_1st_mdfr_cd = ' ') then null else b.hcpcs_1st_mdfr_cd end as modifier_1,
        case when (b.hcpcs_2nd_mdfr_cd is null or b.hcpcs_2nd_mdfr_cd = ' ') then null else b.hcpcs_2nd_mdfr_cd end as modifier_2,
        case when (b.hcpcs_3rd_mdfr_cd is null or b.hcpcs_3rd_mdfr_cd = ' ') then null else b.hcpcs_3rd_mdfr_cd end as modifier_3,
        modifier_4 = null,
        'hospice' as filetype_mcare,
        getdate() as last_run
        from stg_claims.mcare_hospice_base_claims as a
        left join stg_claims.mcare_hospice_revenue_center as b
        on a.clm_id = b.clm_id
        --exclude denined claims using facility claim method
        where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
        	   	  
        --inpatient
        union
        select
        id_mcare,
        claim_header_id,
        first_service_date,
        last_service_date,
        procedure_codes as 'procedure_code',
        cast(substring(procedure_code_number, 3,10) as varchar(200)) as 'procedure_code_number',
        case when (procedure_code_hcps_modifier_1 is null or procedure_code_hcps_modifier_1 = ' ') then null else procedure_code_hcps_modifier_1 end as modifier_1,
        case when (procedure_code_hcps_modifier_2 is null or procedure_code_hcps_modifier_2 = ' ') then null else procedure_code_hcps_modifier_2 end as modifier_2,
        case when (procedure_code_hcps_modifier_3 is null or procedure_code_hcps_modifier_3 = ' ') then null else procedure_code_hcps_modifier_3 end as modifier_3,
        case when (procedure_code_hcps_modifier_4 is null or procedure_code_hcps_modifier_4 = ' ') then null else procedure_code_hcps_modifier_4 end as modifier_4,
        filetype_mcare,
        getdate() as last_run
        
        from (
        	select
        	--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'inpatient' as filetype_mcare,
        	b.hcpcs_cd as pchcpcs,
        	b.hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1,
        	b.hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2,
        	b.hcpcs_3rd_mdfr_cd as procedure_code_hcps_modifier_3,
        	procedure_code_hcps_modifier_4 = null,
      		a.icd_prcdr_cd1 as pc01,
      		a.icd_prcdr_cd2 as pc02,
      		a.icd_prcdr_cd3 as pc03,
      		a.icd_prcdr_cd4 as pc04,
      		a.icd_prcdr_cd5 as pc05,
      		a.icd_prcdr_cd6 as pc06,
      		a.icd_prcdr_cd7 as pc07,
      		a.icd_prcdr_cd8 as pc08,
      		a.icd_prcdr_cd9 as pc09,
      		a.icd_prcdr_cd10 as pc10,
      		a.icd_prcdr_cd11 as pc11,
      		a.icd_prcdr_cd12 as pc12,
      		a.icd_prcdr_cd13 as pc13,
      		a.icd_prcdr_cd14 as pc14,
      		a.icd_prcdr_cd15 as pc15,
      		a.icd_prcdr_cd16 as pc16,
      		a.icd_prcdr_cd17 as pc17,
      		a.icd_prcdr_cd18 as pc18,
      		a.icd_prcdr_cd19 as pc19,
      		a.icd_prcdr_cd20 as pc20,
      		a.icd_prcdr_cd21 as pc21,
      		a.icd_prcdr_cd22 as pc22,
      		a.icd_prcdr_cd23 as pc23,
      		a.icd_prcdr_cd24 as pc24,
      		a.icd_prcdr_cd25 as pc25
        	from stg_claims.mcare_inpatient_base_claims as a
        	left join stg_claims.mcare_inpatient_revenue_center as b
        	on a.clm_id = b.clm_id
    		--exclude denined claims using facility claim method
    		where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
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
    
    	--inpatient data structure j
        union
        select
        id_mcare,
        claim_header_id,
        first_service_date,
        last_service_date,
        procedure_codes as 'procedure_code',
        cast(substring(procedure_code_number, 3,10) as varchar(200)) as 'procedure_code_number',
        case when (procedure_code_hcps_modifier_1 is null or procedure_code_hcps_modifier_1 = ' ') then null else procedure_code_hcps_modifier_1 end as modifier_1,
        case when (procedure_code_hcps_modifier_2 is null or procedure_code_hcps_modifier_2 = ' ') then null else procedure_code_hcps_modifier_2 end as modifier_2,
        case when (procedure_code_hcps_modifier_3 is null or procedure_code_hcps_modifier_3 = ' ') then null else procedure_code_hcps_modifier_3 end as modifier_3,
        case when (procedure_code_hcps_modifier_4 is null or procedure_code_hcps_modifier_4 = ' ') then null else procedure_code_hcps_modifier_4 end as modifier_4,
        filetype_mcare,
        getdate() as last_run
        
        from (
        	select
        	--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'inpatient' as filetype_mcare,
        	b.hcpcs_cd as pchcpcs,
        	procedure_code_hcps_modifier_1 = null,
        	procedure_code_hcps_modifier_2 = null,
        	procedure_code_hcps_modifier_3 = null,
        	procedure_code_hcps_modifier_4 = null,
      		a.icd_prcdr_cd1 as pc01,
      		a.icd_prcdr_cd2 as pc02,
      		a.icd_prcdr_cd3 as pc03,
      		a.icd_prcdr_cd4 as pc04,
      		a.icd_prcdr_cd5 as pc05,
      		a.icd_prcdr_cd6 as pc06,
      		a.icd_prcdr_cd7 as pc07,
      		a.icd_prcdr_cd8 as pc08,
      		a.icd_prcdr_cd9 as pc09,
      		a.icd_prcdr_cd10 as pc10,
      		a.icd_prcdr_cd11 as pc11,
      		a.icd_prcdr_cd12 as pc12,
      		a.icd_prcdr_cd13 as pc13,
      		a.icd_prcdr_cd14 as pc14,
      		a.icd_prcdr_cd15 as pc15,
      		a.icd_prcdr_cd16 as pc16,
      		a.icd_prcdr_cd17 as pc17,
      		a.icd_prcdr_cd18 as pc18,
      		a.icd_prcdr_cd19 as pc19,
      		a.icd_prcdr_cd20 as pc20,
      		a.icd_prcdr_cd21 as pc21,
      		a.icd_prcdr_cd22 as pc22,
      		a.icd_prcdr_cd23 as pc23,
      		a.icd_prcdr_cd24 as pc24,
      		a.icd_prcdr_cd25 as pc25
        	from stg_claims.mcare_inpatient_base_claims_j as a
        	left join stg_claims.mcare_inpatient_revenue_center_j as b
        	on a.clm_id = b.clm_id
    		--exclude denined claims using facility claim method
    		where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
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
        
        --outpatient
        union
        select
        id_mcare,
        claim_header_id,
        first_service_date,
        last_service_date,
        procedure_codes as 'procedure_code',
        cast(substring(procedure_code_number, 3,10) as varchar(200)) as 'procedure_code_number',
        case when (procedure_code_hcps_modifier_1 is null or procedure_code_hcps_modifier_1 = ' ') then null else procedure_code_hcps_modifier_1 end as modifier_1,
        case when (procedure_code_hcps_modifier_2 is null or procedure_code_hcps_modifier_2 = ' ') then null else procedure_code_hcps_modifier_2 end as modifier_2,
        case when (procedure_code_hcps_modifier_3 is null or procedure_code_hcps_modifier_3 = ' ') then null else procedure_code_hcps_modifier_3 end as modifier_3,
        case when (procedure_code_hcps_modifier_4 is null or procedure_code_hcps_modifier_4 = ' ') then null else procedure_code_hcps_modifier_4 end as modifier_4,
        filetype_mcare,
        getdate() as last_run
        
        from (
        	select
        	--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'outpatient' as filetype_mcare,
        	b.hcpcs_cd as pchcpcs,
        	b.hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1,
        	b.hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2,
        	b.hcpcs_3rd_mdfr_cd as procedure_code_hcps_modifier_3,
      		b.hcpcs_4th_mdfr_cd as procedure_code_hcps_modifier_4,
      		a.icd_prcdr_cd1 as pc01,
      		a.icd_prcdr_cd2 as pc02,
      		a.icd_prcdr_cd3 as pc03,
      		a.icd_prcdr_cd4 as pc04,
      		a.icd_prcdr_cd5 as pc05,
      		a.icd_prcdr_cd6 as pc06,
      		a.icd_prcdr_cd7 as pc07,
      		a.icd_prcdr_cd8 as pc08,
      		a.icd_prcdr_cd9 as pc09,
      		a.icd_prcdr_cd10 as pc10,
      		a.icd_prcdr_cd11 as pc11,
      		a.icd_prcdr_cd12 as pc12,
      		a.icd_prcdr_cd13 as pc13,
      		a.icd_prcdr_cd14 as pc14,
      		a.icd_prcdr_cd15 as pc15,
      		a.icd_prcdr_cd16 as pc16,
      		a.icd_prcdr_cd17 as pc17,
      		a.icd_prcdr_cd18 as pc18,
      		a.icd_prcdr_cd19 as pc19,
      		a.icd_prcdr_cd20 as pc20,
      		a.icd_prcdr_cd21 as pc21,
      		a.icd_prcdr_cd22 as pc22,
      		a.icd_prcdr_cd23 as pc23,
      		a.icd_prcdr_cd24 as pc24,
      		a.icd_prcdr_cd25 as pc25
        	from stg_claims.mcare_outpatient_base_claims as a
        	left join stg_claims.mcare_outpatient_revenue_center as b
        	on a.clm_id = b.clm_id
    		--exclude denined claims using facility claim method
    		where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
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
    
    	--outpatient data structure j
        union
        select
        id_mcare,
        claim_header_id,
        first_service_date,
        last_service_date,
        procedure_codes as 'procedure_code',
        cast(substring(procedure_code_number, 3,10) as varchar(200)) as 'procedure_code_number',
        case when (procedure_code_hcps_modifier_1 is null or procedure_code_hcps_modifier_1 = ' ') then null else procedure_code_hcps_modifier_1 end as modifier_1,
        case when (procedure_code_hcps_modifier_2 is null or procedure_code_hcps_modifier_2 = ' ') then null else procedure_code_hcps_modifier_2 end as modifier_2,
        case when (procedure_code_hcps_modifier_3 is null or procedure_code_hcps_modifier_3 = ' ') then null else procedure_code_hcps_modifier_3 end as modifier_3,
        case when (procedure_code_hcps_modifier_4 is null or procedure_code_hcps_modifier_4 = ' ') then null else procedure_code_hcps_modifier_4 end as modifier_4,
        filetype_mcare,
        getdate() as last_run
        
        from (
        	select
        	--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'outpatient' as filetype_mcare,
        	b.hcpcs_cd as pchcpcs,
        	b.hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1,
        	b.hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2,
        	procedure_code_hcps_modifier_3 = null,
      		procedure_code_hcps_modifier_4 = null,
      		a.icd_prcdr_cd1 as pc01,
      		a.icd_prcdr_cd2 as pc02,
      		a.icd_prcdr_cd3 as pc03,
      		a.icd_prcdr_cd4 as pc04,
      		a.icd_prcdr_cd5 as pc05,
      		a.icd_prcdr_cd6 as pc06,
      		a.icd_prcdr_cd7 as pc07,
      		a.icd_prcdr_cd8 as pc08,
      		a.icd_prcdr_cd9 as pc09,
      		a.icd_prcdr_cd10 as pc10,
      		a.icd_prcdr_cd11 as pc11,
      		a.icd_prcdr_cd12 as pc12,
      		a.icd_prcdr_cd13 as pc13,
      		a.icd_prcdr_cd14 as pc14,
      		a.icd_prcdr_cd15 as pc15,
      		a.icd_prcdr_cd16 as pc16,
      		a.icd_prcdr_cd17 as pc17,
      		a.icd_prcdr_cd18 as pc18,
      		a.icd_prcdr_cd19 as pc19,
      		a.icd_prcdr_cd20 as pc20,
      		a.icd_prcdr_cd21 as pc21,
      		a.icd_prcdr_cd22 as pc22,
      		a.icd_prcdr_cd23 as pc23,
      		a.icd_prcdr_cd24 as pc24,
      		a.icd_prcdr_cd25 as pc25
        	from stg_claims.mcare_outpatient_base_claims_j as a
        	left join stg_claims.mcare_outpatient_revenue_center_j as b
        	on a.clm_id = b.clm_id
    		--exclude denined claims using facility claim method
    		where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
        ) as x6
        
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
        procedure_codes as 'procedure_code',
        cast(substring(procedure_code_number, 3,10) as varchar(200)) as 'procedure_code_number',
        case when (procedure_code_hcps_modifier_1 is null or procedure_code_hcps_modifier_1 = ' ') then null else procedure_code_hcps_modifier_1 end as modifier_1,
        case when (procedure_code_hcps_modifier_2 is null or procedure_code_hcps_modifier_2 = ' ') then null else procedure_code_hcps_modifier_2 end as modifier_2,
        case when (procedure_code_hcps_modifier_3 is null or procedure_code_hcps_modifier_3 = ' ') then null else procedure_code_hcps_modifier_3 end as modifier_3,
        case when (procedure_code_hcps_modifier_4 is null or procedure_code_hcps_modifier_4 = ' ') then null else procedure_code_hcps_modifier_4 end as modifier_4,
        filetype_mcare,
        getdate() as last_run
        
        from (
        	select
        	--top 100
      		trim(a.bene_id) as id_mcare,
      		trim(a.clm_id) as claim_header_id,
      		cast(a.clm_from_dt as date) as first_service_date,
      		cast(a.clm_thru_dt as date) as last_service_date,
        	'snf' as filetype_mcare,
        	b.hcpcs_cd as pchcpcs,
        	b.hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1,
        	b.hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2,
        	b.hcpcs_3rd_mdfr_cd as procedure_code_hcps_modifier_3,
        	procedure_code_hcps_modifier_4 = null,
      		a.icd_prcdr_cd1 as pc01,
      		a.icd_prcdr_cd2 as pc02,
      		a.icd_prcdr_cd3 as pc03,
      		a.icd_prcdr_cd4 as pc04,
      		a.icd_prcdr_cd5 as pc05,
      		a.icd_prcdr_cd6 as pc06,
      		a.icd_prcdr_cd7 as pc07,
      		a.icd_prcdr_cd8 as pc08,
      		a.icd_prcdr_cd9 as pc09,
      		a.icd_prcdr_cd10 as pc10,
      		a.icd_prcdr_cd11 as pc11,
      		a.icd_prcdr_cd12 as pc12,
      		a.icd_prcdr_cd13 as pc13,
      		a.icd_prcdr_cd14 as pc14,
      		a.icd_prcdr_cd15 as pc15,
      		a.icd_prcdr_cd16 as pc16,
      		a.icd_prcdr_cd17 as pc17,
      		a.icd_prcdr_cd18 as pc18,
      		a.icd_prcdr_cd19 as pc19,
      		a.icd_prcdr_cd20 as pc20,
      		a.icd_prcdr_cd21 as pc21,
      		a.icd_prcdr_cd22 as pc22,
      		a.icd_prcdr_cd23 as pc23,
      		a.icd_prcdr_cd24 as pc24,
      		a.icd_prcdr_cd25 as pc25
        	from stg_claims.mcare_snf_base_claims as a
        	left join stg_claims.mcare_snf_revenue_center as b
        	on a.clm_id = b.clm_id
    		--exclude denined claims using facility claim method
    		where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
        ) as x7
        
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
    left join stg_claims.mcare_bene_enrollment as w
    on z.id_mcare = w.bene_id
    where w.bene_id is not null;",
        .con = dw_inthealth))
    }

#### Table-level QA script ####
qa_stage.mcare_claim_procedure_qa_f <- function() {
  
  #confirm that claim types with hcpcs codes have data for each year
  res1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_procedure' as 'table',
  'rows with non-null hcpcs code' as qa_type,
  filetype_mcare, year(last_service_date) as service_year, count(*) as qa
  from stg_claims.stage_mcare_claim_procedure
  where procedure_code is not null and procedure_code_number = 'hcpcs'
  group by filetype_mcare, year(last_service_date)
  order by filetype_mcare, year(last_service_date);",
    .con = dw_inthealth))
  
  #confirm that claim types with betos codes have data for each year
  res2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_procedure' as 'table',
  'rows with non-null betos code' as qa_type,
  filetype_mcare, year(last_service_date) as service_year, count(*) as qa
  from stg_claims.stage_mcare_claim_procedure
  where procedure_code is not null and procedure_code_number = 'betos'
  group by filetype_mcare, year(last_service_date)
  order by filetype_mcare, year(last_service_date);",
    .con = dw_inthealth))
  
  #confirm that claim types with ICD procedure code 1 have data for each year
  res3 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_procedure' as 'table',
  'rows with non-null ICD procedure code 1' as qa_type,
  filetype_mcare, year(last_service_date) as service_year, count(*) as qa
  from stg_claims.stage_mcare_claim_procedure
  where procedure_code is not null and procedure_code_number = '01'
  group by filetype_mcare, year(last_service_date)
  order by filetype_mcare, year(last_service_date);",
    .con = dw_inthealth))
  
  #make sure everyone is in bene_enrollment table
  res4 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_procedure' as 'table', '# members not in bene_enrollment, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stg_claims.stage_mcare_claim_procedure as a
    left join stg_claims.mcare_bene_enrollment as b
    on a.id_mcare = b.bene_id
    where b.bene_id is null;",
    .con = dw_inthealth))

res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}