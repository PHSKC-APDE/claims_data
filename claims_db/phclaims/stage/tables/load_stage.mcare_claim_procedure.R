#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_claim_procedure
# Eli Kern, PHSKC (APDE)
#
# 2019-12
#
#2024-05-15 Eli update: Data from HCA, ETL in inthealth_edw
#2024-07-18 Eli update: Remove procedure code number and consolidate modifier codes into single column

### Run from 02_master_mcare_claims_analytic.R script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/02_master_mcare_claims_analytic.R

#### Load script ####
load_stage.mcare_claim_procedure_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(inthealth, glue::glue_sql(
    "----------------------
    --Only HCPCS codes have modifier codes - ICD-PCS and BETOS codes do not
    --The number of HCPCS modifier code columns depends upon claim type and ResDAC vintage
    ----------------------
    
    -------------------------
    --Step 1: BCARRIER claims
    --Yes: HCPCS codes with 2 modifier code fields
    --Yes: BETOS codes
    --No: ICD-PCS codes
    -------------------------
    
    --Base CTE: pulled once from the source table
    with bcarrier_base_data as (
        select
        --top 100
        trim(a.bene_id) as id_mcare,
        trim(a.clm_id) as claim_header_id,
        cast(a.clm_from_dt as date) as first_service_date,
        cast(a.clm_thru_dt as date) as last_service_date,
        'carrier' as filetype_mcare,
        b.hcpcs_cd as pchcpcs,
        b.hcpcs_1st_mdfr_cd as modifier_1,
        b.hcpcs_2nd_mdfr_cd as modifier_2,
        b.betos_cd as pcbetos
        from stg_claims.mcare_bcarrier_claims as a
        left join stg_claims.mcare_bcarrier_line as b
        on a.clm_id = b.clm_id
        --exclude denined claims using carrier/dme claim method
        where a.carr_clm_pmt_dnl_cd in ('1','2','3','4','5','6','7','8','9')
    ),
    --HCPCS codes with associated modifiers
    bcarrier_hcpcs_mods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            modifier_code
        from (
            select
                id_mcare,
                claim_header_id,
                first_service_date,
                last_service_date,
    			filetype_mcare,
                pchcpcs,
                modifier_1,
                modifier_2
            from bcarrier_base_data
        ) mod_src
        unpivot (
            modifier_code for mod_position IN
            (modifier_1, modifier_2)
        ) as unpvt_mod
        where pchcpcs is not null
    ),
    --HCPCS codes that have no modifier codes
    bcarrier_hcpcs_nomods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            null as modifier_code
        from bcarrier_base_data
        where pchcpcs is not null
            and modifier_1 is null
            and modifier_2 is null
    ),
    --BETOS codes
    bcarrier_betos as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pcbetos as procedure_code,
            null as modifier_code
        from bcarrier_base_data
        where pcbetos is not null
    ),
    --Final selection with deduplication via UNION
    bcarrier_final as (
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from bcarrier_hcpcs_mods
    
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from bcarrier_hcpcs_nomods
    
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from bcarrier_betos
    ),
    
    -------------------------
    --Step 2: DME claims
    --Yes: HCPCS codes with 4 modifier code fields
    --Yes: BETOS codes
    --No: ICD-PCS codes
    -------------------------
    
    --Base CTE: pulled once from the source table
    dme_base_data as (
    	select
    	--top 100
    	trim(a.bene_id) as id_mcare,
    	trim(a.clm_id) as claim_header_id,
    	cast(a.clm_from_dt as date) as first_service_date,
    	cast(a.clm_thru_dt as date) as last_service_date,
    	'dme' as filetype_mcare,
    	b.hcpcs_cd as pchcpcs,
    	b.hcpcs_1st_mdfr_cd as modifier_1,
    	b.hcpcs_2nd_mdfr_cd as modifier_2,
    	b.hcpcs_3rd_mdfr_cd as modifier_3,
    	b.hcpcs_4th_mdfr_cd as modifier_4,
    	b.betos_cd as pcbetos
    	from stg_claims.mcare_dme_claims as a
    	left join stg_claims.mcare_dme_line as b
    	on a.clm_id = b.clm_id
    	--exclude denined claims using carrier/dme claim method
    	where a.carr_clm_pmt_dnl_cd in ('1','2','3','4','5','6','7','8','9')
    ),
    --HCPCS codes with associated modifiers
    dme_hcpcs_mods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            modifier_code
        from (
            select
                id_mcare,
                claim_header_id,
                first_service_date,
                last_service_date,
    			filetype_mcare,
                pchcpcs,
                modifier_1,
                modifier_2,
    			modifier_3,
    			modifier_4
            from dme_base_data
        ) mod_src
        unpivot (
            modifier_code for mod_position IN
            (modifier_1, modifier_2, modifier_3, modifier_4)
        ) as unpvt_mod
        where pchcpcs is not null
    ),
    --HCPCS codes that have no modifier codes
    dme_hcpcs_nomods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            null as modifier_code
        from dme_base_data
        where pchcpcs is not null
            and modifier_1 is null
            and modifier_2 is null
    		and modifier_3 is null
    		and modifier_4 is null
    ),
    --BETOS codes
    dme_betos as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pcbetos as procedure_code,
            null as modifier_code
        from dme_base_data
        where pcbetos is not null
    ),
    --Final selection with deduplication via UNION
    dme_final as (
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from dme_hcpcs_mods
    
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from dme_hcpcs_nomods
    
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from dme_betos
    ),
    
    -------------------------
    --Step 3: HHA claims
    --Yes: HCPCS codes with 3 modifier code fields
    --No: BETOS codes
    --No: ICD-PCS codes
    -------------------------
    
    --Base CTE: pulled once from the source table
    hha_base_data as (
    	select
    	--top 100
    	trim(a.bene_id) as id_mcare,
    	trim(a.clm_id) as claim_header_id,
    	cast(a.clm_from_dt as date) as first_service_date,
    	cast(a.clm_thru_dt as date) as last_service_date,
    	b.hcpcs_cd as pchcpcs,
    	case when (b.hcpcs_1st_mdfr_cd is null or b.hcpcs_1st_mdfr_cd = ' ') then null else b.hcpcs_1st_mdfr_cd end as modifier_1,
    	case when (b.hcpcs_2nd_mdfr_cd is null or b.hcpcs_2nd_mdfr_cd = ' ') then null else b.hcpcs_2nd_mdfr_cd end as modifier_2,
    	case when (b.hcpcs_3rd_mdfr_cd is null or b.hcpcs_3rd_mdfr_cd = ' ') then null else b.hcpcs_3rd_mdfr_cd end as modifier_3,
    	'hha' as filetype_mcare,
    	getdate() as last_run
    	from stg_claims.mcare_hha_base_claims as a
    	left join stg_claims.mcare_hha_revenue_center as b
    	on a.clm_id = b.clm_id
    	--exclude denined claims using facility claim method
    	where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
    ),
    --HCPCS codes with associated modifiers
    hha_hcpcs_mods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            modifier_code
        from (
            select
                id_mcare,
                claim_header_id,
                first_service_date,
                last_service_date,
    			filetype_mcare,
                pchcpcs,
                modifier_1,
                modifier_2,
    			modifier_3
            from hha_base_data
        ) mod_src
        unpivot (
            modifier_code for mod_position IN
            (modifier_1, modifier_2, modifier_3)
        ) as unpvt_mod
        where pchcpcs is not null
    ),
    --HCPCS codes that have no modifier codes
    hha_hcpcs_nomods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            null as modifier_code
        from hha_base_data
        where pchcpcs is not null
            and modifier_1 is null
            and modifier_2 is null
    		and modifier_3 is null
    ),
    --Final selection with deduplication via UNION
    hha_final as (
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from hha_hcpcs_mods
    
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from hha_hcpcs_nomods
    ),
    
    -------------------------
    --Step 4: Hospice claims
    --Yes: HCPCS codes with 3 modifier code fields
    --No: BETOS codes
    --No: ICD-PCS codes
    -------------------------
    
    --Base CTE: pulled once from the source table
    hospice_base_data as (
        select
        --top 100
        trim(a.bene_id) as id_mcare,
        trim(a.clm_id) as claim_header_id,
        cast(a.clm_from_dt as date) as first_service_date,
        cast(a.clm_thru_dt as date) as last_service_date,
        b.hcpcs_cd as pchcpcs,
        case when (b.hcpcs_1st_mdfr_cd is null or b.hcpcs_1st_mdfr_cd = ' ') then null else b.hcpcs_1st_mdfr_cd end as modifier_1,
        case when (b.hcpcs_2nd_mdfr_cd is null or b.hcpcs_2nd_mdfr_cd = ' ') then null else b.hcpcs_2nd_mdfr_cd end as modifier_2,
        case when (b.hcpcs_3rd_mdfr_cd is null or b.hcpcs_3rd_mdfr_cd = ' ') then null else b.hcpcs_3rd_mdfr_cd end as modifier_3,
        'hospice' as filetype_mcare,
        getdate() as last_run
        from stg_claims.mcare_hospice_base_claims as a
        left join stg_claims.mcare_hospice_revenue_center as b
        on a.clm_id = b.clm_id
        --exclude denined claims using facility claim method
        where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
    ),
    --HCPCS codes with associated modifiers
    hospice_hcpcs_mods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            modifier_code
        from (
            select
                id_mcare,
                claim_header_id,
                first_service_date,
                last_service_date,
    			filetype_mcare,
                pchcpcs,
                modifier_1,
                modifier_2,
    			modifier_3
            from hospice_base_data
        ) mod_src
        unpivot (
            modifier_code for mod_position IN
            (modifier_1, modifier_2, modifier_3)
        ) as unpvt_mod
        where pchcpcs is not null
    ),
    --HCPCS codes that have no modifier codes
    hospice_hcpcs_nomods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            null as modifier_code
        from hospice_base_data
        where pchcpcs is not null
            and modifier_1 is null
            and modifier_2 is null
    		and modifier_3 is null
    ),
    --Final selection with deduplication via UNION
    hospice_final as (
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from hospice_hcpcs_mods
    
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from hospice_hcpcs_nomods
    ),
    
    -------------------------
    --Step 5: Inpatient claims, current vintage
    --Yes: HCPCS codes with 3 modifier code fields
    --No: BETOS codes
    --Yes: ICD-PCS codes
    -------------------------
    
    --Base CTE: pulled once from the source table
    inpatient_base_data as (
    	select
    	--top 100
    	trim(a.bene_id) as id_mcare,
    	trim(a.clm_id) as claim_header_id,
    	cast(a.clm_from_dt as date) as first_service_date,
    	cast(a.clm_thru_dt as date) as last_service_date,
    	'inpatient' as filetype_mcare,
    	b.hcpcs_cd as pchcpcs,
    	b.hcpcs_1st_mdfr_cd as modifier_1,
    	b.hcpcs_2nd_mdfr_cd as modifier_2,
    	b.hcpcs_3rd_mdfr_cd as modifier_3,
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
    ),
    --HCPCS codes with associated modifiers
    inpatient_hcpcs_mods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            modifier_code
        from (
            select
                id_mcare,
                claim_header_id,
                first_service_date,
                last_service_date,
    			filetype_mcare,
                pchcpcs,
                modifier_1,
                modifier_2,
    			modifier_3
            from inpatient_base_data
        ) mod_src
        unpivot (
            modifier_code for mod_position IN
            (modifier_1, modifier_2, modifier_3)
        ) as unpvt_mod
        where pchcpcs is not null
    ),
    --HCPCS codes that have no modifier codes
    inpatient_hcpcs_nomods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            null as modifier_code
        from inpatient_base_data
        where pchcpcs is not null
            and modifier_1 is null
            and modifier_2 is null
    		and modifier_3 is null
    ),
    --ICD-PCS codes
    inpatient_icdpcs as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            procedure_code,
            null as modifier_code
        FROM (
            select
                id_mcare,
                claim_header_id,
                first_service_date,
                last_service_date,
    			filetype_mcare,
                pc01, pc02, pc03, pc04, pc05, pc06, pc07, pc08, pc09, pc10, pc11, pc12, pc13,
    			pc14, pc15, pc16, pc17, pc18, pc19, pc20, pc21, pc22, pc23, pc24, pc25
            from inpatient_base_data
        ) icdpcs
        unpivot (
            procedure_code FOR code_position IN
            (pc01, pc02, pc03, pc04, pc05, pc06, pc07, pc08, pc09, pc10, pc11, pc12, pc13,
    		 pc14, pc15, pc16, pc17, pc18, pc19, pc20, pc21, pc22, pc23, pc24, pc25)
        ) as unpvt_icdpcs
    	where procedure_code is not null AND procedure_code != ' '
    ),
    --Final selection with deduplication via UNION
    inpatient_final as (
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from inpatient_hcpcs_mods
    
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from inpatient_hcpcs_nomods
    
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from inpatient_icdpcs
    ),
    
    -------------------------
    --Step 6: Inpatient claims, vintage J
    --Yes: HCPCS codes with 0 modifier code fields
    --No: BETOS codes
    --Yes: ICD-PCS codes
    -------------------------
    
    --Base CTE: pulled once from the source table
    inpatientj_base_data as (
    	select
    	--top 100
    	trim(a.bene_id) as id_mcare,
    	trim(a.clm_id) as claim_header_id,
    	cast(a.clm_from_dt as date) as first_service_date,
    	cast(a.clm_thru_dt as date) as last_service_date,
    	'inpatient' as filetype_mcare,
    	b.hcpcs_cd as pchcpcs,
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
    ),
    --All HCPCS codes
    inpatientj_hcpcs as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            null as modifier_code
        from inpatientj_base_data
        where pchcpcs is not null
    ),
    --ICD-PCS codes
    inpatientj_icdpcs as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            procedure_code,
            null as modifier_code
        FROM (
            select
                id_mcare,
                claim_header_id,
                first_service_date,
                last_service_date,
    			filetype_mcare,
                pc01, pc02, pc03, pc04, pc05, pc06, pc07, pc08, pc09, pc10, pc11, pc12, pc13,
    			pc14, pc15, pc16, pc17, pc18, pc19, pc20, pc21, pc22, pc23, pc24, pc25
            from inpatientj_base_data
        ) icdpcs
        unpivot (
            procedure_code FOR code_position IN
            (pc01, pc02, pc03, pc04, pc05, pc06, pc07, pc08, pc09, pc10, pc11, pc12, pc13,
    		 pc14, pc15, pc16, pc17, pc18, pc19, pc20, pc21, pc22, pc23, pc24, pc25)
        ) as unpvt_icdpcs
    	where procedure_code is not null AND procedure_code != ' '
    ),
    --Final selection with deduplication via UNION
    inpatientj_final as (
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from inpatientj_hcpcs
    
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from inpatientj_icdpcs
    ),
    
    -------------------------
    --Step 7: Outpatient claims, current vintage
    --Yes: HCPCS codes with 4 modifier code fields
    --No: BETOS codes
    --Yes: ICD-PCS codes
    -------------------------
    
    --Base CTE: pulled once from the source table
    outpatient_base_data as (
    	select
    	--top 100
    	trim(a.bene_id) as id_mcare,
    	trim(a.clm_id) as claim_header_id,
    	cast(a.clm_from_dt as date) as first_service_date,
    	cast(a.clm_thru_dt as date) as last_service_date,
    	'outpatient' as filetype_mcare,
    	b.hcpcs_cd as pchcpcs,
    	b.hcpcs_1st_mdfr_cd as modifier_1,
    	b.hcpcs_2nd_mdfr_cd as modifier_2,
    	b.hcpcs_3rd_mdfr_cd as modifier_3,
    	b.hcpcs_4th_mdfr_cd as modifier_4,
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
    ),
    --HCPCS codes with associated modifiers
    outpatient_hcpcs_mods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            modifier_code
        from (
            select
                id_mcare,
                claim_header_id,
                first_service_date,
                last_service_date,
    			filetype_mcare,
                pchcpcs,
                modifier_1,
                modifier_2,
    			modifier_3,
    			modifier_4
            from outpatient_base_data
        ) mod_src
        unpivot (
            modifier_code for mod_position IN
            (modifier_1, modifier_2, modifier_3, modifier_4)
        ) as unpvt_mod
        where pchcpcs is not null
    ),
    --HCPCS codes that have no modifier codes
    outpatient_hcpcs_nomods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            null as modifier_code
        from outpatient_base_data
        where pchcpcs is not null
            and modifier_1 is null
            and modifier_2 is null
    		and modifier_3 is null
    		and modifier_4 is null
    ),
    --ICD-PCS codes
    outpatient_icdpcs as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            procedure_code,
            null as modifier_code
        FROM (
            select
                id_mcare,
                claim_header_id,
                first_service_date,
                last_service_date,
    			filetype_mcare,
                pc01, pc02, pc03, pc04, pc05, pc06, pc07, pc08, pc09, pc10, pc11, pc12, pc13,
    			pc14, pc15, pc16, pc17, pc18, pc19, pc20, pc21, pc22, pc23, pc24, pc25
            from outpatient_base_data
        ) icdpcs
        unpivot (
            procedure_code FOR code_position IN
            (pc01, pc02, pc03, pc04, pc05, pc06, pc07, pc08, pc09, pc10, pc11, pc12, pc13,
    		 pc14, pc15, pc16, pc17, pc18, pc19, pc20, pc21, pc22, pc23, pc24, pc25)
        ) as unpvt_icdpcs
    	where procedure_code is not null AND procedure_code != ' '
    ),
    --Final selection with deduplication via UNION
    outpatient_final as (
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from outpatient_hcpcs_mods
    
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from outpatient_hcpcs_nomods
    
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from outpatient_icdpcs
    ),
    
    -------------------------
    --Step 8: Outpatient claims, vintage J
    --Yes: HCPCS codes with 2 modifier code fields
    --No: BETOS codes
    --Yes: ICD-PCS codes
    -------------------------
    
    --Base CTE: pulled once from the source table
    outpatientj_base_data as (
    	select
    	--top 100
    	trim(a.bene_id) as id_mcare,
    	trim(a.clm_id) as claim_header_id,
    	cast(a.clm_from_dt as date) as first_service_date,
    	cast(a.clm_thru_dt as date) as last_service_date,
    	'outpatient' as filetype_mcare,
    	b.hcpcs_cd as pchcpcs,
    	b.hcpcs_1st_mdfr_cd as modifier_1,
    	b.hcpcs_2nd_mdfr_cd as modifier_2,
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
    ),
    --HCPCS codes with associated modifiers
    outpatientj_hcpcs_mods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            modifier_code
        from (
            select
                id_mcare,
                claim_header_id,
                first_service_date,
                last_service_date,
    			filetype_mcare,
                pchcpcs,
                modifier_1,
                modifier_2
            from outpatientj_base_data
        ) mod_src
        unpivot (
            modifier_code for mod_position IN
            (modifier_1, modifier_2)
        ) as unpvt_mod
        where pchcpcs is not null
    ),
    --HCPCS codes that have no modifier codes
    outpatientj_hcpcs_nomods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            null as modifier_code
        from outpatientj_base_data
        where pchcpcs is not null
            and modifier_1 is null
            and modifier_2 is null
    ),
    --ICD-PCS codes
    outpatientj_icdpcs as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            procedure_code,
            null as modifier_code
        FROM (
            select
                id_mcare,
                claim_header_id,
                first_service_date,
                last_service_date,
    			filetype_mcare,
                pc01, pc02, pc03, pc04, pc05, pc06, pc07, pc08, pc09, pc10, pc11, pc12, pc13,
    			pc14, pc15, pc16, pc17, pc18, pc19, pc20, pc21, pc22, pc23, pc24, pc25
            from outpatientj_base_data
        ) icdpcs
        unpivot (
            procedure_code FOR code_position IN
            (pc01, pc02, pc03, pc04, pc05, pc06, pc07, pc08, pc09, pc10, pc11, pc12, pc13,
    		 pc14, pc15, pc16, pc17, pc18, pc19, pc20, pc21, pc22, pc23, pc24, pc25)
        ) as unpvt_icdpcs
    	where procedure_code is not null AND procedure_code != ' '
    ),
    --Final selection with deduplication via UNION
    outpatientj_final as (
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from outpatientj_hcpcs_mods
    
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from outpatientj_hcpcs_nomods
    
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from outpatientj_icdpcs
    ),
    
    -------------------------
    --Step 9: SNF claims
    --Yes: HCPCS codes with 3 modifier code fields
    --No: BETOS codes
    --Yes: ICD-PCS codes
    -------------------------
    
    --Base CTE: pulled once from the source table
    snf_base_data as (
    	select
    	--top 100
    	trim(a.bene_id) as id_mcare,
    	trim(a.clm_id) as claim_header_id,
    	cast(a.clm_from_dt as date) as first_service_date,
    	cast(a.clm_thru_dt as date) as last_service_date,
    	'snf' as filetype_mcare,
    	b.hcpcs_cd as pchcpcs,
    	b.hcpcs_1st_mdfr_cd as modifier_1,
    	b.hcpcs_2nd_mdfr_cd as modifier_2,
    	b.hcpcs_3rd_mdfr_cd as modifier_3,
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
    ),
    --HCPCS codes with associated modifiers
    snf_hcpcs_mods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            modifier_code
        from (
            select
                id_mcare,
                claim_header_id,
                first_service_date,
                last_service_date,
    			filetype_mcare,
                pchcpcs,
                modifier_1,
                modifier_2,
    			modifier_3
            from snf_base_data
        ) mod_src
        unpivot (
            modifier_code for mod_position IN
            (modifier_1, modifier_2, modifier_3)
        ) as unpvt_mod
        where pchcpcs is not null
    ),
    --HCPCS codes that have no modifier codes
    snf_hcpcs_nomods as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            pchcpcs as procedure_code,
            null as modifier_code
        from snf_base_data
        where pchcpcs is not null
            and modifier_1 is null
            and modifier_2 is null
    		and modifier_3 is null
    ),
    --ICD-PCS codes
    snf_icdpcs as (
        select
            id_mcare,
            claim_header_id,
            first_service_date,
            last_service_date,
    		filetype_mcare,
            procedure_code,
            null as modifier_code
        FROM (
            select
                id_mcare,
                claim_header_id,
                first_service_date,
                last_service_date,
    			filetype_mcare,
                pc01, pc02, pc03, pc04, pc05, pc06, pc07, pc08, pc09, pc10, pc11, pc12, pc13,
    			pc14, pc15, pc16, pc17, pc18, pc19, pc20, pc21, pc22, pc23, pc24, pc25
            from snf_base_data
        ) icdpcs
        unpivot (
            procedure_code FOR code_position IN
            (pc01, pc02, pc03, pc04, pc05, pc06, pc07, pc08, pc09, pc10, pc11, pc12, pc13,
    		 pc14, pc15, pc16, pc17, pc18, pc19, pc20, pc21, pc22, pc23, pc24, pc25)
        ) as unpvt_icdpcs
    	where procedure_code is not null AND procedure_code != ' '
    ),
    --Final selection with deduplication via UNION
    snf_final as (
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from snf_hcpcs_mods
    
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from snf_hcpcs_nomods
    
    	union
    	select
    	id_mcare,
    	claim_header_id,
    	first_service_date,
    	last_service_date,
    	procedure_code,
    	modifier_code,
    	filetype_mcare,
    	getdate() as last_run
    	from snf_icdpcs
    ),
    
    -------------------------
    --Step 10: Union all claim types
    -------------------------
    final_union as (
    select * from bcarrier_final
    union select * from dme_final
    union select * from hha_final
    union select * from hospice_final
    union select * from inpatient_final
    union select * from inpatientj_final
    union select * from outpatient_final
    union select * from outpatientj_final
    union select * from snf_final
    )
    
    -------------------------
    --Step 11: Exclude claims among people with no enrollment data and insert into table shell
    -------------------------
    insert into stg_claims.stage_mcare_claim_procedure
    select a.*
    from final_union as a
    left join (select distinct bene_id from stg_claims.mcare_bene_enrollment) as b
    on a.id_mcare = b.bene_id
    where b.bene_id is not null;",
        .con = inthealth))
    }

#### Table-level QA script ####
qa_stage.mcare_claim_procedure_qa_f <- function() {
  
  #confirm that claim types with hcpcs codes have data for each year (HCPCS codes are 5 digits long)
  res1 <- dbGetQuery(conn = inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_procedure' as 'table',
  'rows with non-null hcpcs code' as qa_type,
  filetype_mcare, year(last_service_date) as service_year, count(*) as qa
  from stg_claims.stage_mcare_claim_procedure
  where procedure_code is not null and len(procedure_code) = 5
  group by filetype_mcare, year(last_service_date)
  order by filetype_mcare, year(last_service_date);",
    .con = inthealth))
  
  #confirm that claim types with betos codes have data for each year (BETOS codes are 2-3 digits long and start with letter)
  res2 <- dbGetQuery(conn = inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_procedure' as 'table',
  'rows with non-null betos code' as qa_type,
  filetype_mcare, year(last_service_date) as service_year, count(*) as qa
  from stg_claims.stage_mcare_claim_procedure
  where procedure_code is not null and len(procedure_code) <= 3 and left(procedure_code, 1) like '[A-Z]'
  group by filetype_mcare, year(last_service_date)
  order by filetype_mcare, year(last_service_date);",
    .con = inthealth))
  
  #confirm that claim types with ICD procedure code 1 have data for each year
  res3 <- dbGetQuery(conn = inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_procedure' as 'table',
  'rows with non-null ICD procedure code 1' as qa_type,
  filetype_mcare, year(last_service_date) as service_year, count(*) as qa
  from stg_claims.stage_mcare_claim_procedure
  where procedure_code is not null and 
    (len(procedure_code) = 7 or (len(procedure_code) <=4 and left(procedure_code, 1) like '[0-9]'))
  group by filetype_mcare, year(last_service_date)
  order by filetype_mcare, year(last_service_date);",
    .con = inthealth))
  
  #make sure everyone is in bene_enrollment table
  res4 <- dbGetQuery(conn = inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_procedure' as 'table', '# members not in bene_enrollment, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stg_claims.stage_mcare_claim_procedure as a
    left join stg_claims.mcare_bene_enrollment as b
    on a.id_mcare = b.bene_id
    where b.bene_id is null;",
    .con = inthealth))

res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}