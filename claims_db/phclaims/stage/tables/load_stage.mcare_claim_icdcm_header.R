#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_claim_icdcm_header
# Eli Kern, PHSKC (APDE)
#
# 2019-12
#
#2024-05-13 Eli update: Data from HCA, ETL in inthealth_edw

### Run from 03_master_mcare_claims_analytic.R script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/03_master_mcare_claims_analytic.R

#### Load script ####
load_stage.mcare_claim_icdcm_header_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "--Code to load data to stage.mcare_claim_icdcm_header table
    --ICD-CM codes reshaped to long
    --Eli Kern (PHSKC-APDE)
    --2024-05
    
    ------------------
    --STEP 1: Select and union desired columns from multi-year claim tables on stage schema
    --Exclude all denied claims using proposed approach per ResDAC 01-2020 consult
    --Unpivot and insert into table shell
    -------------------
    insert into stg_claims.stage_mcare_claim_icdcm_header
    
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
  	trim(a.bene_id) as id_mcare,
  	trim(a.clm_id) as claim_header_id,
  	cast(a.clm_from_dt as date) as first_service_date,
  	cast(a.clm_thru_dt as date) as last_service_date,
    'carrier' as filetype_mcare,
    dxadmit = null,
  	icd_dgns_cd1 as dx01,
  	icd_dgns_cd2 as dx02,
  	icd_dgns_cd3 as dx03,
  	icd_dgns_cd4 as dx04,
  	icd_dgns_cd5 as dx05,
  	icd_dgns_cd6 as dx06,
  	icd_dgns_cd7 as dx07,
  	icd_dgns_cd8 as dx08,
  	icd_dgns_cd9 as dx09,
  	icd_dgns_cd10 as dx10,
  	icd_dgns_cd11 as dx11,
  	icd_dgns_cd12 as dx12,
  	dx13 = null,
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
    dxecode_12 = null
    from stg_claims.mcare_bcarrier_claims as a
  	left join stg_claims.mcare_bene_enrollment as b
  	on a.bene_id = b.bene_id
    --exclude denined claims using carrier/dme claim method
    where a.carr_clm_pmt_dnl_cd in ('1','2','3','4','5','6','7','8','9')
    --exclude claims among people who have no eligibility data
    and b.bene_id is not null
      
    --dme
    union
    select
    --top 100
  	trim(a.bene_id) as id_mcare,
  	trim(a.clm_id) as claim_header_id,
  	cast(a.clm_from_dt as date) as first_service_date,
  	cast(a.clm_thru_dt as date) as last_service_date,
    'dme' as filetype_mcare,
    dxadmit = null,
  	icd_dgns_cd1 as dx01,
  	icd_dgns_cd2 as dx02,
  	icd_dgns_cd3 as dx03,
  	icd_dgns_cd4 as dx04,
  	icd_dgns_cd5 as dx05,
  	icd_dgns_cd6 as dx06,
  	icd_dgns_cd7 as dx07,
  	icd_dgns_cd8 as dx08,
  	icd_dgns_cd9 as dx09,
  	icd_dgns_cd10 as dx10,
  	icd_dgns_cd11 as dx11,
  	icd_dgns_cd12 as dx12,
  	dx13 = null,
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
    dxecode_12 = null
    from stg_claims.mcare_dme_claims as a
  	left join stg_claims.mcare_bene_enrollment as b
  	on a.bene_id = b.bene_id
    --exclude denined claims using carrier/dme claim method
    where a.carr_clm_pmt_dnl_cd in ('1','2','3','4','5','6','7','8','9')
    --exclude claims among people who have no eligibility data
    and b.bene_id is not null
      
    --hha
    union
    select
    --top 100
  	trim(a.bene_id) as id_mcare,
  	trim(a.clm_id) as claim_header_id,
  	cast(a.clm_from_dt as date) as first_service_date,
  	cast(a.clm_thru_dt as date) as last_service_date,
    'hha' as filetype_mcare,
    dxadmit = null,
    icd_dgns_cd1 as dx01,
  	icd_dgns_cd2 as dx02,
  	icd_dgns_cd3 as dx03,
  	icd_dgns_cd4 as dx04,
  	icd_dgns_cd5 as dx05,
  	icd_dgns_cd6 as dx06,
  	icd_dgns_cd7 as dx07,
  	icd_dgns_cd8 as dx08,
  	icd_dgns_cd9 as dx09,
  	icd_dgns_cd10 as dx10,
  	icd_dgns_cd11 as dx11,
  	icd_dgns_cd12 as dx12,
  	icd_dgns_cd13 as dx13,
  	icd_dgns_cd14 as dx14,
  	icd_dgns_cd15 as dx15,
  	icd_dgns_cd16 as dx16,
  	icd_dgns_cd17 as dx17,
  	icd_dgns_cd18 as dx18,
  	icd_dgns_cd19 as dx19,
  	icd_dgns_cd20 as dx20,
  	icd_dgns_cd21 as dx21,
  	icd_dgns_cd22 as dx22,
  	icd_dgns_cd23 as dx23,
  	icd_dgns_cd24 as dx24,
  	icd_dgns_cd25 as dx25,
  	icd_dgns_e_cd1 as dxecode_1,
  	icd_dgns_e_cd2 as dxecode_2,
  	icd_dgns_e_cd3 as dxecode_3,
  	icd_dgns_e_cd4 as dxecode_4,
  	icd_dgns_e_cd5 as dxecode_5,
  	icd_dgns_e_cd6 as dxecode_6,
  	icd_dgns_e_cd7 as dxecode_7,
  	icd_dgns_e_cd8 as dxecode_8,
  	icd_dgns_e_cd9 as dxecode_9,
  	icd_dgns_e_cd10 as dxecode_10,
  	icd_dgns_e_cd11 as dxecode_11,
  	icd_dgns_e_cd12 as dxecode_12
    from stg_claims.mcare_hha_base_claims as a
  	left join stg_claims.mcare_bene_enrollment as b
  	on a.bene_id = b.bene_id
    --exclude denined claims using facility claim method
    where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and b.bene_id is not null
      
    --hospice
    union
    select
    --top 100
  	trim(a.bene_id) as id_mcare,
  	trim(a.clm_id) as claim_header_id,
  	cast(a.clm_from_dt as date) as first_service_date,
  	cast(a.clm_thru_dt as date) as last_service_date,
    'hospice' as filetype_mcare,
    dxadmit = null,
    icd_dgns_cd1 as dx01,
  	icd_dgns_cd2 as dx02,
  	icd_dgns_cd3 as dx03,
  	icd_dgns_cd4 as dx04,
  	icd_dgns_cd5 as dx05,
  	icd_dgns_cd6 as dx06,
  	icd_dgns_cd7 as dx07,
  	icd_dgns_cd8 as dx08,
  	icd_dgns_cd9 as dx09,
  	icd_dgns_cd10 as dx10,
  	icd_dgns_cd11 as dx11,
  	icd_dgns_cd12 as dx12,
  	icd_dgns_cd13 as dx13,
  	icd_dgns_cd14 as dx14,
  	icd_dgns_cd15 as dx15,
  	icd_dgns_cd16 as dx16,
  	icd_dgns_cd17 as dx17,
  	icd_dgns_cd18 as dx18,
  	icd_dgns_cd19 as dx19,
  	icd_dgns_cd20 as dx20,
  	icd_dgns_cd21 as dx21,
  	icd_dgns_cd22 as dx22,
  	icd_dgns_cd23 as dx23,
  	icd_dgns_cd24 as dx24,
  	icd_dgns_cd25 as dx25,
  	icd_dgns_e_cd1 as dxecode_1,
  	icd_dgns_e_cd2 as dxecode_2,
  	icd_dgns_e_cd3 as dxecode_3,
  	icd_dgns_e_cd4 as dxecode_4,
  	icd_dgns_e_cd5 as dxecode_5,
  	icd_dgns_e_cd6 as dxecode_6,
  	icd_dgns_e_cd7 as dxecode_7,
  	icd_dgns_e_cd8 as dxecode_8,
  	icd_dgns_e_cd9 as dxecode_9,
  	icd_dgns_e_cd10 as dxecode_10,
  	icd_dgns_e_cd11 as dxecode_11,
  	icd_dgns_e_cd12 as dxecode_12
    from stg_claims.mcare_hospice_base_claims as a
  	left join stg_claims.mcare_bene_enrollment as b
  	on a.bene_id = b.bene_id
    --exclude denined claims using facility claim method
    where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and b.bene_id is not null
      
    --inpatient
    union
    select
    --top 100
  	trim(a.bene_id) as id_mcare,
  	trim(a.clm_id) as claim_header_id,
  	cast(a.clm_from_dt as date) as first_service_date,
  	cast(a.clm_thru_dt as date) as last_service_date,
    'inpatient' as filetype_mcare,
    admtg_dgns_cd as dxadmit,
    icd_dgns_cd1 as dx01,
  	icd_dgns_cd2 as dx02,
  	icd_dgns_cd3 as dx03,
  	icd_dgns_cd4 as dx04,
  	icd_dgns_cd5 as dx05,
  	icd_dgns_cd6 as dx06,
  	icd_dgns_cd7 as dx07,
  	icd_dgns_cd8 as dx08,
  	icd_dgns_cd9 as dx09,
  	icd_dgns_cd10 as dx10,
  	icd_dgns_cd11 as dx11,
  	icd_dgns_cd12 as dx12,
  	icd_dgns_cd13 as dx13,
  	icd_dgns_cd14 as dx14,
  	icd_dgns_cd15 as dx15,
  	icd_dgns_cd16 as dx16,
  	icd_dgns_cd17 as dx17,
  	icd_dgns_cd18 as dx18,
  	icd_dgns_cd19 as dx19,
  	icd_dgns_cd20 as dx20,
  	icd_dgns_cd21 as dx21,
  	icd_dgns_cd22 as dx22,
  	icd_dgns_cd23 as dx23,
  	icd_dgns_cd24 as dx24,
  	icd_dgns_cd25 as dx25,
  	icd_dgns_e_cd1 as dxecode_1,
  	icd_dgns_e_cd2 as dxecode_2,
  	icd_dgns_e_cd3 as dxecode_3,
  	icd_dgns_e_cd4 as dxecode_4,
  	icd_dgns_e_cd5 as dxecode_5,
  	icd_dgns_e_cd6 as dxecode_6,
  	icd_dgns_e_cd7 as dxecode_7,
  	icd_dgns_e_cd8 as dxecode_8,
  	icd_dgns_e_cd9 as dxecode_9,
  	icd_dgns_e_cd10 as dxecode_10,
  	icd_dgns_e_cd11 as dxecode_11,
  	icd_dgns_e_cd12 as dxecode_12
    from stg_claims.mcare_inpatient_base_claims as a
  	left join stg_claims.mcare_bene_enrollment as b
  	on a.bene_id = b.bene_id
    --exclude denined claims using facility claim method
    where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and b.bene_id is not null
  
  	--inpatient data structure j
    union
    select
    --top 100
  	trim(a.bene_id) as id_mcare,
  	trim(a.clm_id) as claim_header_id,
  	cast(a.clm_from_dt as date) as first_service_date,
  	cast(a.clm_thru_dt as date) as last_service_date,
    'inpatient' as filetype_mcare,
    admtg_dgns_cd as dxadmit,
    icd_dgns_cd1 as dx01,
  	icd_dgns_cd2 as dx02,
  	icd_dgns_cd3 as dx03,
  	icd_dgns_cd4 as dx04,
  	icd_dgns_cd5 as dx05,
  	icd_dgns_cd6 as dx06,
  	icd_dgns_cd7 as dx07,
  	icd_dgns_cd8 as dx08,
  	icd_dgns_cd9 as dx09,
  	icd_dgns_cd10 as dx10,
  	icd_dgns_cd11 as dx11,
  	icd_dgns_cd12 as dx12,
  	icd_dgns_cd13 as dx13,
  	icd_dgns_cd14 as dx14,
  	icd_dgns_cd15 as dx15,
  	icd_dgns_cd16 as dx16,
  	icd_dgns_cd17 as dx17,
  	icd_dgns_cd18 as dx18,
  	icd_dgns_cd19 as dx19,
  	icd_dgns_cd20 as dx20,
  	icd_dgns_cd21 as dx21,
  	icd_dgns_cd22 as dx22,
  	icd_dgns_cd23 as dx23,
  	icd_dgns_cd24 as dx24,
  	icd_dgns_cd25 as dx25,
  	icd_dgns_e_cd1 as dxecode_1,
  	icd_dgns_e_cd2 as dxecode_2,
  	icd_dgns_e_cd3 as dxecode_3,
  	icd_dgns_e_cd4 as dxecode_4,
  	icd_dgns_e_cd5 as dxecode_5,
  	icd_dgns_e_cd6 as dxecode_6,
  	icd_dgns_e_cd7 as dxecode_7,
  	icd_dgns_e_cd8 as dxecode_8,
  	icd_dgns_e_cd9 as dxecode_9,
  	icd_dgns_e_cd10 as dxecode_10,
  	icd_dgns_e_cd11 as dxecode_11,
  	icd_dgns_e_cd12 as dxecode_12
    from stg_claims.mcare_inpatient_base_claims_j as a
  	left join stg_claims.mcare_bene_enrollment as b
  	on a.bene_id = b.bene_id
    --exclude denined claims using facility claim method
    where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and b.bene_id is not null
      
    --outpatient
    union
    select
    --top 100
  	trim(a.bene_id) as id_mcare,
  	trim(a.clm_id) as claim_header_id,
  	cast(a.clm_from_dt as date) as first_service_date,
  	cast(a.clm_thru_dt as date) as last_service_date,
    'outpatient' as filetype_mcare,
    dxadmit = null,
    icd_dgns_cd1 as dx01,
  	icd_dgns_cd2 as dx02,
  	icd_dgns_cd3 as dx03,
  	icd_dgns_cd4 as dx04,
  	icd_dgns_cd5 as dx05,
  	icd_dgns_cd6 as dx06,
  	icd_dgns_cd7 as dx07,
  	icd_dgns_cd8 as dx08,
  	icd_dgns_cd9 as dx09,
  	icd_dgns_cd10 as dx10,
  	icd_dgns_cd11 as dx11,
  	icd_dgns_cd12 as dx12,
  	icd_dgns_cd13 as dx13,
  	icd_dgns_cd14 as dx14,
  	icd_dgns_cd15 as dx15,
  	icd_dgns_cd16 as dx16,
  	icd_dgns_cd17 as dx17,
  	icd_dgns_cd18 as dx18,
  	icd_dgns_cd19 as dx19,
  	icd_dgns_cd20 as dx20,
  	icd_dgns_cd21 as dx21,
  	icd_dgns_cd22 as dx22,
  	icd_dgns_cd23 as dx23,
  	icd_dgns_cd24 as dx24,
  	icd_dgns_cd25 as dx25,
  	icd_dgns_e_cd1 as dxecode_1,
  	icd_dgns_e_cd2 as dxecode_2,
  	icd_dgns_e_cd3 as dxecode_3,
  	icd_dgns_e_cd4 as dxecode_4,
  	icd_dgns_e_cd5 as dxecode_5,
  	icd_dgns_e_cd6 as dxecode_6,
  	icd_dgns_e_cd7 as dxecode_7,
  	icd_dgns_e_cd8 as dxecode_8,
  	icd_dgns_e_cd9 as dxecode_9,
  	icd_dgns_e_cd10 as dxecode_10,
  	icd_dgns_e_cd11 as dxecode_11,
  	icd_dgns_e_cd12 as dxecode_12
    from stg_claims.mcare_outpatient_base_claims as a
  	left join stg_claims.mcare_bene_enrollment as b
  	on a.bene_id = b.bene_id
    --exclude denined claims using facility claim method
    where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and b.bene_id is not null
  
  	--outpatient data structure j
    union
    select
    --top 100
  	trim(a.bene_id) as id_mcare,
  	trim(a.clm_id) as claim_header_id,
  	cast(a.clm_from_dt as date) as first_service_date,
  	cast(a.clm_thru_dt as date) as last_service_date,
    'outpatient' as filetype_mcare,
    dxadmit = null,
    icd_dgns_cd1 as dx01,
  	icd_dgns_cd2 as dx02,
  	icd_dgns_cd3 as dx03,
  	icd_dgns_cd4 as dx04,
  	icd_dgns_cd5 as dx05,
  	icd_dgns_cd6 as dx06,
  	icd_dgns_cd7 as dx07,
  	icd_dgns_cd8 as dx08,
  	icd_dgns_cd9 as dx09,
  	icd_dgns_cd10 as dx10,
  	icd_dgns_cd11 as dx11,
  	icd_dgns_cd12 as dx12,
  	icd_dgns_cd13 as dx13,
  	icd_dgns_cd14 as dx14,
  	icd_dgns_cd15 as dx15,
  	icd_dgns_cd16 as dx16,
  	icd_dgns_cd17 as dx17,
  	icd_dgns_cd18 as dx18,
  	icd_dgns_cd19 as dx19,
  	icd_dgns_cd20 as dx20,
  	icd_dgns_cd21 as dx21,
  	icd_dgns_cd22 as dx22,
  	icd_dgns_cd23 as dx23,
  	icd_dgns_cd24 as dx24,
  	icd_dgns_cd25 as dx25,
  	icd_dgns_e_cd1 as dxecode_1,
  	icd_dgns_e_cd2 as dxecode_2,
  	icd_dgns_e_cd3 as dxecode_3,
  	icd_dgns_e_cd4 as dxecode_4,
  	icd_dgns_e_cd5 as dxecode_5,
  	icd_dgns_e_cd6 as dxecode_6,
  	icd_dgns_e_cd7 as dxecode_7,
  	icd_dgns_e_cd8 as dxecode_8,
  	icd_dgns_e_cd9 as dxecode_9,
  	icd_dgns_e_cd10 as dxecode_10,
  	icd_dgns_e_cd11 as dxecode_11,
  	icd_dgns_e_cd12 as dxecode_12
    from stg_claims.mcare_outpatient_base_claims_j as a
  	left join stg_claims.mcare_bene_enrollment as b
  	on a.bene_id = b.bene_id
    --exclude denined claims using facility claim method
    where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and b.bene_id is not null
      
    --snf
    union
    select
    --top 100
  	trim(a.bene_id) as id_mcare,
  	trim(a.clm_id) as claim_header_id,
  	cast(a.clm_from_dt as date) as first_service_date,
  	cast(a.clm_thru_dt as date) as last_service_date,
    'snf' as filetype_mcare,
    admtg_dgns_cd as dxadmit,
    icd_dgns_cd1 as dx01,
  	icd_dgns_cd2 as dx02,
  	icd_dgns_cd3 as dx03,
  	icd_dgns_cd4 as dx04,
  	icd_dgns_cd5 as dx05,
  	icd_dgns_cd6 as dx06,
  	icd_dgns_cd7 as dx07,
  	icd_dgns_cd8 as dx08,
  	icd_dgns_cd9 as dx09,
  	icd_dgns_cd10 as dx10,
  	icd_dgns_cd11 as dx11,
  	icd_dgns_cd12 as dx12,
  	icd_dgns_cd13 as dx13,
  	icd_dgns_cd14 as dx14,
  	icd_dgns_cd15 as dx15,
  	icd_dgns_cd16 as dx16,
  	icd_dgns_cd17 as dx17,
  	icd_dgns_cd18 as dx18,
  	icd_dgns_cd19 as dx19,
  	icd_dgns_cd20 as dx20,
  	icd_dgns_cd21 as dx21,
  	icd_dgns_cd22 as dx22,
  	icd_dgns_cd23 as dx23,
  	icd_dgns_cd24 as dx24,
  	icd_dgns_cd25 as dx25,
  	icd_dgns_e_cd1 as dxecode_1,
  	icd_dgns_e_cd2 as dxecode_2,
  	icd_dgns_e_cd3 as dxecode_3,
  	icd_dgns_e_cd4 as dxecode_4,
  	icd_dgns_e_cd5 as dxecode_5,
  	icd_dgns_e_cd6 as dxecode_6,
  	icd_dgns_e_cd7 as dxecode_7,
  	icd_dgns_e_cd8 as dxecode_8,
  	icd_dgns_e_cd9 as dxecode_9,
  	icd_dgns_e_cd10 as dxecode_10,
  	icd_dgns_e_cd11 as dxecode_11,
  	icd_dgns_e_cd12 as dxecode_12
    from stg_claims.mcare_snf_base_claims as a
  	left join stg_claims.mcare_bene_enrollment as b
  	on a.bene_id = b.bene_id
    --exclude denined claims using facility claim method
    where (a.clm_mdcr_non_pmt_rsn_cd = '' or a.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and b.bene_id is not null
      	
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
    dxecode_12)
  ) as diagnoses
  where diagnoses is not null AND  diagnoses!=' ';",
        .con = dw_inthealth))
}

#### Table-level QA script ####
qa_stage.mcare_claim_icdcm_header_qa_f <- function() {
  
  #confirm that claim types with dx01 have data for each year
  res1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
  "select 'stg_claims.stage_mcare_claim_icdcm_header' as 'table',
  'rows with non-null dx01' as qa_type,
  filetype_mcare, year(last_service_date) as service_year, count(*) as qa
  from stg_claims.stage_mcare_claim_icdcm_header
  where icdcm_norm is not null and icdcm_number = '01'
  group by filetype_mcare, year(last_service_date)
  order by filetype_mcare, year(last_service_date);",
    .con = dw_inthealth))
  
  #confirm that claim types with admit dx have data for each year
  res2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_icdcm_header' as 'table',
  'rows with non-null dx_admit' as qa_type,
  filetype_mcare, year(last_service_date) as service_year, count(*) as qa
  from stg_claims.stage_mcare_claim_icdcm_header
  where icdcm_norm is not null and icdcm_number = 'admit'
  group by filetype_mcare, year(last_service_date)
  order by filetype_mcare, year(last_service_date);",
    .con = dw_inthealth))
  
  #confirm that claim types with ecode dx01 have data for each year
  res3 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_icdcm_header' as 'table',
  'rows with non-null dx ecode 1' as qa_type,
  filetype_mcare, year(last_service_date) as service_year, count(*) as qa
  from stg_claims.stage_mcare_claim_icdcm_header
  where icdcm_norm is not null and icdcm_number = 'ecode_1'
  group by filetype_mcare, year(last_service_date)
  order by filetype_mcare, year(last_service_date);",
    .con = dw_inthealth))
  
  #make sure everyone is in bene_enrollment table
  res4 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_icdcm_header' as 'table', '# members not in bene_enrollment, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stg_claims.stage_mcare_claim_icdcm_header as a
    left join stg_claims.mcare_bene_enrollment as b
    on a.id_mcare = b.bene_id
    where b.bene_id is null;",
    .con = dw_inthealth))

res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}