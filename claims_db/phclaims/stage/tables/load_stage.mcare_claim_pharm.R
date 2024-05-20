#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_claim_pharm
# Eli Kern, PHSKC (APDE)
#
# 2024-05

### Run from 02_master_mcare_claims_analytic.R script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/02_master_mcare_claims_analytic.R

#### Load script ####
load_stage.mcare_claim_pharm_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "------------------
    --STEP 1: Select desired columns from multi-year claim tables on stage schema
    --Include both administered medications on facility claims and Part D pharmacy fills
    --Pad NDC codes to 11 digits with leading zeroes
    --Exclude all denied claims using proposed approach per ResDAC 01-2020 consult
    -------------------
    insert into stg_claims.stage_mcare_claim_pharm
        
    --hha
    --note that although if found no hha claims with NDC codes, this code will capture them in the future if they are present
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.clm_id) as claim_header_id,
    trim(a.clm_line_num) as claim_line_id,
    cast(b.clm_thru_dt as date) as last_service_date,
    prscrbr_npi = null,
    right('00000000000' + rev_cntr_ide_ndc_upc_num, 11) as ndc,
    cast(rev_cntr_ndc_qty as numeric(19,3)) as facility_drug_quantity,
    rev_cntr_ndc_qty_qlfr_cd as facility_drug_quantity_unit,
    cmpnd_cd = null,
    qty_dspnsd_num = null,
    days_suply_num = null,
    fill_num = null,
    ptnt_pay_amt = null,
    othr_troop_amt = null,
    lics_amt = null,
    plro_amt = null,
    cvrd_d_plan_pd_amt = null,
    ncvrd_plan_pd_amt = null,
    tot_rx_cst_amt = null,
    dosage_form_code = null,
    dosage_form_code_desc = null,
    strength = null,
    pharmacy_id = null,
    brand_generic_flag = null,
    pharmacy_type = null,
    'hha' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_hha_revenue_center as a
    left join stg_claims.mcare_hha_base_claims as b
    on a.clm_id = b.clm_id
    left join stg_claims.mcare_bene_enrollment as c
    on a.bene_id = c.bene_id
    --subset to claim lines with NDC codes
    where [rev_cntr_ide_ndc_upc_num] is not null and left(rev_cntr_ide_ndc_upc_num, 1) not like '[A-Z]'
    --exclude denined claims using facility claim method
    and (b.clm_mdcr_non_pmt_rsn_cd = '' or b.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and c.bene_id is not null
        
    --hospice
    union
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.clm_id) as claim_header_id,
    trim(a.clm_line_num) as claim_line_id,
    cast(b.clm_thru_dt as date) as last_service_date,
    prscrbr_npi = null,
    right('00000000000' + rev_cntr_ide_ndc_upc_num, 11) as ndc,
    cast(rev_cntr_ndc_qty as numeric(19,3)) as facility_drug_quantity,
    rev_cntr_ndc_qty_qlfr_cd as facility_drug_quantity_unit,
    cmpnd_cd = null,
    qty_dspnsd_num = null,
    days_suply_num = null,
    fill_num = null,
    ptnt_pay_amt = null,
    othr_troop_amt = null,
    lics_amt = null,
    plro_amt = null,
    cvrd_d_plan_pd_amt = null,
    ncvrd_plan_pd_amt = null,
    tot_rx_cst_amt = null,
    dosage_form_code = null,
    dosage_form_code_desc = null,
    strength = null,
    pharmacy_id = null,
    brand_generic_flag = null,
    pharmacy_type = null,
    'hospice' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_hospice_revenue_center as a
    left join stg_claims.mcare_hospice_base_claims as b
    on a.clm_id = b.clm_id
    left join stg_claims.mcare_bene_enrollment as c
    on a.bene_id = c.bene_id
    --subset to claim lines with NDC codes
    where [rev_cntr_ide_ndc_upc_num] is not null and left(rev_cntr_ide_ndc_upc_num, 1) not like '[A-Z]'
    --exclude denined claims using facility claim method
    and (b.clm_mdcr_non_pmt_rsn_cd = '' or b.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and c.bene_id is not null
        
    --inpatient (beginning in 2015)
    union
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.clm_id) as claim_header_id,
    trim(a.clm_line_num) as claim_line_id,
    cast(b.clm_thru_dt as date) as last_service_date,
    prscrbr_npi = null,
    right('00000000000' + rev_cntr_ide_ndc_upc_num, 11) as ndc,
    cast(rev_cntr_ndc_qty as numeric(19,3)) as facility_drug_quantity,
    rev_cntr_ndc_qty_qlfr_cd as facility_drug_quantity_unit,
    cmpnd_cd = null,
    qty_dspnsd_num = null,
    days_suply_num = null,
    fill_num = null,
    ptnt_pay_amt = null,
    othr_troop_amt = null,
    lics_amt = null,
    plro_amt = null,
    cvrd_d_plan_pd_amt = null,
    ncvrd_plan_pd_amt = null,
    tot_rx_cst_amt = null,
    dosage_form_code = null,
    dosage_form_code_desc = null,
    strength = null,
    pharmacy_id = null,
    brand_generic_flag = null,
    pharmacy_type = null,
    'inpatient' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_inpatient_revenue_center as a
    left join stg_claims.mcare_inpatient_base_claims as b
    on a.clm_id = b.clm_id
    left join stg_claims.mcare_bene_enrollment as c
    on a.bene_id = c.bene_id
    --subset to claim lines with NDC codes
    where [rev_cntr_ide_ndc_upc_num] is not null and left(rev_cntr_ide_ndc_upc_num, 1) not like '[A-Z]'
    --exclude denined claims using facility claim method
    and (b.clm_mdcr_non_pmt_rsn_cd = '' or b.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and c.bene_id is not null
    
    --outpatient (beginning in 2015)
    union
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.clm_id) as claim_header_id,
    trim(a.clm_line_num) as claim_line_id,
    cast(b.clm_thru_dt as date) as last_service_date,
    prscrbr_npi = null,
    right('00000000000' + rev_cntr_ide_ndc_upc_num, 11) as ndc,
    cast(rev_cntr_ndc_qty as numeric(19,3)) as facility_drug_quantity,
    rev_cntr_ndc_qty_qlfr_cd as facility_drug_quantity_unit,
    cmpnd_cd = null,
    qty_dspnsd_num = null,
    days_suply_num = null,
    fill_num = null,
    ptnt_pay_amt = null,
    othr_troop_amt = null,
    lics_amt = null,
    plro_amt = null,
    cvrd_d_plan_pd_amt = null,
    ncvrd_plan_pd_amt = null,
    tot_rx_cst_amt = null,
    dosage_form_code = null,
    dosage_form_code_desc = null,
    strength = null,
    pharmacy_id = null,
    brand_generic_flag = null,
    pharmacy_type = null,
    'outpatient' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_outpatient_revenue_center as a
    left join stg_claims.mcare_outpatient_base_claims as b
    on a.clm_id = b.clm_id
    left join stg_claims.mcare_bene_enrollment as c
    on a.bene_id = c.bene_id
    --subset to claim lines with NDC codes
    where [rev_cntr_ide_ndc_upc_num] is not null and left(rev_cntr_ide_ndc_upc_num, 1) not like '[A-Z]'
    --exclude denined claims using facility claim method
    and (b.clm_mdcr_non_pmt_rsn_cd = '' or b.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and c.bene_id is not null
     
    --snf
    union
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.clm_id) as claim_header_id,
    trim(a.clm_line_num) as claim_line_id,
    cast(b.clm_thru_dt as date) as last_service_date,
    prscrbr_npi = null,
    right('00000000000' + rev_cntr_ide_ndc_upc_num, 11) as ndc,
    cast(rev_cntr_ndc_qty as numeric(19,3)) as facility_drug_quantity,
    rev_cntr_ndc_qty_qlfr_cd as facility_drug_quantity_unit,
    cmpnd_cd = null,
    qty_dspnsd_num = null,
    days_suply_num = null,
    fill_num = null,
    ptnt_pay_amt = null,
    othr_troop_amt = null,
    lics_amt = null,
    plro_amt = null,
    cvrd_d_plan_pd_amt = null,
    ncvrd_plan_pd_amt = null,
    tot_rx_cst_amt = null,
    dosage_form_code = null,
    dosage_form_code_desc = null,
    strength = null,
    pharmacy_id = null,
    brand_generic_flag = null,
    pharmacy_type = null,
    'snf' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_snf_revenue_center as a
    left join stg_claims.mcare_snf_base_claims as b
    on a.clm_id = b.clm_id
    left join stg_claims.mcare_bene_enrollment as c
    on a.bene_id = c.bene_id
    --subset to claim lines with NDC codes
    where [rev_cntr_ide_ndc_upc_num] is not null and left(rev_cntr_ide_ndc_upc_num, 1) not like '[A-Z]'
    --exclude denined claims using facility claim method
    and (b.clm_mdcr_non_pmt_rsn_cd = '' or b.clm_mdcr_non_pmt_rsn_cd is null)
    --exclude claims among people who have no eligibility data
    and c.bene_id is not null
    
    --Part D pharmacy data
    union
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.pde_id) as claim_header_id,
    claim_line_id = null,
    srvc_dt as last_service_date,
    case when prscrbr_id_qlfyr_cd in ('1', '01') then prscrbr_id else null end as prscrbr_npi,
    right('00000000000' + prod_srvc_id, 11) as ndc,
    facility_drug_quantity = null,
    facility_drug_quantity_unit = null,
    cmpnd_cd,
    cast(qty_dspnsd_num as numeric(19,3)) as qty_dspnsd_num,
    cast(days_suply_num as smallint) as days_suply_num,
    cast(fill_num as smallint) as fill_num,
    cast(ptnt_pay_amt as numeric(19,3)) as ptnt_pay_amt,
    cast(othr_troop_amt as numeric(19,3)) as othr_troop_amt,
    cast(lics_amt as numeric(19,3)) as lics_amt,
    cast(plro_amt as numeric(19,3)) as plro_amt,
    cast(cvrd_d_plan_pd_amt as numeric(19,3)) as cvrd_d_plan_pd_amt,
    cast(ncvrd_plan_pd_amt as numeric(19,3)) as ncvrd_plan_pd_amt,
    cast(tot_rx_cst_amt as numeric(19,3)) as tot_rx_cst_amt,
    gcdf as dosage_form_code,
    gcdf_desc as dosage_form_code_desc,
    [str] as strength,
    ncpdp_id as pharmacy_id,
    brnd_gnrc_cd as brand_generic_flag,
    phrmcy_srvc_type_cd as pharmacy_type,
    'pharmacy' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_pde as a
    left join stg_claims.mcare_bene_enrollment as b
    on a.bene_id = b.bene_id
    --exclude claims among people who have no eligibility data
    and b.bene_id is not null
    
    union
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.pde_id) as claim_header_id,
    claim_line_id = null,
    srvc_dt as last_service_date,
    case when prscrbr_id_qlfyr_cd in ('1', '01') then prscrbr_id else null end as prscrbr_npi,
    right('00000000000' + prod_srvc_id, 11) as ndc,
    facility_drug_quantity = null,
    facility_drug_quantity_unit = null,
    cmpnd_cd,
    qty_dspnsd_num,
    days_suply_num,
    fill_num,
    ptnt_pay_amt,
    othr_troop_amt,
    lics_amt,
    plro_amt,
    cvrd_d_plan_pd_amt,
    ncvrd_plan_pd_amt = null,
    tot_rx_cst_amt,
    gcdf as dosage_form_code,
    gcdf_desc as dosage_form_code_desc,
    [str] as strength,
    ncpdp_id as pharmacy_id,
    brnd_gnrc_cd as brand_generic_flag,
    phrmcy_srvc_type_cd as pharmacy_type,
    'pharmacy' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_pde_2014 as a
    left join stg_claims.mcare_bene_enrollment as b
    on a.bene_id = b.bene_id
    --exclude claims among people who have no eligibility data
    and b.bene_id is not null
    
    union
    select
    --top 100
    trim(a.bene_id) as id_mcare,
    trim(a.pde_id) as claim_header_id,
    claim_line_id = null,
    a.srvc_dt as last_service_date,
    case when prscqlfr in ('1', '01') then prscrbid else null end as prscrbr_npi,
    right('00000000000' + prdsrvid, 11) as ndc,
    facility_drug_quantity = null,
    facility_drug_quantity_unit = null,
    cmpnd_cd,
    qtydspns as qty_dspnsd_num,
    dayssply as days_suply_num,
    fill_num,
    ptpayamt as ptnt_pay_amt,
    othtroop as othr_troop_amt,
    lics_amt,
    plro_amt,
    cpp_amt as cvrd_d_plan_pd_amt,
    npp_amt as ncvrd_plan_pd_amt,
    totalcst as tot_rx_cst_amt,
    gcdf as dosage_form_code,
    gcdf_desc as dosage_form_code_desc,
    [str] as strength,
    ncpdp_id as pharmacy_id,
    brndgncd as brand_generic_flag,
    phrmcy_srvc_type_cd as pharmacy_type,
    'pharmacy' as filetype_mcare,
    getdate() as last_run
    from stg_claims.mcare_pde_a as a
    left join stg_claims.mcare_pde_b as b
    on a.pde_id = b.pde_id
    left join stg_claims.mcare_bene_enrollment as c
    on a.bene_id = c.bene_id
    --exclude claims among people who have no eligibility data
    where c.bene_id is not null;",
        .con = dw_inthealth))
    }

#### Table-level QA script ####
qa_stage.mcare_claim_pharm_qa_f <- function() {
  
  #confirm that claim types with facility-administered drugs have data for each year
  res1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_pharm' as 'table',
  'rows with facility drugs' as qa_type,
  filetype_mcare, year(last_service_date) as service_year, count(*) as qa
  from stg_claims.stage_mcare_claim_pharm
  where facility_drug_quantity is not null
  group by filetype_mcare, year(last_service_date)
  order by filetype_mcare, year(last_service_date);",
    .con = dw_inthealth))
  
  #confirm pharmacy fills exist for each year
  res2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_pharm' as 'table',
  'rows with pharmacy fills' as qa_type,
  filetype_mcare, year(last_service_date) as service_year, count(*) as qa
  from stg_claims.stage_mcare_claim_pharm
  where filetype_mcare = 'pharmacy'
  group by filetype_mcare, year(last_service_date)
  order by filetype_mcare, year(last_service_date);",
    .con = dw_inthealth))
  
  #make sure everyone is in bene_enrollment table
  res3 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_pharm' as 'table', '# members not in bene_enrollment, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stg_claims.stage_mcare_claim_pharm as a
    left join stg_claims.mcare_bene_enrollment as b
    on a.id_mcare = b.bene_id
    where b.bene_id is null;",
    .con = dw_inthealth))

res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}