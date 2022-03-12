#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_hha_base_claims
# Eli Kern, PHSKC (APDE)
#
# 2019-12

### Run from master_mcare_full_union script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/master_mcare_full_union.R

#### Load script ####
load_stage.mcare_hha_base_claims_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "--Code to load data to stage.mcare_hha_base_claims
--Union of single-year files
    --Eli Kern (PHSKC-APDE)
    --2019-12
    --Run time: xx min
    ----------------------------
    ----------------------------
    --Shuva Dawadi
    --2/11/2020
    --adding 2017 code block to file 
    
    
    insert into PHClaims.stage.mcare_hha_base_claims_load with (tablock)
    
    --2014 data
    select
    ----top 100
    bene_id as id_mcare
    ,clm_id as claim_header_id
    ,clm_from_dt as first_service_date
    ,clm_thru_dt as last_service_date
    ,clm_mdcr_non_pmt_rsn_cd as denial_code_facility
    ,nch_clm_type_cd as claim_type
    ,clm_fac_type_cd as facility_type_code
    ,clm_srvc_clsfctn_type_cd as service_type_code
    ,clm_admsn_dt as admission_date
    ,nch_bene_dschrg_dt as discharge_date
    ,ptnt_dschrg_stus_cd as patient_status_code
    ,at_physn_npi as provider_attending_npi
    ,at_physn_spclty_cd as provider_attending_specialty
    ,op_physn_npi as provider_operating_npi
    ,op_physn_spclty_cd as provider_operating_specialty
    ,org_npi_num as provider_org_npi
    ,ot_physn_npi as provider_other_npi
    ,ot_physn_spclty_cd as provider_other_specialty
    ,rfr_physn_npi as provider_referring_npi
    ,rfr_physn_spclty_cd as provider_referring_specialty
    ,rndrng_physn_npi as provider_rendering_npi
    ,rndrng_physn_spclty_cd as provider_rendering_specialty
    ,srvc_loc_npi_num as provider_sos_npi
    ,clm_srvc_fac_zip_cd as provider_rendering_zip
    ,prncpal_dgns_cd as dx01
    ,icd_dgns_cd1 as dx02
    ,icd_dgns_cd2 as dx03
    ,icd_dgns_cd3 as dx04
    ,icd_dgns_cd4 as dx05
    ,icd_dgns_cd5 as dx06
    ,icd_dgns_cd6 as dx07
    ,icd_dgns_cd7 as dx08
    ,icd_dgns_cd8 as dx09
    ,icd_dgns_cd9 as dx10
    ,icd_dgns_cd10 as dx11
    ,icd_dgns_cd11 as dx12
    ,icd_dgns_cd12 as dx13
    ,icd_dgns_cd13 as dx14
    ,icd_dgns_cd14 as dx15
    ,icd_dgns_cd15 as dx16
    ,icd_dgns_cd16 as dx17
    ,icd_dgns_cd17 as dx18
    ,icd_dgns_cd18 as dx19
    ,icd_dgns_cd19 as dx20
    ,icd_dgns_cd20 as dx21
    ,icd_dgns_cd21 as dx22
    ,icd_dgns_cd22 as dx23
    ,icd_dgns_cd23 as dx24
    ,icd_dgns_cd24 as dx25
    ,icd_dgns_cd25 as dx26
    ,fst_dgns_e_cd as dxecode_1
    ,icd_dgns_e_cd1 as dxecode_2
    ,icd_dgns_e_cd2 as dxecode_3
    ,icd_dgns_e_cd3 as dxecode_4
    ,icd_dgns_e_cd4 as dxecode_5
    ,icd_dgns_e_cd5 as dxecode_6
    ,icd_dgns_e_cd6 as dxecode_7
    ,icd_dgns_e_cd7 as dxecode_8
    ,icd_dgns_e_cd8 as dxecode_9
    ,icd_dgns_e_cd9 as dxecode_10
    ,icd_dgns_e_cd10 as dxecode_11
    ,icd_dgns_e_cd11 as dxecode_12
    ,icd_dgns_e_cd12 as dxecode_13
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_hha_base_claims_k_14
    
    --2015 data
    union
    select
    --top 100
    bene_id as id_mcare
    ,clm_id as claim_header_id
    ,clm_from_dt as first_service_date
    ,clm_thru_dt as last_service_date
    ,clm_mdcr_non_pmt_rsn_cd as denial_code_facility
    ,nch_clm_type_cd as claim_type
    ,clm_fac_type_cd as facility_type_code
    ,clm_srvc_clsfctn_type_cd as service_type_code
    ,clm_admsn_dt as admission_date
    ,nch_bene_dschrg_dt as discharge_date
    ,ptnt_dschrg_stus_cd as patient_status_code
    ,at_physn_npi as provider_attending_npi
    ,at_physn_spclty_cd as provider_attending_specialty
    ,op_physn_npi as provider_operating_npi
    ,op_physn_spclty_cd as provider_operating_specialty
    ,org_npi_num as provider_org_npi
    ,ot_physn_npi as provider_other_npi
    ,ot_physn_spclty_cd as provider_other_specialty
    ,rfr_physn_npi as provider_referring_npi
    ,rfr_physn_spclty_cd as provider_referring_specialty
    ,rndrng_physn_npi as provider_rendering_npi
    ,rndrng_physn_spclty_cd as provider_rendering_specialty
    ,srvc_loc_npi_num as provider_sos_npi
    ,clm_srvc_fac_zip_cd as provider_rendering_zip
    ,prncpal_dgns_cd as dx01
    ,icd_dgns_cd1 as dx02
    ,icd_dgns_cd2 as dx03
    ,icd_dgns_cd3 as dx04
    ,icd_dgns_cd4 as dx05
    ,icd_dgns_cd5 as dx06
    ,icd_dgns_cd6 as dx07
    ,icd_dgns_cd7 as dx08
    ,icd_dgns_cd8 as dx09
    ,icd_dgns_cd9 as dx10
    ,icd_dgns_cd10 as dx11
    ,icd_dgns_cd11 as dx12
    ,icd_dgns_cd12 as dx13
    ,icd_dgns_cd13 as dx14
    ,icd_dgns_cd14 as dx15
    ,icd_dgns_cd15 as dx16
    ,icd_dgns_cd16 as dx17
    ,icd_dgns_cd17 as dx18
    ,icd_dgns_cd18 as dx19
    ,icd_dgns_cd19 as dx20
    ,icd_dgns_cd20 as dx21
    ,icd_dgns_cd21 as dx22
    ,icd_dgns_cd22 as dx23
    ,icd_dgns_cd23 as dx24
    ,icd_dgns_cd24 as dx25
    ,icd_dgns_cd25 as dx26
    ,fst_dgns_e_cd as dxecode_1
    ,icd_dgns_e_cd1 as dxecode_2
    ,icd_dgns_e_cd2 as dxecode_3
    ,icd_dgns_e_cd3 as dxecode_4
    ,icd_dgns_e_cd4 as dxecode_5
    ,icd_dgns_e_cd5 as dxecode_6
    ,icd_dgns_e_cd6 as dxecode_7
    ,icd_dgns_e_cd7 as dxecode_8
    ,icd_dgns_e_cd8 as dxecode_9
    ,icd_dgns_e_cd9 as dxecode_10
    ,icd_dgns_e_cd10 as dxecode_11
    ,icd_dgns_e_cd11 as dxecode_12
    ,icd_dgns_e_cd12 as dxecode_13
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_hha_base_claims_k_15
    
    --2016 data
    union
    select
    --top 100
    bene_id as id_mcare
    ,clm_id as claim_header_id
    ,clm_from_dt as first_service_date
    ,clm_thru_dt as last_service_date
    ,clm_mdcr_non_pmt_rsn_cd as denial_code_facility
    ,nch_clm_type_cd as claim_type
    ,clm_fac_type_cd as facility_type_code
    ,clm_srvc_clsfctn_type_cd as service_type_code
    ,clm_admsn_dt as admission_date
    ,nch_bene_dschrg_dt as discharge_date
    ,ptnt_dschrg_stus_cd as patient_status_code
    ,at_physn_npi as provider_attending_npi
    ,at_physn_spclty_cd as provider_attending_specialty
    ,op_physn_npi as provider_operating_npi
    ,op_physn_spclty_cd as provider_operating_specialty
    ,org_npi_num as provider_org_npi
    ,ot_physn_npi as provider_other_npi
    ,ot_physn_spclty_cd as provider_other_specialty
    ,rfr_physn_npi as provider_referring_npi
    ,rfr_physn_spclty_cd as provider_referring_specialty
    ,rndrng_physn_npi as provider_rendering_npi
    ,rndrng_physn_spclty_cd as provider_rendering_specialty
    ,srvc_loc_npi_num as provider_sos_npi
    ,clm_srvc_fac_zip_cd as provider_rendering_zip
    ,prncpal_dgns_cd as dx01
    ,icd_dgns_cd1 as dx02
    ,icd_dgns_cd2 as dx03
    ,icd_dgns_cd3 as dx04
    ,icd_dgns_cd4 as dx05
    ,icd_dgns_cd5 as dx06
    ,icd_dgns_cd6 as dx07
    ,icd_dgns_cd7 as dx08
    ,icd_dgns_cd8 as dx09
    ,icd_dgns_cd9 as dx10
    ,icd_dgns_cd10 as dx11
    ,icd_dgns_cd11 as dx12
    ,icd_dgns_cd12 as dx13
    ,icd_dgns_cd13 as dx14
    ,icd_dgns_cd14 as dx15
    ,icd_dgns_cd15 as dx16
    ,icd_dgns_cd16 as dx17
    ,icd_dgns_cd17 as dx18
    ,icd_dgns_cd18 as dx19
    ,icd_dgns_cd19 as dx20
    ,icd_dgns_cd20 as dx21
    ,icd_dgns_cd21 as dx22
    ,icd_dgns_cd22 as dx23
    ,icd_dgns_cd23 as dx24
    ,icd_dgns_cd24 as dx25
    ,icd_dgns_cd25 as dx26
    ,fst_dgns_e_cd as dxecode_1
    ,icd_dgns_e_cd1 as dxecode_2
    ,icd_dgns_e_cd2 as dxecode_3
    ,icd_dgns_e_cd3 as dxecode_4
    ,icd_dgns_e_cd4 as dxecode_5
    ,icd_dgns_e_cd5 as dxecode_6
    ,icd_dgns_e_cd6 as dxecode_7
    ,icd_dgns_e_cd7 as dxecode_8
    ,icd_dgns_e_cd8 as dxecode_9
    ,icd_dgns_e_cd9 as dxecode_10
    ,icd_dgns_e_cd10 as dxecode_11
    ,icd_dgns_e_cd11 as dxecode_12
    ,icd_dgns_e_cd12 as dxecode_13
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_hha_base_claims_k_16
    
    --2017 data
    union
    select
    --top 100
    bene_id as id_mcare
    ,clm_id as claim_header_id
    ,clm_from_dt as first_service_date
    ,clm_thru_dt as last_service_date
    ,clm_mdcr_non_pmt_rsn_cd as denial_code_facility
    ,nch_clm_type_cd as claim_type
    ,clm_fac_type_cd as facility_type_code
    ,clm_srvc_clsfctn_type_cd as service_type_code
    ,clm_admsn_dt as admission_date
    ,nch_bene_dschrg_dt as discharge_date
    ,ptnt_dschrg_stus_cd as patient_status_code
    ,at_physn_npi as provider_attending_npi
    ,at_physn_spclty_cd as provider_attending_specialty
    ,op_physn_npi as provider_operating_npi
    ,op_physn_spclty_cd as provider_operating_specialty
    ,org_npi_num as provider_org_npi
    ,ot_physn_npi as provider_other_npi
    ,ot_physn_spclty_cd as provider_other_specialty
    ,rfr_physn_npi as provider_referring_npi
    ,rfr_physn_spclty_cd as provider_referring_specialty
    ,rndrng_physn_npi as provider_rendering_npi
    ,rndrng_physn_spclty_cd as provider_rendering_specialty
    ,srvc_loc_npi_num as provider_sos_npi
    ,clm_srvc_fac_zip_cd as provider_rendering_zip
    ,prncpal_dgns_cd as dx01
    ,icd_dgns_cd1 as dx02
    ,icd_dgns_cd2 as dx03
    ,icd_dgns_cd3 as dx04
    ,icd_dgns_cd4 as dx05
    ,icd_dgns_cd5 as dx06
    ,icd_dgns_cd6 as dx07
    ,icd_dgns_cd7 as dx08
    ,icd_dgns_cd8 as dx09
    ,icd_dgns_cd9 as dx10
    ,icd_dgns_cd10 as dx11
    ,icd_dgns_cd11 as dx12
    ,icd_dgns_cd12 as dx13
    ,icd_dgns_cd13 as dx14
    ,icd_dgns_cd14 as dx15
    ,icd_dgns_cd15 as dx16
    ,icd_dgns_cd16 as dx17
    ,icd_dgns_cd17 as dx18
    ,icd_dgns_cd18 as dx19
    ,icd_dgns_cd19 as dx20
    ,icd_dgns_cd20 as dx21
    ,icd_dgns_cd21 as dx22
    ,icd_dgns_cd22 as dx23
    ,icd_dgns_cd23 as dx24
    ,icd_dgns_cd24 as dx25
    ,icd_dgns_cd25 as dx26
    ,fst_dgns_e_cd as dxecode_1
    ,icd_dgns_e_cd1 as dxecode_2
    ,icd_dgns_e_cd2 as dxecode_3
    ,icd_dgns_e_cd3 as dxecode_4
    ,icd_dgns_e_cd4 as dxecode_5
    ,icd_dgns_e_cd5 as dxecode_6
    ,icd_dgns_e_cd6 as dxecode_7
    ,icd_dgns_e_cd7 as dxecode_8
    ,icd_dgns_e_cd8 as dxecode_9
    ,icd_dgns_e_cd9 as dxecode_10
    ,icd_dgns_e_cd10 as dxecode_11
    ,icd_dgns_e_cd11 as dxecode_12
    ,icd_dgns_e_cd12 as dxecode_13
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_hha_base_claims_k_17;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.mcare_hha_base_claims_qa_f <- function() {
  
  #load expected counts from YAML file
  table_config <- yaml::yaml.load(httr::GET(config_url))
  
  #confirm that row counts match expected
  row_sum_union <- dbGetQuery(conn = db_claims, glue_sql(
    "select count(*) as qa from PHClaims.stage.mcare_hha_base_claims_load;",
    .con = db_claims))
  
  if(table_config$row_count_expected == row_sum_union$qa) {
    print("row sums match")
  }
  if(table_config$row_count_expected < row_sum_union$qa) {
    print(paste0("union row sum more, single-year sum: ", table_config$row_count_expected, " , union sum: ", row_sum_union$qa))
  }
  if(table_config$row_count_expected > row_sum_union$qa) {
    print(paste0("union row sum less, single-year sum: ", table_config$row_count_expected, " , union sum: ", row_sum_union$qa))
  }
  
  #confirm that col count matches expected
  col_count <- dbGetQuery(conn = db_claims, glue_sql(
    " select count(*) as col_cnt
      from information_schema.columns
      where table_catalog = 'PHClaims' -- the database
      and table_schema = 'stage'
      and table_name = 'mcare_' + 'hha_base_claims_load';",
    .con = db_claims))
  
  if(table_config$col_count_expected == col_count$col_cnt) {
    print("col count matches expected")
  } else {
    print("col count does not match!!")
  }
}