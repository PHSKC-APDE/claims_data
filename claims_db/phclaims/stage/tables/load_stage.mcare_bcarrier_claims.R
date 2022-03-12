#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_BCARRIER_CLAIMS
# Eli Kern, PHSKC (APDE)
#
# 2019-12

### Run from master_mcare_full_union script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/master_mcare_full_union.R

#### Load script ####
load_stage.mcare_bcarrier_claims_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "--Code to load data to stage.mcare_bcarrier_claims
--Union of single-year files
    --Eli Kern (PHSKC-APDE)
    --2019-12
    --Run time: 12 min
    
    
    insert into PHClaims.stage.mcare_bcarrier_claims_load with (tablock)
    
    --2014 data
    select
    ----top 100
    encrypted723beneficiaryid as id_mcare
    ,encryptedclaimid as claim_header_id
    ,claimfromdate as first_service_date
    ,claimthroughdatedeterminesyearof as last_service_date
    ,nchclaimtypecode as claim_type
    ,carrierclaimpaymentdenialcode as denial_code
    ,provider_billing_npi = null
    ,carrierclaimreferingphysiciannpi as provider_referring_npi
    ,provider_cpo_npi = null
    ,provider_sos_npi = null
    ,primaryclaimdiagnosiscode as dx01
    ,primaryclaimdiagnosiscodediagnos as dx01_ver
    ,claimdiagnosiscodei as dx02
    ,claimdiagnosiscodeidiagnosisvers as dx02_ver
    ,claimdiagnosiscodeii as dx03
    ,claimdiagnosiscodeiidiagnosisver as dx03_ver
    ,claimdiagnosiscodeiii as dx04
    ,claimdiagnosiscodeiiidiagnosisve as dx04_ver
    ,claimdiagnosiscodeiv as dx05
    ,claimdiagnosiscodeivdiagnosisver as dx05_ver
    ,claimdiagnosiscodev as dx06
    ,claimdiagnosiscodevdiagnosisvers as dx06_ver
    ,claimdiagnosiscodevi as dx07
    ,claimdiagnosiscodevidiagnosisver as dx07_ver
    ,claimdiagnosiscodevii as dx08
    ,claimdiagnosiscodeviidiagnosisve as dx08_ver
    ,claimdiagnosiscodeviii as dx09
    ,claimdiagnosiscodeviiidiagnosisv as dx09_ver
    ,claimdiagnosiscodeix as dx10
    ,claimdiagnosiscodeixdiagnosisver as dx10_ver
    ,claimdiagnosiscodex as dx11
    ,claimdiagnosiscodexdiagnosisvers as dx11_ver
    ,claimdiagnosiscodexi as dx12
    ,claimdiagnosiscodexidiagnosisver as dx12_ver
    ,claimdiagnosiscodexii as dx13
    ,claimdiagnosiscodexiidiagnosisve as dx13_ver
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_bcarrier_claims_k_14
    
    --2015 data
    union
    select
    ----top 100
    bene_id as id_mcare
    ,clm_id as claim_header_id
    ,from_dt as first_service_date
    ,thru_dt as last_service_date
    ,clm_type as claim_type
    ,pmtdnlcd as denial_code
    ,carr_clm_blg_npi_num as provider_billing_npi
    ,rfr_npi as provider_referring_npi
    ,cpo_org_npi_num as provider_cpo_npi
    ,carr_clm_sos_npi_num as provider_sos_npi
    ,prncpal_dgns_cd as dx01
    ,prncpal_dgns_vrsn_cd as dx01_ver
    ,icd_dgns_cd1 as dx02
    ,icd_dgns_vrsn_cd1 as dx02_ver
    ,icd_dgns_cd2 as dx03
    ,icd_dgns_vrsn_cd2 as dx03_ver
    ,icd_dgns_cd3 as dx04
    ,icd_dgns_vrsn_cd3 as dx04_ver
    ,icd_dgns_cd4 as dx05
    ,icd_dgns_vrsn_cd4 as dx05_ver
    ,icd_dgns_cd5 as dx06
    ,icd_dgns_vrsn_cd5 as dx06_ver
    ,icd_dgns_cd6 as dx07
    ,icd_dgns_vrsn_cd6 as dx07_ver
    ,icd_dgns_cd7 as dx08
    ,icd_dgns_vrsn_cd7 as dx08_ver
    ,icd_dgns_cd8 as dx09
    ,icd_dgns_vrsn_cd8 as dx09_ver
    ,icd_dgns_cd9 as dx10
    ,icd_dgns_vrsn_cd9 as dx10_ver
    ,icd_dgns_cd10 as dx11
    ,icd_dgns_vrsn_cd10 as dx11_ver
    ,icd_dgns_cd11 as dx12
    ,icd_dgns_vrsn_cd11 as dx12_ver
    ,icd_dgns_cd12 as dx13
    ,icd_dgns_vrsn_cd12 as dx13_ver
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_bcarrier_claims_j
    
    --2016 data
    union
    select
    --top 100
    bene_id as id_mcare
    ,clm_id as claim_header_id
    ,clm_from_dt as first_service_date
    ,clm_thru_dt as last_service_date
    ,nch_clm_type_cd as claim_type
    ,carr_clm_pmt_dnl_cd as denial_code
    ,carr_clm_blg_npi_num as provider_billing_npi
    ,rfr_physn_npi as provider_referring_npi
    ,cpo_org_npi_num as provider_cpo_npi
    ,carr_clm_sos_npi_num as provider_sos_npi
    ,prncpal_dgns_cd as dx01
    ,prncpal_dgns_vrsn_cd as dx01_ver
    ,icd_dgns_cd1 as dx02
    ,icd_dgns_vrsn_cd1 as dx02_ver
    ,icd_dgns_cd2 as dx03
    ,icd_dgns_vrsn_cd2 as dx03_ver
    ,icd_dgns_cd3 as dx04
    ,icd_dgns_vrsn_cd3 as dx04_ver
    ,icd_dgns_cd4 as dx05
    ,icd_dgns_vrsn_cd4 as dx05_ver
    ,icd_dgns_cd5 as dx06
    ,icd_dgns_vrsn_cd5 as dx06_ver
    ,icd_dgns_cd6 as dx07
    ,icd_dgns_vrsn_cd6 as dx07_ver
    ,icd_dgns_cd7 as dx08
    ,icd_dgns_vrsn_cd7 as dx08_ver
    ,icd_dgns_cd8 as dx09
    ,icd_dgns_vrsn_cd8 as dx09_ver
    ,icd_dgns_cd9 as dx10
    ,icd_dgns_vrsn_cd9 as dx10_ver
    ,icd_dgns_cd10 as dx11
    ,icd_dgns_vrsn_cd10 as dx11_ver
    ,icd_dgns_cd11 as dx12
    ,icd_dgns_vrsn_cd11 as dx12_ver
    ,icd_dgns_cd12 as dx13
    ,icd_dgns_vrsn_cd12 as dx13_ver
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_bcarrier_claims_k
    
    --2017 data
    union
    select
    --top 100
    bene_id as id_mcare
    ,clm_id as claim_header_id
    ,clm_from_dt as first_service_date
    ,clm_thru_dt as last_service_date
    ,nch_clm_type_cd as claim_type
    ,carr_clm_pmt_dnl_cd as denial_code
    ,carr_clm_blg_npi_num as provider_billing_npi
    ,rfr_physn_npi as provider_referring_npi
    ,cpo_org_npi_num as provider_cpo_npi
    ,carr_clm_sos_npi_num as provider_sos_npi
    ,prncpal_dgns_cd as dx01
    ,prncpal_dgns_vrsn_cd as dx01_ver
    ,icd_dgns_cd1 as dx02
    ,icd_dgns_vrsn_cd1 as dx02_ver
    ,icd_dgns_cd2 as dx03
    ,icd_dgns_vrsn_cd2 as dx03_ver
    ,icd_dgns_cd3 as dx04
    ,icd_dgns_vrsn_cd3 as dx04_ver
    ,icd_dgns_cd4 as dx05
    ,icd_dgns_vrsn_cd4 as dx05_ver
    ,icd_dgns_cd5 as dx06
    ,icd_dgns_vrsn_cd5 as dx06_ver
    ,icd_dgns_cd6 as dx07
    ,icd_dgns_vrsn_cd6 as dx07_ver
    ,icd_dgns_cd7 as dx08
    ,icd_dgns_vrsn_cd7 as dx08_ver
    ,icd_dgns_cd8 as dx09
    ,icd_dgns_vrsn_cd8 as dx09_ver
    ,icd_dgns_cd9 as dx10
    ,icd_dgns_vrsn_cd9 as dx10_ver
    ,icd_dgns_cd10 as dx11
    ,icd_dgns_vrsn_cd10 as dx11_ver
    ,icd_dgns_cd11 as dx12
    ,icd_dgns_vrsn_cd11 as dx12_ver
    ,icd_dgns_cd12 as dx13
    ,icd_dgns_vrsn_cd12 as dx13_ver
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_bcarrier_claims_k_17;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.mcare_bcarrier_claims_qa_f <- function() {
  
  #load expected counts from YAML file
  table_config <- yaml::yaml.load(httr::GET(config_url))
  
  #confirm that row counts match expected
  row_sum_union <- dbGetQuery(conn = db_claims, glue_sql(
    "select count(*) as qa from PHClaims.stage.mcare_bcarrier_claims_load;",
    .con = db_claims))
  
  if(table_config$row_count_expected == row_sum_union$qa) {
    print("row sums match")
  }
  if(table_config$row_count_expected > row_sum_union$qa) {
    print(paste0("union row sum more, single-year sum: ", row_sum_single$qa, " , union sum: ", row_sum_union$qa))
  }
  if(table_config$row_count_expected < row_sum_union$qa) {
    print(paste0("union row sum less, single-year sum: ", row_sum_single$qa, " , union sum: ", row_sum_union$qa))
  }
  
  #confirm that col count matches expected
  col_count <- dbGetQuery(conn = db_claims, glue_sql(
    " select count(*) as col_cnt
      from information_schema.columns
      where table_catalog = 'PHClaims' -- the database
      and table_schema = 'stage'
      and table_name = 'mcare_' + 'bcarrier_claims_load';",
    .con = db_claims))
  
  if(table_config$col_count_expected == col_count$col_cnt) {
    print("col count matches expected")
  } else {
    print("col count does not match!!")
  }
}