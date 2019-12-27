#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_DME_CLAIMS
# Eli Kern, PHSKC (APDE)
#
# 2019-12

### Run from master_mcare_full_union script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcare/master_mcare_full_union.R

#### Load script ####
load_stage.mcare_dme_claims_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "--Code to load data to stage.mcare_dme_claims
    --Union of single-year files
    --Eli Kern (PHSKC-APDE)
    --2019-12
    --Run time: xx min
    
    
    insert into PHClaims.stage.mcare_dme_claims_load with (tablock)
    
    --2015 data
    select
    bene_id as id_mcare
    ,clm_id as claim_header_id
    ,clm_from_dt as first_service_date
    ,clm_thru_dt as last_service_date
    ,nch_clm_type_cd as claim_type
    ,carr_clm_pmt_dnl_cd as denial_code
    ,rfr_physn_npi as provider_referring_npi
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
    from PHClaims.load_raw.mcare_dme_claims_k_15
    
    --2016 data
    union
    select
    bene_id as id_mcare
    ,clm_id as claim_header_id
    ,clm_from_dt as first_service_date
    ,clm_thru_dt as last_service_date
    ,nch_clm_type_cd as claim_type
    ,carr_clm_pmt_dnl_cd as denial_code
    ,rfr_physn_npi as provider_referring_npi
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
    from PHClaims.load_raw.mcare_dme_claims_k_16;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.mcare_dme_claims_qa_f <- function() {
  
  #load expected counts from YAML file
  table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  
  #confirm that row counts match expected
  row_sum_union <- dbGetQuery(conn = db_claims, glue_sql(
    "select count(*) as qa from PHClaims.stage.mcare_dme_claims_load;",
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
      and table_name = 'mcare_' + 'dme_claims_load';",
    .con = db_claims))
  
  if(table_config$col_count_expected == col_count$col_cnt) {
    print("col count matches expected")
  } else {
    print("col count does not match!!")
  }
}