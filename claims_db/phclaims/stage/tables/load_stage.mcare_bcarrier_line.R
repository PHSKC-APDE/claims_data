#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_BCARRIER_LINE
# Eli Kern, PHSKC (APDE)
#
# 2019-12

### Run from master_mcare_full_union script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcare/master_mcare_full_union.R

#### Load script ####
load_stage.mcare_bcarrier_line_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "--Code to load data to stage.mcare_bcarrier_line
    --Union of single-year files
    --Eli Kern (PHSKC-APDE)
    --2019-12
    --Run time: 25 min
    --------------------------
    --------------------------
    --Shuva update
    --2/10/2020
    --added 2017 codeblock 
    
    
    insert into PHClaims.stage.mcare_bcarrier_line_load with (tablock)
    
    --2014 data
    select
    ------top 100 
    encrypted723beneficiaryid as id_mcare
    ,encryptedclaimid as claim_header_id
    ,claimlinenumber as claim_line_id
    ,carrierlineperformingnpinumber as provider_rendering_npi
    ,carrierlineperforminggroupnpinum as provider_org_npi
    ,carrierlineprovidertypecode as provider_rendering_type
    ,lineprovidertaxnumber as provider_rendering_tin
    ,carrierlineperformingproviderzip as provider_rendering_zip
    ,linehcfaproviderspecialtycode as provider_rendering_specialty
    ,linehcfatypeservicecode as type_of_service
    ,lineplaceofservicecode as place_of_service_code
    ,linehealthcarecommonprocedurecod as procedure_code_hcpcs
    ,linehcpcsinitialmodifiercode as procedure_code_hcps_modifier_1
    ,linehcpcssecondmodifiercode as procedure_code_hcps_modifier_2
    ,linenchbetoscode as procedure_code_betos
    ,null as provider_billing_zip
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_bcarrier_line_j_14
    
    --2015 data
    union
    select
    ------top 100
    encrypted723beneficiaryid as id_mcare
    ,encryptedclaimid as claim_header_id
    ,claimlinenumber as claim_line_id
    ,carrierlineperformingnpinumber as provider_rendering_npi
    ,carrierlineperforminggroupnpinum as provider_org_npi
    ,carrierlineprovidertypecode as provider_rendering_type
    ,lineprovidertaxnumber as provider_rendering_tin
    ,carrierlineperformingproviderzip as provider_rendering_zip
    ,linehcfaproviderspecialtycode as provider_rendering_specialty
    ,linehcfatypeservicecode as type_of_service
    ,lineplaceofservicecode as place_of_service_code
    ,linehealthcarecommonprocedurecod as procedure_code_hcpcs
    ,linehcpcsinitialmodifiercode as procedure_code_hcps_modifier_1
    ,linehcpcssecondmodifiercode as procedure_code_hcps_modifier_2
    ,linenchbetoscode as procedure_code_betos
    ,lineplaceofserviceposphysicianzi as provider_billing_zip
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_bcarrier_line_k_15
    
    --2016 data
    union
    select
    ------top 100
    bene_id as id_mcare
    ,clm_id as claim_header_id
    ,line_num as claim_line_id
    ,prf_physn_npi as provider_rendering_npi
    ,org_npi_num as provider_org_npi
    ,carr_line_prvdr_type_cd as provider_rendering_type
    ,tax_num as provider_rendering_tin
    ,prvdr_zip as provider_rendering_zip
    ,prvdr_spclty as provider_rendering_specialty
    ,line_cms_type_srvc_cd as type_of_service
    ,line_place_of_srvc_cd as place_of_service_code
    ,hcpcs_cd as procedure_code_hcpcs
    ,hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1
    ,hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2
    ,betos_cd as procedure_code_betos
    ,physn_zip_cd as provider_billing_zip
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_bcarrier_line_k_16
    
    
    
    --2017 data
    union
    select
    ------top 100
    bene_id as id_mcare
    ,clm_id as claim_header_id
    ,line_num as claim_line_id
    ,prf_physn_npi as provider_rendering_npi
    ,org_npi_num as provider_org_npi
    ,carr_line_prvdr_type_cd as provider_rendering_type
    ,tax_num as provider_rendering_tin
    ,prvdr_zip as provider_rendering_zip
    ,prvdr_spclty as provider_rendering_specialty
    ,line_cms_type_srvc_cd as type_of_service
    ,line_place_of_srvc_cd as place_of_service_code
    ,hcpcs_cd as procedure_code_hcpcs
    ,hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1
    ,hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2
    ,betos_cd as procedure_code_betos
    ,physn_zip_cd as provider_billing_zip
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_bcarrier_line_k_17;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.mcare_bcarrier_line_qa_f <- function() {
  
  #load expected counts from YAML file
  table_config <- yaml::yaml.load(httr::GET(config_url))
  
  #confirm that row counts match expected
  row_sum_union <- dbGetQuery(conn = db_claims, glue_sql(
    "select count(*) as qa from PHClaims.stage.mcare_bcarrier_line_load;",
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
      and table_name = 'mcare_' + 'bcarrier_line_load';",
    .con = db_claims))
  
  if(table_config$col_count_expected == col_count$col_cnt) {
    print("col count matches expected")
  } else {
    print("col count does not match!!")
  }
}