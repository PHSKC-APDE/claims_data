#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_snf_revenue_center
# Eli Kern, PHSKC (APDE)
#
# 2019-12

### Run from master_mcare_full_union script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcare/master_mcare_full_union.R

#### Load script ####
load_stage.mcare_snf_revenue_center_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "--Code to load data to stage.mcare_snf_revenue_center
    --Union of single-year files
    --Eli Kern (PHSKC-APDE)
    --2019-12
    --Run time: xx min
    
    
    insert into PHClaims.stage.mcare_snf_revenue_center_load with (tablock)
    
    --2014 data
    select
    bene_id as id_mcare
    ,clm_id as claim_header_id
    ,clm_line_num as claim_line_id
    ,rev_cntr as revenue_code
    ,hcpcs_cd as procedure_code_hcpcs
    ,hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1
    ,hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2
    ,hcpcs_3rd_mdfr_cd as procedure_code_hcps_modifier_3
    ,rev_cntr_ide_ndc_upc_num as ndc_code
    ,rev_cntr_ndc_qty as drug_quantity
    ,rev_cntr_ndc_qty_qlfr_cd as drug_uom
    ,rndrng_physn_npi as provider_rendering_npi
    ,rndrng_physn_spclty_cd as provider_rendering_specialty
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_snf_revenue_center_k_14
    
    --2015 data
    union
    select
    bene_id as id_mcare
    ,clm_id as claim_header_id
    ,clm_line_num as claim_line_id
    ,rev_cntr as revenue_code
    ,hcpcs_cd as procedure_code_hcpcs
    ,hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1
    ,hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2
    ,hcpcs_3rd_mdfr_cd as procedure_code_hcps_modifier_3
    ,rev_cntr_ide_ndc_upc_num as ndc_code
    ,rev_cntr_ndc_qty as drug_quantity
    ,rev_cntr_ndc_qty_qlfr_cd as drug_uom
    ,rndrng_physn_npi as provider_rendering_npi
    ,rndrng_physn_spclty_cd as provider_rendering_specialty
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_snf_revenue_center_k_15
    
    --2016 data
    union
    select
    bene_id as id_mcare
    ,clm_id as claim_header_id
    ,clm_line_num as claim_line_id
    ,rev_cntr as revenue_code
    ,hcpcs_cd as procedure_code_hcpcs
    ,hcpcs_1st_mdfr_cd as procedure_code_hcps_modifier_1
    ,hcpcs_2nd_mdfr_cd as procedure_code_hcps_modifier_2
    ,hcpcs_3rd_mdfr_cd as procedure_code_hcps_modifier_3
    ,rev_cntr_ide_ndc_upc_num as ndc_code
    ,rev_cntr_ndc_qty as drug_quantity
    ,rev_cntr_ndc_qty_qlfr_cd as drug_uom
    ,rndrng_physn_npi as provider_rendering_npi
    ,rndrng_physn_spclty_cd as provider_rendering_specialty
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_snf_revenue_center_k_16;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.mcare_snf_revenue_center_qa_f <- function() {
  
  #load expected counts from YAML file
  table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  
  #confirm that row counts match expected
  row_sum_union <- dbGetQuery(conn = db_claims, glue_sql(
    "select count(*) as qa from PHClaims.stage.mcare_snf_revenue_center_load;",
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
      and table_name = 'mcare_' + 'snf_revenue_center_load';",
    .con = db_claims))
  
  if(table_config$col_count_expected == col_count$col_cnt) {
    print("col count matches expected")
  } else {
    print("col count does not match!!")
  }
}