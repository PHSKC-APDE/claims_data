#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_outpatient_revenue_center
# Eli Kern, PHSKC (APDE)
#
# 2019-12

### Run from master_mcare_full_union script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcare/master_mcare_full_union.R

#### Load script ####
load_stage.mcare_outpatient_revenue_center_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "--Code to load data to stage.mcare_outpatient_revenue_center
    --Union of single-year files
    --Eli Kern (PHSKC-APDE)
    --2019-12
    --Run time: 21 min
    
    
    insert into PHClaims.stage.mcare_outpatient_revenue_center_load with (tablock)
    
    --2014 data
    select
    encrypted723beneficiaryid as id_mcare
    ,encryptedclaimid as claim_header_id
    ,claimlinenumber as claim_line_id
    ,revenuecentercode as revenue_code
    ,revenuecenterhealthcarecommonpro as procedure_code_hcpcs
    ,revenuecenterhcpcsinitialmodifie as procedure_code_hcps_modifier_1
    ,revenuecenterhcpcssecondmodifier as procedure_code_hcps_modifier_2
    ,revenuecenteridendcupcnumber as ndc_code
    ,revenuecenterndcquantity as drug_quantity
    ,revenuecenterndcquantityqualifie as drug_uom
    ,revenuecenterrenderingphysiciann as provider_rendering_npi
    ,null as provider_rendering_specialty
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_outpatient_revenue_center_k14
    
    --2015 data
    union
    select
    encrypted723beneficiaryid as id_mcare
    ,encryptedclaimid as claim_header_id
    ,claimlinenumber as claim_line_id
    ,revenuecentercode as revenue_code
    ,revenuecenterhealthcarecommonpro as procedure_code_hcpcs
    ,revenuecenterhcpcsinitialmodifie as procedure_code_hcps_modifier_1
    ,revenuecenterhcpcssecondmodifier as procedure_code_hcps_modifier_2
    ,revenuecenteridendcupcnumber as ndc_code
    ,revenuecenterndcquantity as drug_quantity
    ,revenuecenterndcquantityqualifie as drug_uom
    ,revenuecenterrenderingphysiciann as provider_rendering_npi
    ,revenuecenterrenderingphysicians as provider_rendering_specialty
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_outpatient_revenue_center_k15
    
    --2016 data
    union
    select
    encrypted723beneficiaryid as id_mcare
    ,encryptedclaimid as claim_header_id
    ,claimlinenumber as claim_line_id
    ,revenuecentercode as revenue_code
    ,revenuecenterhealthcarecommonpro as procedure_code_hcpcs
    ,revenuecenterhcpcsinitialmodifie as procedure_code_hcps_modifier_1
    ,revenuecenterhcpcssecondmodifier as procedure_code_hcps_modifier_2
    ,revenuecenteridendcupcnumber as ndc_code
    ,revenuecenterndcquantity as drug_quantity
    ,revenuecenterndcquantityqualifie as drug_uom
    ,revenuecenterrenderingphysiciann as provider_rendering_npi
    ,revenuecenterrenderingphysicians as provider_rendering_specialty
    ,getdate() as last_run
    from PHClaims.load_raw.mcare_outpatient_revenue_center_k16;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.mcare_outpatient_revenue_center_qa_f <- function() {
  
  #load expected counts from YAML file
  table_config <- yaml::yaml.load(RCurl::getURL(config_url))
  
  #confirm that row counts match expected
  row_sum_union <- dbGetQuery(conn = db_claims, glue_sql(
    "select count(*) as qa from PHClaims.stage.mcare_outpatient_revenue_center_load;",
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
      and table_name = 'mcare_' + 'outpatient_revenue_center_load';",
    .con = db_claims))
  
  if(table_config$col_count_expected == col_count$col_cnt) {
    print("col count matches expected")
  } else {
    print("col count does not match!!")
  }
}