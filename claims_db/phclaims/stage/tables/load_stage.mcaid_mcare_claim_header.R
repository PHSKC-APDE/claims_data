#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCAID_MCARE_CLAIM_HEADER
# Eli Kern, PHSKC (APDE), 2019-10
# Alastair Mathesonm PHSKC (APDE), 2020-01
#
# This code is designed to be run as part of the master Medicaid/Medicare script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_mcare_analytic.R
#


#### Create table ####
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_claim_header.yaml",
               overall = T, ind_yr = F, overwrite = T, test_mode = F)


#### Load script ####
# Run time: 62 min
system.time(DBI::dbExecute(db_claims, glue::glue_sql("
    -------------------
    --STEP 1: Union mcaid and mcare claim header tables and insert into table shell
    --Run time: 62 min
    -------------------
    insert into PHClaims.stage.mcaid_mcare_claim_header with (tablock)
    
    --Medicaid claim header
    select
    b.id_apde
    ,'mcaid' as source_desc
    ,cast(a.claim_header_id as varchar(255)) --because mcare uses alpha characters
    ,a.clm_type_mcaid_id --note to normalize column name
    ,claim_type_mcare_id = null
    ,a.claim_type_id
    ,file_type_mcare = null
    ,a.first_service_date
    ,a.last_service_date
    ,a.patient_status
    ,a.admsn_source
    ,a.admsn_date
    ,a.admsn_time
    ,a.dschrg_date
    ,a.place_of_service_code
    ,a.type_of_bill_code
    ,a.clm_status_code
    ,a.billing_provider_npi
    ,a.drvd_drg_code
    ,a.insrnc_cvrg_code
    ,a.last_pymnt_date
    ,a.bill_date
    ,a.system_in_date
    ,a.claim_header_id_date
    ,a.primary_diagnosis
    ,a.icdcm_version
    ,a.primary_diagnosis_poa
    ,a.mental_dx1
    ,a.mental_dxany
    ,a.mental_dx_rda_any
    ,a.sud_dx_rda_any
    ,a.maternal_dx1
    ,a.maternal_broad_dx1
    ,a.newborn_dx1
    ,a.ed
    ,a.ed_nohosp
    ,a.ed_bh
    ,a.ed_avoid_ca
    ,a.ed_avoid_ca_nohosp
    ,a.ed_ne_nyu
    ,a.ed_pct_nyu
    ,a.ed_pa_nyu
    ,a.ed_npa_nyu
    ,a.ed_mh_nyu
    ,a.ed_sud_nyu
    ,a.ed_alc_nyu
    ,a.ed_injury_nyu
    ,a.ed_unclass_nyu
    ,a.ed_emergent_nyu
    ,a.ed_nonemergent_nyu
    ,a.ed_intermediate_nyu
    ,a.inpatient
    ,a.ipt_medsurg
    ,a.ipt_bh
    ,a.intent
    ,a.mechanism
    ,a.sdoh_any
    ,a.ed_sdoh
    ,a.ipt_sdoh
    ,a.ccs
    ,a.ccs_description
    ,a.ccs_description_plain_lang
    ,a.ccs_mult1
    ,a.ccs_mult1_description
    ,a.ccs_mult2
    ,a.ccs_mult2_description
    ,a.ccs_mult2_plain_lang
    ,a.ccs_final_description
    ,a.ccs_final_plain_lang
    ,{Sys.time()} as last_run
    from PHClaims.final.mcaid_claim_header as a
    left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
    on a.id_mcaid = b.id_mcaid
    
    union
    
    --Medicare claim header
    select
    b.id_apde
    ,'mcare' as source_desc
    ,a.claim_header_id
    ,clm_type_mcaid_id = null --note to normalize column name
    ,a.claim_type_mcare_id
    ,a.claim_type_id
    ,a.file_type_mcare
    ,a.first_service_date
    ,a.last_service_date
    ,patient_status = null
    ,admsn_source = null
    ,admsn_date = null
    ,admsn_time = null
    ,dschrg_date = null
    ,place_of_service_code = null
    ,type_of_bill_code = null
    ,clm_status_code = null
    ,billing_provider_npi = null
    ,drvd_drg_code = null
    ,insrnc_cvrg_code = null
    ,last_pymnt_date = null
    ,bill_date = null
    ,system_in_date = null
    ,claim_header_id_date = null
    ,primary_diagnosis = null
    ,icdcm_version = null
    ,primary_diagnosis_poa = null
    ,mental_dx1 = null
    ,mental_dxany = null
    ,mental_dx_rda_any = null
    ,sud_dx_rda_any = null
    ,maternal_dx1 = null
    ,maternal_broad_dx1 = null
    ,newborn_dx1 = null
    ,ed = null
    ,ed_nohosp = null
    ,ed_bh = null
    ,ed_avoid_ca = null
    ,ed_avoid_ca_nohosp = null
    ,ed_ne_nyu = null
    ,ed_pct_nyu = null
    ,ed_pa_nyu = null
    ,ed_npa_nyu = null
    ,ed_mh_nyu = null
    ,ed_sud_nyu = null
    ,ed_alc_nyu = null
    ,ed_injury_nyu = null
    ,ed_unclass_nyu = null
    ,ed_emergent_nyu = null
    ,ed_nonemergent_nyu = null
    ,ed_intermediate_nyu = null
    ,inpatient = null
    ,ipt_medsurg = null
    ,ipt_bh = null
    ,intent = null
    ,mechanism = null
    ,sdoh_any = null
    ,ed_sdoh = null
    ,ipt_sdoh = null
    ,ccs = null
    ,ccs_description = null
    ,ccs_description_plain_lang = null
    ,ccs_mult1 = null
    ,ccs_mult1_description = null
    ,ccs_mult2 = null
    ,ccs_mult2_description = null
    ,ccs_mult2_plain_lang = null
    ,ccs_final_description = null
    ,ccs_final_plain_lang = null
    ,{Sys.time()} as last_run
    from PHClaims.final.mcare_claim_header as a
    left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
    on a.id_mcare = b.id_mcare;",
    .con = db_claims)))


#### Table-level QA script ####
qa_stage.mcaid_mcare_claim_header_f <- function() {
  
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcaid_mcare_claim_header' as 'table', 'count_total' as qa_type, count(*) as qa from stage.mcaid_mcare_claim_header",
    .con = db_claims))
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcaid_mcare_claim_header' as 'table', 'count_mcaid' as qa_type, count(*) as qa from stage.mcaid_mcare_claim_header
      where source_desc = 'mcaid'",
    .con = db_claims))
  res3 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcaid_mcare_claim_header' as 'table', 'count_mcare' as qa_type, count(*) as qa from stage.mcaid_mcare_claim_header
      where source_desc = 'mcare'",
    .con = db_claims))
  res4 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'final.mcaid_claim_header' as 'table', 'count_mcaid' as qa_type, count(*) as qa from final.mcaid_claim_header",
    .con = db_claims))
  res5 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'final.mcare_claim_header' as 'table', 'count_mcare' as qa_type, count(*) as qa from final.mcare_claim_header",
    .con = db_claims))
  res_final <- bind_rows(res1, res2, res3, res4, res5)
}

### Run QA
system.time(qa_mcaid_mcare_claim_header <- qa_stage.mcaid_mcare_claim_header_f())


#### Archive current table ####
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcaid_mcare_claim_header")


#### Alter schema ####
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcaid_mcare_claim_header")


#### Create clustered columnstore index ####
# Run time: 21 min
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_final_mcaid_mcare_claim_header on final.mcaid_mcare_claim_header")))
