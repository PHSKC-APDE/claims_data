#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCAID_MCARE_claim_procedure
# Eli Kern, PHSKC (APDE), 2020-02
# Alastair Mathesonm PHSKC (APDE), 2020-01
#
# This code is designed to be run as part of the master Medicaid/Medicare script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_mcare_analytic.R
#

#### Load script ####
load_stage.mcaid_mcare_claim_procedure_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "--Code to load data to stage.mcaid_mcare_claim_procedure
    --Union of mcaid and mcare claim tables
    --Eli Kern (PHSKC-APDE)
    --2020-02
    --Run time: X min
    
    -------------------
    --STEP 1: Union mcaid and mcare tables and insert into table shell
    -------------------
    insert into PHClaims.stage.mcaid_mcare_claim_procedure with (tablock)
    
    --Medicaid claims
    select
    --top 100
    b.id_apde
    ,'mcaid' as source_desc
    ,cast(a.claim_header_id as varchar(255)) --because mcare uses alpha characters
    ,a.first_service_date
    ,a.last_service_date
    ,a.procedure_code
    ,a.procedure_code_number
    ,a.modifier_1
    ,a.modifier_2
    ,a.modifier_3
    ,a.modifier_4
    ,filetype_mcare = null
    ,getdate() as last_run
    from PHClaims.final.mcaid_claim_procedure as a
    left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
    on a.id_mcaid = b.id_mcaid
    
    union
    
    --Medicare claims
    select
    --top 100
    b.id_apde
    ,'mcare' as source_desc
    ,a.claim_header_id
    ,first_service_date
    ,last_service_date
    ,a.procedure_code
    ,a.procedure_code_number
    ,a.modifier_1
    ,a.modifier_2
    ,a.modifier_3
    ,a.modifier_4
    ,a.filetype_mcare
    ,getdate() as last_run
    from PHClaims.final.mcare_claim_procedure as a
    left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
    on a.id_mcare = b.id_mcare;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.mcaid_mcare_claim_procedure_qa_f <- function() {
  
  #confirm that claim row counts match as expected for union
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcaid_mcare_claim_procedure' as 'table', 'row count, expect match with sum of mcaid and mcare' as qa_type,
    count(*) as qa
    from stage.mcaid_mcare_claim_procedure;",
    .con = db_claims))
  
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcaid_claim_procedure' as 'table', 'row count' as qa_type,
    count(*) as qa
    from final.mcaid_claim_procedure;",
    .con = db_claims))
  
  res3 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcare_claim_procedure' as 'table', 'row count' as qa_type,
    count(*) as qa
    from final.mcare_claim_procedure;",
    .con = db_claims))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}
