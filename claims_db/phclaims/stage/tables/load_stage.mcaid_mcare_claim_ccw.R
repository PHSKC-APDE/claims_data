#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCAID_MCARE_CLAIM_CCW
# Eli Kern, PHSKC (APDE)
#
# 2019-10

### Eventually run from master analytic script

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, RCurl, configr, glue)

db_claims <- dbConnect(odbc(), "PHClaims51")

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/alter_schema.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/claim_ccw.R")


#### Create table ####
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_claim_icdcm_header.yaml",
               overall = T, ind_yr = F, overwrite = T, test_mode = F)


#### Load script ####
load_stage.mcaid_mcare_claim_icdcm_header_f <- function() {
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "
    -------------------
    --STEP 1: Union mcaid and mcare claim ICD-CM header tables and insert into table shell
    --Run time: 18 min
    -------------------
    insert into PHClaims.stage.mcaid_mcare_claim_icdcm_header with (tablock)
    
    --Medicaid claim ICD-CM header
    select
    b.id_apde
    ,'mcaid' as source_desc
    ,cast(a.claim_header_id as varchar(255)) --because mcare uses alpha characters
    ,a.first_service_date
    ,a.last_service_date
    ,a.icdcm_raw
    ,a.icdcm_norm
    ,a.icdcm_version
    ,a.icdcm_number
    ,file_type_mcare = null
    ,getdate() as last_run
    from PHClaims.final.mcaid_claim_icdcm_header as a
    left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
    on a.id_mcaid = b.id_mcaid
    
    union
    
    --Medicare claim ICD-CM header
    select
    b.id_apde
    ,'mcare' as source_desc
    ,a.claim_header_id
    ,first_service_date = null
    ,last_service_date = null
    ,a.icdcm_raw
    ,a.icdcm_norm
    ,a.icdcm_version
    ,cast(a.icdcm_number as varchar(255)) collate SQL_Latin1_General_CP1_CI_AS --resolve collation conflict
    ,a.filetype as file_type_mcare
    ,getdate() as last_run
    from PHClaims.final.mcare_claim_icdcm_header as a
    left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
    on a.id_mcare = b.id_mcare;",
    .con = db_claims))
}


### Run load function
# Run time: 18 min
system.time(load_stage.mcaid_mcare_claim_icdcm_header_f())


#### Table-level QA script ####
qa_stage.mcaid_mcare_claim_icdcm_header_f <- function() {
  
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcaid_mcare_claim_icdcm_header' as 'table', 'count_total' as qa_type, count(*) as qa from stage.mcaid_mcare_claim_icdcm_header",
    .con = db_claims))
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcaid_mcare_claim_icdcm_header' as 'table', 'count_mcaid' as qa_type, count(*) as qa from stage.mcaid_mcare_claim_icdcm_header
      where source_desc = 'mcaid'",
    .con = db_claims))
  res3 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.mcaid_mcare_claim_icdcm_header' as 'table', 'count_mcare' as qa_type, count(*) as qa from stage.mcaid_mcare_claim_icdcm_header
      where source_desc = 'mcare'",
    .con = db_claims))
  res4 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'final.mcaid_claim_icdcm_header' as 'table', 'count_mcaid' as qa_type, count(*) as qa from final.mcaid_claim_icdcm_header",
    .con = db_claims))
  res5 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'final.mcare_claim_icdcm_header' as 'table', 'count_mcare' as qa_type, count(*) as qa from final.mcare_claim_icdcm_header",
    .con = db_claims))
  res_final <- bind_rows(res1, res2, res3, res4, res5)
}

### Run QA
system.time(qa_mcaid_mcare_claim_icdcm_header <- qa_stage.mcaid_mcare_claim_icdcm_header_f())


#### Archive current table ####
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcaid_mcare_claim_icdcm_header")


#### Alter schema ####
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcaid_mcare_claim_icdcm_header")


#### Create clustered columnstore index ####
# Run time: 18 min
system.time(dbSendQuery(conn = db_claims, glue_sql(
  "create clustered columnstore index idx_ccs_final_mcaid_mcare_claim_icdcm_header on final.mcaid_mcare_claim_icdcm_header")))
