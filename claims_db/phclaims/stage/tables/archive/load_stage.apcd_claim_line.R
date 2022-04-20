#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_LINE
# Eli Kern, PHSKC (APDE)
#
# 2019-10

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_claim_line_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "
    ------------------
    --STEP 1: Select (distinct) desired columns from claim line table
    --Exclude all denied/orphaned claim lines
    -------------------
    insert into PHClaims.stage.apcd_claim_line with (tablock)
    select distinct
    internal_member_id as id_apcd,
    a.medical_claim_header_id as claim_header_id,
    medical_claim_service_line_id as claim_line_id,
    line_counter,
    first_service_dt as first_service_date,
    last_service_dt as last_service_date,
    charge_amt,
    revenue_code,
    place_of_service_code,
    getdate() as last_run
    from PHClaims.stage.apcd_medical_claim as a
    --exclude denined/orphaned claims
    left join PHClaims.ref.apcd_denied_orphaned_header as b
    on a.medical_claim_header_id = b.claim_header_id
    where b.denied_header_min = 0 and b.orphaned_header_min = 0;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.apcd_claim_line_f <- function() {
  
  #compare sum of member ID and claim_line ID columns
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_line' as 'table', 'sum of member ID' as qa_type,
    sum(cast(id_apcd as decimal(38,0))) as qa
    from stage.apcd_claim_line;",
    .con = db_claims))
  
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_line' as 'table', 'sum of claim line ID' as qa_type,
    sum(cast(claim_line_id as decimal(38,0))) as qa
    from stage.apcd_claim_line;",
    .con = db_claims))
  
  res3 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_medical_claim' as 'table', 'sum of member ID' as qa_type,
    sum(cast(internal_member_id as decimal(38,0))) as qa
    from PHClaims.stage.apcd_medical_claim as a
    --exclude denined/orphaned claims
    left join PHClaims.ref.apcd_denied_orphaned_header as b
    on a.medical_claim_header_id = b.claim_header_id
    where b.denied_header_min = 0 and b.orphaned_header_min = 0;",
    .con = db_claims))
  
  res4 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_medical_claim' as 'table', 'sum of claim line ID' as qa_type,
    sum(cast(medical_claim_service_line_id as decimal(38,0))) as qa
    from PHClaims.stage.apcd_medical_claim as a
    --exclude denined/orphaned claims
    left join PHClaims.ref.apcd_denied_orphaned_header as b
    on a.medical_claim_header_id = b.claim_header_id
    where b.denied_header_min = 0 and b.orphaned_header_min = 0;",
    .con = db_claims))
  
  #make sure everyone is in elig_demo
  res5 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_line' as 'table', '# members not in elig_demo, expect 0' as qa_type,
    count(a.id_apcd) as qa
    from stage.apcd_claim_line as a
    left join final.apcd_elig_demo as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = db_claims))
  
  #make sure everyone is in elig_timevar
  res6 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_line' as 'table', '# members not in elig_timevar, expect 0' as qa_type,
    count(a.id_apcd) as qa
    from stage.apcd_claim_line as a
    left join final.apcd_elig_timevar as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = db_claims))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}