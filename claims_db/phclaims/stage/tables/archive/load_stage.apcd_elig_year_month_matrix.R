#### CODE TO LOAD & QA REF.APCD_ID_YEAR_MONTH_MATRIX
# Eli Kern, PHSKC (APDE)
#
# Planned update: add function for partial update
#
# 2019-10

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_elig_year_month_matrix_full_f <- function(extract_end_date = NULL) {
  
  ### Require extract_end_date
  if (is.null(extract_end_date)) {
    stop("Enter the end date for this APCD extract: \"YYYY-MM-DD\"")
  }
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "
    -------------------
    --STEP 1: Query distinct member IDs from member month table
    -------------------
    if object_id('tempdb..#id') is not null drop table #id;
    select distinct internal_member_id, 1 as flag
    into #id
    from phclaims.stage.apcd_member_month_detail;
    
    -------------------
    --STEP 2: Join distinct member IDs with year-month matrix
    -------------------
    insert into PHClaims.ref.apcd_id_year_month_matrix with (tablock)
    select a.internal_member_id, b.first_day_month, b.last_day_month, getdate() as last_run
    from #id as a
    full join (select first_day_month, last_day_month, 1 as flag from phclaims.ref.date where first_day_month >= '2014-01-01' and last_day_month <= {extract_end_date}) as b
    on a.flag = b.flag;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.apcd_elig_year_month_matrix_f <- function() {
  
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_elig_year_month_matrix' as 'table', 'distinct count' as qa_type, count(distinct id_apcd) as qa from stage.apcd_elig_year_month_matrix",
    .con = db_claims))
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_member_month_detail' as 'table', 'distinct count' as qa_type, count(distinct internal_member_id) as qa from stage.apcd_member_month_detail",
    .con = db_claims))
  res_final <- bind_rows(res1, res2)
  
}