#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_LINE
# Eli Kern, PHSKC (APDE)
#
# 2019-10

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/apcd/master_apcd_analytic.R

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
    medical_claim_header_id as claim_header_id,
    medical_claim_service_line_id as claim_line_id,
    line_counter,
    first_service_dt as first_service_date,
    last_service_dt as last_service_date,
    charge_amt,
    revenue_code,
    place_of_service_code,
    getdate() as last_run
    from PHClaims.stage.apcd_medical_claim
    --exclude denined/orphaned claims
    where denied_claim_flag = 'N' and orphaned_adjustment_flag = 'N';",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.apcd_claim_line_f <- function(year = NULL) {
  
  table_name <- paste0("apcd_elig_plr_", year)

  #all members are distinct
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.{`table_name`}' as 'table', '# members with >1 row, expect 0' as qa_type, count(a.id_apcd) as qa
      from (
        select id_apcd, count(id_apcd) as id_cnt
        from stage.{`table_name`}
        group by id_apcd
      ) as a
      where a.id_cnt > 1;",
    .con = db_claims))
  
  #wa vs ach 11-month
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.{`table_name`}' as 'table', 'in state but not ach 11-month cohort, expect < 91.7' as qa_type, max(geo_ach_covper) as qa
      from stage.{`table_name`}
      where performance_11_wa = 1 and performance_11_ach = 0;",
    .con = db_claims))
  
  #wa vs ach 11-month
  res3 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.{`table_name`}' as 'table', 'in state but not ach 7-month cohort, expect <58.3' as qa_type, max(geo_ach_covper) as qa
      from stage.{`table_name`}
      where performance_7_wa = 1 and performance_7_ach = 0;",
    .con = db_claims))
  
  #number of members in WA state with non-WA county
  res4 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.{`table_name`}' as 'table', 'non-WA county for WA resident, expect 0' as qa_type, count(id_apcd) as qa
      from stage.{`table_name`}
      where geo_wa = 1 and geo_county is null;",
    .con = db_claims))
  
  #number of non-WA residents
  res5 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.{`table_name`}' as 'table', 'non-WA residents, expect 0' as qa_type, count(id_apcd) as qa
      from stage.{`table_name`}
      where geo_wa = 0 and geo_county is not null;",
    .con = db_claims))
  
  #number of overall Medicaid members
  res6 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.{`table_name`}' as 'table', '# of overall Medicaid members' as qa_type, count(id_apcd) as qa
      from stage.{`table_name`}
      where overall_mcaid = 1;",
    .con = db_claims))
  
  #number of members with medical but not pharmacy Medicaid coverage
  res7 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.{`table_name`}' as 'table', '# of members with medical but not pharmacy Medicaid' as qa_type, count(id_apcd) as qa
      from stage.{`table_name`}
      where overall_mcaid_med = 1 and overall_mcaid_pharm = 0;",
    .con = db_claims))
  
  #number of members with pharmacy but not medical Medicaid coverage
  res8 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.{`table_name`}' as 'table', '# of members with pharmacy but not medical Medicaid, expect low' as qa_type, count(id_apcd) as qa
      from stage.{`table_name`}
      where overall_mcaid_med = 0 and overall_mcaid_pharm = 1;",
    .con = db_claims))
  
  #number of members with day counts over 365
  res9 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.{`table_name`}' as 'table', '# of members with day counts >365, expect 0' as qa_type, count(*) as qa
      from stage.{`table_name`}
      where med_total_covd > 365 or med_medicaid_covd > 365 or med_commercial_covd > 365 or
        med_medicare_covd > 365 or dual_covd > 365 or geo_ach_covd > 365 or pharm_total_covd > 365 or
        pharm_medicaid_covd > 365 or pharm_medicare_covd > 365 or pharm_commercial_covd > 365;",
    .con = db_claims))
  
  #number of members with percents > 100
  res10 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.{`table_name`}' as 'table', '# of members with percents >100, expect 0' as qa_type, count(*) as qa
      from stage.{`table_name`}
      where med_total_covper > 100 or med_medicaid_covper > 100 or med_commercial_covper > 100 or
        med_medicare_covper > 100 or dual_covper > 100 or geo_ach_covper > 100 or pharm_total_covper > 100 or
        pharm_medicaid_covper > 100 or pharm_medicare_covper > 100 or pharm_commercial_covper > 100;",
    .con = db_claims))
  
  #number of overall Medicaid members who are out of state
  res11 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.{`table_name`}' as 'table', '# of overall Medicaid members out of state, expect 0' as qa_type, count(id_apcd) as qa
      from stage.{`table_name`}
      where overall_mcaid = 1 and geo_county is null;",
    .con = db_claims))
  
  #number of 11-month cohort members who are out of state
  res12 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.{`table_name`}' as 'table', '# of 11-month cohort members out of state, expect 0' as qa_type, count(id_apcd) as qa
      from stage.{`table_name`}
      where performance_11_wa = 1 and geo_county is null;",
    .con = db_claims))

  #number of 7-month cohort members who are out of state
  res13 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.{`table_name`}' as 'table', '# of 7-month cohort members out of state, expect 0' as qa_type, count(id_apcd) as qa
      from stage.{`table_name`}
      where performance_7_wa = 1 and geo_county is null;",
    .con = db_claims))
  
  #number of 11-month cohort members who are in ACH but not state cohort
  res14 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.{`table_name`}' as 'table', '# of 11-month cohort members in ACH but not state cohort, expect 0' as qa_type, count(id_apcd) as qa
      from stage.{`table_name`}
      where performance_11_wa = 0 and performance_11_ach = 1;",
    .con = db_claims))
  
  #number of 7-month cohort members who are in ACH but not state cohort
  res15 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.{`table_name`}' as 'table', '# of 7-month cohort members in ACH but not state cohort, expect 0' as qa_type, count(id_apcd) as qa
      from stage.{`table_name`}
      where performance_7_wa = 0 and performance_7_ach = 1;",
    .con = db_claims))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}