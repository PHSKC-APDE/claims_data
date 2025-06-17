#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_ELIG_MONTH
# Eli Kern, PHSKC (APDE)
#
# 2025-06

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_elig_month_f <- function(conn = NULL,
                                         config_url = NULL) {
  
  ### Set up variables from config file
  if (is.null(config_url) == T){
    stop("A URL must be specified in config_url")
  } else {
    config <- yaml::yaml.load(RCurl::getURL(config_url))
  }
  
  from_schema <- config[["from_schema"]]
  from_table <- config[["from_table"]]
  to_schema <- config[["to_schema"]]
  to_table <- config[["to_table"]]
  ref_schema <- config[["ref_schema"]]
  ref_apcd_zip_group <- config[["ref_apcd_zip_group"]]
  ref_geo_county_code_wa <- config[["ref_geo_county_code_wa"]]
  ref_date <- config[["ref_date"]]
  apcd_elig_demo <- config[["apcd_elig_demo"]]
  apcd_eligibility <- config[["apcd_eligibility"]]
  
  message("Creating ", to_schema, ".", to_table, ". This will take ~40 minutes to run.")
  
  ### Run SQL query
  odbc::dbGetQuery(conn, glue::glue_sql(
    "
    --Use member_month_detail table to create coverage group variables
    with apcd_month_temp1 as (
      select
      internal_member_id,
      convert(date, cast(year_month as varchar(200)) + '01') as from_date,
      dateadd(day, -1, dateadd(month, 1, convert(date, cast(year_month as varchar(200)) + '01'))) as to_date,
      year_month,
      zip_code, 
      --create empirical dual flag based on presence of medicaid and medicare ID
      case when (med_medicaid_eligibility_id is not null or rx_medicaid_eligibility_id is not null or dental_medicaid_eligibility_id is not null)
          and (med_medicare_eligibility_id is not null or rx_medicare_eligibility_id is not null or dental_medicare_eligibility_id is not null)
          then 1 else 0
      end as dual_flag,
            
      --create coverage categorical variable for medical coverage
      case
      when med_medicaid_eligibility_id is not null and med_commercial_eligibility_id is null and med_medicare_eligibility_id is null then 1 --Medicaid only
      when med_medicaid_eligibility_id is null and med_commercial_eligibility_id is null and med_medicare_eligibility_id is not null then 2 --Medicare only
      when med_medicaid_eligibility_id is null and med_commercial_eligibility_id is not null and med_medicare_eligibility_id is null then 3 --Commercial only
      when med_medicaid_eligibility_id is not null and med_commercial_eligibility_id is null and med_medicare_eligibility_id is not null then 4 -- Medicaid-Medicare dual
      when med_medicaid_eligibility_id is not null and med_commercial_eligibility_id is not null and med_medicare_eligibility_id is null then 5 --Medicaid-commercial dual
      when med_medicaid_eligibility_id is null and med_commercial_eligibility_id is not null and med_medicare_eligibility_id is not null then 6 --Medicare-commercial dual
      when med_medicaid_eligibility_id is not null and med_commercial_eligibility_id is not null and med_medicare_eligibility_id is not null then 7 -- All three
      when medical_eligibility_id is not null then 8 -- Unknown market
      else 0 --no medical coverage
      end as med_covgrp,
            
      --create coverage categorical variable for pharmacy coverage
      case
      when rx_medicaid_eligibility_id is not null and rx_commercial_eligibility_id is null and rx_medicare_eligibility_id is null then 1 --Medicaid only
      when rx_medicaid_eligibility_id is null and rx_commercial_eligibility_id is null and rx_medicare_eligibility_id is not null then 2 --Medicare only
      when rx_medicaid_eligibility_id is null and rx_commercial_eligibility_id is not null and rx_medicare_eligibility_id is null then 3 --Commercial only
      when rx_medicaid_eligibility_id is not null and rx_commercial_eligibility_id is null and rx_medicare_eligibility_id is not null then 4 -- Medicaid-Medicare dual
      when rx_medicaid_eligibility_id is not null and rx_commercial_eligibility_id is not null and rx_medicare_eligibility_id is null then 5 --Medicaid-commercial dual
      when rx_medicaid_eligibility_id is null and rx_commercial_eligibility_id is not null and rx_medicare_eligibility_id is not null then 6 --Medicare-commercial dual
      when rx_medicaid_eligibility_id is not null and rx_commercial_eligibility_id is not null and rx_medicare_eligibility_id is not null then 7 -- All three
      when pharmacy_eligibility_id is not null then 8 -- Unknown market
      else 0 --no pharm coverage
      end as pharm_covgrp,
            
      --create coverage categorical variable for dental coverage
      case
      when dental_medicaid_eligibility_id is not null and dental_commercial_eligibility_id is null and dental_medicare_eligibility_id is null then 1 --Medicaid only
      when dental_medicaid_eligibility_id is null and dental_commercial_eligibility_id is null and dental_medicare_eligibility_id is not null then 2 --Medicare only
      when dental_medicaid_eligibility_id is null and dental_commercial_eligibility_id is not null and dental_medicare_eligibility_id is null then 3 --Commercial only
      when dental_medicaid_eligibility_id is not null and dental_commercial_eligibility_id is null and dental_medicare_eligibility_id is not null then 4 -- Medicaid-Medicare dual
      when dental_medicaid_eligibility_id is not null and dental_commercial_eligibility_id is not null and dental_medicare_eligibility_id is null then 5 --Medicaid-commercial dual
      when dental_medicaid_eligibility_id is null and dental_commercial_eligibility_id is not null and dental_medicare_eligibility_id is not null then 6 --Medicare-commercial dual
      when dental_medicaid_eligibility_id is not null and dental_commercial_eligibility_id is not null and dental_medicare_eligibility_id is not null then 7 -- All three
      when dental_eligibility_id is not null then 8 -- Unknown market
      else 0 --no dental coverage
      end as dental_covgrp
            
      from {`from_schema`}.{`from_table`}
    )
    
    --Add additional coverage flag and geo variables, calculate cov time, add time period vars, and insert into table shell
    insert into {`to_schema`}.{`to_table`}
    select 
    a.internal_member_id as id_apcd,
    a.from_date,
    a.to_date,
    e.year_month,
    e.year_quarter,
    e.[year],
    a.med_covgrp,
    a.pharm_covgrp,
    a.dental_covgrp,
    --Binary flags for medical, pharmacy, dental coverage type
    case when a.med_covgrp in (1,4,5,7) then 1 else 0 end as med_medicaid,
    case when a.med_covgrp in (2,4,6,7) then 1 else 0 end as med_medicare,
    case when a.med_covgrp in (3,5,6,7) then 1 else 0 end as med_commercial,
    case when a.med_covgrp = 8 then 1 else 0 end as med_unknown,
    case when a.pharm_covgrp in (1,4,5,7) then 1 else 0 end as pharm_medicaid,
    case when a.pharm_covgrp in (2,4,6,7) then 1 else 0 end as pharm_medicare,
    case when a.pharm_covgrp in (3,5,6,7) then 1 else 0 end as pharm_commercial,
    case when a.pharm_covgrp = 8 then 1 else 0 end as pharm_unknown,
    case when a.dental_covgrp in (1,4,5,7) then 1 else 0 end as dental_medicaid,
    case when a.dental_covgrp in (2,4,6,7) then 1 else 0 end as dental_medicare,
    case when a.dental_covgrp in (3,5,6,7) then 1 else 0 end as dental_commercial,
    case when a.dental_covgrp = 8 then 1 else 0 end as dental_unknown,
    a.dual_flag as dual,
    a.zip_code as geo_zip,
    d.geo_county_code_fips as geo_county_code,
    b.zip_group_desc as geo_county,
    c.zip_group_code as geo_ach_code,
    c.zip_group_desc as geo_ach,
    case when b.zip_group_desc is not null then 1 else 0 end as geo_wa,
    case when b.zip_group_desc = 'King' then 1 else 0 end as geo_kc,
    datediff(day, a.from_date, a.to_date) + 1 as cov_time_day,
    getdate() as last_run
    from apcd_month_temp1 as a
    left join (select distinct zip_code, zip_group_desc from {`ref_schema`}.{`ref_apcd_zip_group`} where zip_group_type_desc = 'County') as b
    on a.zip_code = b.zip_code
    left join (select distinct zip_code, zip_group_code, zip_group_desc from {`ref_schema`}.{`ref_apcd_zip_group`} where left(zip_group_type_desc, 3) = 'Acc') as c
    on a.zip_code = c.zip_code
    left join {`ref_schema`}.{`ref_geo_county_code_wa`} as d
    on b.zip_group_desc = d.geo_county_name
    left join (select distinct year_month, year_quarter, [year] from {`ref_schema`}.{`ref_date`}) as e
    on a.year_month = e.year_month;",
    .con = conn))
}

#### Table-level QA script ####
qa_stage.apcd_elig_month_f <- function(conn = NULL,
                                         config_url = NULL) {
  
  ### Set up variables from config file
  if (is.null(config_url) == T){
    stop("A URL must be specified in config_url")
  } else {
    config <- yaml::yaml.load(RCurl::getURL(config_url))
  }
  
  from_schema <- config[["from_schema"]]
  from_table <- config[["from_table"]]
  to_schema <- config[["to_schema"]]
  to_table <- config[["to_table"]]
  ref_schema <- config[["ref_schema"]]
  ref_apcd_zip_group <- config[["ref_apcd_zip_group"]]
  ref_geo_county_code_wa <- config[["ref_geo_county_code_wa"]]
  ref_date <- config[["ref_date"]]
  apcd_elig_demo <- config[["apcd_elig_demo"]]
  apcd_eligibility <- config[["apcd_eligibility"]]
  
  ### Run QA code
  res1 <- dbGetQuery(conn = conn, glue_sql(
    "select '{`to_schema`}.{`to_table`}' as 'table', 'member count, expect match to raw tables' as qa_type, count(distinct id_apcd) as qa
    from {`to_schema`}.{`to_table`}",
    .con = conn))
  res2 <- dbGetQuery(conn = conn, glue_sql(
    "select '{`from_schema`}.{`from_table`}' as 'table', 'member count, expect match to timevar' as qa_type, count(distinct internal_member_id) as qa
    from {`from_schema`}.{`from_table`}",
    .con = conn))
  res3 <- dbGetQuery(conn = conn, glue_sql(
    "select '{`from_schema`}.{`apcd_elig_demo`}' as 'table', 'member count, expect match to timevar' as qa_type, count(distinct id_apcd) as qa
    from {`from_schema`}.{`apcd_elig_demo`}",
    .con = conn))
  res4 <- dbGetQuery(conn = conn, glue_sql(
    "select '{`to_schema`}.{`to_table`}' as 'table', 'member count, King 2016, expect match to member_month' as qa_type, count(distinct id_apcd) as qa
    from {`to_schema`}.{`to_table`}
      where from_date <= '2016-12-31' and to_date >= '2016-01-01'
      and geo_ach = 'HealthierHere'",
    .con = conn))
  res5 <- dbGetQuery(conn = conn, glue_sql(
    "select '{`from_schema`}.{`from_table`}' as 'table', 'member count, King 2016, expect match to timevar' as qa_type, count(distinct internal_member_id) as qa
    from {`from_schema`}.{`from_table`}
      where left(year_month,4) = '2016'
      and zip_code in (select zip_code from {`ref_schema`}.{`ref_apcd_zip_group`} where zip_group_desc = 'King' and zip_group_type_desc = 'County')",
    .con = conn))
  res6 <- dbGetQuery(conn = conn, glue_sql(
    "select '{`from_schema`}.{`apcd_eligibility`}' as 'table', 'member count, King 2016, expect slightly more than timevar' as qa_type, count(distinct internal_member_id) as qa
    from {`from_schema`}.{`apcd_eligibility`}
      where eligibility_start_dt <= '2016-12-31' and eligibility_end_dt >= '2016-01-01'
      and zip in (select zip_code from {`ref_schema`}.{`ref_apcd_zip_group`} where zip_group_desc = 'King' and zip_group_type_desc = 'County')",
    .con = conn))
  res7 <- dbGetQuery(conn = conn, glue_sql(
    "select '{`to_schema`}.{`to_table`}' as 'table', 'count of member elig segments with no coverage, expect 0' as qa_type, count(distinct id_apcd) as qa
    from {`to_schema`}.{`to_table`}
    where med_covgrp = 0 and pharm_covgrp = 0 and dental_covgrp = 0;",
    .con = conn))
  res8 <- dbGetQuery(conn = conn, glue_sql(
    "select '{`to_schema`}.{`to_table`}' as 'table', 'max to_date, expect max to_date of latest extract' as qa_type,
    cast(left(max(to_date),4) + SUBSTRING(cast(max(to_date) as varchar(255)),6,2) + right(max(to_date),2) as integer) as qa
    from {`to_schema`}.{`to_table`};",
    .con = conn))
  res9 <- dbGetQuery(conn = conn, glue_sql(
    "select '{`to_schema`}.{`to_table`}' as 'table', 'mcaid-mcare duals with dual flag = 0, expect 0' as qa_type, count(*) as qa
    from {`to_schema`}.{`to_table`}
    where (med_covgrp = 4 or pharm_covgrp = 4) and dual = 0;",
    .con = conn))
  res10 <- dbGetQuery(conn = conn, glue_sql(
    "select '{`to_schema`}.{`to_table`}' as 'table', 'non-WA resident segments with non-null county name, expect 0' as qa_type, count(*) as qa
    from {`to_schema`}.{`to_table`}
    where geo_wa = 0 and geo_county is not null;",
    .con = conn))
  res11 <- dbGetQuery(conn = conn, glue_sql(
    "select '{`to_schema`}.{`to_table`}' as 'table', 'WA resident segments with null county name, expect 0' as qa_type, count(*) as qa
    from {`to_schema`}.{`to_table`}
    where geo_wa = 1 and geo_county is null;",
    .con = conn))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}