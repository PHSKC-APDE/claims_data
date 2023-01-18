#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_ELIG_TIMEVAR
# Eli Kern, PHSKC (APDE)
#
# 2019-10

#2022-04-26 update: Added new variables for dental coverage, and added geo_kc flag for KC residence

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_elig_timevar_f <- function(extract_end_date = NULL) {
  
  ### Require extract_end_date
  if (is.null(extract_end_date)) {
    stop("Enter the end date for this APCD extract: \"YYYY-MM-DD\"")
  }
  
  ### Process extract end date
  extract_end_yearmo <- as.integer(stringr::str_sub(stringr::str_replace_all(extract_end_date, "-", ""), 1, 6))
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "
    -------------------
    --STEP 1: Convert eligibility table to 1 month per row format
    -------------------
    if object_id('tempdb..#temp1') is not null drop table #temp1;
    select a.internal_member_id, b.first_day_month, b.last_day_month, a.rac_code_id, a.product_code_id
    into #temp1
    from (
    	select internal_member_id, eligibility_start_dt,
    	--set ongoing eligibility end dates to latest extract end date 
    	case 
    	when eligibility_end_dt > {extract_end_date} then {extract_end_date}
    	else eligibility_end_dt
    	end as eligibility_end_dt,
    	cast(aid_category_id as int) as rac_code_id,
    	product_code_id
	    from phclaims.stage.apcd_eligibility
    ) as a
    inner join (select distinct year_month, first_day_month, last_day_month from PHClaims.ref.date) as b
    on (a.eligibility_start_dt <= b.last_day_month) and (a.eligibility_end_dt >= b.first_day_month)
    where b.year_month between 201401 and {extract_end_yearmo};
    
    
    -------------------
    --STEP 2: Group by member month and select one RAC code
    --Convert RAC codes to BSP codes, and add full benefit flag (based on RAC)
    --Some Medicaid members have more than 1 RAC for an eligibility period (as we know), but as RACs
    --are not desginated primary, we have to choose one
    --Thus, take max of full_benefit flag, and then take max of BSP code (thus higher numbered BSPs chosen)
    -------------------
    if object_id('tempdb..#temp2') is not null drop table #temp2;
    select x.internal_member_id, x.first_day_month, x.last_day_month,
    	max(x.bsp_group_cid) as bsp_group_cid, max(x.full_benefit) as full_benefit
    into #temp2
    from (
    select a.internal_member_id, a.first_day_month, a.last_day_month, c.bsp_group_cid,
    	case
    		--when Medicaid coverage and RAC code is present check if full benefit
    		when a.product_code_id = 14 and a.rac_code_id > 0 and c.full_benefit = 'Y' then 1
    		--when Medicaid coverage and RAC code is missing (likely MCO) assume full benefit
    		when a.product_code_id = 14 and (a.rac_code_id < 0 or a.rac_code_id is null) then 1
    		--otherwise set as not full-benefit
    		else 0
    	end as full_benefit
    from #temp1 as a
    left join PHClaims.ref.apcd_aid_category as b
    on a.rac_code_id = b.aid_category_id
    left join PHClaims.ref.mcaid_rac_code as c
    on b.aid_category_code = c.rac_code
    ) as x
    group by x.internal_member_id, x.first_day_month, x.last_day_month;
    
    if object_id('tempdb..#temp1') is not null drop table #temp1;
    
    
    -------------------
    --STEP 3: Join eligibility-based dual and RAC info to member_month_detail table and create coverage group variables
    -------------------
    if object_id('tempdb..#temp3') is not null drop table #temp3;
    select a.internal_member_id, a.first_day_month as from_date,
      dateadd(day, -1, dateadd(month, 1, a.first_day_month)) as to_date,
      a.zip_code, 
    	--create empirical dual flag based on presence of medicaid and medicare ID
    	case when (a.med_medicaid_eligibility_id is not null or a.rx_medicaid_eligibility_id is not null or a.dental_medicaid_eligibility_id is not null or b.bsp_group_cid is not null)
    		and (a.med_medicare_eligibility_id is not null or a.rx_medicare_eligibility_id is not null or a.dental_medicare_eligibility_id is not null)
    		then 1 else 0
    	end as dual_flag,
      b.bsp_group_cid, b.full_benefit,
      
      --create coverage categorical variable for medical coverage
      case
        when (a.med_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.med_commercial_eligibility_id is null and (a.med_medicare_eligibility_id is null) then 1 --Medicaid only
        when (a.med_medicaid_eligibility_id is null and b.bsp_group_cid is null) and a.med_commercial_eligibility_id is null and (a.med_medicare_eligibility_id is not null) then 2 --Medicare only
        when (a.med_medicaid_eligibility_id is null and b.bsp_group_cid is null) and a.med_commercial_eligibility_id is not null and a.med_medicare_eligibility_id is null then 3 --Commercial only
	      when (a.med_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.med_commercial_eligibility_id is null and (a.med_medicare_eligibility_id is not null) then 4 -- Medicaid-Medicare dual
        when (a.med_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.med_commercial_eligibility_id is not null and (a.med_medicare_eligibility_id is null) then 5 --Medicaid-commercial dual
        when (a.med_medicaid_eligibility_id is null and b.bsp_group_cid is null) and a.med_commercial_eligibility_id is not null and (a.med_medicare_eligibility_id is not null) then 6 --Medicare-commercial dual
        when (a.med_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.med_commercial_eligibility_id is not null and (a.med_medicare_eligibility_id is not null) then 7 -- All three
        when a.medical_eligibility_id is not null then 8 -- Unknown market
        else 0 --no medical coverage
       end as med_covgrp,
       
      --create coverage categorical variable for pharmacy coverage
      case
        when (a.rx_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.rx_commercial_eligibility_id is null and (a.rx_medicare_eligibility_id is null) then 1 --Medicaid only
        when (a.rx_medicaid_eligibility_id is null and b.bsp_group_cid is null) and a.rx_commercial_eligibility_id is null and (a.rx_medicare_eligibility_id is not null) then 2 --Medicare only
        when (a.rx_medicaid_eligibility_id is null and b.bsp_group_cid is null) and a.rx_commercial_eligibility_id is not null and a.rx_medicare_eligibility_id is null then 3 --Commercial only
	      when (a.rx_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.rx_commercial_eligibility_id is null and (a.rx_medicare_eligibility_id is not null) then 4 -- Medicaid-Medicare dual
        when (a.rx_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.rx_commercial_eligibility_id is not null and (a.rx_medicare_eligibility_id is null) then 5 --Medicaid-commercial dual
        when (a.rx_medicaid_eligibility_id is null and b.bsp_group_cid is null) and a.rx_commercial_eligibility_id is not null and (a.rx_medicare_eligibility_id is not null) then 6 --Medicare-commercial dual
        when (a.rx_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.rx_commercial_eligibility_id is not null and (a.rx_medicare_eligibility_id is not null) then 7 -- All three
        when a.pharmacy_eligibility_id is not null then 8 -- Unknown market
        else 0 --no pharm coverage
       end as pharm_covgrp,
       
      --create coverage categorical variable for dental coverage
      case
        when (a.dental_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.dental_commercial_eligibility_id is null and (a.dental_medicare_eligibility_id is null) then 1 --Medicaid only
        when (a.dental_medicaid_eligibility_id is null and b.bsp_group_cid is null) and a.dental_commercial_eligibility_id is null and (a.dental_medicare_eligibility_id is not null) then 2 --Medicare only
        when (a.dental_medicaid_eligibility_id is null and b.bsp_group_cid is null) and a.dental_commercial_eligibility_id is not null and a.dental_medicare_eligibility_id is null then 3 --Commercial only
	      when (a.dental_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.dental_commercial_eligibility_id is null and (a.dental_medicare_eligibility_id is not null) then 4 -- Medicaid-Medicare dual
        when (a.dental_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.dental_commercial_eligibility_id is not null and (a.dental_medicare_eligibility_id is null) then 5 --Medicaid-commercial dual
        when (a.dental_medicaid_eligibility_id is null and b.bsp_group_cid is null) and a.dental_commercial_eligibility_id is not null and (a.dental_medicare_eligibility_id is not null) then 6 --Medicare-commercial dual
        when (a.dental_medicaid_eligibility_id is not null or b.bsp_group_cid is not null) and a.dental_commercial_eligibility_id is not null and (a.dental_medicare_eligibility_id is not null) then 7 -- All three
        when a.dental_eligibility_id is not null then 8 -- Unknown market
        else 0 --no dental coverage
       end as dental_covgrp
      
    into #temp3
    from (
    select *, convert(date, cast(year_month as varchar(200)) + '01') as first_day_month
    from phclaims.stage.apcd_member_month_detail
    ) as a
    left join #temp2 as b
    on a.internal_member_id = b.internal_member_id and a.first_day_month = b.first_day_month;
    
    if object_id('tempdb..#temp2') is not null drop table #temp2;
    
    
    ------------
    --STEP 4: Assign a group number to each set of contiguous months by person, covgrp, dual_flag, BSP code, and full benefit flag, and ZIP code
    ----------------
    if object_id('tempdb..#temp4') is not null drop table #temp4;
    select distinct internal_member_id, from_date, to_date, zip_code, med_covgrp, pharm_covgrp, dental_covgrp, dual_flag, bsp_group_cid, full_benefit,
    	datediff(month, '1900-01-01', from_date) - row_number() 
    	over (partition by internal_member_id, zip_code, med_covgrp, pharm_covgrp, dental_covgrp, dual_flag, bsp_group_cid, full_benefit order by from_date) as group_num
    into #temp4
    from #temp3;
    
    if object_id('tempdb..#temp3') is not null drop table #temp3;
    
    
    ------------
    --STEP 5: Taking the max and min of each contiguous period, collapse to one row
    ----------------
    if object_id('tempdb..#temp5') is not null drop table #temp5;
    select internal_member_id, zip_code, med_covgrp, pharm_covgrp, dental_covgrp, dual_flag, bsp_group_cid, full_benefit, min(from_date) as from_date, max(to_date) as to_date,
      datediff(day, min(from_date), max(to_date)) + 1 as cov_time_day
    into #temp5
    from #temp4
    group by internal_member_id, zip_code, med_covgrp, pharm_covgrp, dental_covgrp, dual_flag, bsp_group_cid, full_benefit, group_num;
    
    if object_id('tempdb..#temp4') is not null drop table #temp4;
    
    
    ------------
    --STEP 6: Add additional coverage flag and geographic variables and insert data into table shell
    ----------------
    insert into PHClaims.stage.apcd_elig_timevar with (tablock)
    select 
    a.internal_member_id as id_apcd,
    a.from_date,
    a.to_date,
    --Contiguous flag (contiguous with prior row)
    case when datediff(day, lag(a.to_date, 1) over (partition by a.internal_member_id order by a.internal_member_id, a.from_date), a.from_date) = 1
    then 1 else 0 end as contiguous,
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
    a.bsp_group_cid,
    a.full_benefit,
    a.zip_code as geo_zip,
    d.geo_county_code_fips as geo_county_code,
    b.zip_group_desc as geo_county,
    c.zip_group_code as geo_ach_code,
    c.zip_group_desc as geo_ach,
    case when b.zip_group_desc is not null then 1 else 0 end as geo_wa,
    case when b.zip_group_desc = 'King' then 1 else 0 end as geo_kc,
    a.cov_time_day,
    getdate() as last_run
    from #temp5 as a
    left join (select distinct zip_code, zip_group_desc from phclaims.ref.apcd_zip_group where zip_group_type_desc = 'County') as b
    on a.zip_code = b.zip_code
    left join (select distinct zip_code, zip_group_code, zip_group_desc from phclaims.ref.apcd_zip_group where left(zip_group_type_desc, 3) = 'Acc') as c
    on a.zip_code = c.zip_code
    left join PHClaims.ref.geo_county_code_wa as d
    on on b.zip_group_desc = d.geo_county_name;
    
    if object_id('tempdb..#temp5') is not null drop table #temp5;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.apcd_elig_timevar_f <- function() {
  
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_elig_timevar' as 'table', 'member count, expect match to raw tables' as qa_type, count(distinct id_apcd) as qa
    from stage.apcd_elig_timevar",
    .con = db_claims))
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_member_month_detail' as 'table', 'member count, expect match to timevar' as qa_type, count(distinct internal_member_id) as qa
    from stage.apcd_member_month_detail",
    .con = db_claims))
  res3 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'final.apcd_elig_demo' as 'table', 'member count, expect match to timevar' as qa_type, count(distinct id_apcd) as qa
    from final.apcd_elig_demo",
    .con = db_claims))
  res4 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_elig_timevar' as 'table', 'member count, King 2016, expect match to member_month' as qa_type, count(distinct id_apcd) as qa
    from stage.apcd_elig_timevar
      where from_date <= '2016-12-31' and to_date >= '2016-01-01'
      and geo_ach = 'HealthierHere'",
    .con = db_claims))
  res5 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_member_month_detail' as 'table', 'member count, King 2016, expect match to timevar' as qa_type, count(distinct internal_member_id) as qa
    from stage.apcd_member_month_detail
      where left(year_month,4) = '2016'
      and zip_code in (select zip_code from phclaims.ref.apcd_zip_group where zip_group_desc = 'King' and zip_group_type_desc = 'County')",
    .con = db_claims))
  res6 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_eligibility' as 'table', 'member count, King 2016, expect slightly more than timevar' as qa_type, count(distinct internal_member_id) as qa
    from stage.apcd_eligibility
      where eligibility_start_dt <= '2016-12-31' and eligibility_end_dt >= '2016-01-01'
      and zip in (select zip_code from phclaims.ref.apcd_zip_group where zip_group_desc = 'King' and zip_group_type_desc = 'County')",
    .con = db_claims))
  res7 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_elig_timevar' as 'table', 'count of member elig segments with no coverage, expect 0' as qa_type, count(distinct id_apcd) as qa
    from stage.apcd_elig_timevar
    where med_covgrp = 0 and pharm_covgrp = 0 and dental_covgrp = 0;",
    .con = db_claims))
  res8 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_elig_timevar' as 'table', 'max to_date, expect max to_date of latest extract' as qa_type,
    cast(left(max(to_date),4) + SUBSTRING(cast(max(to_date) as varchar(255)),6,2) + right(max(to_date),2) as integer) as qa
    from stage.apcd_elig_timevar;",
    .con = db_claims))
  res9 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_elig_timevar' as 'table', 'mcaid-mcare duals with dual flag = 0, expect 0' as qa_type, count(*) as qa
    from stage.apcd_elig_timevar
    where (med_covgrp = 4 or pharm_covgrp = 4) and dual = 0;",
    .con = db_claims)),
  res10 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_elig_timevar' as 'table', 'non-WA resident segments with non-null county name, expect 0' as qa_type, count(*) as qa
    from stage.apcd_elig_timevar
    where geo_wa = 0 and geo_county is not null;",
    .con = db_claims)),
  res11 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_elig_timevar' as 'table', 'WA resident segments with null county name, expect 0' as qa_type, count(*) as qa
    from stage.apcd_elig_timevar
    where geo_wa = 1 and geo_county is null;",
    .con = db_claims))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}