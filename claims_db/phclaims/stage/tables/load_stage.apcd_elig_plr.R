#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_ELIG_PLR
# Eli Kern, PHSKC (APDE)
#
# 2019-10

#2023-08-02 update: Remove full benefit and performance cohort variables, add flags for any medical coverage 7-mo and 11-mo cohort
#2024-04-29 update: Modify for HHSAW migration, remove continuous coverage and coverage gap variables to simplify code,
  #and add a 6-month any medical coverage cohort flag

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_elig_plr_f <- function(from_date = NULL, to_date = NULL, calendar_year = T, table_name = NULL) {
  
  ### Require extract_end_date
  if (is.null(from_date) | is.null(to_date)) {
    stop("Enter the from and to date for this PLR table: \"YYYY-MM-DD\"")
  }
  
  ### Require table name if not running on a complete calendar year
  if (calendar_year == F & is.null(table_name)) {
    stop("Enter a table name for this non-calendar year table: \"YYYYMMDD\"")
  }
  
  ### Process year for table name
  if (calendar_year == T) {
    table_name_year <- stringr::str_sub(from_date,1,4)
    table_name_year <- glue::glue_sql(paste0("apcd_elig_plr_", table_name_year))
  }
  
  if (calendar_year == F) {
    table_name_year <- glue::glue_sql(paste0("apcd_elig_plr_", table_name))
  }
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "
    --------------------------
    --STEP 1: Calculate coverage days and gaps in date range
    --------------------------
    
    if object_id('tempdb..#cov1') is not null drop table #cov1;
    select distinct id_apcd, from_date, to_date,
    ---------
    --MEDICAL coverage days
    ---------
    --calculate total medical coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and med_covgrp != 0 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and med_covgrp != 0 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and med_covgrp != 0 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and med_covgrp != 0 then datediff(day, from_date, to_date) + 1
      else 0
     end as med_total_covd,
    
    --calculate Medicaid medical coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and med_medicaid = 1 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and med_medicaid = 1 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and med_medicaid = 1 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and med_medicaid = 1 then datediff(day, from_date, to_date) + 1
      else 0
     end as med_medicaid_covd,
     
    --calculate Medicare medical coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and med_medicare = 1 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and med_medicare = 1 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and med_medicare = 1 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and med_medicare = 1 then datediff(day, from_date, to_date) + 1
      else 0
     end as med_medicare_covd,
     
    --calculate Commercial medical coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and med_commercial = 1 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and med_commercial = 1 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and med_commercial = 1 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and med_commercial = 1 then datediff(day, from_date, to_date) + 1
      else 0
     end as med_commercial_covd,
     
    ---------
    --PHARMACY coverage days
    ---------
    --calculate total pharmacy coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and pharm_covgrp != 0 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and pharm_covgrp != 0 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and pharm_covgrp != 0 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and pharm_covgrp != 0 then datediff(day, from_date, to_date) + 1
      else 0
     end as pharm_total_covd,
    
    --calculate Medicaid pharmacy coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and pharm_medicaid = 1 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and pharm_medicaid = 1 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and pharm_medicaid = 1 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and pharm_medicaid = 1 then datediff(day, from_date, to_date) + 1
      else 0
     end as pharm_medicaid_covd,
     
    --calculate Medicare pharmacy coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and pharm_medicare = 1 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and pharm_medicare = 1 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and pharm_medicare = 1 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and pharm_medicare = 1 then datediff(day, from_date, to_date) + 1
      else 0
     end as pharm_medicare_covd,
     
    --calculate Commercial pharmacy coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and pharm_commercial = 1 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and pharm_commercial = 1 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and pharm_commercial = 1 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and pharm_commercial = 1 then datediff(day, from_date, to_date) + 1
      else 0
     end as pharm_commercial_covd,
     
    ---------
    --Medicaid-Medicare DUAL (medical or pharm) coverage days
    ---------
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and ((med_medicaid = 1 or pharm_medicaid = 1) and (med_medicare = 1 or pharm_medicare = 1))
        then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and 
        ((med_medicaid = 1 or pharm_medicaid = 1) and (med_medicare = 1 or pharm_medicare = 1)) then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and 
        ((med_medicaid = 1 or pharm_medicaid = 1) and (med_medicare = 1 or pharm_medicare = 1)) then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and ((med_medicaid = 1 or pharm_medicaid = 1) and (med_medicare = 1 or pharm_medicare = 1))
        then datediff(day, from_date, to_date) + 1
      else 0
     end as dual_covd
    
    into #cov1
    from claims.final_apcd_elig_timevar
    where from_date <= {to_date} and to_date >= {from_date};
     
    
    --------------------------
    --STEP 2: Summarize coverage information to person level
    --------------------------
    if object_id('tempdb..#cov2') is not null drop table #cov2;
      ---------
      --MEDICAL variables
      ---------
      select id_apcd as id, sum(med_total_covd) as med_total_covd, 
        cast(sum((med_total_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as med_total_covper, 
        sum(dual_covd) as dual_covd,
        cast(sum((dual_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as dual_covper,
        case when sum(dual_covd) > 0 then 1 else 0 end as dual_flag,
        sum(med_medicaid_covd) as med_medicaid_covd, sum(med_medicare_covd) as med_medicare_covd, sum(med_commercial_covd) as med_commercial_covd,
        cast(sum((med_medicaid_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as med_medicaid_covper,
        cast(sum((med_medicare_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as med_medicare_covper,
        cast(sum((med_commercial_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as med_commercial_covper,
    
      ---------
      --PHARMACY variables
      ---------
        sum(pharm_total_covd) as pharm_total_covd, 
        cast(sum((pharm_total_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as pharm_total_covper,
        sum(pharm_medicaid_covd) as pharm_medicaid_covd, sum(pharm_medicare_covd) as pharm_medicare_covd, sum(pharm_commercial_covd) as pharm_commercial_covd,
        cast(sum((pharm_medicaid_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as pharm_medicaid_covper,
        cast(sum((pharm_medicare_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as pharm_medicare_covper,
        cast(sum((pharm_commercial_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as pharm_commercial_covper
    
      into #cov2
      from #cov1
      group by id_apcd;

    if object_id('tempdb..#cov1') is not null drop table #cov2;

    
    --------------------------
    --STEP 3: Summarize geographic information for member residence
    --------------------------
    if object_id('tempdb..#geo') is not null drop table #geo;
    ---------
    --Assign each member to a single ZIP code for requested date range
    ---------
    select c.id, c.geo_zip, d.geo_county, e.geo_ach
    into #geo
    from (
      select b.id, b.geo_zip, b.zip_dur, row_number() over (partition by b.id order by b.zip_dur desc, b.geo_zip) as zipr
    	from (
    		select a.id, a.geo_zip, sum(a.covd) + 1 as zip_dur
    		from (
      		select id_apcd as id, geo_zip,
        		case
        			/**if coverage period fully contains date range then person time is just date range */
        		  when from_date <= {from_date} and to_date >= {to_date} then
        		    datediff(day, {from_date}, {to_date}) + 1
        			/**if coverage period begins before date range start and ends within date range */
        			when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} then 
        			 datediff(day, {from_date}, to_date) + 1
        			/**if coverage period begins within date range and ends after date range end */
        			when from_date > {from_date} and to_date >= {to_date} and from_date <= {to_date} then 
        			 datediff(day, from_date, {to_date}) + 1
        			/**if coverage period begins after date range start and ends before date range end */
        			when from_date > {from_date} and to_date < {to_date} then datediff(day, from_date, to_date) + 1
        			else null
        		end as covd
          from claims.final_apcd_elig_timevar
          where from_date <= {to_date} and to_date >= {from_date}
    			) as a
    			group by a.id, a.geo_zip
        ) as b
      ) as c
    left join (select distinct zip_code, zip_group_desc as geo_county from claims.ref_apcd_zip_group where zip_group_type_desc = 'County') as d
    on c.geo_zip = d.zip_code
    left join (select distinct zip_code, zip_group_desc as geo_ach from claims.ref_apcd_zip_group where left(zip_group_type_desc, 3) = 'Acc') as e
    on c.geo_zip = e.zip_code
    where c.zipr = 1;
    
    
    --------------------------
    --STEP 4: For each member's selected ACH, calculate duration (days) and percentage of time spent in ACH
    --------------------------
    if object_id('tempdb..#ach') is not null drop table #ach;
    ---------
    --Assign each member to a single ZIP code for requested date range
    ---------
    select c.id, c.geo_ach, sum(c.geo_ach_covd) as geo_ach_covd
    into #ach
    from (
    select a.id, a.geo_ach,
      case
        /**if coverage period fully contains date range then person time is just date range */
        when b.from_date <= {from_date} and b.to_date >= {to_date} then
          datediff(day, {from_date}, {to_date}) + 1
        /**if coverage period begins before date range start and ends within date range */
        when b.from_date <= {from_date} and b.to_date < {to_date} and b.to_date >= {from_date} then 
         datediff(day, {from_date}, b.to_date) + 1
        /**if coverage period begins within date range and ends after date range end */
        when b.from_date > {from_date} and b.to_date >= {to_date} and b.from_date <= {to_date} then 
         datediff(day, b.from_date, {to_date}) + 1
        /**if coverage period begins after date range start and ends before date range end */
        when b.from_date > {from_date} and b.to_date < {to_date} then datediff(day, b.from_date, b.to_date) + 1
        else null
      end as geo_ach_covd
    from (select id, geo_ach from #geo) as a
    inner join (select id_apcd, geo_ach, from_date, to_date from claims.final_apcd_elig_timevar) as b
    on a.id = b.id_apcd
    where b.from_date <= {to_date} and b.to_date >= {from_date} and a.geo_ach = b.geo_ach
    ) as c
    group by c.id, c.geo_ach;
     		
    
    --------------------------
    --STEP 5: Join coverage and geo, and pull in demographics
    --------------------------
    if object_id('tempdb..#merge1') is not null drop table #merge1;
    select a.id as id_apcd, 
      
      --DEMOGRAPHICS
      b.geo_zip, b.geo_county, b.geo_ach, c.geo_ach_covd, 
      cast((c.geo_ach_covd * 1.0) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as geo_ach_covper, d.age,
    	case
    			when d.age >= 0 and d.age < 5 then '0-4'
    			when d.age >= 5 and d.age < 12 then '5-11'
    			when d.age >= 12 and d.age < 18 then '12-17'
    			when d.age >= 18 and d.age < 25 then '18-24'
    			when d.age >= 25 and d.age < 45 then '25-44'
    			when d.age >= 45 and d.age < 65 then '45-64'
    			when d.age >= 65 or d.ninety_only = 1 then '65 and over'
    	end as age_grp7,
    	d.gender_me, d.gender_recent, d.gender_female, d.gender_male, d.race_eth_me, d.race_me, d.race_eth_recent, d.race_recent, d.race_aian,
    	d.race_asian, d.race_black, d.race_latino, d.race_nhpi, d.race_white, d.race_unknown,
    	
      --COVERAGE STATS
      a.med_total_covd, a.med_total_covper,
      a.dual_covd, a.dual_covper, a.dual_flag, a.med_medicaid_covd, a.med_medicare_covd, a.med_commercial_covd,
      a.med_medicaid_covper, a.med_medicare_covper, a.med_commercial_covper,
      a.pharm_total_covd, a.pharm_total_covper, a.pharm_medicaid_covd, a.pharm_medicare_covd, a.pharm_commercial_covd, a.pharm_medicaid_covper, 
      a.pharm_medicare_covper, a.pharm_commercial_covper
    
    into #merge1
    from #cov2 as a
    left join #geo as b
    on a.id = b.id
    left join #ach as c
    on a.id = c.id
    left join (
    select *, case
    	when (floor((datediff(day, dob, {to_date}) + 1) / 365.25) >= 90) or (ninety_only = 1) then 90
    	when floor((datediff(day, dob, {to_date}) + 1) / 365.25) >=0 then floor((datediff(day, dob, {to_date}) + 1) / 365.25)
    	when floor((datediff(day, dob, {to_date}) + 1) / 365.25) = -1 then 0
    end as age
    from claims.final_apcd_elig_demo
    ) as d
    on a.id = d.id_apcd;
    
    if object_id('tempdb..#cov2') is not null drop table #cov3;
    if object_id('tempdb..#geo') is not null drop table #geo;
    if object_id('tempdb..#ach') is not null drop table #ach;

    
    --------------------------
    --STEP 6: Create final coverage cohort variables and select into table shell
    --------------------------
    insert into claims.stage_{`table_name_year`} with (tablock)
    select id_apcd, 
    
    --flags for various geographic and coverage cohorts (all coverage cohorts variables computed for WA residents only)
    case when geo_county is not null then 1 else 0 end as geo_wa, -- WA state residents
    case when (geo_county is not null and (med_medicaid_covd >= 1 or pharm_medicaid_covd >= 1)) then 1 else 0 end as overall_mcaid, -- 1+ days Medicaid
    case when (geo_county is not null and med_medicaid_covd >= 1) then 1 else 0 end as overall_mcaid_med, -- 1+ days Medicaid medical coverage
    case when (geo_county is not null and pharm_medicaid_covd >= 1) then 1 else 0 end as overall_mcaid_pharm, -- 1+ days Medicaid pharm coverage
    case when geo_county is not null and med_total_covper >= 50.0 then 1 else 0 end as medical_coverage_6mo, -- 6+ months of ANY medical coverage
    case when geo_county is not null and med_total_covper >= 58.3 then 1 else 0 end as medical_coverage_7mo, -- 7+ months of ANY medical coverage
    case when geo_county is not null and med_total_covper >= 91.7 then 1 else 0 end as medical_coverage_11mo, -- 11+ months of ANY medical coverage

    geo_zip, geo_county, geo_ach, geo_ach_covd, geo_ach_covper, age, age_grp7, gender_me, gender_recent, gender_female, gender_male, race_eth_me, race_me,
    race_eth_recent, race_recent, race_aian, race_asian, race_black, race_latino, race_nhpi, race_white, race_unknown, med_total_covd, med_total_covper, 
    dual_covd, dual_covper, dual_flag, med_medicaid_covd, med_medicare_covd, med_commercial_covd,
    med_medicaid_covper, med_medicare_covper, med_commercial_covper,
    pharm_total_covd, pharm_total_covper, pharm_medicaid_covd, pharm_medicare_covd, pharm_commercial_covd, pharm_medicaid_covper, 
    pharm_medicare_covper, pharm_commercial_covper,
    getdate() as last_run
    from #merge1;

    if object_id('tempdb..#merge1') is not null drop table #merge1;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.apcd_elig_plr_f <- function(year = NULL) {
  
  table_name <- glue::glue_sql(paste0("apcd_elig_plr_", year))

  #all members are distinct
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'claims.stage_{`table_name`}' as 'table', '# members with >1 row, expect 0' as qa_type, count(a.id_apcd) as qa
      from (
        select id_apcd, count(id_apcd) as id_cnt
        from claims.stage_{`table_name`}
        group by id_apcd
      ) as a
      where a.id_cnt > 1;",
    .con = db_claims))
  
  #number of members in WA state with non-WA county
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'claims.stage_{`table_name`}' as 'table', 'non-WA county for WA resident, expect 0' as qa_type, count(id_apcd) as qa
        from claims.stage_{`table_name`}
      where geo_wa = 1 and geo_county is null;",
    .con = db_claims))
  
  #number of non-WA residents
  res3 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'claims.stage_{`table_name`}' as 'table', 'non-WA residents, expect 0' as qa_type, count(id_apcd) as qa
        from claims.stage_{`table_name`}
      where geo_wa = 0 and geo_county is not null;",
    .con = db_claims))
  
  #number of overall Medicaid members
  res4 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'claims.stage_{`table_name`}' as 'table', '# of overall Medicaid members' as qa_type, count(id_apcd) as qa
        from claims.stage_{`table_name`}
      where overall_mcaid = 1;",
    .con = db_claims))
  
  #number of members with medical but not pharmacy Medicaid coverage
  res5 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'claims.stage_{`table_name`}' as 'table', '# of members with medical but not pharmacy Medicaid' as qa_type, count(id_apcd) as qa
        from claims.stage_{`table_name`}
      where overall_mcaid_med = 1 and overall_mcaid_pharm = 0;",
    .con = db_claims))
  
  #number of members with pharmacy but not medical Medicaid coverage
  res6 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'claims.stage_{`table_name`}' as 'table', '# of members with pharmacy but not medical Medicaid, expect low' as qa_type, count(id_apcd) as qa
        from claims.stage_{`table_name`}
      where overall_mcaid_med = 0 and overall_mcaid_pharm = 1;",
    .con = db_claims))
  
  #number of members with day counts over 365 or 366
  
  if(nchar(year) == 4) {
    if(leap_year(as.numeric(year))==T) {days <- 366} else {days <- 365}
  }
  
  if(nchar(year) > 4) {
    
    start_date <- ymd(year) %m-% months(12) %m+% days(1)
    end_date <- ymd(year)
    interval <- interval(start_date, end_date)
    
    start_feb <- ymd(paste0(str_sub(start_date,1,4),"0201"))
    end_feb <- ymd(paste0(str_sub(end_date,1,4),"0201"))
    
    if((ymd(start_feb) %within% interval & leap_year(start_date)==T) | (ymd(end_feb) %within% interval & leap_year(end_date)==T)) {
      days <- 366} else {
        days <- 365}
  }
  
  res7 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'claims.stage_{`table_name`}' as 'table', '# of members with day counts >{days}, expect 0' as qa_type, count(*) as qa
        from claims.stage_{`table_name`}
      where med_total_covd > {days} or med_medicaid_covd > {days} or med_commercial_covd > {days} or
        med_medicare_covd > {days} or dual_covd > {days} or geo_ach_covd > {days} or pharm_total_covd > {days} or
        pharm_medicaid_covd > {days} or pharm_medicare_covd > {days} or pharm_commercial_covd > {days};",
    .con = db_claims))
  
  #number of members with percents > 100
  res8 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'claims.stage_{`table_name`}' as 'table', '# of members with percents >100, expect 0' as qa_type, count(*) as qa
        from claims.stage_{`table_name`}
      where med_total_covper > 100 or med_medicaid_covper > 100 or med_commercial_covper > 100 or
        med_medicare_covper > 100 or dual_covper > 100 or geo_ach_covper > 100 or pharm_total_covper > 100 or
        pharm_medicaid_covper > 100 or pharm_medicare_covper > 100 or pharm_commercial_covper > 100;",
    .con = db_claims))
  
  #number of overall Medicaid members who are out of state
  res9 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'claims.stage_{`table_name`}' as 'table', '# of overall Medicaid members out of state, expect 0' as qa_type, count(id_apcd) as qa
        from claims.stage_{`table_name`}
      where overall_mcaid = 1 and geo_county is null;",
    .con = db_claims))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}