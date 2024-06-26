#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_elig_timevar
# Eli Kern, PHSKC (APDE)
#
# 2024-05

### Run from 02_master_mcare_claims_analytic.R script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/02_master_mcare_claims_analytic.R

#### Load script ####
load_stage.mcare_elig_timevar_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(inthealth, glue::glue_sql(
    "---------------------
    --Create elig_timevar table from bene_enrollment table
    --Eli Kern, adapted from Danny Colombara script
    --2024-05
    ----------------------
    
    ----------------------
    --STEP 1: Pull out desired enrollment columns, minor transformations, reshape wide to long
    ----------------------
    if object_id(N'tempdb..#timevar_01') is not null drop table #timevar_01;
    with buyins as (
    	select
    	--top 1000
    	bene_id,
    	bene_enrollmt_ref_yr as cal_year,
    	right(cal_mon,2) as cal_mon,
    	case when len(zip_cd) < 5 then null else left(zip_cd,5) end as geo_zip,
    	buyins as buyins
    	from stg_claims.mcare_bene_enrollment as a
    	unpivot(buyins for cal_mon in (
    		mdcr_entlmt_buyin_ind_01,
    		mdcr_entlmt_buyin_ind_02,
    		mdcr_entlmt_buyin_ind_03,
    		mdcr_entlmt_buyin_ind_04,
    		mdcr_entlmt_buyin_ind_05,
    		mdcr_entlmt_buyin_ind_06,
    		mdcr_entlmt_buyin_ind_07,
    		mdcr_entlmt_buyin_ind_08,
    		mdcr_entlmt_buyin_ind_09,
    		mdcr_entlmt_buyin_ind_10,
    		mdcr_entlmt_buyin_ind_11,
    		mdcr_entlmt_buyin_ind_12)
    	) as buyins
    ),
    hmos as (
    	select
    	--top 1000
    	bene_id,
    	bene_enrollmt_ref_yr as cal_year,
    	right(cal_mon,2) as cal_mon,
    	hmos as hmos
    	from stg_claims.mcare_bene_enrollment as a
    	unpivot(hmos for cal_mon in (
    		hmo_ind_01,
    		hmo_ind_02,
    		hmo_ind_03,
    		hmo_ind_04,
    		hmo_ind_05,
    		hmo_ind_06,
    		hmo_ind_07,
    		hmo_ind_08,
    		hmo_ind_09,
    		hmo_ind_10,
    		hmo_ind_11,
    		hmo_ind_12)
    	) as hmos
    ),
    rx as (
    	select
    	--top 1000
    	bene_id,
    	bene_enrollmt_ref_yr as cal_year,
    	right(cal_mon,2) as cal_mon,
    	rx as rx
    	from stg_claims.mcare_bene_enrollment as a
    	unpivot(rx for cal_mon in (
    		ptd_cntrct_id_01,
    		ptd_cntrct_id_02,
    		ptd_cntrct_id_03,
    		ptd_cntrct_id_04,
    		ptd_cntrct_id_05,
    		ptd_cntrct_id_06,
    		ptd_cntrct_id_07,
    		ptd_cntrct_id_08,
    		ptd_cntrct_id_09,
    		ptd_cntrct_id_10,
    		ptd_cntrct_id_11,
    		ptd_cntrct_id_12)
    	) as rx
    ),
    duals as (
    	select
    	--top 1000
    	bene_id,
    	bene_enrollmt_ref_yr as cal_year,
    	right(cal_mon,2) as cal_mon,
    	duals as duals
    	from stg_claims.mcare_bene_enrollment as a
    	unpivot(duals for cal_mon in (
    		dual_stus_cd_01,
    		dual_stus_cd_02,
    		dual_stus_cd_03,
    		dual_stus_cd_04,
    		dual_stus_cd_05,
    		dual_stus_cd_06,
    		dual_stus_cd_07,
    		dual_stus_cd_08,
    		dual_stus_cd_09,
    		dual_stus_cd_10,
    		dual_stus_cd_11,
    		dual_stus_cd_12)
    	) as duals
    )
    select x.*, y.hmos, z.rx, w.duals
    into #timevar_01
    from buyins as x
    left join hmos as y
    on (x.bene_id = y.bene_id) and (x.cal_year = y.cal_year) and (x.cal_mon = y.cal_mon)
    left join rx as z
    on (x.bene_id = z.bene_id) and (x.cal_year = z.cal_year) and (x.cal_mon = z.cal_mon)
    left join duals as w
    on (x.bene_id = w.bene_id) and (x.cal_year = w.cal_year) and (x.cal_mon = w.cal_mon);
    
    
    ----------------------
    --STEP 2a: Recode and create new enrollment indicators, and create start and end date columns
    --Medicare entitlement and buyins (Parts A+B): https://resdac.org/cms-data/variables/medicare-entitlementbuy-indicator
    --Medicare Advantage (Part C): https://www.resdac.org/articles/identifying-medicare-managed-care-beneficiaries-master-beneficiary-summary-or-denominator
    --Prescription coverage (Part D): https://resdac.org/cms-data/variables/monthly-part-d-contract-number-january
    --Medicare-Medicaid dual eligibility: https://resdac.org/cms-data/variables/medicare-medicaid-dual-eligibility-code-january
    ----------------------
    if object_id(N'tempdb..#timevar_02') is not null drop table #timevar_02;
    select
    bene_id as id_mcare,
    b.first_day_month as from_date,
    b.last_day_month as to_date,
    geo_zip,
    --Part A coverage (inpatient)
    case
    	when buyins in ('1','3','A','C') then 1
    	when buyins in ('0','2','B') then 0
    end as part_a,
    --Part B coverage (outpatient)
    case
    	when buyins in ('2','3','B','C') then 1
    	when buyins in ('0','1','A') then 0
    end as part_b,
    --Part C coverage (Medicare Advantage)
    case
    	when hmos in ('1','2','A','B','C') then 1
    	when hmos in ('0','4') then 0
    end as part_c,
    --Part D coverage (prescriptions)
    case
    	when rx in ('N', 'NULL', '*', '0', 'NA') or rx is null then 0
    	when left(rx,1) in ('E', 'H', 'R', 'S', 'X') then 1
    end as part_d,
    --State buy-in
    case
    	when buyins in ('0','1','2','3') then 0
    	when buyins in ('A','B','C') then 1
    end as state_buyin,
    --Partial dual
    case
    	when duals in ('NULL', '**', '0', '00', '2', '02', '4', '04', '8', '08', '9', '09', '99', '10', 'NA') or duals is null then 0
    	when duals in ('1', '01', '3', '03', '5', '05', '6', '06') then 1
    end as partial_dual,
    --Full dual
    case
    	when duals in ('NULL', '**', '0', '00', '9', '09', '99', 'NA', '1', '01', '3', '03', '5', '05', '6', '06') or duals is null then 0
    	when duals in ('2', '02', '4', '04', '8', '08', '10') then 1
    end as full_dual
    into #timevar_02
    from #timevar_01 as a
    left join (select distinct year_month, first_day_month, last_day_month from stg_claims.ref_date) as b
    on cast(cast(a.cal_year as varchar(4)) + a.cal_mon as int) = b.year_month;
    
    
    ----------------------
    --STEP 2b: Drop months with no coverage (data), drop months that occur after death_dt, truncate to_date to death_dt where relevant
    ----------------------
    if object_id(N'tempdb..#timevar_02b') is not null drop table #timevar_02b;
    with cov_type_sum as (
    select *,
    part_a + part_b + part_c + part_d + state_buyin + partial_dual + full_dual as cov_type_sum
    from #timevar_02
    )
    select
    a.id_mcare,
    a.from_date,
    case
    	when b.death_dt is not null and a.from_date <= b.death_dt and a.to_date > b.death_dt then b.death_dt
    	else a.to_date
    end as to_date,
    a.geo_zip,
    a.part_a,
    a.part_b,
    a.part_c,
    a.part_d,
    a.state_buyin,
    a.partial_dual,
    a.full_dual
    into #timevar_02b
    from cov_type_sum as a
    left join stg_claims.final_mcare_elig_demo as b
    on a.id_mcare = b.id_mcare
    where a.cov_type_sum > 0
    	and (a.from_date <= b.death_dt or b.death_dt is null);
    
    
    ----------------------
    --STEP 3: Identify contiguous periods
    ----------------------
    if object_id(N'tempdb..#timevar_03') is not null drop table #timevar_03;
    select distinct
    id_mcare,
    from_date,
    to_date,
    geo_zip,
    part_a,
    part_b,
    part_c,
    part_d,
    state_buyin,
    partial_dual,
    full_dual,
    datediff(day, lag(to_date) over (
    	partition by
    	id_mcare,
    	geo_zip,
    	part_a,
    	part_b,
    	part_c,
    	part_d,
    	state_buyin,
    	partial_dual,
    	full_dual
    	order by id_mcare, from_date), from_date
    ) as group_num
    into #timevar_03
    from #timevar_02b;
    
    
    ----------------------
    --STEP 4: Assign unique identifier (row number) to first date in a contiguous series of dates
    ----------------------
    if object_id(N'tempdb..#timevar_04') is not null drop table #timevar_04;
    select distinct
    id_mcare,
    from_date,
    to_date,
    geo_zip,
    part_a,
    part_b,
    part_c,
    part_d,
    state_buyin,
    partial_dual,
    full_dual,
    case
    	when group_num > 1 or group_num is null then row_number() over (partition by id_mcare order by from_date) + 1
    	when group_num <= 1 then null
    end as group_num
    into #timevar_04
    from #timevar_03;
    
    
    ----------------------
    --STEP 5: Spread group_number to all rows with contiguous dates for each person
    ----------------------
    if object_id(N'tempdb..#timevar_05') is not null drop table #timevar_05;
    select distinct
    id_mcare,
    from_date,
    to_date,
    geo_zip,
    part_a,
    part_b,
    part_c,
    part_d,
    state_buyin,
    partial_dual,
    full_dual,
    max(group_num) over (
    	partition by
    	id_mcare,
    	geo_zip,
    	part_a,
    	part_b,
    	part_c,
    	part_d,
    	state_buyin,
    	partial_dual,
    	full_dual
    	order by from_date
    ) as group_num
    into #timevar_05
    from #timevar_04;
    
    
    ----------------------
    --STEP 6: Find min/max dates for all contiguous periods
    ----------------------
    if object_id(N'tempdb..#timevar_06') is not null drop table #timevar_06;
    select distinct
    id_mcare,
    geo_zip,
    part_a,
    part_b,
    part_c,
    part_d,
    state_buyin,
    partial_dual,
    full_dual,
    min(from_date) as from_date,
    max(to_date) as to_date
    into #timevar_06
    from #timevar_05
    group by
    id_mcare,
    geo_zip,
    part_a,
    part_b,
    part_c,
    part_d,
    state_buyin,
    partial_dual,
    full_dual,
    group_num;
    
    
    ----------------------
    --STEP 7: Calculate days of coverage
    ----------------------
    if object_id(N'tempdb..#timevar_07') is not null drop table #timevar_07;
    select
    id_mcare,
    from_date,
    to_date,
    geo_zip,
    part_a,
    part_b,
    part_c,
    part_d,
    state_buyin,
    partial_dual,
    full_dual,
    datediff(dd, from_date, to_date) + 1 as cov_time_day
    into #timevar_07
    from #timevar_06;
    
    
    ----------------------
    --STEP 8: Flag contiguous periods (considering time only), add geo_kc flag, load to persistent table
    ----------------------
    insert into stg_claims.stage_mcare_elig_timevar
    select
    a.id_mcare,
    a.from_date,
    a.to_date,
    case
    	when datediff(day, lag(a.to_date, 1) over
    	(partition by a.id_mcare order by a.id_mcare, a.from_date), a.from_date) = 1
    	then 1 else 0
    end as contiguous,
    a.part_a,
    a.part_b,
    a.part_c,
    a.part_d,
    a.full_dual,
    a.partial_dual,
    a.state_buyin,
    a.geo_zip,
    b.geo_kc,
    a.cov_time_day,
    getdate() as last_run
    from #timevar_07 as a
    left join (select distinct geo_zip, geo_kc from stg_claims.ref_geo_kc_zip) as b
    on a.geo_zip = b.geo_zip;",
    .con = inthealth))
}

#### Table-level QA script ####
qa_stage.mcare_elig_timevar_qa_f <- function() {
  
  #Count of people with to_date after death_dt, expect 0
  res1 <- dbGetQuery(conn = inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_elig_timevar' as 'table',
    'people with to_date after death_dt, expect 0' as qa_type,
    count(distinct a.id_mcare) as qa
    from stg_claims.stage_mcare_elig_timevar as a
    left join stg_claims.final_mcare_elig_demo as b
    on a.id_mcare = b.id_mcare
    where a.to_date > b.death_dt;",
    .con = inthealth))
  
  #Count of people with to_date before from_date or from_date after to_date, expect 0
  res2 <- dbGetQuery(conn = inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_elig_timevar' as 'table',
    'people with to_date before from_date or from_date after to_date, expect 0' as qa_type,
    count(distinct id_mcare) as qa
    from stg_claims.stage_mcare_elig_timevar
    where (to_date < from_date) or (from_date > to_date);",
    .con = inthealth))
  
  #Part A coverage 1-month person count matches bene_enrollment table, after accounting for death date censoring
  qa3a_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct a.bene_id) as qa
    from stg_claims.mcare_bene_enrollment as a
    left join stg_claims.final_mcare_elig_demo as b
    on a.bene_id = b.id_mcare
    where a.bene_enrollmt_ref_yr = 2016 and a.mdcr_entlmt_buyin_ind_04 in ('1','3','A','C')
    	and (b.death_dt >= '2016-04-01' or b.death_dt is null);", .con = inthealth))
  qa3b_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct id_mcare) as qa
    from stg_claims.stage_mcare_elig_timevar
    where from_date <= '2016-04-30' and to_date >= '2016-04-01' and part_a = 1;", .con = inthealth))
  if(qa3a_result$qa == qa3b_result$qa) {
    qa3 <- 0L
  } else {
    qa3 <- 1L
  }
  res3 <- as.data.frame(list(
    "table" = "stg_claims.stage_mcare_elig_timevar",
    "qa_type" = "part A 1-mon person count different from bene_enrollment table, expect 0",
    "qa" = qa3
  )) 
  
  #Part B coverage 1-month person count matches bene_enrollment table, after accounting for death date censoring
  qa4a_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct a.bene_id) as qa
    from stg_claims.mcare_bene_enrollment as a
    left join stg_claims.final_mcare_elig_demo as b
    on a.bene_id = b.id_mcare
    where a.bene_enrollmt_ref_yr = 2018 and a.mdcr_entlmt_buyin_ind_07 in ('2','3','B','C')
    	and (b.death_dt >= '2018-07-01' or b.death_dt is null);", .con = inthealth))
  qa4b_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct id_mcare) as qa
    from stg_claims.stage_mcare_elig_timevar
    where from_date <= '2018-07-31' and to_date >= '2018-07-01' and part_b = 1;", .con = inthealth))
  if(qa4a_result$qa == qa4b_result$qa) {
    qa4 <- 0L
  } else {
    qa4 <- 1L
  }
  res4 <- as.data.frame(list(
    "table" = "stg_claims.stage_mcare_elig_timevar",
    "qa_type" = "part B 1-mon person count different from bene_enrollment table, expect 0",
    "qa" = qa4
  ))
  
  #Part C coverage 1-month person count matches bene_enrollment table, after accounting for death date censoring
  qa5a_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct a.bene_id) as qa
    from stg_claims.mcare_bene_enrollment as a
    left join stg_claims.final_mcare_elig_demo as b
    on a.bene_id = b.id_mcare
    where a.bene_enrollmt_ref_yr = 2020 and a.hmo_ind_01 in ('1','2','A','B','C')
    	and (b.death_dt >= '2020-01-01' or b.death_dt is null);", .con = inthealth))
  qa5b_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct id_mcare) as qa
    from stg_claims.stage_mcare_elig_timevar
    where from_date <= '2020-01-31' and to_date >= '2020-01-01' and part_c = 1;", .con = inthealth))
  if(qa5a_result$qa == qa5b_result$qa) {
    qa5 <- 0L
  } else {
    qa5 <- 1L
  }
  res5 <- as.data.frame(list(
    "table" = "stg_claims.stage_mcare_elig_timevar",
    "qa_type" = "part C 1-mon person count different from bene_enrollment table, expect 0",
    "qa" = qa5
  ))
  
  #Part D coverage 1-month person count matches bene_enrollment table, after accounting for death date censoring
  qa6a_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct a.bene_id) as qa
    from stg_claims.mcare_bene_enrollment as a
    left join stg_claims.final_mcare_elig_demo as b
    on a.bene_id = b.id_mcare
    where a.bene_enrollmt_ref_yr = 2014 and left(ptd_cntrct_id_11,1) in ('E', 'H', 'R', 'S', 'X')
    	and (b.death_dt >= '2014-11-01' or b.death_dt is null);", .con = inthealth))
  qa6b_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct id_mcare) as qa
    from stg_claims.stage_mcare_elig_timevar
    where from_date <= '2014-11-30' and to_date >= '2014-11-01' and part_d = 1;", .con = inthealth))
  if(qa6a_result$qa == qa6b_result$qa) {
    qa6 <- 0L
  } else {
    qa6 <- 1L
  }
  res6 <- as.data.frame(list(
    "table" = "stg_claims.stage_mcare_elig_timevar",
    "qa_type" = "part D 1-mon person count different from bene_enrollment table, expect 0",
    "qa" = qa6
  ))
  
  #Partial dual coverage 1-month person count matches bene_enrollment table, after accounting for death date censoring
  qa7a_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct a.bene_id) as qa
    from stg_claims.mcare_bene_enrollment as a
    left join stg_claims.final_mcare_elig_demo as b
    on a.bene_id = b.id_mcare
    where a.bene_enrollmt_ref_yr = 2021 and a.dual_stus_cd_03 in ('1', '01', '3', '03', '5', '05', '6', '06')
    	and (b.death_dt >= '2021-03-01' or b.death_dt is null);", .con = inthealth))
  qa7b_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct id_mcare) as qa
    from stg_claims.stage_mcare_elig_timevar
    where from_date <= '2021-03-31' and to_date >= '2021-03-01' and partial_dual = 1;", .con = inthealth))
  if(qa7a_result$qa == qa7b_result$qa) {
    qa7 <- 0L
  } else {
    qa7 <- 1L
  }
  res7 <- as.data.frame(list(
    "table" = "stg_claims.stage_mcare_elig_timevar",
    "qa_type" = "partial dual 1-mon person count different from bene_enrollment table, expect 0",
    "qa" = qa7
  ))
  
  #Full dual coverage 1-month person count matches bene_enrollment table, after accounting for death date censoring
  qa8a_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct a.bene_id) as qa
    from stg_claims.mcare_bene_enrollment as a
    left join stg_claims.final_mcare_elig_demo as b
    on a.bene_id = b.id_mcare
    where a.bene_enrollmt_ref_yr = 2021 and a.dual_stus_cd_03 in ('2', '02', '4', '04', '8', '08', '10')
    	and (b.death_dt >= '2021-03-01' or b.death_dt is null);", .con = inthealth))
  qa8b_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct id_mcare) as qa
    from stg_claims.stage_mcare_elig_timevar
    where from_date <= '2021-03-31' and to_date >= '2021-03-01' and full_dual = 1;", .con = inthealth))
  if(qa8a_result$qa == qa8b_result$qa) {
    qa8 <- 0L
  } else {
    qa8 <- 1L
  }
  res8 <- as.data.frame(list(
    "table" = "stg_claims.stage_mcare_elig_timevar",
    "qa_type" = "full dual 1-mon person count different from bene_enrollment table, expect 0",
    "qa" = qa8
  ))
  
  #State buy-in coverage 1-month person count matches bene_enrollment table, after accounting for death date censoring
  qa9a_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct a.bene_id) as qa
    from stg_claims.mcare_bene_enrollment as a
    left join stg_claims.final_mcare_elig_demo as b
    on a.bene_id = b.id_mcare
    where a.bene_enrollmt_ref_yr = 2021 and a.mdcr_entlmt_buyin_ind_03 in ('A','B','C')
    	and (b.death_dt >= '2021-03-01' or b.death_dt is null);", .con = inthealth))
  qa9b_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct id_mcare) as qa
    from stg_claims.stage_mcare_elig_timevar
    where from_date <= '2021-03-31' and to_date >= '2021-03-01' and state_buyin = 1;", .con = inthealth))
  if(qa9a_result$qa == qa9b_result$qa) {
    qa9 <- 0L
  } else {
    qa9 <- 1L
  }
  res9 <- as.data.frame(list(
    "table" = "stg_claims.stage_mcare_elig_timevar",
    "qa_type" = "state buy-in 1-mon person count different from bene_enrollment table, expect 0",
    "qa" = qa9
  ))
  
  #Count of distinct people matches bene_enrollment table
  qa10a_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct id_mcare) as qa from stg_claims.stage_mcare_elig_timevar;", .con = inthealth))
  qa10b_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct bene_id) as qa from stg_claims.mcare_bene_enrollment;", .con = inthealth))
  if(qa10a_result$qa == qa10b_result$qa) {
    qa10 <- 0L
  } else {
    qa10 <- 1L
  }
  res10 <- as.data.frame(list(
    "table" = "stg_claims.stage_mcare_elig_timevar",
    "qa_type" = "distinct person count difference from bene_enrollment table, expect 0",
    "qa" = qa10
  ))

res_final <- mget(ls(pattern="^res")) %>% bind_rows()  
}