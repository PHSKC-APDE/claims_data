#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_elig_month
# Eli Kern, PHSKC (APDE)
#
# 2025-06

### Run from 02_master_mcare_claims_analytic.R script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/02_master_mcare_claims_analytic.R

#### Load script ####
load_stage.mcare_elig_month_f <- function(conn = NULL,
                                          config_url = NULL) {
  
  ### Set up variables from config file
  if (is.null(config_url) == T){
    stop("A URL must be specified in config_url")
    } else {
      config <- yaml::yaml.load(RCurl::getURL(config_url))
    }
  
  to_schema <- config[["schema"]]
  to_table <- config[["table"]]
  ref_schema <- config[["ref_schema"]]
  geokc_table <- config[["geokc_table"]]
  date_table <- config[["date_table"]]
  bene_enrollment_table <- config[["bene_enrollment_table"]]
  elig_demo_table <- config[["elig_demo_table"]]
  
  message("Creating ", to_schema, ".", to_table, ". This will take ~10 minutes to run.")
  
  ### Run SQL query
  odbc::dbGetQuery(inthealth, glue::glue_sql(
    "---------------------
    --Create mcare_elig_month table from bene_enrollment table
    --Eli Kern
    --2025-06
    ----------------------
        
    ----------------------
    --STEP 1: Pull out desired enrollment columns, minor transformations, reshape wide to long
    ----------------------
    if object_id(N'tempdb..#month_01') is not null drop table #month_01;
    with buyins as (
        select
        --top 1000
        bene_id,
        bene_enrollmt_ref_yr as cal_year,
        right(cal_mon,2) as cal_mon,
        case when len(zip_cd) < 5 then null else left(zip_cd,5) end as geo_zip,
        buyins as buyins
        from {`to_schema`}.{`bene_enrollment_table`} as a
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
        from {`to_schema`}.{`bene_enrollment_table`} as a
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
        from {`to_schema`}.{`bene_enrollment_table`} as a
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
        from {`to_schema`}.{`bene_enrollment_table`} as a
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
    into #month_01
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
    if object_id(N'tempdb..#month_02') is not null drop table #month_02;
    select
    bene_id as id_mcare,
    b.first_day_month as from_date,
    b.last_day_month as to_date,
    cast(cast(a.cal_year as varchar(4)) + a.cal_mon as int) as year_month,
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
    into #month_02
    from #month_01 as a
    left join (select distinct year_month, first_day_month, last_day_month from {`to_schema`}.{`date_table`}) as b
    on cast(cast(a.cal_year as varchar(4)) + a.cal_mon as int) = b.year_month;
        
        
    ----------------------
    --STEP 2b: Drop months with no coverage (data), drop months that occur after death_dt, truncate to_date to death_dt where relevant
    ----------------------
    if object_id(N'tempdb..#month_02b') is not null drop table #month_02b;
    with cov_type_sum as (
    select *,
    part_a + part_b + part_c + part_d + state_buyin + partial_dual + full_dual as cov_type_sum
    from #month_02
    )
    select
    a.id_mcare,
    a.from_date,
    case
        when b.death_dt is not null and a.from_date <= b.death_dt and a.to_date > b.death_dt then b.death_dt
        else a.to_date
    end as to_date,
    a.year_month,
    a.geo_zip,
    a.part_a,
    a.part_b,
    a.part_c,
    a.part_d,
    a.state_buyin,
    a.partial_dual,
    a.full_dual
    into #month_02b
    from cov_type_sum as a
    left join {`to_schema`}.{`elig_demo_table`} as b
    on a.id_mcare = b.id_mcare
    where a.cov_type_sum > 0
        and (a.from_date <= b.death_dt or b.death_dt is null);
        
    
    ----------------------
    --STEP 3: Calculate days of coverage
    ----------------------
    if object_id(N'tempdb..#month_03') is not null drop table #month_03;
    select
    id_mcare,
    from_date,
    to_date,
    year_month,
    geo_zip,
    part_a,
    part_b,
    part_c,
    part_d,
    state_buyin,
    partial_dual,
    full_dual,
    datediff(dd, from_date, to_date) + 1 as cov_time_day
    into #month_03
    from #month_02b;
        
        
    ----------------------
    --STEP 8: Add geo_kc flag, other date parts, and load to persistent table
    ----------------------
    insert into {`to_schema`}.{`to_table`}
    select
    a.id_mcare,
    a.from_date,
    a.to_date,
    c.year_month,
    c.year_quarter,
    c.[year],
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
    from #month_03 as a
    left join (select distinct geo_zip, geo_kc from {`to_schema`}.{`geokc_table`}) as b
    on a.geo_zip = b.geo_zip
    left join (select distinct year_month, year_quarter, [year] from {`to_schema`}.{`date_table`}) as c
    on a.year_month = c.year_month;",
    .con = conn))
}

#### Table-level QA script ####
qa_stage.mcare_elig_month_qa_f <- function() {
  
  #Count of people with to_date after death_dt, expect 0
  res1 <- dbGetQuery(conn = conn, glue_sql(
    "select '{`to_schema`}.{`to_table`}' as 'table',
    'people with to_date after death_dt, expect 0' as qa_type,
    count(distinct a.id_mcare) as qa
    from {`to_schema`}.{`to_table`} as a
    left join {`to_schema`}.{`elig_demo_table`} as b
    on a.id_mcare = b.id_mcare
    where a.to_date > b.death_dt;",
    .con = conn))
  
  #Count of people with to_date before from_date or from_date after to_date, expect 0
  res2 <- dbGetQuery(conn = conn, glue_sql(
    "select '{`to_schema`}.{`to_table`}' as 'table',
    'people with to_date before from_date or from_date after to_date, expect 0' as qa_type,
    count(distinct id_mcare) as qa
    from {`to_schema`}.{`to_table`}
    where (to_date < from_date) or (from_date > to_date);",
    .con = inthealth))
  
  #Part A coverage 1-month person count matches bene_enrollment table, after accounting for death date censoring
  qa3a_result <- dbGetQuery(conn = conn, glue_sql(
    "select count(distinct a.bene_id) as qa
    from {`to_schema`}.{`bene_enrollment_table`} as a
    left join {`to_schema`}.{`elig_demo_table`} as b
    on a.bene_id = b.id_mcare
    where a.bene_enrollmt_ref_yr = 2016 and a.mdcr_entlmt_buyin_ind_04 in ('1','3','A','C')
    	and (b.death_dt >= '2016-04-01' or b.death_dt is null);", .con = conn))
  qa3b_result <- dbGetQuery(conn = conn, glue_sql(
    "select count(distinct id_mcare) as qa
    from {`to_schema`}.{`to_table`}
    where from_date <= '2016-04-30' and to_date >= '2016-04-01' and part_a = 1;", .con = conn))
  if(qa3a_result$qa == qa3b_result$qa) {
    qa3 <- 0L
  } else {
    qa3 <- 1L
  }
  res3 <- as.data.frame(list(
    "table" = "{`to_schema`}.{`to_table`}",
    "qa_type" = "part A 1-mon person count different from bene_enrollment table, expect 0",
    "qa" = qa3
  )) 
  
  #Part B coverage 1-month person count matches bene_enrollment table, after accounting for death date censoring
  qa4a_result <- dbGetQuery(conn = conn, glue_sql(
    "select count(distinct a.bene_id) as qa
    from {`to_schema`}.{`bene_enrollment_table`} as a
    left join {`to_schema`}.{`elig_demo_table`} as b
    on a.bene_id = b.id_mcare
    where a.bene_enrollmt_ref_yr = 2018 and a.mdcr_entlmt_buyin_ind_07 in ('2','3','B','C')
    	and (b.death_dt >= '2018-07-01' or b.death_dt is null);", .con = conn))
  qa4b_result <- dbGetQuery(conn = inthealth, glue_sql(
    "select count(distinct id_mcare) as qa
    from {`to_schema`}.{`to_table`}
    where from_date <= '2018-07-31' and to_date >= '2018-07-01' and part_b = 1;", .con = conn))
  if(qa4a_result$qa == qa4b_result$qa) {
    qa4 <- 0L
  } else {
    qa4 <- 1L
  }
  res4 <- as.data.frame(list(
    "table" = "{`to_schema`}.{`to_table`}",
    "qa_type" = "part B 1-mon person count different from bene_enrollment table, expect 0",
    "qa" = qa4
  ))
  
  #Part C coverage 1-month person count matches bene_enrollment table, after accounting for death date censoring
  qa5a_result <- dbGetQuery(conn = conn, glue_sql(
    "select count(distinct a.bene_id) as qa
    from {`to_schema`}.{`bene_enrollment_table`} as a
    left join {`to_schema`}.{`elig_demo_table`} as b
    on a.bene_id = b.id_mcare
    where a.bene_enrollmt_ref_yr = 2020 and a.hmo_ind_01 in ('1','2','A','B','C')
    	and (b.death_dt >= '2020-01-01' or b.death_dt is null);", .con = conn))
  qa5b_result <- dbGetQuery(conn = conn, glue_sql(
    "select count(distinct id_mcare) as qa
    from {`to_schema`}.{`to_table`}
    where from_date <= '2020-01-31' and to_date >= '2020-01-01' and part_c = 1;", .con = conn))
  if(qa5a_result$qa == qa5b_result$qa) {
    qa5 <- 0L
  } else {
    qa5 <- 1L
  }
  res5 <- as.data.frame(list(
    "table" = "{`to_schema`}.{`to_table`}",
    "qa_type" = "part C 1-mon person count different from bene_enrollment table, expect 0",
    "qa" = qa5
  ))
  
  #Part D coverage 1-month person count matches bene_enrollment table, after accounting for death date censoring
  qa6a_result <- dbGetQuery(conn = conn, glue_sql(
    "select count(distinct a.bene_id) as qa
    from {`to_schema`}.{`bene_enrollment_table`} as a
    left join {`to_schema`}.{`elig_demo_table`} as b
    on a.bene_id = b.id_mcare
    where a.bene_enrollmt_ref_yr = 2014 and left(ptd_cntrct_id_11,1) in ('E', 'H', 'R', 'S', 'X')
    	and (b.death_dt >= '2014-11-01' or b.death_dt is null);", .con = conn))
  qa6b_result <- dbGetQuery(conn = conn, glue_sql(
    "select count(distinct id_mcare) as qa
    from {`to_schema`}.{`to_table`}
    where from_date <= '2014-11-30' and to_date >= '2014-11-01' and part_d = 1;", .con = conn))
  if(qa6a_result$qa == qa6b_result$qa) {
    qa6 <- 0L
  } else {
    qa6 <- 1L
  }
  res6 <- as.data.frame(list(
    "table" = "{`to_schema`}.{`to_table`}",
    "qa_type" = "part D 1-mon person count different from bene_enrollment table, expect 0",
    "qa" = qa6
  ))
  
  #Partial dual coverage 1-month person count matches bene_enrollment table, after accounting for death date censoring
  qa7a_result <- dbGetQuery(conn = conn, glue_sql(
    "select count(distinct a.bene_id) as qa
    from {`to_schema`}.{`bene_enrollment_table`} as a
    left join {`to_schema`}.{`elig_demo_table`} as b
    on a.bene_id = b.id_mcare
    where a.bene_enrollmt_ref_yr = 2021 and a.dual_stus_cd_03 in ('1', '01', '3', '03', '5', '05', '6', '06')
    	and (b.death_dt >= '2021-03-01' or b.death_dt is null);", .con = conn))
  qa7b_result <- dbGetQuery(conn = conn, glue_sql(
    "select count(distinct id_mcare) as qa
    from {`to_schema`}.{`to_table`}
    where from_date <= '2021-03-31' and to_date >= '2021-03-01' and partial_dual = 1;", .con = conn))
  if(qa7a_result$qa == qa7b_result$qa) {
    qa7 <- 0L
  } else {
    qa7 <- 1L
  }
  res7 <- as.data.frame(list(
    "table" = "{`to_schema`}.{`to_table`}",
    "qa_type" = "partial dual 1-mon person count different from bene_enrollment table, expect 0",
    "qa" = qa7
  ))
  
  #Full dual coverage 1-month person count matches bene_enrollment table, after accounting for death date censoring
  qa8a_result <- dbGetQuery(conn = conn, glue_sql(
    "select count(distinct a.bene_id) as qa
    from {`to_schema`}.{`bene_enrollment_table`} as a
    left join {`to_schema`}.{`elig_demo_table`} as b
    on a.bene_id = b.id_mcare
    where a.bene_enrollmt_ref_yr = 2021 and a.dual_stus_cd_03 in ('2', '02', '4', '04', '8', '08', '10')
    	and (b.death_dt >= '2021-03-01' or b.death_dt is null);", .con = conn))
  qa8b_result <- dbGetQuery(conn = conn, glue_sql(
    "select count(distinct id_mcare) as qa
    from {`to_schema`}.{`to_table`}
    where from_date <= '2021-03-31' and to_date >= '2021-03-01' and full_dual = 1;", .con = conn))
  if(qa8a_result$qa == qa8b_result$qa) {
    qa8 <- 0L
  } else {
    qa8 <- 1L
  }
  res8 <- as.data.frame(list(
    "table" = "{`to_schema`}.{`to_table`}",
    "qa_type" = "full dual 1-mon person count different from bene_enrollment table, expect 0",
    "qa" = qa8
  ))
  
  #State buy-in coverage 1-month person count matches bene_enrollment table, after accounting for death date censoring
  qa9a_result <- dbGetQuery(conn = conn, glue_sql(
    "select count(distinct a.bene_id) as qa
    from {`to_schema`}.{`bene_enrollment_table`} as a
    left join {`to_schema`}.{`elig_demo_table`} as b
    on a.bene_id = b.id_mcare
    where a.bene_enrollmt_ref_yr = 2021 and a.mdcr_entlmt_buyin_ind_03 in ('A','B','C')
    	and (b.death_dt >= '2021-03-01' or b.death_dt is null);", .con = conn))
  qa9b_result <- dbGetQuery(conn = conn, glue_sql(
    "select count(distinct id_mcare) as qa
    from {`to_schema`}.{`to_table`}
    where from_date <= '2021-03-31' and to_date >= '2021-03-01' and state_buyin = 1;", .con = conn))
  if(qa9a_result$qa == qa9b_result$qa) {
    qa9 <- 0L
  } else {
    qa9 <- 1L
  }
  res9 <- as.data.frame(list(
    "table" = "{`to_schema`}.{`to_table`}",
    "qa_type" = "state buy-in 1-mon person count different from bene_enrollment table, expect 0",
    "qa" = qa9
  ))
  
  #Count of distinct people matches bene_enrollment table
  qa10a_result <- dbGetQuery(conn = conn, glue_sql(
    "select count(distinct id_mcare) as qa from {`to_schema`}.{`to_table`};", .con = conn))
  qa10b_result <- dbGetQuery(conn = conn, glue_sql(
    "select count(distinct bene_id) as qa from {`to_schema`}.{`bene_enrollment_table`};", .con = conn))
  if(qa10a_result$qa == qa10b_result$qa) {
    qa10 <- 0L
  } else {
    qa10 <- 1L
  }
  res10 <- as.data.frame(list(
    "table" = "{`to_schema`}.{`to_table`}",
    "qa_type" = "distinct person count difference from bene_enrollment table, expect 0",
    "qa" = qa10
  ))

res_final <- mget(ls(pattern="^res")) %>% bind_rows()  
}