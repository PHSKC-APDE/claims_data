#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_elig_demo
# Eli Kern, PHSKC (APDE)
#
# 2024-05

### Run from 02_master_mcare_claims_analytic.R script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/02_master_mcare_claims_analytic.R

#### Load script ####
load_stage.mcare_elig_demo_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "---------------------
    --Create elig_demo table from bene_enrollment table
    --Eli Kern, adapted from Danny Colombara script
    --2024-05
    ----------------------
    
    --Select most recent DOB for each person
    if object_id(N'tempdb..#dob_rank') is not null drop table #dob_rank;
    with dob_distinct as (
    	select bene_id, bene_birth_dt, max(bene_enrollmt_ref_yr) as year_max
    	from stg_claims.mcare_bene_enrollment
    	where bene_birth_dt is not null
    	group by bene_id, bene_birth_dt
    ),
    dob_rank as (
    	select *, rank() over(partition by bene_id order by year_max desc) as dob_rank
    	from dob_distinct
    )
    select a.bene_id, a.bene_birth_dt
    into #dob_rank
    from dob_distinct as a
    left join dob_rank as b
    on (a.bene_id = b.bene_id) and (a.bene_birth_dt = b.bene_birth_dt)
    where dob_rank = 1;
    
    --Select most recent date of death for each person
    if object_id(N'tempdb..#dod_rank') is not null drop table #dod_rank;
    with dod_distinct as (
    	select bene_id, bene_death_dt, max(bene_enrollmt_ref_yr) as year_max
    	from stg_claims.mcare_bene_enrollment
    	where bene_death_dt is not null
    	group by bene_id, bene_death_dt
    ),
    dod_rank as (
    	select *, rank() over(partition by bene_id order by year_max desc) as dod_rank
    	from dod_distinct
    )
    select a.bene_id, a.bene_death_dt
    into #dod_rank
    from dod_distinct as a
    left join dod_rank as b
    on (a.bene_id = b.bene_id) and (a.bene_death_dt = b.bene_death_dt)
    where b.dod_rank = 1;
    
    --Create KC ever indicator (ZIP code based)
    if object_id(N'tempdb..#kc_ever') is not null drop table #kc_ever;
    with kc_zip as (
    	select distinct a.bene_id, left(a.zip_cd, 5) as geo_zip, b.geo_kc
    	from stg_claims.mcare_bene_enrollment as a
    	left join stg_claims.ref_geo_kc_zip as b
    	on left(a.zip_cd, 5) = b.geo_zip
    )
    select bene_id, max(geo_kc) as geo_kc_ever
    into #kc_ever
    from kc_zip
    group by bene_id;
    
    --Process sex data
    --https://resdac.org/cms-data/variables/sex
    if object_id(N'tempdb..#sex') is not null drop table #sex;
    with sex_distinct as (
    	select bene_id, sex_ident_cd, max(bene_enrollmt_ref_yr) as year_max
    	from stg_claims.mcare_bene_enrollment
    	where sex_ident_cd is not null and sex_ident_cd != '0'
    	group by bene_id, sex_ident_cd
    ),
    sex_rank as (
    	select *, rank() over(partition by bene_id order by year_max desc) as sex_rank
    	from sex_distinct
    ),
    sex_mi as (
    	select bene_id,
    	max(case when sex_ident_cd = '1' then 1 else 0 end) as gender_male,
    	max(case when sex_ident_cd = '2' then 1 else 0 end) as gender_female
    	from sex_distinct
    	group by bene_id
    ),
    sex_me as (
    	select bene_id, case
    		when gender_male = 1 and gender_female = 1 then 'Multiple'
    		when gender_female = 1 then 'Female'
    		when gender_male = 1 then 'Male'
    		else 'Unknown'
    	end as gender_me
    	from sex_mi
    )
    select distinct a.bene_id,
    c.gender_me,
    case
    	when d.sex_ident_cd = '1' then 'Male'
    	when d.sex_ident_cd = '2' then 'Female'
    	else 'Unknown'
    end as gender_recent,
    b.gender_female,
    b.gender_male
    into #sex
    from sex_distinct as a
    left join sex_mi as b
    on (a.bene_id = b.bene_id)
    left join sex_me as c
    on (a.bene_id = c.bene_id)
    left join (select bene_id, sex_ident_cd from sex_rank where sex_rank = 1) as d
    on (a.bene_id = d.bene_id);
    
    --Process race/ethnicity data
    --Use rti_race_cd variable instead of bene_race_cd variable because former better allocates people to Hispanic and Asian/PI race
    --https://resdac.org/cms-data/variables/research-triangle-institute-rti-race-code
    if object_id(N'tempdb..#race') is not null drop table #race;
    with race_distinct as (
    	select bene_id, rti_race_cd, max(bene_enrollmt_ref_yr) as year_max
    	from stg_claims.mcare_bene_enrollment
    	where rti_race_cd is not null and rti_race_cd not in ('0','3')
    	group by bene_id, rti_race_cd
    ),
    race_eth_rank as (
    	select *, rank() over(partition by bene_id order by year_max desc) as race_eth_rank
    	from race_distinct
    ),
    race_rank as (
    	select *, rank() over(partition by bene_id order by year_max desc) as race_rank
    	from race_distinct
    	where rti_race_cd != '5'
    ),
    race_mi as (
    	select bene_id,
    	max(case when rti_race_cd = '1' then 1 else 0 end) as race_white,
    	max(case when rti_race_cd = '2' then 1 else 0 end) as race_black,
    	max(case when rti_race_cd = '4' then 1 else 0 end) as race_asian_pi,
    	max(case when rti_race_cd = '5' then 1 else 0 end) as race_latino,
    	max(case when rti_race_cd = '6' then 1 else 0 end) as race_aian
    	from race_distinct
    	group by bene_id
    ),
    race_sum as (
    	select *,
    	race_white + race_black + race_asian_pi + race_latino + race_aian as race_eth_sum,
    	race_white + race_black + race_asian_pi + race_aian as race_sum
    	from race_mi
    ),
    race_me as (
    	select bene_id,
    	case
    		when race_eth_sum > 1 then 'Multiple'
    		when race_white = 1 then 'White'
    		when race_black = 1 then 'Black'
    		when race_asian_pi = 1 then 'Asian/PI'
    		when race_latino = 1 then 'Latino'
    		when race_aian = 1 then 'AI/AN'
    		else 'Unknown'
    	end as race_eth_me,
    	case
    		when race_sum > 1 then 'Multiple'
    		when race_white = 1 then 'White'
    		when race_black = 1 then 'Black'
    		when race_asian_pi = 1 then 'Asian/PI'
    		when race_aian = 1 then 'AI/AN'
    		else 'Unknown'
    	end as race_me,
    	case when race_eth_sum = 0 then 1 else 0 end as race_eth_unk,
    	case when race_sum = 0 then 1 else 0 end as race_unk
    	from race_sum
    )
    select distinct a.bene_id,
    c.race_me,
    c.race_eth_me,
    case
    	when e.rti_race_cd = '1' then 'White'
    	when e.rti_race_cd = '2' then 'Black'
    	when e.rti_race_cd = '4' then 'Asian/PI'
    	when e.rti_race_cd = '6' then 'AI/AN'
    	else 'Unknown'
    end as race_recent,
    case
    	when d.rti_race_cd = '1' then 'White'
    	when d.rti_race_cd = '2' then 'Black'
    	when d.rti_race_cd = '4' then 'Asian/PI'
    	when d.rti_race_cd = '5' then 'Latino'
    	when d.rti_race_cd = '6' then 'AI/AN'
    	else 'Unknown'
    end as race_eth_recent,
    b.race_aian,
    b.race_asian_pi,
    b.race_black,
    b.race_latino,
    b.race_white,
    c.race_unk,
    c.race_eth_unk
    into #race
    from race_distinct as a
    left join race_mi as b
    on (a.bene_id = b.bene_id)
    left join race_me as c
    on (a.bene_id = c.bene_id)
    left join (select bene_id, rti_race_cd from race_eth_rank where race_eth_rank = 1) as d
    on (a.bene_id = d.bene_id)
    left join (select bene_id, rti_race_cd from race_rank where race_rank = 1) as e
    on (a.bene_id = e.bene_id);
    
    --Merge everything, add last_run, and insert into persistent table
    insert into stg_claims.stage_mcare_elig_demo
    
    select 
    a.bene_id as id_mcare,
    b.bene_birth_dt as dob,
    c.bene_death_dt as death_dt,
    d.geo_kc_ever,
    e.gender_me,
    e.gender_recent,
    e.gender_female,
    e.gender_male,
    f.race_me,
    f.race_eth_me,
    f.race_recent,
    f.race_eth_recent,
    f.race_aian,
    f.race_asian_pi,
    f.race_black,
    f.race_latino,
    f.race_white,
    f.race_unk,
    f.race_eth_unk,
    getdate() as last_run
    
    from (select distinct bene_id from stg_claims.mcare_bene_enrollment) as a
    left join #dob_rank as b
    on a.bene_id = b.bene_id
    left join #dod_rank as c
    on a.bene_id = c.bene_id
    left join #kc_ever as d
    on a.bene_id = d.bene_id
    left join #sex as e
    on a.bene_id = e.bene_id
    left join #race as f
    on a.bene_id = f.bene_id;",
        .con = dw_inthealth))
    }

#### Table-level QA script ####
qa_stage.mcare_elig_demo_qa_f <- function() {
  
  #confirm that no one has more than one row
  res1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "with test_1 as (
    select id_mcare, count(*) as row_count
    from stg_claims.stage_mcare_elig_demo
    group by id_mcare
    )
    select 'stg_claims.stage_mcare_elig_demo' as 'table',
    'people with more than 1 row' as qa_type, count(*) as qa
    from test_1
    where row_count >1;",
    .con = dw_inthealth))
  
  #confirm row count matches with bene_enrollment table
  qa2a_result <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select count(*) as row_count from stg_claims.stage_mcare_elig_demo;", .con = dw_inthealth))
  qa2b_result <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select count(*) as row_count from stg_claims.stage_mcare_bene_enrollment;", .con = dw_inthealth))
  qa2 <- qa2a_result$row_count == qa2b_result$row_count
  res2 <- as.data.frame(list(
    "table" = "stg_claims.stage_mcare_elig_demo",
    "qa_type" = "row count comparison with bene_enrollment table",
    "qa" = qa2
  ))
  
  #check multiple gender logic
  res3 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_elig_demo' as 'table', '# rows where multiple gender logic is wrong, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_mcare_elig_demo
    where (gender_me = 'Multiple' and gender_female = 0 and gender_male = 0)
	    or (gender_me != 'Multiple' and gender_female = 1 and gender_male = 1);",
    .con = dw_inthealth))
  
  #check race_eth vs race logic
  res4 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_elig_demo' as 'table', '# rows where race_eth vs race logic is wrong, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_mcare_elig_demo
    where race_eth_me = 'Multiple' and race_me != 'Multiple' and race_latino = 0;",
    .con = dw_inthealth))

res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}