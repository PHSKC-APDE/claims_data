#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_ELIG_DEMO
# Eli Kern, PHSKC (APDE)
#
# 2019-10

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_elig_demo_f <- function() {
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "--Code to load data to stage.apcd_elig_demo table
    --A historical record of each person's non time-varying demographics (e.g. date of birth, gender, race/ethnicity)
    --Eli Kern (PHSKC-APDE)
    --2019-10
    --Takes 60 min to run
    
    ------------------
    --STEP 1: Estimate date of birth and create gender variables from member_month_detail table
    -------------------
    if object_id('tempdb..#mm_temp1') is not null drop table #mm_temp1;
    select distinct a.internal_member_id as id_apcd, 
    	--when multiple ages were present across contiguous months, use this for DOB, otherwise use single age-based DOB
    	case when max(a.dob_1) is not null then max(a.dob_1) else max(a.dob_2) end as dob,
    	max(a.female) as female, max(a.male) as male, min(a.gender_unk) as gender_unk, 
    	case when min(a.age) = 90 then 1 else 0 end as ninety_only,
    	max(gender_recent) as gender_recent
    into #mm_temp1
    from (
    select internal_member_id, year_month, age, 
    	--when age changes between two contiguous months (year_month diff = 1 or 89 [for 12 to 01], use this change to estimate DOB
    	case when lag(age,1) over (partition by internal_member_id order by internal_member_id, year_month) < age and 
    		(year_month - lag(year_month,1) over (partition by internal_member_id order by internal_member_id, year_month)) in (1, 89)
    		then convert(date, cast(year_month - lag((age + 1) * 100,1) over (partition by internal_member_id order by internal_member_id, year_month) as varchar(200)) + '01')
    	end as dob_1,
    	--when only a single age is available for all history, use the last recorded age and month to estimate age (will overestimate age, thus choose an earlier DOB)
    	case when lead(age,1) over (partition by internal_member_id order by internal_member_id, year_month) is null 
    		then dateadd(month, 1, convert(date, cast((year_month - ((age + 1) * 100)) as varchar(200)) + '01'))
    	end as dob_2,
      --create alone or in combination gender variables
      case when gender_code = 'F' then 1 when gender_code = 'U' then null else 0 end as female,
      case when gender_code = 'M' then 1 when gender_code = 'U' then null else 0 end as male,
      case when gender_code = 'U' then 1 else 0 end as gender_unk,
      --create variable to hold most recent gender, ignore null and 'Unknown' values
      last_value(gender_code) over (partition by internal_member_id
    	order by internal_member_id, case when gender_code = 'U' or gender_code is null then null else year_month end
    		rows between unbounded preceding and unbounded following) as gender_recent
    from PHClaims.stage.apcd_member_month_detail
    ) as a
    group by a.internal_member_id;
    
    
    ------------------
    --STEP 2: Create final age and gender variables
    -------------------
    if object_id('tempdb..#mm_final') is not null drop table #mm_final;
    select
    	id_apcd,
    	--age is missing when only age 90 is available in member month, ninety_only flag is equal to 1
    	case
    		when ninety_only = 0 then dob
    		else null
    	end as dob,
    	ninety_only,
    	--mutally inclusive gender
    	female as gender_female,
    	male as gender_male,
    	--recode values of recent gender variable
    	case
    		when gender_recent = 'F' then 'Female'
    		when gender_recent = 'M' then 'Male'
    		when gender_recent = 'U' then 'Unknown'
    	end as gender_recent,
    	--mutually exclusive gender
    	case
    		when female = 1 and male = 1 then 'Multiple'
    		when female = 1 then 'Female'
    		when male = 1 then 'Male'
    		when gender_unk = 1 then 'Unknown'
    	end as gender_me
    into #mm_final
    from #mm_temp1;
    
    
    ------------------
    --STEP 3: Recode APCD race and latino variables
    -------------------
    --recode null/unknown/other race as 0 for later math
    --latino (1=Yes, 2=No)
    --race variable codes in ref.apcd_race
    if object_id('tempdb..#elig_temp1') is not null drop table #elig_temp1;
    select eligibility_id, internal_member_id as id_apcd, eligibility_end_dt,
    case when race_id1 in (1,2,3,4,5) then race_id1 else 0 end as race_id1,
    case when race_id2 in (1,2,3,4,5) then race_id1 else 0 end as race_id2,
    case when hispanic_id in (1,2) then hispanic_id else 0 end as latino_id
    into #elig_temp1
    from PHClaims.stage.apcd_eligibility;
    
    
    ------------------
    --STEP 4: Recode APCD ethnicity variables
    --Reference: --https://www.nap.edu/catalog/12696/race-ethnicity-and-language-data-standardization-for-health-care-quality (Table E-1)
    -------------------
    if object_id('tempdb..#elig_temp2') is not null drop table #elig_temp2;
    select a.eligibility_id, a.internal_member_id as id_apcd, a.eligibility_end_dt,
    case when b.ethnicity_id is null then 0 else b.race_id end as race_id3
    into #elig_temp2
    from PHClaims.stage.apcd_eligibility as a
    left join PHClaims.ref.apcd_ethnicity_race_map as b
    on a.ethnicity_id1 = b.ethnicity_id;
    
    if object_id('tempdb..#elig_temp3') is not null drop table #elig_temp3;
    select a.eligibility_id, a.internal_member_id as id_apcd, a.eligibility_end_dt,
    case when b.ethnicity_id is null then 0 else b.race_id end as race_id4
    into #elig_temp3
    from PHClaims.stage.apcd_eligibility as a
    left join PHClaims.ref.apcd_ethnicity_race_map as b
    on a.ethnicity_id2 = b.ethnicity_id;
    
    
    ------------------
    --STEP 5: Merge race and ethnicity-based race variables
    -------------------
    if object_id('tempdb..#elig_temp4') is not null drop table #elig_temp4;
    select a.id_apcd, a.eligibility_end_dt, a.race_id1, a.race_id2, a.latino_id, b.race_id3, c.race_id4
    into #elig_temp4
    from #elig_temp1 as a
    left join #elig_temp2 as b
    on a.eligibility_id = b.eligibility_id
    left join #elig_temp3 as c
    on a.eligibility_id = c.eligibility_id;
    
    
    ------------------
    --STEP 6: Create normalized race variables
    --Select distinct as eligibility_id is no longer needed
    -------------------
    if object_id('tempdb..#elig_temp5') is not null drop table #elig_temp5;
    select distinct id_apcd, eligibility_end_dt,
      --create mutually exclusive race variables
      case when race_id1 = 1 or race_id2 = 1 or race_id3 = 1 or race_id4 = 1 then 1 else 0 end as race_aian,
      case when race_id1 = 2 or race_id2 = 2 or race_id3 = 2 or race_id4 = 2 then 1 else 0 end as race_asian,
      case when race_id1 = 3 or race_id2 = 3 or race_id3 = 3 or race_id4 = 3 then 1 else 0 end as race_black,
      case when latino_id = 1 or race_id3 = 8 or race_id4 = 8 then 1 else 0 end as race_latino,
      case when race_id1 = 4 or race_id2 = 4 or race_id3 = 4 or race_id4 = 4 then 1 else 0 end as race_nhpi,
      case when race_id1 = 5 or race_id2 = 5 or race_id3 = 5 or race_id4 = 5 then 1 else 0 end as race_white,
      case when race_id1 = 0 and race_id2 = 0 and (latino_id = 0 or latino_id = 2) and race_id3 = 0 and race_id4 = 0 then 1 else 0 end as race_unknown
    into #elig_temp5
    from #elig_temp4;
    
    
    ------------------
    --STEP 7: Create mutually exclusive race variables (two flavors)
    --Temporarily code Multiple as z_Multiple so that it is chosen as last value if multiple races exist with same eligibility end_date
    -------------------
    if object_id('tempdb..#elig_temp6') is not null drop table #elig_temp6;
    select *,
    case
    	when race_aian + race_asian + race_black + race_latino + race_nhpi + race_white > 1 then 'z_Multiple'
    	when race_aian = 1 and race_asian = 0 and race_black = 0 and race_latino = 0 and race_nhpi = 0 and race_white = 0 then 'AI/AN'
    	when race_asian = 1 and race_aian = 0 and race_black = 0 and race_latino = 0 and race_nhpi = 0 and race_white = 0 then 'Asian'
    	when race_black = 1 and race_aian = 0 and race_asian = 0 and race_latino = 0 and race_nhpi = 0 and race_white = 0 then 'Black'
    	when race_latino = 1 and race_aian = 0 and race_asian = 0 and race_black = 0 and race_nhpi = 0 and race_white = 0 then 'Latino'
    	when race_nhpi = 1 and race_aian = 0 and race_asian = 0 and race_black = 0 and race_latino = 0 and race_white = 0 then 'NH/PI'
    	when race_white = 1 and race_aian = 0 and race_asian = 0 and race_black = 0 and race_latino = 0 and race_nhpi = 0 then 'White'
    	when race_unknown = 1 then 'Unknown'
    end as race_eth_me,
    case
    	when race_aian + race_asian + race_black + race_nhpi + race_white > 1 then 'z_Multiple'
    	when race_aian = 1 and race_asian = 0 and race_black = 0 and race_nhpi = 0 and race_white = 0 then 'AI/AN'
    	when race_asian = 1 and race_aian = 0 and race_black = 0 and race_nhpi = 0 and race_white = 0 then 'Asian'
    	when race_black = 1 and race_aian = 0 and race_asian = 0 and race_nhpi = 0 and race_white = 0 then 'Black'
    	when race_nhpi = 1 and race_aian = 0 and race_asian = 0 and race_black = 0 and race_white = 0 then 'NH/PI'
    	when race_white = 1 and race_aian = 0 and race_asian = 0 and race_black = 0 and race_nhpi = 0 then 'White'
    	when race_unknown = 1 then 'Unknown'
    	else 'Unknown'
    end as race_me
    into #elig_temp6
    from #elig_temp5;
    
    
    ------------------
    --STEP 8: Flag most recent race, ignore null values, sorts by race to choose same tie-breaker each time
    -------------------
    if object_id('tempdb..#elig_temp7') is not null drop table #elig_temp7;
    select *,
    last_value(race_eth_me) over (partition by id_apcd
    	order by id_apcd, case when race_unknown = 1 then null else eligibility_end_dt end, race_eth_me
    	rows between unbounded preceding and unbounded following) as race_eth_recent,
    last_value(race_me) over (partition by id_apcd
    	order by id_apcd, case when race_unknown = 1 then null else eligibility_end_dt end, race_me
    	rows between unbounded preceding and unbounded following) as race_recent
    into #elig_temp7
    from #elig_temp6;
    
    
    ------------------
    --STEP 9: Collapse to individual member level for joining with age and gender data
    --Note the recreation of the multually exclusive race variables has to ignore Unknown race in order to return correct value
    -------------------
    if object_id('tempdb..#elig_final') is not null drop table #elig_final;
    select id_apcd, 
    	case
    		when max(race_aian) + max(race_asian) + max(race_black) + max(race_latino) + max(race_nhpi) + max(race_white) > 1 then 'Multiple'
    		else max(case when race_eth_me = 'Unknown' then null else race_eth_me end)
    	end as race_eth_me,
    	case
    		when max(race_aian) + max(race_asian) + max(race_black) + max(race_nhpi) + max(race_white) > 1 then 'Multiple'
    		else max(case when race_me = 'Unknown' then null else race_me end)
    	end as race_me,
    	case when max(race_eth_recent) = 'z_Multiple' then 'Multiple' else max(race_eth_recent) end as race_eth_recent,
    	case when max(race_recent) = 'z_Multiple' then 'Multiple' else max(race_recent) end as race_recent,
    	max(race_aian) as race_aian, max(race_asian) as race_asian, max(race_black) as race_black,
    	max(race_latino) as race_latino, max(race_nhpi) as race_nhpi, max(race_white) as race_white, min(race_unknown) as race_unknown
    into #elig_final
    from #elig_temp7
    group by id_apcd;
    
    
    ------------------
    --STEP 10: Join age, gender and race variables on member ID, and insert data
    --Note that left join ensures that only people who have made it to member_month_detail table (per OnPoint processing) are in elig_demo
    --For extract 187, 658 people were in eligibility but not member_month_detail table
    -------------------
    insert into PHClaims.stage.apcd_elig_demo with (tablock)
    select
    	a.id_apcd,
    	a.dob,
    	a.ninety_only,
    	a.gender_me,
    	a.gender_recent,
    	a.gender_female,
    	a.gender_male,
    	b.race_eth_me,
    	b.race_me,
    	b.race_eth_recent,
    	b.race_recent,
    	b.race_aian,
    	b.race_asian,
    	b.race_black,
    	b.race_latino,
    	b.race_nhpi,
    	b.race_white,
    	b.race_unknown,
    	getdate() as last_run
    from #mm_final as a
    left join #elig_final as b
    on a.id_apcd = b.id_apcd;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.apcd_elig_demo_f <- function() {
  
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_elig_demo' as 'table', 'distinct count' as qa_type, count(distinct id_apcd) as qa from stage.apcd_elig_demo",
    .con = db_claims))
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_member_month_detail' as 'table', 'distinct count' as qa_type, count(distinct internal_member_id) as qa from stage.apcd_member_month_detail",
    .con = db_claims))
  res3 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_elig_demo' as 'table', 'count' as qa_type, count(id_apcd) as qa from stage.apcd_elig_demo",
    .con = db_claims))
  res_final <- bind_rows(res1, res2, res3)
}