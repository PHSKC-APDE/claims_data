-- Code to create apcd_demo_ever table
--A historical record of each person's non time-varying demographics (e.g. date of birth, gender)
-- Eli Kern (PHSKC-APDE)
-- 2019-3-28
-- Takes X min to run

------------------
--STEP 1: Estimate date of birth and create gender variables
-------------------
if object_id('tempdb..#temp1') is not null drop table #temp1;
select distinct a.internal_member_id as id, max(a.dob) as dob, max(a.female) as female, max(a.male) as male, min(a.gender_unk) as gender_unk, case when min(a.age) = 90 then 1 else 0 end as ninety_only
into #temp1
from (
select internal_member_id, year_month, age, 
	--estimate date of birth from year_month and changes in integer age
	case
	--when age changes between two contiguous months, use this change to estimate DOB
	when lag(age,1) over (partition by internal_member_id order by internal_member_id, year_month) < age and 
		(year_month - lag(year_month,1) over (partition by internal_member_id order by internal_member_id, year_month)) = 1
		then convert(date, cast(year_month - lag((age + 1) * 100,1) over (partition by internal_member_id order by internal_member_id, year_month) as varchar(200)) + '01')
	--when only a single age is available for all history, use the last recorded age and month to estimate age (will overestimate age, thus choose an earlier DOB)
	when lead(age,1) over (partition by internal_member_id order by internal_member_id, year_month) is null 
		then convert(date, cast(year_month - ((age + 1) * 100) as varchar(200)) + '01')
	else null
	end as dob,
  --create alone or in combination gender variables
  case when gender_code = 'F' then 1 else 0 end as female,
  case when gender_code = 'M' then 1 else 0 end as male,
  case when gender_code = 'U' then 1 else 0 end as gender_unk
from PHClaims.stage.apcd_member_month_detail
) as a
group by a.internal_member_id;

------------------
--STEP 2: Insert data, creating additional age and gender variables
-------------------
insert into PHClaims.stage.apcd_elig_demo with (tablock)
select
	id,
	--age is missing when only age 90 is available in member month, ninety_only flag is equal to 1
	case
		when ninety_only = 0 then dob
		else null
	end as dob,
	ninety_only,
	female,
	male,
	--multiple gender variable
	case
		when female = 1 and male = 1 then 'Multiple'
		when female = 1 then 'Female'
		when male = 1 then 'Male'
		when gender_unk = 1 then 'Unknown'
	end as gender_me
from #temp1;



