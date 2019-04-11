-- Code to create apcd_demo_ever table
--A historical record of each person's non time-varying demographics (e.g. date of birth, gender)
-- Eli Kern (PHSKC-APDE)
-- 2019-3-28
-- Takes X min to run


--convert redshift code to T-SQL

select top 100 age,
  --estimate date of birth from year_month and changes in integer age
  case
    when lag(age,1) over (partition by internal_member_id order by internal_member_id, year_month) < age then convert(datetime, cast(year_month - lag((age + 1) * 100,1)
      over (partition by internal_member_id order by internal_member_id, year_month) as varchar(200)) + '01')
    when lead(age,1) over (partition by internal_member_id order by internal_member_id, year_month) is null then convert(datetime, cast(year_month - ((age + 1) * 100) as varchar(200)) + '01')
    else null
  end as dob_est
from PHClaims.stage.apcd_member_month_detail



-------------------
--STEP 1: Estimate date of birth and create gender variables
-------------------
if object_id('tempdb..#temp1') is not null drop table #temp1;
select distinct a.internal_member_id, max(a.dob_est) as dob_estimate, max(a.female) as female, max(a.male) as male, min(a.gender_unk) as gender_unk, case when min(a.age) = 90 then 1 else 0 end as ninety_only
into #temp1
from (
select internal_member_id, year_month, age, 
  --estimate date of birth from year_month and changes in integer age
  case
    when lag(age,1) over (partition by internal_member_id order by internal_member_id, year_month) < age then trunc(convert(datetime, cast(year_month - lag((age + 1) * 100,1)
      over (partition by internal_member_id order by internal_member_id, year_month) as varchar(200)) + '01'))
    when lead(age,1) over (partition by internal_member_id order by internal_member_id, year_month) is null then trunc(convert(datetime, cast(year_month - ((age + 1) * 100) as varchar(200)) + '01'))
    else null
  end as dob_est,
  --create alone or in combination gender variables
  case when gender_code = 'F' then 1 else 0 end as female,
  case when gender_code = 'M' then 1 else 0 end as male,
  case when gender_code = 'U' then 1 else 0 end as gender_unk
from PHClaims.stage.apcd_member_month_detail
) as a
group by a.internal_member_id;


-------------------
--STEP 2: Create table shell
-------------------
if object_id('PHClaims.stage.apcd_elig_demo', 'U') is not null drop table PHClaims.stage.apcd_elig_demo;
CREATE TABLE PHClaims.stage.apcd_elig_demo (
   internal_member_id bigint null,
   dob_estimate date null,
   ninety_only int null,
   female int null,
   male int null,
   gender_mx varchar(8) null,
   gender_unk int null
);

-------------------
--STEP 2: Insert data, creating additional age and gender variables
-------------------
insert into PHClaims.stage.mcaid_claim with (tablock)
select
	internal_member_id,
	--age is missing when only age 90 is available in member month, ninety_only flag is equal to 1
	case
		when ninety_only = 0 then dob_estimate
		else null
	end as dob_estimate,
	ninety_only,
	female,
	male,
	--multiple gender variable
	case
		when female = 1 and male = 1 then 'Multiple'
		when female = 1 then 'Female'
		when male = 1 then 'Male'
	end as gender_mx,
	--unknown gender
	gender_unk
from #temp1;



