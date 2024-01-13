--Line-level QA of stage.apcd_elig_demo table
--2019-10
--Eli Kern

--------------------
--Age
--use online age calculator to QA:
--https://www.calculator.net/age-calculator.html?today=03%2F01%2F1995&ageat=03%2F01%2F2020&x=105&y=14
--enter birthday and then enter date of any month when age increases by 1 year to confirm age is correct
--note that when multiple possible DOBs are estimated, the script will choose the MAXIMUM DOB
--------------------

--Age < 1
--kid with multiple possible birthdays, though code will select the only 1 that is calculated from an age change = 1 year
select id_apcd, dob, ninety_only from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747392;

select internal_member_id, year_month, age
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747392
order by internal_member_id, year_month;

--Age 1-5
--kid with multiple possible birthdays, though code will select the only 1 that is calculated from an age change = 1 year
select id_apcd, dob, ninety_only from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747025;

select internal_member_id, year_month, age
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747025
order by internal_member_id, year_month;

--Age 6-17
--kid with no 1-year change in age and thus last month is used to estimate age
select id_apcd, dob, ninety_only from phclaims.stage.apcd_elig_demo
where id_apcd = 11268610588;

select internal_member_id, year_month, age
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11268610588
order by internal_member_id, year_month;

--Age 18-44
select id_apcd, dob, ninety_only from phclaims.stage.apcd_elig_demo
where id_apcd = 11268610582;

select internal_member_id, year_month, age
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11268610582
order by internal_member_id, year_month;

--Age 45-89
select id_apcd, dob, ninety_only from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747030;

select internal_member_id, year_month, age
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747030
order by internal_member_id, year_month;

--Someone with coverage gaps
select id_apcd, dob, ninety_only from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747044;

select internal_member_id, year_month, age
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747044
order by internal_member_id, year_month;

--Someone with only a single age
select id_apcd, dob, ninety_only from phclaims.stage.apcd_elig_demo
where id_apcd = 23624700073448401;

select internal_member_id, year_month, age
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 23624700073448401
order by internal_member_id, year_month;

	----Code to find a new example for above
	--if object_id('tempdb..#temp1') is not null drop table #temp1;
	--select a.internal_member_id, a.age_dcount
	--into #temp1
	--from (
	--select internal_member_id, count(distinct age) as age_dcount
	--from phclaims.stage.apcd_member_month_detail
	--group by internal_member_id
	--) as a
	--where a.age_dcount = 1;
	--select top 10 * from #temp1;	

--Age 90+ with multiple ages present in member_month
select id_apcd, dob, ninety_only from phclaims.stage.apcd_elig_demo
where id_apcd = 18066785763;

select internal_member_id, year_month, age
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 18066785763
order by internal_member_id, year_month;

	----Code to find a new example for above
	--if object_id('tempdb..#temp1') is not null drop table #temp1;
	--select a.internal_member_id, max(age90_flag) as age90_flag, count(distinct age) as age_dcount
	--into #temp1
	--from (
	--select internal_member_id, age, case when age = 90 then 1 else 0 end as age90_flag
	--from phclaims.stage.apcd_member_month_detail
	--) as a
	--group by internal_member_id;
	--select top 10 * from #temp1 where age90_flag = 1 and age_dcount >1;

--Age 90+ with age of 90 only in member_month
--dob should be null
select id_apcd, dob, ninety_only from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747327;

select internal_member_id, year_month, age
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747327
order by internal_member_id, year_month;

--------------------
--Gender
--------------------

--Male only
select id_apcd, gender_me, gender_recent, gender_female, gender_male from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747063;

select internal_member_id, year_month, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747063
order by internal_member_id, year_month;

--Female only
select id_apcd, gender_me, gender_recent, gender_female, gender_male from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747032;

select internal_member_id, year_month, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747032
order by internal_member_id, year_month;

--Multiple gender
select id_apcd, gender_me, gender_recent, gender_female, gender_male from phclaims.stage.apcd_elig_demo
where id_apcd = 11050747626;

select internal_member_id, year_month, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 11050747626
order by internal_member_id, year_month;

--Gender unknown for just partial time (should NOT be gender unknown in final table)
select id_apcd, gender_me, gender_recent, gender_female, gender_male from phclaims.stage.apcd_elig_demo
where id_apcd = 17164935367;

select internal_member_id, year_month, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 17164935367
order by internal_member_id, year_month;

--Gender fully unknown
select id_apcd, gender_me, gender_recent, gender_female, gender_male from phclaims.stage.apcd_elig_demo
where id_apcd = 24638000712;

select internal_member_id, year_month, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = 24638000712
order by internal_member_id, year_month;

	----Code to find examples for above two QA checks
	--if object_id('tempdb..#temp1') is not null drop table #temp1;
	--select a.*
	--into #temp1
	--from (
	--	select internal_member_id, max(case when gender_code = 'U' then 1 else 0 end) as gender_unknown_flag,
	--		max(case when gender_code != 'U' then 1 else 0 end) as gender_known_flag
	--	from phclaims.stage.apcd_member_month_detail 
	--	group by internal_member_id
	--) as a
	--where a.gender_unknown_flag = 1;
	--select top 1 * from #temp1 where gender_unknown_flag = 1 and gender_known_flag = 1;
	--select top 1 * from #temp1 where gender_unknown_flag = 1 and gender_known_flag = 0;

--Gender unknown for partial time, unknown for last month
--Find Example code:
--select top 1 * from PHClaims.stage.apcd_elig_demo where gender_me != 'Unknown' and gender_recent = 'Unknown';
select id_apcd, gender_me, gender_recent, gender_female, gender_male from phclaims.stage.apcd_elig_demo
where id_apcd = [no one found last extract];

select internal_member_id, year_month, gender_code
from phclaims.stage.apcd_member_month_detail 
where internal_member_id = [no one found last extract]
order by internal_member_id, year_month;

--------------------
--Race/ethnicity
--Extract 10001 - hispanic_id codes (1 = yes, 2 = no, 3 = unknown, -1 = unknown)
--------------------

--Single race: Black
select id_apcd, race_eth_me, race_me, race_eth_recent, race_recent, race_aian, race_asian, race_black,
race_latino, race_nhpi, race_white, race_unknown
from phclaims.stage.apcd_elig_demo
where id_apcd = 11268758877;

select internal_member_id, eligibility_end_dt, race_id1, race_id2, hispanic_id, ethnicity_id1, ethnicity_id2
from PHClaims.stage.apcd_eligibility
where internal_member_id = 11268758877
order by eligibility_end_dt;

--Single race: Latino
select id_apcd, race_eth_me, race_me, race_eth_recent, race_recent, race_aian, race_asian, race_black,
race_latino, race_nhpi, race_white, race_unknown
from phclaims.stage.apcd_elig_demo
where id_apcd = 11268758909;

select internal_member_id, eligibility_end_dt, race_id1, race_id2, hispanic_id, ethnicity_id1, ethnicity_id2
from PHClaims.stage.apcd_eligibility
where internal_member_id = 11268758909
order by eligibility_end_dt;

--Multiple race if Latino included, single race if not
select id_apcd, race_eth_me, race_me, race_eth_recent, race_recent, race_aian, race_asian, race_black,
race_latino, race_nhpi, race_white, race_unknown
from phclaims.stage.apcd_elig_demo
where id_apcd = 11671583225;

select internal_member_id, eligibility_end_dt, race_id1, race_id2, hispanic_id, ethnicity_id1, ethnicity_id2
from PHClaims.stage.apcd_eligibility
where internal_member_id = 11671583225
order by eligibility_end_dt;

--Multiple race if Latino included or excluded
select id_apcd, race_eth_me, race_me, race_eth_recent, race_recent, race_aian, race_asian, race_black,
race_latino, race_nhpi, race_white, race_unknown
from phclaims.stage.apcd_elig_demo
where id_apcd = 11268759039;

select internal_member_id, eligibility_end_dt, race_id1, race_id2, hispanic_id, ethnicity_id1, ethnicity_id2
from PHClaims.stage.apcd_eligibility
where internal_member_id = 11268759039
order by eligibility_end_dt;

--Two race variables unknown for a given eligibility segment, but ethnicity maps to race
select id_apcd, race_eth_me, race_me, race_eth_recent, race_recent, race_aian, race_asian, race_black,
race_latino, race_nhpi, race_white, race_unknown
from phclaims.stage.apcd_elig_demo
where id_apcd = 11416956468;

select internal_member_id, eligibility_end_dt, race_id1, race_id2, hispanic_id, ethnicity_id1, ethnicity_id2
from PHClaims.stage.apcd_eligibility
where internal_member_id = 11416956468
order by eligibility_end_dt;

--Race unknown for partial time, unknown for last period
select id_apcd, race_eth_me, race_me, race_eth_recent, race_recent, race_aian, race_asian, race_black,
race_latino, race_nhpi, race_white, race_unknown
from phclaims.stage.apcd_elig_demo
where id_apcd = 12265906395;

select internal_member_id, eligibility_end_dt, race_id1, race_id2, hispanic_id, ethnicity_id1, ethnicity_id2
from PHClaims.stage.apcd_eligibility
where internal_member_id = 12265906395
order by eligibility_end_dt;

--Race fully unknown
select id_apcd, race_eth_me, race_me, race_eth_recent, race_recent, race_aian, race_asian, race_black,
race_latino, race_nhpi, race_white, race_unknown
from phclaims.stage.apcd_elig_demo
where id_apcd = 11268758868;

select internal_member_id, eligibility_end_dt, race_id1, race_id2, hispanic_id, ethnicity_id1, ethnicity_id2
from PHClaims.stage.apcd_eligibility
where internal_member_id = 11268758868
order by eligibility_end_dt;

--Race different across race variables
select id_apcd, race_eth_me, race_me, race_eth_recent, race_recent, race_aian, race_asian, race_black,
race_latino, race_nhpi, race_white, race_unknown
from phclaims.stage.apcd_elig_demo
where id_apcd = 34952570590;

select internal_member_id, eligibility_end_dt, race_id1, race_id2, hispanic_id, ethnicity_id1, ethnicity_id2
from PHClaims.stage.apcd_eligibility
where internal_member_id = 34952570590
order by eligibility_end_dt;

	----code to find examples for above
	--if object_id('tempdb..#temp1') is not null drop table #temp1;
	--select b.internal_member_id, count(distinct b.race_id1) as race_id1_dcount
	--into #temp1
	--from (
	--	select a.*
	--		from (
	--		select internal_member_id, race_id1, race_id2 from PHClaims.stage.apcd_eligibility
	--		where race_id1 in (1,2,3,4,5) and race_id2 in (1,2,3,4,5)
	--		) as a
	--	where a. race_id1 != race_id2
	--) as b
	--group by b.internal_member_id;
	--select top 10 * from #temp1 where race_id1_dcount = 1;

--Race different across race variables and first race variable is unknown/missing
select id_apcd, race_eth_me, race_me, race_eth_recent, race_recent, race_aian, race_asian, race_black,
race_latino, race_nhpi, race_white, race_unknown
from phclaims.stage.apcd_elig_demo
where id_apcd = 11062148115;

select internal_member_id, eligibility_end_dt, race_id1, race_id2, hispanic_id, ethnicity_id1, ethnicity_id2
from PHClaims.stage.apcd_eligibility
where internal_member_id = 11062148115
order by eligibility_end_dt;

	--code to find examples for above
	--if object_id('tempdb..#temp1') is not null drop table #temp1;
	--select b.internal_member_id, count(distinct b.race_id1) as race_id1_dcount
	--into #temp1
	--from (
	--	select a.*
	--		from (
	--		select internal_member_id, race_id1, race_id2 from PHClaims.stage.apcd_eligibility
	--		where race_id1 not in (1,2,3,4,5) and race_id2 in (1,2,3,4,5)
	--		) as a
	--) as b
	--group by b.internal_member_id;
	--select top 10 * from #temp1 where race_id1_dcount = 1;