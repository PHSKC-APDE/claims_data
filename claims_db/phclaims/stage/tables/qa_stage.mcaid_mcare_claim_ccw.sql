--QA of stage.mcaid_mcare_claim_ccw table
--Eli Kern
--2019-11


------------------------
--STEP 1: Table-wide checks for Medicare population only
------------------------

--Count Medicare members who are in stacked mcaid_mcare_claim_header but not in xwalk table, expect 0
select count(distinct a.id_apde) as qa
from PHClaims.final.mcaid_mcare_claim_header as a
left join (select * from PHClaims.final.xwalk_apde_mcaid_mcare_pha where id_mcare is not null) as b
on a.id_apde = b.id_apde
where a.source_desc = 'mcare' --limit to mcare claims
	and b.id_apde is null;

--Count claims in stacked mcaid_mcare_claim_header and have no id_apde
--these are members with claim but no elig information
--these claims should likely be removed from this table (like we did in APCD)
--count: 10 claims as of 11/4/2019
select count(*) as qa
from PHClaims.final.mcaid_mcare_claim_header
where id_apde is null;

--Count # of distinct conditions to make sure they've all run
select count(distinct ccw_code) as cond_count
from PHClaims.stage.mcaid_mcare_claim_ccw;

--Select Medicare members with any King County residency, full Medicare, full part A, and full part B coverage in 2016
--Use 2016 as measurement year
if object_id('tempdb..#temp1') is not null drop table #temp1;
select a.*
into #temp1
	from (
	select id_apde, max(geo_kc) as geo_kc_max, min(geo_kc) as geo_kc_min, min(mcare) as mcare_min,
		min(case when part_a is null then 0 else part_a end) as part_a_min,
		min(case when part_b is null then 0 else part_b end) as part_b_min,
		max(case when part_c is null then 0 else part_c end) as part_c_max
	from PHClaims.final.mcaid_mcare_elig_timevar
	where from_date <= '2016-12-31' and to_date >= '2016-01-01'
	group by id_apde
) as a
where geo_kc_max = 1
	and mcare_min = 1 --redundant
	and part_a_min = 1
	and part_b_min = 1
	and part_c_max = 0;

--Select Medicare members for WA State, full Medicare, full part A, and full part B coverage in 2016
if object_id('tempdb..#temp2') is not null drop table #temp2;
select a.*
into #temp2
	from (
	select id_apde, max(geo_kc) as geo_kc_max, min(mcare) as mcare_min,
		min(case when part_a is null then 0 else part_a end) as part_a_min,
		min(case when part_b is null then 0 else part_b end) as part_b_min,
		max(case when part_c is null then 0 else part_c end) as part_c_max
	from PHClaims.final.mcaid_mcare_elig_timevar
	where from_date <= '2016-12-31' and to_date >= '2016-01-01'
	group by id_apde
) as a
where mcare_min = 1
	and (part_a_min = 1 or part_b_min = 1) and part_c_max = 0;

--Number of WA state Medicare FFS (part a or part b) members, excluding part c
select count(distinct id_apde) as id_dcount
from #temp2;

--Counting person-years instead of people for Medicare enrollment (CMS approach)
--https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/CMSProgramStatistics/2016/Downloads/MDCR_ENROLL_AB/2016_CPS_MDCR_ENROLL_AB_2.pdf
--https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/CMSProgramStatistics/Downloads/MED_ENROLL_METHODS.pdf

--try converting elig_timevar back to monthly record to count months of coverage, for Medicare member-time periods only
if object_id('tempdb..#temp3') is not null drop table #temp3;
select a.id_apde, b.first_day_month, b.last_day_month, a.mcare, a.part_a, a.part_b, a.part_c, a.geo_kc
into #temp3
from (
	select id_apde, from_date, to_date, mcare, part_a, part_b, part_c, geo_kc
	from phclaims.final.mcaid_mcare_elig_timevar
	where mcare = 1
) as a
inner join (select distinct year_month, first_day_month, last_day_month from PHClaims.ref.date) as b
on (a.from_date <= b.last_day_month) and (a.to_date >= b.first_day_month)
where b.year_month between 201601 and 201612;

--then count months meeting inclusion criteria total Medicare, FFS, and part C and compare to CMS for 2016
select count(*) / 12 as person_years_total
from #temp3;

select count(*) / 12 as person_years_ffs
from #temp3
where (part_a = 1 or part_b = 1) and part_c = 0;

select count(*) / 12 as person_years_part_c
from #temp3
where part_c = 1;

--Number of Medicare members in King County in 2016
select count(distinct id_apde) as id_dcount
from #temp1;

--Number of KC Medicare members in 2016 by condition using 2016 only
select b.ccw_code, b.ccw_desc, count(distinct a.id_apde) as id_dcount
from #temp1 as a
inner join PHClaims.stage.mcaid_mcare_claim_ccw as b
on a.id_apde = b.id_apde
left join PHClaims.ref.ccw_lookup as c
on cast(b.ccw_code as varchar(255)) = c.ccw_code
where b.from_date <= '2016-12-31' and b.to_date >= '2016-01-01'
group by b.ccw_code, b.ccw_desc
order by b.ccw_code;

--Number of KC Medicare members in 2016 by condition using condition-specific lookback period
select b.ccw_code, b.ccw_desc, count(distinct a.id_apde) as id_dcount
from #temp1 as a
inner join PHClaims.stage.mcaid_mcare_claim_ccw as b
on a.id_apde = b.id_apde
left join PHClaims.ref.ccw_lookup as c
on cast(b.ccw_code as varchar(255)) = c.ccw_code
where b.from_date <= '2016-12-31' and b.to_date >= dateadd(year, (c.years-1)*-1, '2016-01-01') --condition-specific lookback period
group by b.ccw_code, b.ccw_desc
order by b.ccw_code;

--Number of KC Medicare members in 2016 by condition using all time (i.e. ever)
select b.ccw_code, b.ccw_desc, count(distinct a.id_apde) as id_dcount
from #temp1 as a
inner join PHClaims.stage.mcaid_mcare_claim_ccw as b
on a.id_apde = b.id_apde
left join PHClaims.ref.ccw_lookup as c
on cast(b.ccw_code as varchar(255)) = c.ccw_code
group by b.ccw_code, b.ccw_desc
order by b.ccw_code;

--Denominator by age groups
select b.age_grp7, count(distinct id_apde) as id_dcount
from (
	select a.id_apde, 
	case
		when a.age >= 0 and a.age < 65 then '0-64'
		when a.age >= 65 and a.age < 70 then '65-69'
		when a.age >= 70 and a.age < 75 then '70-74'
		when a.age >= 75 and a.age < 80 then '75-79'
		when a.age >= 80 and a.age < 85 then '80-84'
		when a.age >= 85 and a.age < 90 then '85-89'
		when a.age >= 90 then '90 and over'
	end as age_grp7
	--calculate age from demo table for selected Medicaid members
	from (
		select a.id_apde,
		case
			when floor((datediff(day, b.dob, '2017-12-31') + 1) / 365.25) >=0 then floor((datediff(day, b.dob, '2017-12-31') + 1) / 365.25)
			when floor((datediff(day, b.dob, '2017-12-31') + 1) / 365.25) = -1 then 0
		end as age
		from #temp1 as a
		inner join PHClaims.final.mcaid_mcare_elig_demo as b
		on a.id_apde = b.id_apde
	) as a
) as b
group by b.age_grp7
order by b.age_grp7;

--count people with conditions by same age groups
select b.ccw_code, b.ccw_desc, b.age_grp7, count(distinct b.id_apde) as id_dcount
from (
	select a.id_apde, 
	case
		when a.age >= 0 and a.age < 65 then '0-64'
		when a.age >= 65 and a.age < 70 then '65-69'
		when a.age >= 70 and a.age < 75 then '70-74'
		when a.age >= 75 and a.age < 80 then '75-79'
		when a.age >= 80 and a.age < 85 then '80-84'
		when a.age >= 85 and a.age < 90 then '85-89'
		when a.age >= 90 then '90 and over'
	end as age_grp7,
	a.ccw_code, a.ccw_desc
	--calculate age from demo table for selected Medicaid members
	from (
		select a.id_apde,
		case
			when floor((datediff(day, b.dob, '2017-12-31') + 1) / 365.25) >=0 then floor((datediff(day, b.dob, '2017-12-31') + 1) / 365.25)
			when floor((datediff(day, b.dob, '2017-12-31') + 1) / 365.25) = -1 then 0
		end as age,
		a.ccw_code, a.ccw_desc
		from (
		--flag people with conditions using lookback period
			select x.id_apde, y.ccw_code, y.ccw_desc
			from #temp1 as x
			inner join PHClaims.stage.mcaid_mcare_claim_ccw as y
			on x.id_apde = y.id_apde
			left join PHClaims.ref.ccw_lookup as z
			on cast(y.ccw_code as varchar(255)) = z.ccw_code
			where y.from_date <= '2016-12-31' and y.to_date >= dateadd(year, (z.years-1)*-1, '2016-01-01') --condition-specific lookback period
		) as a
		inner join PHClaims.final.mcaid_mcare_elig_demo as b
		on a.id_apde = b.id_apde
	) as a
) as b
group by b.ccw_code, b.ccw_desc, b.age_grp7
order by b.ccw_code, b.ccw_desc, b.age_grp7;