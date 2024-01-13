--Line-level QA of stage.apcd_elig_timevar table
--2019-10
--Eli Kern

----------------
--Count KC Medicaid members by month to make sure there are no outliers (i.e. ETL errors)
--2.5 min
----------------
if object_id('tempdb..#temp4') is not null drop table #temp4;
select distinct a.id_apcd, c.year_month, c.first_day_month, c.last_day_month, a.dual, a.bsp_group_cid
into #temp4
from PHClaims.stage.apcd_elig_timevar as a
--join to date reference table to allocate people by month
inner join (select distinct year_month, first_day_month, last_day_month from PHClaims.ref.date) as c
on (a.from_date <= c.last_day_month) and (a.to_date >= c.first_day_month)
where a.geo_county = 'King' and (a.med_medicaid = 1 or a.pharm_medicaid = 1)
	and c.year_month between 201401 and 202206; --update end year-month each time

--QA: Check some members to make sure they have residence in King and Medicaid coverage
--select top 10 * from #temp4 where year_month = 201706
select id_apcd, geo_county, max(med_medicaid) as med_medicaid, max(pharm_medicaid) as pharm_medicaid
from PHClaims.stage.apcd_elig_timevar
where id_apcd in (
'11540261101',
'11631765305',
'11632043354',
'11387207461',
'11058091947')
and from_date <= '2017-06-30' and to_date >= '2017-06-01'
group by id_apcd, geo_county;

--QA: Tabulate results and review for months that have suspiciously high or low member counts
--Run time: 1 min
select 'Overall Medicaid' as 'cohort', 'WA-APCD' as 'data_source', year_month, count(id_apcd) as id_dcount
from #temp4
group by year_month;


----------------
--Count WA Medicaid, Medicare, and Commercial members by month and coverage type to make sure there are no outliers (i.e. ETL errors)
--35 min
----------------
if object_id('tempdb..#temp5') is not null drop table #temp5;
select distinct a.id_apcd, c.year_month, c.first_day_month, c.last_day_month,
	a.med_medicaid, a.med_medicare, a.med_commercial,
	a.pharm_medicaid, a.pharm_medicare, a.pharm_commercial,
	a.dental_medicaid, a.dental_medicare, a.dental_commercial
into #temp5
from PHClaims.stage.apcd_elig_timevar as a
--join to date reference table to allocate people by month
inner join (select distinct year_month, first_day_month, last_day_month from PHClaims.ref.date) as c
on (a.from_date <= c.last_day_month) and (a.to_date >= c.first_day_month)
where a.geo_wa = 1
	and c.year_month between 201401 and 202206; -- update end year-month each time

--QA: Tabulate results and review for months that have suspiciously high or low member counts
--Run time: 9 min
select 'WA-APCD' as 'data_source', 'Medicaid medical' as coverage, year_month, count(id_apcd) as id_dcount
from #temp5
where med_medicaid = 1
group by year_month

union
select 'WA-APCD' as 'data_source', 'Medicare medical' as coverage, year_month, count(id_apcd) as id_dcount
from #temp5
where med_medicare = 1
group by year_month

union
select 'WA-APCD' as 'data_source', 'Commercial medical' as coverage, year_month, count(id_apcd) as id_dcount
from #temp5
where med_commercial = 1
group by year_month

union
select 'WA-APCD' as 'data_source', 'Medicaid pharmacy' as coverage, year_month, count(id_apcd) as id_dcount
from #temp5
where pharm_medicaid = 1
group by year_month

union
select 'WA-APCD' as 'data_source', 'Medicare pharmacy' as coverage, year_month, count(id_apcd) as id_dcount
from #temp5
where pharm_medicare = 1
group by year_month

union
select 'WA-APCD' as 'data_source', 'Commercial pharmacy' as coverage, year_month, count(id_apcd) as id_dcount
from #temp5
where pharm_commercial = 1
group by year_month

union
select 'WA-APCD' as 'data_source', 'Medicaid dental' as coverage, year_month, count(id_apcd) as id_dcount
from #temp5
where dental_medicaid = 1
group by year_month

union
select 'WA-APCD' as 'data_source', 'Medicare dental' as coverage, year_month, count(id_apcd) as id_dcount
from #temp5
where dental_medicare = 1
group by year_month

union
select 'WA-APCD' as 'data_source', 'Commercial dental' as coverage, year_month, count(id_apcd) as id_dcount
from #temp5
where dental_commercial = 1
group by year_month;


----------------
--QA approach - Use existing member IDs and just run first block of code below to verify results are still applicable
--If not, find a new person and do detailed review using all 3 blocks of code

--Confirm that eligibility data has been transformed correctly for each value of med_covgrp variable
--select top 1 * from phclaims.stage.apcd_elig_timevar where med_covgrp = 0;
--select top 1 * from phclaims.stage.apcd_elig_timevar where med_covgrp = 1;
--select top 1 * from phclaims.stage.apcd_elig_timevar where med_covgrp = 2;
--select top 1 * from phclaims.stage.apcd_elig_timevar where med_covgrp = 3;
--select top 1 * from phclaims.stage.apcd_elig_timevar where med_covgrp = 4;
--select top 1 * from phclaims.stage.apcd_elig_timevar where med_covgrp = 5;
--select top 1 * from phclaims.stage.apcd_elig_timevar where med_covgrp = 6;
--select top 1 * from phclaims.stage.apcd_elig_timevar where med_covgrp = 7;
--select top 1 * from phclaims.stage.apcd_elig_timevar where med_covgrp = 8;
----------------
--member with med_covgrp = 0: 11050747035, dental coverage only from 2014-01 to 2015-04
--member with med_covgrp = 1: 11050747058, mcaid only from 2015-01 through 2017-01, commercial & medicaid before and commerial after
--member with med_covgrp = 2: 11050747041, mcare only from 2015-10 and 2017-12; 2015-11 to 2017-12 medicare and commerical; 2018-1 to 2022-06 medicare only
--member with med_covgrp = 3: 11050747024, commercial only from 2014-01 to 2014-08, no other coverage pre or post
--member with med_covgrp = 4: 11050747133, mcaid-mcare dual (linked) from 2017-09 to 2018-72, mcaid pre and post, commerical pre, medicare post, then only dental
--member with med_covgrp = 5: 11050747029, mcaid-commercial dual from 2015-07 to 2016-06, commercial only pre and post
--member with med_covgrp = 6: 11050747034, mcare-commercial dual from 2015-01 to 2015-10, commercial only pre, nothing post
--member with med_covgrp = 7: 11050747290,  from 2015-01 to 2017-12 mcaid-mcare-commercial; 1-2015 to 12-2017 medicaid medicare commerial; post medicaid 
--member with med_covgrp = 8: this is a person who has unknown type of medical coverage:14511094388
--member with multiple ZIP codes and contiguous and non-contiguous rows: 11269028924

select * from phclaims.stage.apcd_elig_timevar
where id_apcd =14511094388
order by from_date;

select * from phclaims.stage.apcd_member_month_detail
where internal_member_id =14511094388
order by year_month;

select eligibility_id, submitter_id, internal_member_id, coverage_class, eligibility_start_dt, eligibility_end_dt,
  product_code_id, dual_eligibility_code_id, aid_category_id
from phclaims.stage.apcd_eligibility
where internal_member_id =14511094388
--and coverage_class = 'MEDICAL'
order by eligibility_start_dt;