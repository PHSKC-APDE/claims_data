--Line-level QA of stage.mcaid_claim_ccw table on HHSAW using new CCW criteria tested by Alastair
--Eli Kern
--2022-06

----------------------
------------------
--QA goal: line-level QA of 1-2 conditions for each of the following phenotypes
  --years = 1 : ccw_mi
  --years = 2 : condition_type = 1 : ccw_hip_fracture
  --years = 2 : condition_type = 2 : ccw_anemia
  --years = 2 : condition_type = 2 : ccw_depression
------------------
----------------------

--------------------
--Generic code to find people with more than row per condition
--------------------
select top 1 a.*
from (
select id_mcaid, count(from_date) as time_cnt
from hhs_analytics_workspace.claims.stage_mcaid_claim_ccw
where ccw_code = 13 -- change this to select different condition
group by id_mcaid
) as a
--where a.time_cnt > 1;
where a.time_cnt > 2;


--------------------
--ccw_mi QA result: FAIL

--from_date of 1st time period should be 2015-03-01 because prior 12-month period (2015-02 -> 2016-01) would miss 2016-02-28 inpatient claim: FAIL
--to_date of 1st time period is 2017-01-31 because next 12-month period (2016-03 -> 2017-02) would miss 2016-02-28 inpatient claim: PASS
--from_date of 2nd time period is 2019-04-01 because prior 12-month period (2019-03 -> 2020-02) would miss 2020-03-09 inpatient claim: PASS
--to_date of 2nd time period is 2021-02-28 because next 12-month period (2020-04 -> 2021-03) would miss 2020-03-09 inpatient claim: PASS

--Eli's thought - I'm assuming something funky is going on with the from_date for the 1st period because 2015-10-01 was the transition from ICD-9-CM
	--to ICD-10-CM. Thus perhaps the actual from_date is being truncated to be no earlier than this transition date?
--------------------
declare @id varchar(255), @ccw_code tinyint, @clm_type varchar(100);
set @id = '';
set @ccw_code = 2;

select *
from hhs_analytics_workspace.claims.stage_mcaid_claim_ccw
where ccw_code = @ccw_code and id_mcaid = @id
order by from_date;

select a.id_mcaid, a.first_service_date, a.claim_type_id, b.icdcm_norm, b.icdcm_version, b.icdcm_number, c.ccw_mi
from hhs_analytics_workspace.claims.final_mcaid_claim_header as a
left join hhs_analytics_workspace.claims.final_mcaid_claim_icdcm_header as b
on a.claim_header_id = b.claim_header_id
left join hhs_analytics_workspace.claims.ref_dx_lookup as c
on b.icdcm_norm = c.dx and b.icdcm_version = c.dx_ver
where a.claim_type_id in (1) -- !! update this as needed for each condition !! --
and a.id_mcaid = @id
and c.ccw_mi = 1
order by a.first_service_date;


--------------------
--ccw_hip_fracture QA result: PASS

--from_date of 1st time period is 2013-07-01 because prior 12-month period (2013-06 -> 2014-05) would miss 2014-06-03 carrier claim: PASS
--to_date of 1st time period is 2015-09-30 because next 12-month period (2014-11 -> 2015-10) would miss 2014-10-15 carrier claim: PASS
--from_date of 2nd time period is 2018-06-01 because prior 12-month period (2018-05 -> 2019-04) would miss 2019-05-01 inpatient claim: PASS
--to_date of 2nd time period is 2020-04-30 because next 12-month period (2019-06 -> 2020-05) woudl miss 2019-05-03 carrier claim: PASS
--------------------
declare @id varchar(255), @ccw_code tinyint, @clm_type varchar(100);
set @id = '';
set @ccw_code = 19;

select *
from hhs_analytics_workspace.claims.stage_mcaid_claim_ccw
where ccw_code = @ccw_code and id_mcaid = @id
order by from_date;

select a.id_mcaid, a.first_service_date, a.claim_type_id, b.icdcm_norm, b.icdcm_version, b.icdcm_number, c.ccw_hip_fracture
from hhs_analytics_workspace.claims.final_mcaid_claim_header as a
left join hhs_analytics_workspace.claims.final_mcaid_claim_icdcm_header as b
on a.claim_header_id = b.claim_header_id
left join hhs_analytics_workspace.claims.ref_dx_lookup as c
on b.icdcm_norm = c.dx and b.icdcm_version = c.dx_ver
where a.claim_type_id in (1,2,4,5) -- !! update this as needed for each condition !! --
and a.id_mcaid = @id
and c.ccw_hip_fracture = 1
order by a.first_service_date;


--------------------
--ccw_anemia QA result: PASS

--from_date of 1st time period is 2012-01-01 because earliest from_date is truncated at 2012-01-01: PASS
--to_date of 1st time period is 2014-01-31 because next 24-month period (2012-03 -> 2014-02) would miss 2012-02-01 carrier claim (2 are required)
--break in prevalence period is accurate because the 2015-03-11 carrier claim does not contribute sufficient info because 2 claims are required
--from_date of 2nd time period is 2016-08-01 because prior 24-month period (2016-07 -> 2018-06) would miss 2018-07-16 carrier claim (2 are required)
--to_date of 2nd time period is 2019-03-31 because next 24-month period (2017-05 -> 2019-04) would miss 2017-04-10 carrier claim (2 are required)
--from_date of 3rd time period is 2019-11-01 because prior 24-month period (2019-10 -> 2021-09) would miss 2021-10-05 carrier claim (2 are required)
--to_date of 3rd time period is 2023-10-31 because next 24-month period (2021-12 -> 2023-11) would miss 2021-11-02 carrier claim (2 are required)
--------------------
declare @id varchar(255), @ccw_code tinyint, @clm_type varchar(100);
set @id = '';
set @ccw_code = 5;

select *
from hhs_analytics_workspace.claims.stage_mcaid_claim_ccw
where ccw_code = @ccw_code and id_mcaid = @id
order by from_date;

select a.id_mcaid, a.first_service_date, a.claim_type_id, b.icdcm_norm, b.icdcm_version, b.icdcm_number, c.ccw_anemia
from hhs_analytics_workspace.claims.final_mcaid_claim_header as a
left join hhs_analytics_workspace.claims.final_mcaid_claim_icdcm_header as b
on a.claim_header_id = b.claim_header_id
left join hhs_analytics_workspace.claims.ref_dx_lookup as c
on b.icdcm_norm = c.dx and b.icdcm_version = c.dx_ver
where a.claim_type_id in (1,2,3,4,5) -- !! update this as needed for each condition !! --
and a.id_mcaid = @id
and c.ccw_anemia = 1
order by a.first_service_date;


--------------------
--ccw_depression QA result: FAIL

--from_date of 1st time period is 2012-01-01 because earliest from_date is truncated at 2021-01-01: PASS
--1st and 2nd periods should actually be a single period: FILL - 2015-10-01 transition date appears to be screwing with things again
--to_date of 2nd (actually 1st) period is 2018-07-31 because next 24-month period (2016-09 -> 2018-08) would miss 2016-08-17 carrier claim (2 are required)
--from_date of 3rd time period is 2018-11-01 because prior 24-month period (2018-10 -> 2020-09) would miss 2020-10-09 carrier claim (2 are required)
--to_date of 3rd time perios is 2023-08-31 because next 24-month period (2021-10 -> 2023-09) would miss 2021-09-08 carrier claim (2 are required)
--------------------
declare @id varchar(255), @ccw_code tinyint, @clm_type varchar(100);
set @id = '';
set @ccw_code = 13;

select *
from hhs_analytics_workspace.claims.stage_mcaid_claim_ccw
where ccw_code = @ccw_code and id_mcaid = @id
order by from_date;

select a.id_mcaid, a.first_service_date, a.claim_type_id, b.icdcm_norm, b.icdcm_version, b.icdcm_number, c.ccw_depression
from hhs_analytics_workspace.claims.final_mcaid_claim_header as a
left join hhs_analytics_workspace.claims.final_mcaid_claim_icdcm_header as b
on a.claim_header_id = b.claim_header_id
left join hhs_analytics_workspace.claims.ref_dx_lookup as c
on b.icdcm_norm = c.dx and b.icdcm_version = c.dx_ver
where a.claim_type_id in (1,2,3,4,5) -- !! update this as needed for each condition !! --
and a.id_mcaid = @id
and c.ccw_depression = 1
order by a.first_service_date;