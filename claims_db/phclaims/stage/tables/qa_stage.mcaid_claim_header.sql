
--QA of stage.mcaid_claim_header table
--7/8/19
--Philip Sylling

use PHClaims;
go

delete from [metadata].[qa_mcaid] where table_name = 'stage.mcaid_claim_header';

--Confirm that claim header is distinct
select count(distinct claim_header_id) as header_cnt
from [stage].[mcaid_claim_header];
select count(claim_header_id) as header_cnt
from [stage].[mcaid_claim_header];
select count(distinct TCN) as header_cnt
from [stage].[mcaid_claim];
go

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_header]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_header'
,'Claim header distinct count check'
,'PASS'
,getdate()
,'Claim header is distinct in mcaid_claim_header';

--All members should be in elig_demo and table
select count(a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_header] as a
where not exists
(
select 1 
from [stage].[mcaid_elig_demo] as b
where a.id_mcaid = b.id_mcaid
);
go

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_header]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_header'
,'mcaid_elig_demo.id_mcaid check'
,'PASS'
,getdate()
,'All members in mcaid_claim_header are in mcaid_elig_demo';

--All members should be in elig_timevar table
select count(a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_header] as a
where not exists
(
select 1 
from [final].[mcaid_elig_timevar] as b
where a.id_mcaid = b.id_mcaid
);
go

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_header]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_header'
,'mcaid_elig_time_var.id_mcaid check'
,'PASS'
,getdate()
,'All members in mcaid_claim_header are in mcaid_elig_time_var';

-- Compare number of claim headers in current vs. prior analytic tables
WITH [final] AS
(
SELECT
 YEAR([from_date]) AS [claim_year]
,COUNT([tcn]) AS [prior_claim_header]
FROM [PHClaims].[dbo].[mcaid_claim_summary] AS a
GROUP BY YEAR([from_date])
),

[stage] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,COUNT([claim_header_id]) AS [current_claim_header]
FROM [stage].[mcaid_claim_header] AS a
GROUP BY YEAR([first_service_date])
)

SELECT
 COALESCE(a.[claim_year], b.[claim_year]) AS [claim_year]
,[prior_claim_header]
,[current_claim_header]
,CAST([current_claim_header] AS NUMERIC) / [prior_claim_header] AS [pct_change]
FROM [final] AS a
FULL JOIN [stage] AS b
ON a.[claim_year] = b.[claim_year]
ORDER BY [claim_year];
GO

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_header]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_header'
,'Compare new-to-prior claim headers'
,'PASS'
,getdate()
,'Ratio 2%-4% more claim headers per year';

-- Compare number of ed visits in current vs. prior analytic tables
WITH [final] AS
(
SELECT
 YEAR([from_date]) AS [claim_year]
,SUM([ed]) AS [prior_ed]
FROM [PHClaims].[dbo].[mcaid_claim_summary] AS a
GROUP BY YEAR([from_date])
),

[stage] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,SUM([ed]) AS [current_ed]
FROM [stage].[mcaid_claim_header] AS a
GROUP BY YEAR([first_service_date])
)

SELECT
 COALESCE(a.[claim_year], b.[claim_year]) AS [claim_year]
,[prior_ed]
,[current_ed]
,CAST([current_ed] AS NUMERIC) / [prior_ed] AS [pct_change]
FROM [final] AS a
FULL JOIN [stage] AS b
ON a.[claim_year] = b.[claim_year]
ORDER BY [claim_year];
GO

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_header]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_header'
,'Compare new-to-prior ed visits'
,'PASS'
,getdate()
,'Very close - except 2017 ED visits dropped 3.5% in new extract';

-- Compare number of inpatient stays in current vs. prior analytic tables
WITH [final] AS
(
SELECT
 YEAR([from_date]) AS [claim_year]
,SUM([inpatient]) AS [prior_inpatient]
FROM [PHClaims].[dbo].[mcaid_claim_summary] AS a
GROUP BY YEAR([from_date])
),

[stage] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,SUM([inpatient]) AS [current_inpatient]
FROM [stage].[mcaid_claim_header] AS a
GROUP BY YEAR([first_service_date])
)

SELECT
 COALESCE(a.[claim_year], b.[claim_year]) AS [claim_year]
,[prior_inpatient]
,[current_inpatient]
,CAST([current_inpatient] AS NUMERIC) / [prior_inpatient] AS [pct_change]
FROM [final] AS a
FULL JOIN [stage] AS b
ON a.[claim_year] = b.[claim_year]
ORDER BY [claim_year];
GO

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_header]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_header'
,'Compare new-to-prior extract inpatient stays'
,'PASS'
,getdate()
,'Generally 5% higher - except 2017 inpatient stays only 1% higher';

SELECT [etl_batch_id]
      ,[last_run]
      ,[table_name]
      ,[qa_item]
      ,[qa_result]
      ,[qa_date]
      ,[note]
FROM [PHClaims].[metadata].[qa_mcaid]
WHERE [table_name] = 'stage.mcaid_claim_header';







--Compare ED visit counts
select sum(ed_flag) as ed_cnt
from PHClaims.stage.apcd_claim_header;

select sum(case when emergency_room_flag = 'Y' then 1 else 0 end) as ed_cnt
from #temp1
where denied_line_min = 0 and orphaned_line_min = 0;

--Compare OR event counts
select sum(or_flag) as or_cnt
from PHClaims.stage.apcd_claim_header;

select sum(case when operating_room_flag = 'Y' then 1 else 0 end) as or_cnt
from #temp1
where denied_line_min = 0 and orphaned_line_min = 0;

--Compare acute inpatient stay counts
--Run time for this plus next block of code: 32 min
select sum(a.ipt_flag) as ipt_count
from (
select id_apcd, discharge_dt, max(ipt_flag) as ipt_flag
from PHClaims.stage.apcd_claim_header
group by id_apcd, discharge_dt
) as a;

--This code appears convoluted, but it's because it has to mimic process through which ipt claim lines are grouped by
--claim header (taking max of discharge dt), then grouped by id and discharge date, then summed
--Note that this code appropriately excludes members with no elig information
select sum(e.ipt_flag) as ipt_count
from (
	select d.id_apcd, d.discharge_dt, max(d.ipt_flag) as ipt_flag
	from (
		select c.medical_claim_header_id, max(a.internal_member_id) as id_apcd, max(a.discharge_dt) as discharge_dt,
			max(a.ipt_flag) as ipt_flag
		from (
		select medical_claim_service_line_id, internal_member_id, discharge_dt, 1 as ipt_flag,
			denied_claim_flag, orphaned_adjustment_flag
		from PHClaims.stage.apcd_medical_claim 
		where claim_type_id = '1' and type_of_setting_id = '1' and place_of_setting_id = '1'
			and (denied_claim_flag = 'N' AND orphaned_adjustment_flag = 'N')
			and claim_status_id in (-1, -2, 1, 5, 2, 6)
			and discharge_dt is not null
			and internal_member_id not in (select id_apcd from PHClaims.ref.apcd_claim_no_elig)
		) as a
		left join PHClaims.stage.apcd_medical_crosswalk as b
		on a.medical_claim_service_line_id = b.medical_claim_service_line_id
		left join PHClaims.stage.apcd_medical_claim_header as c
		on b.medical_claim_header_id = c.medical_claim_header_id
		group by c.medical_claim_header_id
	) as d
	group by d.id_apcd, d.discharge_dt
) as e;

--Check an individual inpatient stay to make sure discharge date has come through correctly
select id_apcd, claim_header_id, ipt_flag, discharge_dt
from PHClaims.stage.apcd_claim_header
where claim_header_id = 629250577489368;

select a.*
from PHClaims.stage.apcd_medical_claim as a
left join PHClaims.stage.apcd_medical_crosswalk as b
on a.medical_claim_service_line_id = b.medical_claim_service_line_id
where b.medical_claim_header_id = 629250577489368;

--Verify that an example of a denied and orphaned claim header is excluded
select * 
from PHClaims.stage.apcd_claim_header
where claim_header_id = 629246595882276;

select * 
from PHClaims.stage.apcd_claim_header
where claim_header_id = 629250026243188;

--Query distinct APCD claim types by KC claim type
select distinct claim_type_id, claim_type_apcd_id
from PHClaims.stage.apcd_claim_header;

--Should be no claims where ipt_flag = 1 and discharge_dt is null
select count(*)
from PHClaims.stage.apcd_claim_header
where ipt_flag = 1 and discharge_dt is null;




-------------------------
--SAVING OLD CODE USED FOR EXPLORING DIFFERENCE IN HEADER VS LINE IPT COUNTS
-------------------------
--It appears due to >1 discharge dates being linkd to a single header
--Run time for these 3 temp tables: 10 min
if object_id('tempdb..#temp2') is not null drop table #temp2;
select distinct id_apcd, discharge_dt
into #temp2
from PHClaims.stage.apcd_claim_header
where ipt_flag = 1;

if object_id('tempdb..#temp3') is not null drop table #temp3;
select distinct internal_member_id as id_apcd, discharge_dt
into #temp3
from PHClaims.stage.apcd_medical_claim 
where claim_type_id = '1' and type_of_setting_id = '1' and place_of_setting_id = '1'
	and (denied_claim_flag = 'N' AND orphaned_adjustment_flag = 'N')
	and claim_status_id in (-1, -2, 1, 5, 2, 6)
	and discharge_dt is not null;

if object_id('tempdb..#temp4') is not null drop table #temp4;
select id_apcd, discharge_dt into #temp4 from #temp3
except
select id_apcd, discharge_dt from #temp2;

select top 1000 * from #temp4
where id_apcd = 12168952259;

select * from #temp2
where id_apcd = 12168952259;

select *
from PHClaims.stage.apcd_medical_claim 
where internal_member_id = 12760940196 and discharge_dt >= '2018-02-01' and discharge_dt <= '2018-02-28';

select distinct medical_claim_header_id, inpatient_discharge_id
from PHClaims.stage.apcd_medical_crosswalk
where medical_claim_service_line_id in (629257854668948
,629257854668945
,629257854668951
,629257854668946
,629257854668953
,629257854668944
,629257854668943
,629257854668952
,629257854668941
,629257854668954
,629257854668950
,629257854668942
,629257854668955
,629257854668949
,629257854668947
,629257854369542
,629257854369537
,629257854369543
,629257854369539
,629257854369545
,629257854369544
,629257854369541
,629257854369540
,629257854369538);
