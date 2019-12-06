
use [PHClaims];
go

delete from [metadata].[qa_mcaid] 
where table_name = 'stage.mcaid_claim_header';

declare @last_run as datetime;
declare @distinct_claim_header_count as varchar(255);
declare @mcaid_elig_check as varchar(255);
declare @mcaid_elig_demo_check as varchar(255);
declare @compare_current_prior_header_min as varchar(255);
declare @compare_current_prior_header_max as varchar(255);
declare @compare_current_prior_ed_min as varchar(255);
declare @compare_current_prior_ed_max as varchar(255);

set @last_run = (select max(last_run) from [stage].[mcaid_claim_header]);

--Confirm that claim header is distinct
set @distinct_claim_header_count = 
(
(select count(claim_header_id) from [stage].[mcaid_claim_header]) - 
(select count(distinct claim_header_id) from [stage].[mcaid_claim_header])
);

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_header'
,'Claim header distinct count check'
,CASE WHEN @distinct_claim_header_count = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@distinct_claim_header_count + ' rows difference between count and distinct count';

--All members should be in [mcaid_elig] table
set @mcaid_elig_check = 
(
select count(distinct a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_header] as a
where not exists
(
select 1 
from [stage].[mcaid_elig] as b
where a.id_mcaid = b.MEDICAID_RECIPIENT_ID
));

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_header'
,'All members should be in [mcaid_elig] table'
,CASE WHEN @mcaid_elig_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@mcaid_elig_check + ' members in mcaid_claim_header and are not in [mcaid_elig]';

--All members should be in [mcaid_elig_demo] table
set @mcaid_elig_demo_check = 
(
select count(distinct a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_header] as a
where not exists
(
select 1 
from (SELECT [id_mcaid] FROM [final].[mcaid_elig_demo] UNION SELECT [id_mcaid] FROM [stage].[mcaid_elig_demo]) as b
where a.id_mcaid = b.id_mcaid
));

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_header'
,'All members should be in [mcaid_elig_demo] table'
,CASE WHEN @mcaid_elig_demo_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@mcaid_elig_demo_check + ' members in mcaid_claim_header and are not in [mcaid_elig_demo]';

--Compare number of claim headers in current vs. prior analytic tables
WITH [final] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,COUNT(*) AS [prior_num_header]
FROM [final].[mcaid_claim_header] AS a
GROUP BY YEAR([first_service_date])
),

[stage] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,COUNT(*) AS [current_num_header]
FROM [stage].[mcaid_claim_header] AS a
GROUP BY YEAR([first_service_date])
),

[compare] AS
(
SELECT
 COALESCE(a.[claim_year], b.[claim_year]) AS [claim_year]
,[prior_num_header]
,[current_num_header]
,CAST([current_num_header] AS NUMERIC) / [prior_num_header] AS [pct_change]
FROM [final] AS a
FULL JOIN [stage] AS b
ON a.[claim_year] = b.[claim_year]
)

SELECT 
 @compare_current_prior_header_min = MIN([pct_change])
,@compare_current_prior_header_max = MAX([pct_change])
FROM [compare]
WHERE [claim_year] >= YEAR(GETDATE()) - 3;

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_header'
,'Compare current vs. prior claim headers'
,NULL
,getdate()
,'Min: ' + @compare_current_prior_header_min + ', Max: ' + @compare_current_prior_header_max + ' ratio of current to prior claim headers';

-- Compare number of ed visits in current vs. prior analytic tables
WITH [final] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,SUM([ed]) AS [prior_ed]
FROM [final].[mcaid_claim_header] AS a
GROUP BY YEAR([first_service_date])
),

[stage] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,SUM([ed]) AS [current_ed]
FROM [stage].[mcaid_claim_header] AS a
GROUP BY YEAR([first_service_date])
),

[compare] AS
(
SELECT
 COALESCE(a.[claim_year], b.[claim_year]) AS [claim_year]
,[prior_ed]
,[current_ed]
,CAST([current_ed] AS NUMERIC) / [prior_ed] AS [pct_change]
FROM [final] AS a
FULL JOIN [stage] AS b
ON a.[claim_year] = b.[claim_year]
)

SELECT 
 @compare_current_prior_ed_min = MIN([pct_change])
,@compare_current_prior_ed_max = MAX([pct_change])
FROM [compare]
WHERE [claim_year] >= YEAR(GETDATE()) - 3;

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_header'
,'Compare current vs. prior ed visits'
,NULL
,getdate()
,'Min: ' + @compare_current_prior_ed_min + ', Max: ' + @compare_current_prior_ed_max + ' ratio of current to prior ed visits';