
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
--from [final].[mcaid_elig_timevar] as b
from [stage].[mcaid_elig] as b
--where a.id_mcaid = b.id_mcaid
where a.id_mcaid = b.MEDICAID_RECIPIENT_ID
);
go

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_header]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_header'
--,'mcaid_elig_time_var.id_mcaid check'
,'mcaid_elig.MEDICAID_RECIPIENT_ID check'
,'PASS'
,getdate()
--,'All members in mcaid_claim_header are in mcaid_elig_time_var';
,'All members in mcaid_claim_header are in mcaid_elig';

-- Compare number of claim headers in current vs. prior analytic tables
WITH [final] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,COUNT([claim_header_id]) AS [prior_claim_header]
FROM [final].[mcaid_claim_header] AS a
GROUP BY YEAR([first_service_date])
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
,'Compare new stage-to-prior final claim headers'
,'PASS'
,getdate()
,'0.1% increase in 2018';

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
,'Stable';

SELECT [etl_batch_id]
      ,[last_run]
      ,[table_name]
      ,[qa_item]
      ,[qa_result]
      ,[qa_date]
      ,[note]
FROM [PHClaims].[metadata].[qa_mcaid]
WHERE [table_name] = 'stage.mcaid_claim_header';