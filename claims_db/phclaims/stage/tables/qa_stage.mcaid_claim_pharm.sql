
use PHClaims;
go

delete from [metadata].[qa_mcaid] where table_name = 'stage.mcaid_claim_pharm';

--All members should be in elig_demo and table
select count(a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_pharm] as a
where not exists
(
select 1 
from [stage].[mcaid_elig_demo] as b
where a.id_mcaid = b.id_mcaid
);

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_pharm]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_pharm'
,'mcaid_elig_demo.id_mcaid foreign key check'
,'PASS'
,getdate()
,'All members in mcaid_claim_pharm are in mcaid_elig_demo';
go

--All members should be in elig_timevar table
select count(a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_pharm] as a
where not exists
(
select 1 
--from [final].[mcaid_elig_timevar] as b
from [stage].[mcaid_elig] as b
--where a.id_mcaid = b.id_mcaid
where a.id_mcaid = b.MEDICAID_RECIPIENT_ID
);
go

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_pharm]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_pharm'
--,'mcaid_elig_time_var.id_mcaid check'
,'mcaid_elig.MEDICAID_RECIPIENT_ID check'
,'PASS'
,getdate()
--,'All members in mcaid_claim_pharm are in mcaid_elig_time_var';
,'All members in mcaid_claim_pharm are in mcaid_elig';

--Check that ndc codes are properly formed type
--25,968,547
SELECT COUNT(*)
FROM [PHClaims].[stage].[mcaid_claim_pharm]
WHERE LEN([ndc]) <> 11
OR ISNUMERIC([ndc]) = 1;
GO

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_pharm]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_pharm'
,'Check ndc code digit structure'
,'PASS'
,getdate()
,'All codes are 11-digit left-zero-padded numeric';

--Count ndc codes that do not join to reference table
select distinct
 [ndc]
from [stage].[mcaid_claim_pharm] as a
where not exists
(
select 1
from [ref].[pharm] as b
where a.[ndc] = b.[ndc_code]
);
go

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_pharm]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_pharm'
,'ndc_code foreign key check'
,'FAIL'
,getdate()
,'3,964 ndc codes not in [ref].[pharm]';

/*
--ndc codes that do not join to reference table
select
 [ndc]
,count(*) as [num_claims]
from [stage].[mcaid_claim_pharm] as a
where not exists
(
select 1
from [ref].[pharm] as b
where a.[ndc] = b.[ndc_code]
)
GROUP BY
 [ndc]
ORDER BY
 [num_claims] desc;
*/

--Compare number of people with claim_header table
select
 (select count(distinct id_mcaid) as id_dcount
  from [stage].[mcaid_claim_pharm])
,(select count(distinct id_mcaid) as id_dcount
  from [final].[mcaid_claim_header])
,cast((select count(distinct id_mcaid) as id_dcount
  from [stage].[mcaid_claim_pharm]) as numeric) /
 (select count(distinct id_mcaid) as id_dcount
  from [final].[mcaid_claim_header]);
go

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_pharm]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_pharm'
,'Compare pharm to header (number of people)'
,'PASS'
,getdate()
,'77.3% of people in pharm are in header';

-- Compare number of ndc codes in current vs. prior analytic tables
WITH [final] AS
(
SELECT
 YEAR([rx_fill_date]) AS [claim_year]
,COUNT(*) AS [prior_num_pharm]
FROM [final].[mcaid_claim_pharm] AS a
GROUP BY YEAR([rx_fill_date])
),

[stage] AS
(
SELECT
 YEAR([rx_fill_date]) AS [claim_year]
,COUNT(*) AS [current_num_pharm]
FROM [stage].[mcaid_claim_pharm] AS a
GROUP BY YEAR([rx_fill_date])
)

SELECT
 COALESCE(a.[claim_year], b.[claim_year]) AS [claim_year]
,[prior_num_pharm]
,[current_num_pharm]
,CAST([current_num_pharm] AS NUMERIC) / [prior_num_pharm] AS [pct_change]
FROM [final] AS a
FULL JOIN [stage] AS b
ON a.[claim_year] = b.[claim_year]
ORDER BY [claim_year];
GO

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_pharm]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_pharm'
,'Compare new-to-prior pharm counts'
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
WHERE [table_name] = 'stage.mcaid_claim_pharm';