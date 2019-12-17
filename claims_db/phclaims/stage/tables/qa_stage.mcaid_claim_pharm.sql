
use [PHClaims];
go

delete from [metadata].[qa_mcaid] 
where table_name = 'stage.mcaid_claim_pharm';

declare @last_run as datetime;
declare @mcaid_elig_check as varchar(255);
declare @mcaid_elig_demo_check as varchar(255);
declare @ndc_code_check as varchar(255);
declare @ndc_lookup_check as varchar(255);
declare @pct_claim_header_id_with_rx as varchar(255);
declare @compare_current_prior_min as varchar(255);
declare @compare_current_prior_max as varchar(255);

set @last_run = (select max(last_run) from [stage].[mcaid_claim_pharm]);

--All members should be in [mcaid_elig] table
set @mcaid_elig_check = 
(
select count(distinct a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_pharm] as a
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
,'stage.mcaid_claim_pharm'
,'All members should be in [mcaid_elig] table'
,CASE WHEN @mcaid_elig_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@mcaid_elig_check + ' members in mcaid_claim_pharm and are not in [mcaid_elig]';

--All members should be in [mcaid_elig_demo] table
set @mcaid_elig_demo_check = 
(
select count(distinct a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_pharm] as a
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
,'stage.mcaid_claim_pharm'
,'All members should be in [mcaid_elig_demo] table'
,CASE WHEN @mcaid_elig_demo_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@mcaid_elig_demo_check + ' members in mcaid_claim_pharm and are not in [mcaid_elig_demo]';

--Check that ndc codes are properly formed
set @ndc_code_check =
(
select count(*)
from [stage].[mcaid_claim_pharm]
where len([ndc]) <> 11 or isnumeric([ndc]) = 1
);

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_pharm'
,'Check ndc code digit structure'
,CASE WHEN @ndc_code_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@ndc_code_check + ' codes are not 11-digit left-zero-padded numeric';

--Count if ndc codes do not join to reference table
set @ndc_lookup_check =
(
select count(distinct 'NDC - ' + [ndc])
from [stage].[mcaid_claim_pharm] as a
where not exists
(
select 1
from [ref].[pharm] as b
where a.[ndc] = b.[ndc_code]
));

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_pharm'
,'Count if ndc codes do not join to reference table'
,CASE WHEN @ndc_lookup_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@ndc_lookup_check + ' NDC codes not in [ref].[pharm]';

--Compare number of people with claim_header table
set @pct_claim_header_id_with_rx = 
(
select
 cast((select count(distinct id_mcaid) as id_dcount
 from [stage].[mcaid_claim_pharm]) as numeric) /
 (select count(distinct id_mcaid) as id_dcount
  from [stage].[mcaid_claim_header])
);

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_pharm'
,'Compare number of people with claim_header table'
,NULL
,getdate()
,@pct_claim_header_id_with_rx + ' proportion of members with a claim header have a rx';

-- Compare number of ndc codes in current vs. prior analytic tables
WITH [final] AS
(
SELECT
 YEAR([rx_fill_date]) AS [claim_year]
,COUNT(*) AS [prior_num_rx]
FROM [final].[mcaid_claim_pharm] AS a
GROUP BY YEAR([rx_fill_date])
),

[stage] AS
(
SELECT
 YEAR([rx_fill_date]) AS [claim_year]
,COUNT(*) AS [current_num_rx]
FROM [stage].[mcaid_claim_pharm] AS a
GROUP BY YEAR([rx_fill_date])
),

[compare] AS
(
SELECT
 COALESCE(a.[claim_year], b.[claim_year]) AS [claim_year]
,[prior_num_rx]
,[current_num_rx]
,CAST([current_num_rx] AS NUMERIC) / [prior_num_rx] AS [pct_change]
FROM [final] AS a
FULL JOIN [stage] AS b
ON a.[claim_year] = b.[claim_year]
)

SELECT 
 @compare_current_prior_min = MIN([pct_change])
,@compare_current_prior_max = MAX([pct_change])
FROM [compare]
WHERE [claim_year] >= YEAR(GETDATE()) - 3;

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_pharm'
,'Compare current vs. prior analytic tables'
,NULL
,getdate()
,'Min: ' + @compare_current_prior_min + ', Max: ' + @compare_current_prior_max + ' ratio of current to prior rows';