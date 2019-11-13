
use [PHClaims];
go

delete from [metadata].[qa_mcaid] 
where table_name = 'stage.mcaid_claim_line';

declare @last_run as datetime;
declare @mcaid_elig_check as varchar(255);
declare @mcaid_elig_demo_check as varchar(255);
declare @compare_claim_line_to_claim_header_rows as varchar(255);
declare @rev_code_check as varchar(255);
declare @rac_code_lookup_check as varchar(255);
declare @compare_current_prior_min as varchar(255);
declare @compare_current_prior_max as varchar(255);

set @last_run = (select max(last_run) from [stage].[mcaid_claim_line]);

--All members should be in [mcaid_elig] table
set @mcaid_elig_check = 
(
select count(distinct a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_line] as a
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
,'stage.mcaid_claim_line'
,'All members should be in [mcaid_elig] table'
,CASE WHEN @mcaid_elig_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@mcaid_elig_check + ' members in mcaid_claim_line and are not in [mcaid_elig]';

--All members should be in [mcaid_elig_demo] table
set @mcaid_elig_demo_check = 
(
select count(distinct a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_line] as a
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
,'stage.mcaid_claim_line'
,'All members should be in [mcaid_elig_demo] table'
,CASE WHEN @mcaid_elig_demo_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@mcaid_elig_demo_check + ' members in mcaid_claim_line and are not in [mcaid_elig_demo]';

--Same number of claim lines in [stage].[mcaid_claim] and [stage].[mcaid_claim_line]
set @compare_claim_line_to_claim_header_rows =
((
select count(distinct [claim_line_id])
from [stage].[mcaid_claim_line]) - 
(
select count(distinct [CLM_LINE_TCN])
from [stage].[mcaid_claim]
));

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_line'
,'Compare mcaid_claim to mcaid_claim_line'
,CASE WHEN @compare_claim_line_to_claim_header_rows = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@compare_claim_line_to_claim_header_rows + ' difference in [stage].[mcaid_claim] and [stage].[mcaid_claim_line] rows';

--Check that [rev_code] is properly formed
set @rev_code_check = 
(
select count(*) 
from [stage].[mcaid_claim_line]
where [rev_code] is not null
and (len([rev_code]) <> 4 or isnumeric([rev_code]) = 0)
);

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_line'
,'Check that [rev_code] is properly formed'
,CASE WHEN @rev_code_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@rev_code_check + ' revenue codes are not 4-digit numeric left-zero-padded';

--Count line rac codes that do not join to RAC Code reference table
set @rac_code_lookup_check = 
(
select count(distinct 'RAC Code - ' + CAST([rac_code_line] AS VARCHAR(255)))
from [stage].[mcaid_claim_line] as a
where not exists
(
select 1
from [ref].[mcaid_rac_code] as b
where a.[rac_code_line] = b.[rac_code]
));

insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_line'
,'rac_code_line foreign key check'
,CASE WHEN @rac_code_lookup_check = '0' THEN 'PASS' ELSE 'FAIL' END
,getdate()
,@rac_code_lookup_check + ' line rac codes are not in [ref].[mcaid_rac_code]';

--Compare number of claim lines in current vs. prior analytic tables
WITH [final] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,COUNT(*) AS [prior_claim_line]
FROM [final].[mcaid_claim_line] AS a
GROUP BY YEAR([first_service_date])
),

[stage] AS
(
SELECT
 YEAR([first_service_date]) AS [claim_year]
,COUNT(*) AS [current_claim_line]
FROM [stage].[mcaid_claim_line] AS a
GROUP BY YEAR([first_service_date])
),

[compare] AS
(
SELECT
 COALESCE(a.[claim_year], b.[claim_year]) AS [claim_year]
,[prior_claim_line]
,[current_claim_line]
,CAST([current_claim_line] AS NUMERIC) / [prior_claim_line] AS [pct_change]
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
,'stage.mcaid_claim_line'
,'Compare current vs. prior analytic tables'
,NULL
,getdate()
,'Min: ' + @compare_current_prior_min + ', Max: ' + @compare_current_prior_max + ' ratio of current to prior rows';