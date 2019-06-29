
use PHClaims;
go

delete from [metadata].[qa_mcaid] where table_name = 'stage.mcaid_claim_line';

--All members should be in elig_demo and table
select count(a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_line] as a
where not exists
(
select 1 
from [stage].[mcaid_elig_demo] as b
where a.id_mcaid = b.id_mcaid
);
go

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_line]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_line'
,'mcaid_elig_demo.id_mcaid check'
,'PASS'
,getdate()
,'All members in mcaid_claim_line are in mcaid_elig_demo';

--All members should be in elig_timevar table
select count(a.id_mcaid) as id_dcount
from [stage].[mcaid_claim_line] as a
where not exists
(
select 1 
from [stage].[mcaid_elig_timevar] as b
where a.id_mcaid = b.id_mcaid
);
go

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_line]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_line'
,'mcaid_elig_time_var.id_mcaid check'
,'PASS'
,getdate()
,'All members in mcaid_claim_line are in mcaid_elig_time_var';

-- Same number claim lines in [stage].[mcaid_claim] and [stage].[mcaid_claim_line]
select count(distinct [claim_line_id])
from [PHClaims].[stage].[mcaid_claim_line]
select count(distinct [CLM_LINE_TCN])
from [PHClaims].[stage].[mcaid_claim];
go

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_line]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_line'
,'Check count of claim lines'
,'PASS'
,getdate()
,'Same distinct count in [stage].[mcaid_claim] and [stage].[mcaid_claim_line]';

-- Check that [rev_code] is properly formed
select count(*) 
from [PHClaims].[stage].[mcaid_claim_line]
where [rev_code] is not null
and (len([rev_code]) <> 4 or isnumeric([rev_code]) = 0);
go

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_line]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_line'
,'Check [rev_code] format'
,'PASS'
,getdate()
,'[rev_code] is 4-digit numeric left-zero-padded';

-- Count line rac codes that do not join to RAC Code reference table
select 
 [rac_code_line]
,count(*)
from [stage].[mcaid_claim_line] as a
where not exists
(
select 1
from [ref].[mcaid_rac_code] as b
where a.[rac_code_line] = b.[rac_code]
)
group by [rac_code_line]
order by [rac_code_line];
go

declare @last_run as datetime = (select max(last_run) from [stage].[mcaid_claim_line]);
insert into [metadata].[qa_mcaid]
select 
 NULL
,@last_run
,'stage.mcaid_claim_line'
,'rac_code_line foreign key check'
,'FAIL'
,getdate()
,'39 line rac codes (2000 or higher) not in [ref].[mcaid_rac_code]';

SELECT [etl_batch_id]
      ,[last_run]
      ,[table_name]
      ,[qa_item]
      ,[qa_result]
      ,[qa_date]
      ,[note]
FROM [PHClaims].[metadata].[qa_mcaid]
WHERE [table_name] = 'stage.mcaid_claim_line';