
--QA of final.mcaid_claim_* tables
--7/12/19
--Philip Sylling

use [PHClaims];
go

insert into [metadata].[qa_mcaid_values]
([table_name]
,[qa_item]
,[qa_value]
,[qa_date]
,[note])

select 
 'final.mcaid_claim_header' as [table_name]
,'row_count' as [qa_item]
,(select count(*) from [final].[mcaid_claim_header]) as [qa_value]
,getdate() as [qa_date]
,'' as [note]

union all

select 
 'final.mcaid_claim_icdcm_header' as [table_name]
,'row_count' as [qa_item]
,(select count(*) from [final].[mcaid_claim_icdcm_header]) as [qa_value]
,getdate() as [qa_date]
,'' as [note]

union all

select 
 'final.mcaid_claim_line' as [table_name]
,'row_count' as [qa_item]
,(select count(*) from [final].[mcaid_claim_line]) as [qa_value]
,getdate() as [qa_date]
,'' as [note]

union all

select 
 'final.mcaid_claim_pharm' as [table_name]
,'row_count' as [qa_item]
,(select count(*) from [final].[mcaid_claim_pharm]) as [qa_value]
,getdate() as [qa_date]
,'' as [note]

union all

select 
 'final.mcaid_claim_procedure' as [table_name]
,'row_count' as [qa_item]
,(select count(*) from [final].[mcaid_claim_procedure]) as [qa_value]
,getdate() as [qa_date]
,'' as [note];

select * from [PHClaims].[metadata].[qa_mcaid_values];