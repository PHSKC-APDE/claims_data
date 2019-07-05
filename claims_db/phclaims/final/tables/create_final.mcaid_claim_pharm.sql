
use [PHClaims];
go

if object_id('[final].[mcaid_claim_procedure]', 'U') is not null
drop table [final].[mcaid_claim_procedure];
create table [final].[mcaid_claim_procedure]
([id_mcaid] varchar(200)
,[claim_header_id] bigint
,[first_service_date] date
,[last_service_date] date
,[procedure_code] varchar(200)
,[procedure_code_number] varchar(4)
,[modifier_1] varchar(200)
,[modifier_2] varchar(200)
,[modifier_3] varchar(200)
,[modifier_4] varchar(200)
,[last_run] datetime)
on [PHClaims_FG2];
go

create clustered index [idx_cl_mcaid_claim_procedure_claim_header_id] 
on [final].[mcaid_claim_procedure]([claim_header_id]);
create nonclustered index [idx_nc_mcaid_claim_procedure_procedure_code] 
on [final].[mcaid_claim_procedure]([procedure_code]);
create nonclustered index [idx_nc_mcaid_claim_procedure_first_service_date] 
on [final].[mcaid_claim_procedure]([first_service_date]);
go