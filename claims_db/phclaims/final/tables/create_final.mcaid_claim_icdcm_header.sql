
use [PHClaims];
go

if object_id('[final].[mcaid_claim_icdcm_header]', 'U') is not null
drop table [final].[mcaid_claim_icdcm_header];
create table [final].[mcaid_claim_icdcm_header]
([id_mcaid] varchar(255)
,[claim_header_id] bigint
,[first_service_date] date
,[last_service_date] date
,[icdcm_raw] varchar(255)
,[icdcm_norm] varchar(255)
,[icdcm_version] tinyint
,[icdcm_number] varchar(5)
,[last_run] datetime);
go

--create indexes
create clustered index [idx_cl_mcaid_claim_icdcm_header_claim_header_id_icdcm_number]
on [final].[mcaid_claim_icdcm_header]([claim_header_id], [icdcm_number]);
create nonclustered index [idx_nc_mcaid_claim_icdcm_header_first_service_date] 
on [final].[mcaid_claim_icdcm_header]([first_service_date]);
create nonclustered index [idx_nc_mcaid_claim_icdcm_header_icdcm_version_icdcm_norm] 
on [final].[mcaid_claim_icdcm_header]([icdcm_version], [icdcm_norm]);
go