
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
,[last_run] datetime)
on [PHClaims_FG2];
go

--create indexes
create clustered index [idx_cl_mcaid_claim_icdcm_header_claim_header_id_icdcm_number]
on [final].[mcaid_claim_icdcm_header]([claim_header_id], [icdcm_number]);
create nonclustered index [idx_nc_mcaid_claim_icdcm_header_first_service_date] 
on [final].[mcaid_claim_icdcm_header]([first_service_date]);
create nonclustered index [idx_nc_mcaid_claim_icdcm_header_icdcm_version_icdcm_norm] 
on [final].[mcaid_claim_icdcm_header]([icdcm_version], [icdcm_norm]);
go

if object_id('[final].[mcaid_claim_pharm]', 'U') is not null
drop table [final].[mcaid_claim_pharm];
create table [final].[mcaid_claim_pharm]
([id_mcaid] varchar(255)
,[claim_header_id] bigint
,[ndc] varchar(255)
,[rx_days_supply] smallint
,[rx_quantity] numeric(19,3)
,[rx_fill_date] date
,[prescriber_id_format] varchar(10)
,[prescriber_id] varchar(255)
,[pharmacy_npi] bigint
,[last_run] datetime)
on [PRIMARY];
go

--create indexes
create clustered index [idx_cl_mcaid_claim_pharm_claim_header_id] 
on [final].[mcaid_claim_pharm]([claim_header_id]);
create nonclustered index [idx_nc_mcaid_claim_pharm_ndc] 
on [final].[mcaid_claim_pharm]([ndc]);
create nonclustered index [idx_nc_mcaid_claim_pharm_rx_fill_date] 
on [final].[mcaid_claim_pharm]([rx_fill_date]);

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

--create indexes
create clustered index [idx_cl_mcaid_claim_procedure_claim_header_id] 
on [final].[mcaid_claim_procedure]([claim_header_id]);
create nonclustered index [idx_nc_mcaid_claim_procedure_procedure_code] 
on [final].[mcaid_claim_procedure]([procedure_code]);
create nonclustered index [idx_nc_mcaid_claim_procedure_first_service_date] 
on [final].[mcaid_claim_procedure]([first_service_date]);
go

/*
if object_id('[stage].[mcaid_claim_icdcm_header]', 'U') is not null
drop table [stage].[mcaid_claim_icdcm_header];
create table [stage].[mcaid_claim_icdcm_header]
([id_mcaid] varchar(255)
,[claim_header_id] bigint
,[first_service_date] date
,[last_service_date] date
,[icdcm_raw] varchar(255)
,[icdcm_norm] varchar(255)
,[icdcm_version] tinyint
,[icdcm_number] varchar(5)
,[last_run] datetime)
on [PHClaims_FG2];
go

--create indexes
create clustered index [idx_cl_mcaid_claim_icdcm_header_claim_header_id_icdcm_number]
on [stage].[mcaid_claim_icdcm_header]([claim_header_id], [icdcm_number]);
create nonclustered index [idx_nc_mcaid_claim_icdcm_header_first_service_date] 
on [stage].[mcaid_claim_icdcm_header]([first_service_date]);
create nonclustered index [idx_nc_mcaid_claim_icdcm_header_icdcm_version_icdcm_norm] 
on [stage].[mcaid_claim_icdcm_header]([icdcm_version], [icdcm_norm]);
go

if object_id('[stage].[mcaid_claim_pharm]', 'U') is not null
drop table [stage].[mcaid_claim_pharm];
create table [stage].[mcaid_claim_pharm]
([id_mcaid] varchar(255)
,[claim_header_id] bigint
,[ndc] varchar(255)
,[rx_days_supply] smallint
,[rx_quantity] numeric(19,3)
,[rx_fill_date] date
,[prescriber_id_format] varchar(10)
,[prescriber_id] varchar(255)
,[pharmacy_npi] bigint
,[last_run] datetime)
on [PRIMARY];
go

--create indexes
create clustered index [idx_cl_mcaid_claim_pharm_claim_header_id] 
on [stage].[mcaid_claim_pharm]([claim_header_id]);
create nonclustered index [idx_nc_mcaid_claim_pharm_ndc] 
on [stage].[mcaid_claim_pharm]([ndc]);
create nonclustered index [idx_nc_mcaid_claim_pharm_rx_fill_date] 
on [stage].[mcaid_claim_pharm]([rx_fill_date]);

if object_id('[stage].[mcaid_claim_procedure]', 'U') is not null
drop table [stage].[mcaid_claim_procedure];
create table [stage].[mcaid_claim_procedure]
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

--create indexes
create clustered index [idx_cl_mcaid_claim_procedure_claim_header_id] 
on [stage].[mcaid_claim_procedure]([claim_header_id]);
create nonclustered index [idx_nc_mcaid_claim_procedure_procedure_code] 
on [stage].[mcaid_claim_procedure]([procedure_code]);
create nonclustered index [idx_nc_mcaid_claim_procedure_first_service_date] 
on [stage].[mcaid_claim_procedure]([first_service_date]);
go
*/