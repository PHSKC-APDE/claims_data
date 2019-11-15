
use [PHClaims];
go

if object_id('[archive].[mcaid_claim_icdcm_header]', 'U') is not null
drop table [archive].[mcaid_claim_icdcm_header];
create table [archive].[mcaid_claim_icdcm_header]
([id_mcaid] varchar(255)
,[claim_header_id] bigint
,[first_service_date] date
,[last_service_date] date
,[icdcm_raw] varchar(255)
,[icdcm_norm] varchar(255)
,[icdcm_version] tinyint
,[icdcm_number] varchar(5)
,[last_run] datetime)
on [PRIMARY];
go

--create indexes
create clustered index [idx_cl_mcaid_claim_icdcm_header_claim_header_id_icdcm_number]
on [archive].[mcaid_claim_icdcm_header]([claim_header_id], [icdcm_number]);
create nonclustered index [idx_nc_mcaid_claim_icdcm_header_first_service_date] 
on [archive].[mcaid_claim_icdcm_header]([first_service_date]);
create nonclustered index [idx_nc_mcaid_claim_icdcm_header_icdcm_version_icdcm_norm] 
on [archive].[mcaid_claim_icdcm_header]([icdcm_version], [icdcm_norm]);
go

if object_id('[archive].[mcaid_claim_line]', 'U') is not null
drop table [archive].[mcaid_claim_line];
create table [archive].[mcaid_claim_line]
([id_mcaid] varchar(200)
,[claim_header_id] bigint
,[claim_line_id] bigint
,[first_service_date] date
,[last_service_date] date
,[rev_code] varchar(200)
,[rac_code_line] int
,[last_run] datetime) 
on [PRIMARY];
go

--create indexes
create clustered index [idx_cl_mcaid_claim_line_claim_header_id] 
on [archive].[mcaid_claim_line]([claim_header_id]);
create nonclustered index [idx_nc_mcaid_claim_line_first_service_date] 
on [archive].[mcaid_claim_line]([first_service_date]);
create nonclustered index [idx_nc_mcaid_claim_line_rev_code] 
on [archive].[mcaid_claim_line]([rev_code]);
go

if object_id('[archive].[mcaid_claim_pharm]', 'U') is not null
drop table [archive].[mcaid_claim_pharm];
create table [archive].[mcaid_claim_pharm]
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
on [archive].[mcaid_claim_pharm]([claim_header_id]);
create nonclustered index [idx_nc_mcaid_claim_pharm_ndc] 
on [archive].[mcaid_claim_pharm]([ndc]);
create nonclustered index [idx_nc_mcaid_claim_pharm_rx_fill_date] 
on [archive].[mcaid_claim_pharm]([rx_fill_date]);
go

if object_id('[archive].[mcaid_claim_procedure]', 'U') is not null
drop table [archive].[mcaid_claim_procedure];
create table [archive].[mcaid_claim_procedure]
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
on [PRIMARY];
go

--create indexes
create clustered index [idx_cl_mcaid_claim_procedure_claim_header_id] 
on [archive].[mcaid_claim_procedure]([claim_header_id]);
create nonclustered index [idx_nc_mcaid_claim_procedure_procedure_code] 
on [archive].[mcaid_claim_procedure]([procedure_code]);
create nonclustered index [idx_nc_mcaid_claim_procedure_first_service_date] 
on [archive].[mcaid_claim_procedure]([first_service_date]);
go

if object_id('[archive].[mcaid_claim_header]', 'U') is not null 
drop table [archive].[mcaid_claim_header]
create table [archive].[mcaid_claim_header]
(id_mcaid varchar(255)
,claim_header_id bigint
,clm_type_mcaid_id varchar(20)
,claim_type_id tinyint
,first_service_date date
,last_service_date date
,patient_status varchar(255)
,admsn_source varchar(255)
,admsn_date date
,admsn_time time(0)
,dschrg_date date
,place_of_service_code varchar(255)
,type_of_bill_code varchar(255)
,clm_status_code tinyint
,billing_provider_npi bigint
,drvd_drg_code varchar(255)
,insrnc_cvrg_code varchar(255)
,last_pymnt_date date
,bill_date date
,system_in_date date
,claim_header_id_date date
,primary_diagnosis varchar(255)
,icdcm_version tinyint
,primary_diagnosis_poa varchar(255)
,mental_dx1 tinyint
,mental_dxany tinyint
,mental_dx_rda_any tinyint
,sud_dx_rda_any tinyint
,maternal_dx1 tinyint
,maternal_broad_dx1 tinyint
,newborn_dx1 tinyint
,ed tinyint
,ed_nohosp tinyint
,ed_bh tinyint
,ed_avoid_ca tinyint
,ed_avoid_ca_nohosp tinyint
,ed_ne_nyu tinyint
,ed_pct_nyu tinyint
,ed_pa_nyu tinyint
,ed_npa_nyu tinyint
,ed_mh_nyu tinyint
,ed_sud_nyu tinyint
,ed_alc_nyu tinyint
,ed_injury_nyu tinyint
,ed_unclass_nyu tinyint
,ed_emergent_nyu tinyint
,ed_nonemergent_nyu tinyint
,ed_intermediate_nyu tinyint
,inpatient tinyint
,ipt_medsurg tinyint
,ipt_bh tinyint
,intent varchar(255)
,mechanism varchar(255)
,sdoh_any tinyint
,ed_sdoh tinyint
,ipt_sdoh tinyint
,ccs varchar(255)
,ccs_description varchar(500)
,ccs_description_plain_lang varchar(500)
,ccs_mult1 varchar(255)
,ccs_mult1_description varchar(500)
,ccs_mult2 varchar(255)
,ccs_mult2_description varchar(500)
,ccs_mult2_plain_lang varchar(500)
,ccs_final_description varchar(500)
,ccs_final_plain_lang varchar(500)
,last_run datetime)
on [PRIMARY];
go

--create indexes
create clustered index [idx_cl_mcaid_claim_header_claim_header_id] 
on [archive].[mcaid_claim_header]([claim_header_id]);
create nonclustered index [idx_nc_mcaid_claim_header_type_of_bill_code] 
on [archive].[mcaid_claim_header]([type_of_bill_code]);
create nonclustered index [idx_nc_mcaid_claim_header_clm_type_mcaid_id] 
on [archive].[mcaid_claim_header]([clm_type_mcaid_id]);
create nonclustered index [idx_nc_mcaid_claim_header_drvd_drg_code] 
on [archive].[mcaid_claim_header]([drvd_drg_code]);
create nonclustered index [idx_nc_mcaid_claim_header_first_service_date] 
on [archive].[mcaid_claim_header]([first_service_date]);
create nonclustered index [idx_nc_mcaid_claim_header_id_mcaid] 
on [archive].[mcaid_claim_header]([id_mcaid]);
create nonclustered index [idx_nc_mcaid_claim_header_place_of_service_code] 
on [archive].[mcaid_claim_header]([place_of_service_code]);
go
