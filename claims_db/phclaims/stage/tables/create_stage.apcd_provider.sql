--Code to create stage.apcd_provider
--Eli Kern (PHSKC-APDE)
--2019-6-27

if object_id('PHClaims.stage.apcd_provider', 'U') is not null
	drop table PHClaims.stage.apcd_provider;
create table PHClaims.stage.apcd_provider (
	[provider_id] [bigint] NULL,
	[extract_id] [bigint] NULL,
	[submitter_id] [bigint] NULL,
	[internal_provider_id] [bigint] NULL,
	[organization_name] [varchar](100) NULL,
	[last_name] [varchar](100) NULL,
	[first_name] [varchar](35) NULL,
	[middle_name] [varchar](25) NULL,
	[generation_suffix] [varchar](5) NULL,
	[entity_type] [varchar](20) NULL,
	[professional_credential_code] [varchar](20) NULL,
	[orig_npi] [varchar](20) NULL,
	[primary_specialty_id] [bigint] NULL,
	[primary_specialty_code] [varchar](20) NULL,
	[city] [varchar](100) NULL,
	[state] [varchar](2) NULL,
	[zip] [varchar](5) NULL
)
on [PRIMARY];



