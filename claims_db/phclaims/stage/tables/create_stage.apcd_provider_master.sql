--Code to create stage.apcd_provider_master
--Eli Kern (PHSKC-APDE)
--2019-6-27

if object_id('PHClaims.stage.apcd_provider_master', 'U') is not null
	drop table PHClaims.stage.apcd_provider_master;
create table PHClaims.stage.apcd_provider_master (
	[internal_provider_id] [int] NULL,
	[extract_id] [int] NULL,
	[entity_type] [varchar](20) NULL,
	[organization_name_legal] [varchar](100) NULL,
	[last_name_legal] [varchar](100) NULL,
	[first_name_legal] [varchar](100) NULL,
	[middle_name_legal] [varchar](100) NULL,
	[organization_name_other] [varchar](100) NULL,
	[organization_name_other_type] [varchar](50) NULL,
	[last_name_other] [varchar](100) NULL,
	[first_name_other] [varchar](100) NULL,
	[middle_name_other] [varchar](100) NULL,
	[generation_suffix] [varchar](5) NULL,
	[professional_credential_code] [varchar](20) NULL,
	[npi] [numeric](38, 0) NULL,
	[primary_taxonomy] [varchar](20) NULL,
	[secondary_taxonomy] [varchar](20) NULL,
	[city_physical] [varchar](100) NULL,
	[state_physical] [varchar](2) NULL,
	[zip_physical] [varchar](5) NULL,
	[county_physical] [varchar](100) NULL,
	[country_physical] [varchar](100) NULL,
	[ach_region_physical] [varchar](100) NULL
)
on [PRIMARY];



