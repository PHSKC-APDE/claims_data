--Code to create stage.apcd_provider_practice_roster
--Eli Kern (PHSKC-APDE)
--2019-6-27

if object_id('PHClaims.stage.apcd_provider_practice_roster', 'U') is not null
	drop table PHClaims.stage.apcd_provider_practice_roster;
create table PHClaims.stage.apcd_provider_practice_roster (
	[npi] [numeric](38, 0) NULL,
	[first_name] [varchar](100) NULL,
	[middle_name] [varchar](100) NULL,
	[last_name] [varchar](100) NULL,
	[professional_credential] [varchar](100) NULL,
	[practice_organization_id] [numeric](38, 0) NULL,
	[primary_organization_ind] [numeric](1, 0) NULL,
	[affiliation_begin_dt] [date] NULL,
	[affiliation_end_dt] [date] NULL,
	[organization_name] [varchar](400) NULL,
	[organization_type] [varchar](100) NULL,
	[practice_street_address1] [varchar](100) NULL,
	[practice_street_address2] [varchar](100) NULL,
	[practice_city] [varchar](100) NULL,
	[practice_state] [varchar](2) NULL,
	[practice_zip] [varchar](5) NULL
)
on [PRIMARY];



