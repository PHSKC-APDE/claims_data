--Code to create stage.apcd_medical_crosswalk
--Eli Kern (PHSKC-APDE)
--2019-6-27

if object_id('PHClaims.stage.apcd_medical_crosswalk', 'U') is not null
	drop table PHClaims.stage.apcd_medical_crosswalk;
create table PHClaims.stage.apcd_medical_crosswalk (
	[medical_claim_service_line_id] [bigint] NULL,
	[extract_id] [int] NULL,
	[inpatient_discharge_id] [int] NULL,
	[medical_claim_header_id] [bigint] NULL
)
on [PRIMARY];



