--Code to create stage.apcd_medical_claim_header
--Eli Kern (PHSKC-APDE)
--2019-6-27

if object_id('PHClaims.stage.apcd_medical_claim_header', 'U') is not null
	drop table PHClaims.stage.apcd_medical_claim_header;
create table PHClaims.stage.apcd_medical_claim_header (
	[medical_claim_header_id] [bigint] NULL,
	[extract_id] [int] NULL,
	[submitter_id] [int] NULL,
	[internal_member_id] [bigint] NULL,
	[internal_provider_id] [int] NULL,
	[product_code_id] [int] NULL,
	[product_code] [varchar](20) NULL,
	[age] [numeric](2, 0) NULL,
	[first_service_dt] [date] NULL,
	[last_service_dt] [date] NULL,
	[first_paid_dt] [date] NULL,
	[last_paid_dt] [date] NULL,
	[charge_amt] [numeric](38, 2) NULL,
	[diagnosis_code] [varchar](20) NULL,
	[icd_version_ind] [varchar](2) NULL,
	[header_status] [varchar](2) NULL,
	[denied_header_flag] [varchar](1) NULL,
	[orphaned_header_flag] [varchar](1) NULL,
	[claim_type_id] [int] NULL,
	[type_of_setting_id] [int] NULL,
	[place_of_setting_id] [int] NULL,
	[type_of_bill_code] [varchar](4) NULL,
	[emergency_room_flag] [varchar](1) NULL,
	[operating_room_flag] [varchar](1) NULL
)
on [PRIMARY];



