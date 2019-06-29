--Code to create stage.apcd_member_month_detail
--Eli Kern (PHSKC-APDE)
--2019-6-27

if object_id('PHClaims.stage.apcd_member_month_detail', 'U') is not null
	drop table PHClaims.stage.apcd_member_month_detail;
create table PHClaims.stage.apcd_member_month_detail (
	[internal_member_id] [bigint] NULL,
	[extract_id] [int] NULL,
	[year_month] [int] NULL,
	[medical_product_code_id] [int] NULL,
	[medical_product_code] [varchar](20) NULL,
	[medical_submitter_id] [int] NULL,
	[medical_eligibility_id] [bigint] NULL,
	[med_commercial_eligibility_id] [bigint] NULL,
	[med_medicare_eligibility_id] [bigint] NULL,
	[med_medicaid_eligibility_id] [bigint] NULL,
	[pharmacy_product_code_id] [int] NULL,
	[pharmacy_product_code] [varchar](20) NULL,
	[pharmacy_submitter_id] [int] NULL,
	[pharmacy_eligibility_id] [bigint] NULL,
	[rx_commercial_eligibility_id] [bigint] NULL,
	[rx_medicare_eligibility_id] [bigint] NULL,
	[rx_medicaid_eligibility_id] [bigint] NULL,
	[age] [int] NULL,
	[gender_code] [varchar](1) NULL,
	[zip_code] [varchar](10) NULL
)
on [PRIMARY];



