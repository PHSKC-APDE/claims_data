--Code to create stage.apcd_dental_claim
--Eli Kern (PHSKC-APDE)
--2019-6-27

if object_id('PHClaims.stage.apcd_dental_claim', 'U') is not null
	drop table PHClaims.stage.apcd_dental_claim;
create table PHClaims.stage.apcd_dental_claim (
	[dental_claim_service_line_id] [bigint] NULL,
	[extract_id] [bigint] NULL,
	[submitter_id] [int] NULL,
	[internal_member_id] [bigint] NULL,
	[submitter_clm_control_num] [varchar](272) NULL,
	[product_code_id] [int] NULL,
	[product_code] [varchar](20) NULL,
	[gender_code] [varchar](2) NULL,
	[age] [numeric](2, 0) NULL,
	[age_in_months] [numeric](2, 0) NULL,
	[subscriber_relationship_id] [int] NULL,
	[subscriber_relationship_code] [varchar](10) NULL,
	[line_counter] [int] NULL,
	[first_service_dt] [date] NULL,
	[last_service_dt] [date] NULL,
	[first_paid_dt] [date] NULL,
	[last_paid_dt] [date] NULL,
	[place_of_service_code] [varchar](2) NULL,
	[procedure_code] [varchar](20) NULL,
	[procedure_modifier_code_1] [varchar](20) NULL,
	[procedure_modifier_code_2] [varchar](20) NULL,
	[dental_tooth_system_id] [int] NULL,
	[dental_tooth_system_code] [varchar](2) NULL,
	[dental_tooth_code] [varchar](2) NULL,
	[dental_quadrant_id] [int] NULL,
	[dental_quadrant_code] [varchar](10) NULL,
	[dental_tooth_surface_id] [int] NULL,
	[dental_tooth_surface_code] [varchar](5) NULL,
	[claim_status_id] [int] NULL,
	[claim_status_code] [varchar](20) NULL,
	[quantity] [numeric](38, 2) NULL,
	[charge_amt] [numeric](38, 2) NULL,
	[icd_version_ind] [varchar](2) NULL,
	[principal_diagnosis_code] [varchar](7) NULL,
	[rendering_provider_id] [int] NULL,
	[rendering_internal_provider_id] [int] NULL,
	[billing_provider_id] [int] NULL,
	[billing_internal_provider_id] [int] NULL,
	[network_indicator_id] [int] NULL,
	[network_indicator_code] [varchar](20) NULL,
	[city] [varchar](100) NULL,
	[state] [varchar](2) NULL,
	[zip] [varchar](5) NULL,
	[age_65_flag] [varchar](1) NULL,
	[out_of_state_flag] [varchar](1) NULL,
	[orphaned_adjustment_flag] [varchar](1) NULL,
	[denied_claim_flag] [varchar](1) NULL
)
on [PRIMARY];



