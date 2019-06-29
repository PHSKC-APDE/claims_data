--Code to create stage.apcd_pharmacy_claim
--Eli Kern (PHSKC-APDE)
--2019-6-27

if object_id('PHClaims.stage.apcd_pharmacy_claim', 'U') is not null
	drop table PHClaims.stage.apcd_pharmacy_claim;
create table PHClaims.stage.apcd_pharmacy_claim (
	[pharmacy_claim_service_line_id] [bigint] NULL,
	[extract_id] [int] NULL,
	[submitter_id] [int] NULL,
	[internal_member_id] [bigint] NULL,
	[submitter_clm_control_num] [varchar](272) NULL,
	[product_code_id] [int] NULL,
	[product_code] [varchar](20) NULL,
	[gender_code] [varchar](2) NULL,
	[age] [int] NULL,
	[age_in_months] [int] NULL,
	[subscriber_relationship_id] [bigint] NULL,
	[subscriber_relationship_code] [varchar](10) NULL,
	[line_counter] [int] NULL,
	[prescription_filled_dt] [date] NULL,
	[first_paid_dt] [date] NULL,
	[last_paid_dt] [date] NULL,
	[national_drug_code] [varchar](11) NULL,
	[drug_name] [varchar](80) NULL,
	[claim_status_id] [int] NULL,
	[claim_status_code] [varchar](20) NULL,
	[quantity] [numeric](38, 2) NULL,
	[days_supply] [int] NULL,
	[thirty_day_equivalent] [int] NULL,
	[charge_amt] [numeric](38, 2) NULL,
	[refill_number] [int] NULL,
	[generic_drug_ind_id] [int] NULL,
	[generic_drug_ind_code] [varchar](10) NULL,
	[compound_drug_code_id] [int] NULL,
	[compound_drug_code] [varchar](10) NULL,
	[dispense_as_written_id] [int] NULL,
	[dispense_as_written_code] [varchar](20) NULL,
	[pharmacy_mail_order_code] [varchar](2) NULL,
	[pharmacy_provider_id] [int] NULL,
	[pharmacy_internal_provider_id] [int] NULL,
	[prscrbing_provider_id] [int] NULL,
	[prscrbing_internal_provider_id] [int] NULL,
	[network_indicator_id] [int] NULL,
	[network_indicator_code] [varchar](20) NULL,
	[city] [varchar](100) NULL,
	[state] [varchar](2) NULL,
	[zip] [varchar](5) NULL,
	[age_65_flag] [varchar](1) NULL,
	[out_of_state_flag] [varchar](1) NULL,
	[orphaned_adjustment_flag] [varchar](1) NULL,
	[denied_claim_flag] [varchar](1) NULL,
	[dup_flag_pbm_tpa] [varchar](1) NULL,
	[dup_flag_managed_care] [varchar](1) NULL,
	[dup_flag_part_d] [varchar](1) NULL,
	[medicaid_ffs_flag] [varchar](2) NULL,
	[injury_dt] [date] NULL,
	[benefits_exhausted_dt] [date] NULL
)
on [PRIMARY];



