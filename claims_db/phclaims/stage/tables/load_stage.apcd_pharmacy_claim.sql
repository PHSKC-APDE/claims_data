--Code to load data to stage.apcd_pharmacy_claim
--Eli Kern (PHSKC-APDE)
--2019-6-27

------------------
--STEP 1: Set cutoff date for pulling rows from archive table
-------------------
declare @cutoff_date date;
set @cutoff_date = '2017-12-31';

------------------
--STEP 2: Insert data into table shell
-------------------
insert into PHClaims.stage.apcd_pharmacy_claim with (tablock)
--archived rows before cutoff date
select
[pharmacy_claim_service_line_id]
,[extract_id]
,[submitter_id]
,[internal_member_id]
,[submitter_clm_control_num]
,[product_code_id]
,[product_code]
,[gender_code]
,[age]
,[age_in_months]
,[subscriber_relationship_id]
,[subscriber_relationship_code]
,[line_counter]
,[prescription_filled_dt]
,[first_paid_dt]
,[last_paid_dt]
,[national_drug_code]
,[drug_name]
,[claim_status_id]
,[claim_status_code]
,[quantity]
,[days_supply]
,[thirty_day_equivalent]
,[charge_amt]
,[refill_number]
,[generic_drug_ind_id]
,[generic_drug_ind_code]
,[compound_drug_code_id]
,[compound_drug_code]
,[dispense_as_written_id]
,[dispense_as_written_code]
,[pharmacy_mail_order_code]
,[pharmacy_provider_id]
,[pharmacy_internal_provider_id]
,[prscrbing_provider_id]
,[prscrbing_internal_provider_id]
,[network_indicator_id]
,[network_indicator_code]
,[city]
,[state]
,[zip]
,[age_65_flag]
,[out_of_state_flag]
,[orphaned_adjustment_flag]
,[denied_claim_flag]
,[dup_flag_pbm_tpa]
,[dup_flag_managed_care]
,[dup_flag_part_d]
,[medicaid_ffs_flag]
,[injury_dt]
,[benefits_exhausted_dt]
from PHclaims.archive.apcd_pharmacy_claim
where first_paid_dt <= @cutoff_date
--new rows from new extract
union
select
[pharmacy_claim_service_line_id]
,[extract_id]
,[submitter_id]
,[internal_member_id]
,[submitter_clm_control_num]
,[product_code_id]
,[product_code]
,[gender_code]
,[age]
,[age_in_months]
,[subscriber_relationship_id]
,[subscriber_relationship_code]
,[line_counter]
,[prescription_filled_dt]
,[first_paid_dt]
,[last_paid_dt]
,[national_drug_code]
,[drug_name]
,[claim_status_id]
,[claim_status_code]
,[quantity]
,[days_supply]
,[thirty_day_equivalent]
,[charge_amt]
,[refill_number]
,[generic_drug_ind_id]
,[generic_drug_ind_code]
,[compound_drug_code_id]
,[compound_drug_code]
,[dispense_as_written_id]
,[dispense_as_written_code]
,[pharmacy_mail_order_code]
,[pharmacy_provider_id]
,[pharmacy_internal_provider_id]
,[prscrbing_provider_id]
,[prscrbing_internal_provider_id]
,[network_indicator_id]
,[network_indicator_code]
,[city]
,[state]
,[zip]
,[age_65_flag]
,[out_of_state_flag]
,[orphaned_adjustment_flag]
,[denied_claim_flag]
,[dup_flag_pbm_tpa]
,[dup_flag_managed_care]
,[dup_flag_part_d]
,[medicaid_ffs_flag]
,[injury_dt]
,[benefits_exhausted_dt]
from PHclaims.load_raw.apcd_pharmacy_claim;



