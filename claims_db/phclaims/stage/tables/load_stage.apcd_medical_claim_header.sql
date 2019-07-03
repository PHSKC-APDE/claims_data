--Code to load data to stage.apcd_medical_claim_header
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
insert into PHClaims.stage.apcd_medical_claim_header with (tablock)
--archived rows before cutoff date
select
[medical_claim_header_id]
,[extract_id]
,[submitter_id]
,[internal_member_id]
,[internal_provider_id]
,[product_code_id]
,[product_code]
,[age]
,[first_service_dt]
,[last_service_dt]
,[first_paid_dt]
,[last_paid_dt]
,[charge_amt]
,[diagnosis_code]
,[icd_version_ind]
,[header_status]
,[denied_header_flag]
,[orphaned_header_flag]
,[claim_type_id]
,[type_of_setting_id]
,[place_of_setting_id]
,[type_of_bill_code]
,[emergency_room_flag]
,[operating_room_flag]
from PHclaims.archive.apcd_medical_claim_header
where first_service_dt <= @cutoff_date
--new rows from new extract
union
select
[medical_claim_header_id]
,[extract_id]
,[submitter_id]
,[internal_member_id]
,[internal_provider_id]
,[product_code_id]
,[product_code]
,[age]
,[first_service_dt]
,[last_service_dt]
,[first_paid_dt]
,[last_paid_dt]
,[charge_amt]
,[diagnosis_code]
,[icd_version_ind]
,[header_status]
,[denied_header_flag]
,[orphaned_header_flag]
,[claim_type_id]
,[type_of_setting_id]
,[place_of_setting_id]
,[type_of_bill_code]
,[emergency_room_flag]
,[operating_room_flag]
from PHclaims.load_raw.apcd_medical_claim_header;



