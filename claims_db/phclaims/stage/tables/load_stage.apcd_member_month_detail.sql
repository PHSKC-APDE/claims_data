--Code to load data to stage.apcd_member_month_detail
--Eli Kern (PHSKC-APDE)
--2019-6-27

------------------
--STEP 1: Insert data into table shell
-------------------
insert into PHClaims.stage.apcd_member_month_detail with (tablock)
select
[internal_member_id]
,[extract_id]
,[year_month]
,[medical_product_code_id]
,[medical_product_code]
,[medical_submitter_id]
,[medical_eligibility_id]
,[med_commercial_eligibility_id]
,[med_medicare_eligibility_id]
,[med_medicaid_eligibility_id]
,[pharmacy_product_code_id]
,[pharmacy_product_code]
,[pharmacy_submitter_id]
,[pharmacy_eligibility_id]
,[rx_commercial_eligibility_id]
,[rx_medicare_eligibility_id]
,[rx_medicaid_eligibility_id]
,[age]
,[gender_code]
,[zip_code]
from PHclaims.load_raw.apcd_member_month_detail;



