-----------------------
--Drop row_number column from all big tables
--Eli Kern, APDE, PHSKC
--6/26/2019
-----------------------

alter table PHClaims.load_raw.apcd_dental_claim
drop column row_number;

alter table PHClaims.load_raw.apcd_eligibility
drop column row_number;

alter table PHClaims.load_raw.apcd_medical_claim
drop column row_number;

alter table PHClaims.load_raw.apcd_medical_claim_column_add
drop column row_number;

alter table PHClaims.load_raw.apcd_medical_claim_header
drop column row_number;

alter table PHClaims.load_raw.apcd_medical_crosswalk
drop column row_number;

alter table PHClaims.load_raw.apcd_member_month_detail
drop column row_number;

alter table PHClaims.load_raw.apcd_pharmacy_claim
drop column row_number;

alter table PHClaims.load_raw.apcd_provider
drop column row_number;