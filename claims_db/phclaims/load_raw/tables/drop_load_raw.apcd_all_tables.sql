--Drop all load_raw APCD tables after stage tables pass QA
--Quarterly refresh
--Eli Kern
--June 2019

--extract 159
drop table PHClaims.load_raw.apcd_cmsdrg_output_multi_ver;
drop table PHClaims.load_raw.apcd_dental_claim;
drop table PHClaims.load_raw.apcd_eligibility;
drop table PHClaims.load_raw.apcd_inpatient_stay_summary_ltd;
drop table PHClaims.load_raw.apcd_medical_claim;
drop table PHClaims.load_raw.apcd_medical_claim_column_add;
drop table PHClaims.load_raw.apcd_medical_claim_header;
drop table PHClaims.load_raw.apcd_medical_crosswalk;
drop table PHClaims.load_raw.apcd_member_month_detail;
drop table PHClaims.load_raw.apcd_pharmacy_claim;
drop table PHClaims.load_raw.apcd_provider;
drop table PHClaims.load_raw.apcd_provider_master;
drop table PHClaims.load_raw.apcd_provider_practice_roster;