
/*
SELECT TOP(100000) * FROM [claims].[stage_apcd_claim_icdcm_raw]
SELECT TOP(100000) * FROM [claims].[stage_apcd_claim_line_raw]
SELECT TOP(100000) * FROM [claims].[stage_apcd_claim_procedure_raw]
SELECT TOP(100000) * FROM [claims].[stage_apcd_claim_provider_raw]
SELECT TOP(100000) * FROM [claims].[stage_apcd_dental_claim]
SELECT TOP(100000) * FROM [claims].[stage_apcd_eligibility]
SELECT TOP(100000) * FROM [claims].[stage_apcd_medical_claim_header]
SELECT TOP(100000) * FROM [claims].[stage_apcd_member_month_detail]
SELECT TOP(100000) * FROM [claims].[stage_apcd_pharmacy_claim]
SELECT TOP(100000) * FROM [claims].[stage_apcd_provider]
SELECT TOP(100000) * FROM [claims].[stage_apcd_provider_master]
*/

/*
DROP EXTERNAL TABLE [claims].[stage_apcd_claim_icdcm_raw]
DROP EXTERNAL TABLE [claims].[stage_apcd_claim_line_raw]
DROP EXTERNAL TABLE [claims].[stage_apcd_claim_procedure_raw]
DROP EXTERNAL TABLE [claims].[stage_apcd_claim_provider_raw]
DROP EXTERNAL TABLE [claims].[stage_apcd_dental_claim]
DROP EXTERNAL TABLE [claims].[stage_apcd_eligibility]
DROP EXTERNAL TABLE [claims].[stage_apcd_medical_claim_header]
DROP EXTERNAL TABLE [claims].[stage_apcd_member_month_detail]
DROP EXTERNAL TABLE [claims].[stage_apcd_pharmacy_claim]
DROP EXTERNAL TABLE [claims].[stage_apcd_provider]
DROP EXTERNAL TABLE [claims].[stage_apcd_provider_master]
*/

CREATE EXTERNAL TABLE [claims].[stage_apcd_claim_icdcm_raw]
(internal_member_id bigint NULL,
medical_claim_header_id bigint NULL,
first_service_dt date NULL,
last_service_dt date NULL,
icdcm_raw varchar(200) NULL,
icdcm_norm varchar(200) NULL,
icdcm_version int NULL,
icdcm_number varchar(5) NULL,
etl_batch_id int NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'apcd_claim_icdcm_raw');

CREATE EXTERNAL TABLE [claims].[stage_apcd_claim_line_raw]
(id_apcd bigint NULL,
claim_header_id bigint NULL,
claim_line_id bigint NULL,
line_counter int NULL,
first_service_dt date NULL,
last_service_dt date NULL,
charge_amt numeric(38,2) NULL,
revenue_code varchar(10) NULL,
place_of_service_code varchar(10) NULL,
admission_dt date NULL,
discharge_dt date NULL,
discharge_status_code varchar(10) NULL,
admission_point_of_origin_code varchar(10) NULL,
admission_type varchar(10) NULL,
etl_batch_id int NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'apcd_claim_line_raw');

CREATE EXTERNAL TABLE [claims].[stage_apcd_claim_procedure_raw]
(internal_member_id bigint NULL,
medical_claim_header_id bigint NULL,
first_service_dt date NULL,
last_service_dt date NULL,
procedure_code varchar(200) NULL,
procedure_code_number varchar(200) NULL,
procedure_modifier_code_1 varchar(200) NULL,
procedure_modifier_code_2 varchar(200) NULL,
procedure_modifier_code_3 varchar(200) NULL,
procedure_modifier_code_4 varchar(200) NULL,
etl_batch_id int NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'apcd_claim_procedure_raw');

CREATE EXTERNAL TABLE [claims].[stage_apcd_claim_provider_raw]
(internal_member_id bigint NULL,
medical_claim_header_id bigint NULL,
first_service_dt date NULL,
last_service_dt date NULL,
provider_id_apcd bigint NULL,
provider_id_raw_apcd bigint NULL,
provider_type varchar(9) NULL,
etl_batch_id int NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'apcd_claim_provider_raw');

CREATE EXTERNAL TABLE [claims].[stage_apcd_dental_claim]
(dental_claim_service_line_id bigint NULL,
submitter_id bigint NULL,
internal_member_id bigint NULL,
submitter_clm_control_num varchar(272) NULL,
product_code_id bigint NULL,
subscriber_relationship_id bigint NULL,
line_counter int NULL,
first_service_dt date NULL,
last_service_dt date NULL,
first_paid_dt date NULL,
last_paid_dt date NULL,
place_of_service_code varchar(10) NULL,
procedure_code varchar(10) NULL,
procedure_modifier_code_1 varchar(10) NULL,
procedure_modifier_code_2 varchar(10) NULL,
dental_tooth_code varchar(10) NULL,
dental_quadrant_id bigint NULL,
dental_tooth_surface_code varchar(10) NULL,
claim_status_id bigint NULL,
quantity numeric(38,2) NULL,
charge_amt numeric(38,2) NULL,
icd_version_ind varchar(10) NULL,
principal_diagnosis_code varchar(10) NULL,
rendering_provider_id bigint NULL,
rendering_internal_provider_id bigint NULL,
billing_provider_id bigint NULL,
billing_internal_provider_id bigint NULL,
network_indicator_id bigint NULL,
out_of_state_flag varchar(1) NULL,
orphaned_adjustment_flag varchar(1) NULL,
denied_claim_flag varchar(1) NULL,
etl_batch_id int NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'apcd_dental_claim');

CREATE EXTERNAL TABLE [claims].[stage_apcd_eligibility]
(eligibility_id bigint NULL,
submitter_id bigint NULL,
internal_member_id bigint NULL,
coverage_class varchar(10) NULL,
eligibility_start_dt date NULL,
eligibility_end_dt date NULL,
product_code_id bigint NULL,
primary_insurance_indicator_id bigint NULL,
subscriber_relationship_id bigint NULL,
gender_code varchar(10) NULL,
coverage_level_id bigint NULL,
coverage_type_id bigint NULL,
market_category_id bigint NULL,
entitlement_code_id bigint NULL,
entitlement_code_id_orig bigint NULL,
medicare_status_id bigint NULL,
dual_eligibility_code_id bigint NULL,
aid_category_id bigint NULL,
managed_care_flag varchar(10) NULL,
purchased_through_exchange varchar(10) NULL,
exchange_market_type varchar(10) NULL,
exchange_metallic_tier_id bigint NULL,
city varchar(100) NULL,
state varchar(2) NULL,
zip varchar(15) NULL,
medicaid_ffs_flag varchar(2) NULL,
race_id1 bigint NULL,
race_id2 bigint NULL,
ethnicity_id1 bigint NULL,
ethnicity_id2 bigint NULL,
hispanic_id bigint NULL,
behavioral_health_ind_code varchar(10) NULL,
out_of_state_flag varchar(1) NULL,
dup_flag_pbm_tpa varchar(1) NULL,
dup_flag_managed_care varchar(1) NULL,
dup_flag_part_d varchar(1) NULL,
etl_batch_id int NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'apcd_eligibility');

CREATE EXTERNAL TABLE [claims].[stage_apcd_medical_claim_header]
(medical_claim_header_id bigint NULL,
submitter_id bigint NULL,
internal_member_id bigint NULL,
billing_internal_provider_id bigint NULL,
product_code_id bigint NULL,
first_service_dt date NULL,
last_service_dt date NULL,
first_paid_dt date NULL,
last_paid_dt date NULL,
charge_amt numeric(38,2) NULL,
diagnosis_code varchar(10) NULL,
icd_version_ind varchar(20) NULL,
header_status varchar(20) NULL,
denied_header_flag varchar(1) NULL,
orphaned_header_flag varchar(1) NULL,
claim_type_id bigint NULL,
type_of_setting_id bigint NULL,
place_of_setting_id bigint NULL,
type_of_bill_code varchar(4) NULL,
emergency_room_flag varchar(1) NULL,
operating_room_flag varchar(1) NULL,
claim_header_id bigint NULL,
cardiac_imaging_and_tests_flag int NULL,
chiropractic_flag int NULL,
consultations_flag int NULL,
covid19_flag int NULL,
dialysis_flag int NULL,
durable_medical_equip_flag int NULL,
echography_flag int NULL,
endoscopic_procedure_flag int NULL,
evaluation_and_management_flag int NULL,
health_home_utilization_flag int NULL,
hospice_utilization_flag int NULL,
imaging_advanced_flag int NULL,
imaging_standard_flag int NULL,
inpatient_acute_flag int NULL,
inpatient_nonacute_flag int NULL,
lab_and_pathology_flag int NULL,
oncology_and_chemotherapy_flag int NULL,
physical_therapy_rehab_flag int NULL,
preventive_screenings_flag int NULL,
preventive_vaccinations_flag int NULL,
preventive_visits_flag int NULL,
psychiatric_visits_flag int NULL,
surgery_and_anesthesia_flag int NULL,
telehealth_flag int NULL,
etl_batch_id int NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'apcd_medical_claim_header');

CREATE EXTERNAL TABLE [claims].[stage_apcd_member_month_detail]
(internal_member_id bigint NULL,
year_month varchar(6) NULL,
medical_product_code_id bigint NULL,
medical_submitter_id bigint NULL,
medical_eligibility_id bigint NULL,
med_commercial_eligibility_id bigint NULL,
med_medicare_eligibility_id bigint NULL,
med_medicaid_eligibility_id bigint NULL,
pharmacy_product_code_id bigint NULL,
pharmacy_submitter_id bigint NULL,
pharmacy_eligibility_id bigint NULL,
rx_commercial_eligibility_id bigint NULL,
rx_medicare_eligibility_id bigint NULL,
rx_medicaid_eligibility_id bigint NULL,
dental_product_code_id bigint NULL,
dental_submitter_id bigint NULL,
dental_eligibility_id bigint NULL,
dental_commercial_eligibility_id bigint NULL,
dental_medicare_eligibility_id bigint NULL,
dental_medicaid_eligibility_id bigint NULL,
age int NULL,
age_in_months int NULL,
gender_code varchar(15) NULL,
zip_code varchar(15) NULL,
etl_batch_id int NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'apcd_member_month_detail');

CREATE EXTERNAL TABLE [claims].[stage_apcd_pharmacy_claim]
(pharmacy_claim_service_line_id bigint NULL,
submitter_id bigint NULL,
internal_member_id bigint NULL,
submitter_clm_control_num varchar(272) NULL,
product_code_id bigint NULL,
subscriber_relationship_id bigint NULL,
line_counter int NULL,
prescription_filled_dt date NULL,
first_paid_dt date NULL,
last_paid_dt date NULL,
national_drug_code varchar(80) NULL,
drug_name varchar(80) NULL,
claim_status_id bigint NULL,
quantity numeric(38,2) NULL,
days_supply int NULL,
thirty_day_equivalent numeric(38,0) NULL,
charge_amt numeric(38,2) NULL,
refill_number varchar(10) NULL,
generic_drug_ind_id bigint NULL,
compound_drug_code_id bigint NULL,
dispense_as_written_id bigint NULL,
pharmacy_mail_order_code varchar(10) NULL,
pharmacy_provider_id bigint NULL,
pharmacy_internal_provider_id bigint NULL,
prscrbing_provider_id bigint NULL,
prscrbing_internal_provider_id bigint NULL,
network_indicator_id bigint NULL,
out_of_state_flag varchar(1) NULL,
orphaned_adjustment_flag varchar(1) NULL,
denied_claim_flag varchar(1) NULL,
dup_flag_pbm_tpa varchar(1) NULL,
dup_flag_managed_care varchar(1) NULL,
dup_flag_part_d varchar(2) NULL,
medicaid_ffs_flag varchar(2) NULL,
injury_dt date NULL,
benefits_exhausted_dt date NULL,
etl_batch_id int NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'apcd_pharmacy_claim');

CREATE EXTERNAL TABLE [claims].[stage_apcd_provider]
(provider_id bigint NULL,
extract_id bigint NULL,
submitter_id bigint NULL,
internal_provider_id bigint NULL,
organization_name varchar(100) NULL,
last_name varchar(100) NULL,
first_name varchar(60) NULL,
middle_name varchar(10) NULL,
generation_suffix varchar(10) NULL,
entity_type varchar(20) NULL,
professional_credential_code varchar(20) NULL,
orig_npi bigint NULL,
primary_specialty_id bigint NULL,
primary_specialty_code varchar(50) NULL,
city varchar(30) NULL,
state varchar(15) NULL,
zip varchar(15) NULL,
etl_batch_id int NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'apcd_provider');

CREATE EXTERNAL TABLE [claims].[stage_apcd_provider_master]
(internal_provider_id bigint NULL,
extract_id bigint NULL,
entity_type varchar(100) NULL,
organization_name_legal varchar(100) NULL,
last_name_legal varchar(100) NULL,
first_name_legal varchar(60) NULL,
middle_name_legal varchar(2) NULL,
organization_name_other varchar(100) NULL,
organization_name_other_type varchar(50) NULL,
last_name_other varchar(100) NULL,
first_name_other varchar(100) NULL,
middle_name_other varchar(100) NULL,
generation_suffix varchar(20) NULL,
professional_credential_code varchar(20) NULL,
npi bigint NULL,
primary_taxonomy varchar(20) NULL,
secondary_taxonomy_physical varchar(30) NULL,
city_physical varchar(30) NULL,
state_physical varchar(15) NULL,
zip_physical varchar(100) NULL,
county_physical varchar(100) NULL,
country_physical varchar(100) NULL,
ach_region_physical varchar(100) NULL,
etl_batch_id int NULL)
WITH (DATA_SOURCE = [datascr_WS_EDW], SCHEMA_NAME = N'stg_claims', OBJECT_NAME = N'apcd_provider_master');