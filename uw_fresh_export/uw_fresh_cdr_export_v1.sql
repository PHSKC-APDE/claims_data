/*------------------------------------
--Eli Kern, PHSKC-HSci-APDE, 11/2025
--Prep Clinical Data Repository (CDR) extract for UW Fresh Study team
--Include select ProviderOne (P1) tables
--Subset to KC residents during measurement window (6/1/17 - 12/31/25)
--Indirect identifiers to include dates of service, single-year age, census tract and ZIP code of residence
--No direct identifiers shall be included in any data tables
--Updated April 2026:
	--Prep March 2026 CDR extract for sharing (date period extended through 12/31/25)
	--Add chr_patients table
	--Revise subsetting method to include patients in chr_patients table missing from mpm_indexpatient table
------------------------------------*/

------------------------------------
--STEP 1: Create reference table for subsetting CDR patients to KC residents during measurement window 6/1/17-12/31/25
------------------------------------

--Use MPM_Person table to create time-varying flag for ZIP-based KC residence
--Then subset to people with KC residence between 201706 and 202512 and add in patient_id from MPM_IndexPatient table
--Then add in people from CHR_Patients table who are missing from MPM table
--892,507 distinct CDR patient IDs

if object_id(N'stg_cdr.uwf_kc_subset', N'U') is not null drop table stg_cdr.uwf_kc_subset;
--pull ZIP code and insurance start dates from time-varying CDR data table
with mpm_1 as (
	select provideroneid,
	cast(insurance_start_date as date) as insurance_start_date,
	left(zip, 5) as cdr_zip
	from stg_cdr.raw_MPM_Person_20260319
),
--create ZIP-based KC residence flag
mpm_2 as (
	select a.*, b.geo_kc
	from mpm_1 as a
	left join stg_claims.ref_geo_kc_zip as b
	on a.cdr_zip = b.geo_zip
),
--create person-level flags to identify study cohort
mpm_3 as (
	select provideroneid,
	max(case when insurance_start_date < '2017-06-01' and geo_kc = 1 then 1 else 0 end) as geo_kc_pre_period,
	max(case when insurance_start_date > '2017-06-01' and geo_kc is null then 1 else 0 end) as geo_non_kc_post_period_start,
	max(case when insurance_start_date between '2017-06-01' and '2025-12-31' and geo_kc = 1 then 1 else 0 end) as geo_kc_study_period
	from mpm_2
	group by provideroneid
),
--add in CDR patientid and subset to study cohort
mpm_final as (
	select a.provideroneid, c.patientid, max(a.geo_kc) as geo_kc
	from mpm_2 as a
	left join mpm_3 as b
	on a.provideroneid = b.provideroneid
	left join stg_cdr.raw_MPM_IndexPatient_20260319 as c
	on a.provideroneid = c.provideroneid
	where (b.geo_kc_study_period = 1 OR (b.geo_kc_pre_period = 1 and b.geo_non_kc_post_period_start = 1))
		and (c.patientid is not null) -- removes a small number of people (~5) who are in MPM_Person but not MPM_IndexPatient table
	group by a.provideroneid, c.patientid
),
--flag people from CHR_Patients table who are missing from MPM_IndexPatient table
chr_1 as (
	select distinct patient_id from stg_cdr.raw_CHR_Patients_20260319
	except select distinct patientid as patient_id from stg_cdr.raw_MPM_IndexPatient_20260319
),
--subset CHR_Patients table those living in KC ZIP codes with last updated dates during or after the study period
chr_2 as (
	select a.patient_id
	from stg_cdr.raw_CHR_Patients_20260319 as a
	inner join stg_claims.ref_geo_kc_zip as b
	on left(a.zip, 5) = b.geo_zip and a.record_change_date >= '2017-06-01'
),
--combine two chr temp tables
	chr_final as (
	select null as provideroneid, a.patient_id, 1 as geo_kc
	from chr_1 as a
	inner join chr_2 as b
	on a.patient_id = b.patient_id
)
--combine data from mpm and chr tables to create final person reference table (people missing from MPM tables will have null P1 ID)
select *
into stg_cdr.uwf_kc_subset
from mpm_final
union select * from chr_final;

select count(distinct patientid) from stg_cdr.uwf_kc_subset;


------------------------------------
--STEP 2: Prep CDR tables, subsetting to KC residents, collapsing to distinct rows, and removing direct identifiers as needed
------------------------------------

--Prep MPM_IndexPatient table
--Exclude direct identifiers
--Convert dob to age as of 12/31/25
if object_id(N'stg_cdr.export_uwf_mpm_indexpatient', N'U') is not null drop table stg_cdr.export_uwf_mpm_indexpatient;
select distinct
a.patientid,
a.gender,
a.last_modified,
a.city,
a.zip,
a.[state],
case
	when (datediff(day, a.birthdate, '2025-12-31') + 1) >= 0 then floor((datediff(day, a.birthdate, '2025-12-31') + 1) / 365.25)
	when datediff(day, a.birthdate, '2025-12-31') < 0 then null
end as age_20251231,
a.primary_language_code,
cast(a.insurance_start_date as date) as insurance_start_date,
insurance_updated,
mco_name,
race_ethnicity_code,
getdate() as apde_last_run
into stg_cdr.export_uwf_mpm_indexpatient
from stg_cdr.raw_MPM_IndexPatient_20260319 as a
inner join (select distinct patientid from stg_cdr.uwf_kc_subset) as b
on a.patientid = b.patientid;

--Prep MPM_Person table
--Exclude direct identifiers
--Convert dob to age as of 12/31/25
if object_id(N'stg_cdr.export_uwf_mpm_person', N'U') is not null drop table stg_cdr.export_uwf_mpm_person;
select distinct
b.patientid,
a.gender,
a.date_updated_by_ohp,
a.city,
a.zip,
a.[state],
case
	when (datediff(day, a.birthdate, '2025-12-31') + 1) >= 0 then floor((datediff(day, a.birthdate, '2025-12-31') + 1) / 365.25)
	when datediff(day, a.birthdate, '2025-12-31') < 0 then null
end as age_20251231,
a.primary_language_code,
cast(a.insurance_start_date as date) as insurance_start_date,
insurance_updated,
mco_name,
race_ethnicity_code,
getdate() as apde_last_run
into stg_cdr.export_uwf_mpm_person
from stg_cdr.raw_MPM_Person_20260319 as a
inner join stg_cdr.uwf_kc_subset as b
on a.provideroneid = b.provideroneid;

--Prep CHR_Patients table
--Exclude direct identifiers
--Convert dob to age as of 12/31/25
if object_id(N'stg_cdr.export_uwf_chr_patients', N'U') is not null drop table stg_cdr.export_uwf_chr_patients;
select distinct
a.patient_id,
a.city,
a.zip,
a.[state],
case
	when (datediff(day, a.date_of_birth, '2025-12-31') + 1) >= 0 then floor((datediff(day, a.date_of_birth, '2025-12-31') + 1) / 365.25)
	when datediff(day, a.date_of_birth, '2025-12-31') < 0 then null
end as age_20251231,
a.sex,
a.active,
a.primary_language,
a.race_code,
a.record_create_date,
a.record_change_date,
getdate() as apde_last_run
into stg_cdr.export_uwf_chr_patients
from stg_cdr.raw_CHR_Patients_20260319 as a
inner join (select distinct patientid from stg_cdr.uwf_kc_subset) as b
on a.patient_id = b.patientid;


--Prep REF_RaceEthnicityCode table
--No exclusions or modifications necessary
if object_id(N'stg_cdr.export_uwf_ref_raceeth_code', N'U') is not null drop table stg_cdr.export_uwf_ref_raceeth_code;
select distinct
code,
hca_description,
getdate() as apde_last_run
into stg_cdr.export_uwf_ref_raceeth_code
from stg_cdr.REF_RaceEthnicityCode;

--Prep CCD_Header table
--No exclusions or modifications necessary
if object_id(N'stg_cdr.export_uwf_ccd_header', N'U') is not null drop table stg_cdr.export_uwf_ccd_header;
select distinct
a.patient_id as patientid,
a.organization_abbreviation,
a.organization_code,
a.organization_name,
a.ccd_id,
a.document_type_code,
a.document_type_code_oid,
a.document_type_description,
a.date_document_received_at_ohp,
a.available_or_deprecated_flag,
a.document_timestamp,
a.discriminator,
getdate() as apde_last_run
into stg_cdr.export_uwf_ccd_header
from stg_cdr.raw_CCD_Header_20260319 as a
inner join (select distinct patientid from stg_cdr.uwf_kc_subset) as b
on a.patient_id = b.patientid;

--Prep CHR_Allergies table
--No exclusions or modifications necessary
if object_id(N'stg_cdr.export_uwf_chr_allergy', N'U') is not null drop table stg_cdr.export_uwf_chr_allergy;
select distinct
a.patient_id as patientid,
a.service_date,
a.ccd_id,
a.alert_category_code_description,
a.alert_category_code,
a.alert_category_code_system_oid,
a.alert_category_description_from_ccd,
a.alert_status_description,
a.agent_code_description,
a.agent_code,
a.agent_code_system_oid,
a.agent_description_from_ccd,
a.reaction_code_description,
a.reaction_code,
a.reaction_code_system_oid,
a.reaction_description_from_ccd,
a.reaction_severity_code_description,
a.reaction_severity_code,
a.reaction_severity_code_system_oid,
a.reaction_severity_description_from_ccd,
a.alert_severity_code_description,
a.alert_severity_code,
a.alert_severity_code_system_oid,
a.alert_severity_description_from_ccd,
getdate() as apde_last_run
into stg_cdr.export_uwf_chr_allergy
from stg_cdr.raw_CHR_Allergies_20260319 as a
inner join (select distinct patientid from stg_cdr.uwf_kc_subset) as b
on a.patient_id = b.patientid;

--Prep CHR_Labs table
--Parse test_result column to set text values to null to avoid sharing direct identifiers
--Confirmed that all distinct values of cwe_answer_score do not contain sensitive information
--Confirmed that numeric test_result values do not contain alpha characters (with exception of small # of rows containing an exponent)
if object_id(N'stg_cdr.export_uwf_chr_lab', N'U') is not null drop table stg_cdr.export_uwf_chr_lab;
select distinct
a.patient_id as patientid,
a.service_date,
a.ccd_id,
a.test_completion_status,
a.test_code,
a.test_code_system_oid,
a.test_code_description,
a.test_description_from_facility,
case when a.result_format_text_numerical_other = 'TX' then null else cast(a.test_result as varchar(255)) end as test_result,
a.measurement_units,
a.reference_range,
a.result_format_text_numerical_other,
a.test_result_system_oid,
a.cwe_code,
a.cwe_system_oid,
a.cwe_question,
a.cwe_answer_score,
getdate() as apde_last_run
into stg_cdr.export_uwf_chr_lab
from stg_cdr.raw_CHR_Labs_20260319 as a
inner join (select distinct patientid from stg_cdr.uwf_kc_subset) as b
on a.patient_id = b.patientid;

--Prep CHR_Meds table (42 min on 4/21/26)
--Exclude patient_instructions column to avoid potential sharing of sensitive information
if object_id(N'stg_cdr.export_uwf_chr_med', N'U') is not null drop table stg_cdr.export_uwf_chr_med;
select distinct
a.patient_id as patientid,
a.ccd_section,
a.service_date,
a.ccd_id,
a.drug_code_system_description,
a.drug_code,
a.drug_code_system_oid,
a.drug_code_description,
a.drug_description_from_ccd,
a.drug_lot_number,
a.manufacturer_name,
a.product_name,
a.route_code,
a.route_code_system_oid,
a.route_code_description,
a.route_text_from_ccd,
a.complex_dose_indicator,
a.dose_unit_high,
a.dose_amount_high,
a.dose_unit_low,
a.dose_amount_low,
a.dosage_measurement_unit,
a.dose_unit_quantity,
--a.patient_instructions, -- excluded to avoid potential sharing of sensitive information
a.planned_administration_flag,
a.rx_status,
a.rx_status_code_system,
a.supply_instructions,
a.supply_number_of_fills,
a.supply_planned_flag,
a.supply_product_code,
a.supply_product_code_system,
a.supply_code_description,
a.supply_text_from_ccd,
a.supply_manufacturer_name,
a.supply_product_name,
a.supply_quantity_unit,
a.supply_quantity_amount,
a.supplysequencenumber,
a.medflag,
a.immunization_refusal_reason_code,
a.immunization_refusal_reason_code_system,
a.immunization_refusal_reason_text,
a.injection_location_code,
a.injection_location_code_system,
a.injection_location_code_description,
a.injection_location_text_from_ccd,
a.[dose_frequency_unit_&_value] as dose_frequency_unit_and_value,
getdate() as apde_last_run
into stg_cdr.export_uwf_chr_med
from stg_cdr.raw_CHR_MedicationAndImmunizations_20260319 as a
inner join (select distinct patientid from stg_cdr.uwf_kc_subset) as b
on a.patient_id = b.patientid;

--Prep CHR_Problems table
--No exclusions or modifications necessary
if object_id(N'stg_cdr.export_uwf_chr_problem', N'U') is not null drop table stg_cdr.export_uwf_chr_problem;
select distinct
a.patient_id as patientid,
a.service_date,
a.ccd_id,
a.diagnosis_code,
a.oid_of_dx_coding_system,
a.diagnosis_code_description,
a.diagnosis_text_decription_from_the_ccd,
a.diagnosis_status,
getdate() as apde_last_run
into stg_cdr.export_uwf_chr_problem
from stg_cdr.raw_CHR_Problems_20260319 as a
inner join (select distinct patientid from stg_cdr.uwf_kc_subset) as b
on a.patient_id = b.patientid;

--Prep CHR_Procedures table
--No exclusions or modifications necessary
if object_id(N'stg_cdr.export_uwf_chr_procedure', N'U') is not null drop table stg_cdr.export_uwf_chr_procedure;
select distinct
a.patient_id as patientid,
a.service_date,
a.ccd_id,
a.procedure_status,
a.procedure_code,
a.oid_of_procedure_code_system,
a.procedure_code_description,
a.procedure_description_from_ccd,
a.procedure_description,
getdate() as apde_last_run
into stg_cdr.export_uwf_chr_procedure
from stg_cdr.raw_CHR_Procedures_20260319 as a
inner join (select distinct patientid from stg_cdr.uwf_kc_subset) as b
on a.patient_id = b.patientid;

--Prep CHR_Vitals table
--No exclusions or modifications necessary
if object_id(N'stg_cdr.export_uwf_chr_vital', N'U') is not null drop table stg_cdr.export_uwf_chr_vital;
select distinct
a.patient_id as patientid,
a.service_date,
a.ccd_id,
a.test_code,
a.oid_of_coding_system,
a.test_code_description,
a.test_decription_from_the_ccd,
a.blood_pressure_diastolic_code,
a.blood_pressure_diastolic_value,
a.blood_pressure_systolic_code,
a.blood_pressure_systolic_value,
a.vital_sign_result,
a.vital_sign_value,
a.vital_sign_unit,
getdate() as apde_last_run
into stg_cdr.export_uwf_chr_vital
from stg_cdr.raw_CHR_VitalSigns_20260319 as a
inner join (select distinct patientid from stg_cdr.uwf_kc_subset) as b
on a.patient_id = b.patientid;


------------------------------------
--STEP 3: Prep Medicaid/P1 tables
------------------------------------

--Create cross-walk for id_mcaid to p1_id, subset to people in UW Fresh Study subset
if object_id('tempdb..#mcaid_ids') is not null drop table #mcaid_ids;
select distinct
a.medicaid_recipient_id as id_p1,
a.mbr_h_sid as id_mcaid,
b.patientid
into #mcaid_ids
from stg_claims.stage_mcaid_elig as a
inner join stg_cdr.uwf_kc_subset as b
on a.medicaid_recipient_id = b.provideroneid
where b.patientid is not null;

--Prep mcaid_claim_ccw table
if object_id(N'stg_cdr.export_uwf_mcaid_claim_ccw', N'U') is not null drop table stg_cdr.export_uwf_mcaid_claim_ccw;
select distinct
b.patientid,
a.first_encounter_date,
a.last_encounter_date,
a.ccw_code,
a.ccw_desc,
a.last_run as apde_last_run
into stg_cdr.export_uwf_mcaid_claim_ccw
from stg_claims.stage_mcaid_claim_ccw as a
inner join #mcaid_ids as b
on a.id_mcaid = b.id_mcaid;

--Prep mcaid_claim_bh table
if object_id(N'stg_cdr.export_uwf_mcaid_claim_bh', N'U') is not null drop table stg_cdr.export_uwf_mcaid_claim_bh;
select distinct
b.patientid,
a.first_encounter_date,
a.last_encounter_date,
a.bh_cond,
a.last_run as apde_last_run
into stg_cdr.export_uwf_mcaid_claim_bh
from stg_claims.stage_mcaid_claim_bh as a
inner join #mcaid_ids as b
on a.id_mcaid = b.id_mcaid;