------------------------------------
--Eli Kern, PHSKC-HSci-APDE, 11/2025
--Prep Clinical Data Repository (CDR) extract for UW Fresh Study team
--Include select ProviderOne (P1) tables
--Subset to KC residents during measurement window (6/1/17 - 12/31/23)
--Indirect identifiers to include dates of service, single-year age, census tract and ZIP code of residence
--No direct identifiers shall be included in any data tables
------------------------------------

------------------------------------
--STEP 1: Create reference table for subsetting CDR patients to KC residents during measurement window 6/1/17-12/31/23
------------------------------------

--Use MPM_Person table to create time-varying flag for ZIP-based KC residence
--Then subset to people with KC residence between 201706 and 202312 and add in patient_id from MPM_IndexPatient table
--442,045 distinct P1 IDs, 442,040 distinct CDR patient IDs
if object_id(N'stg_cdr.uwf_kc_subset', N'U') is not null drop table stg_cdr.uwf_kc_subset;
with temp1 as (
	select provideroneid,
	cast(insurance_start_date as date) as insurance_start_date,
	zip as cdr_zip
	from stg_cdr.MPM_Person
),
temp2 as (
	select a.*, b.geo_kc
	from temp1 as a
	left join stg_claims.ref_geo_kc_zip as b
	on a.cdr_zip = b.geo_zip
)
select distinct a.provideroneid, b.patientid, a.geo_kc
into stg_cdr.uwf_kc_subset
from temp2 as a
left join stg_cdr.MPM_IndexPatient as b
on a.provideroneid = b.provideroneid
where geo_kc = 1 and a.insurance_start_date between '2017-06-01' and '2023-12-31';


------------------------------------
--STEP 2: Prep CDR tables, subsetting to KC residents, collapsing to distinct rows, and removing direct identifiers as needed
------------------------------------

--Prep MPM_IndexPatient table
if object_id(N'stg_cdr.export_uwf_mpm_indexpatient', N'U') is not null drop table stg_cdr.export_uwf_mpm_indexpatient;
select distinct
a.patientid,
a.gender,
a.last_modified,
a.city,
a.zip,
a.[state],
case
	when (datediff(day, a.birthdate, '2023-12-31') + 1) >= 0 then floor((datediff(day, a.birthdate, '2023-12-31') + 1) / 365.25)
	when datediff(day, a.birthdate, '2023-12-31') < 0 then null
end as age_20231231,
a.primary_language_code,
cast(a.insurance_start_date as date) as insurance_start_date,
insurance_updated,
mco_name,
race_ethnicity_code,
getdate() as apde_last_run
into stg_cdr.export_uwf_mpm_indexpatient
from stg_cdr.MPM_IndexPatient as a
inner join (select distinct patientid from stg_cdr.uwf_kc_subset) as b
on a.patientid = b.patientid;

--Prep MPM_Person table
if object_id(N'stg_cdr.export_uwf_mpm_person', N'U') is not null drop table stg_cdr.export_uwf_mpm_person;
select distinct
b.patientid,
a.gender,
a.date_updated_by_ohp,
a.city,
a.zip,
a.[state],
case
	when (datediff(day, a.birthdate, '2023-12-31') + 1) >= 0 then floor((datediff(day, a.birthdate, '2023-12-31') + 1) / 365.25)
	when datediff(day, a.birthdate, '2023-12-31') < 0 then null
end as age_20231231,
a.primary_language_code,
cast(a.insurance_start_date as date) as insurance_start_date,
insurance_updated,
mco_name,
race_ethnicity_code,
getdate() as apde_last_run
into stg_cdr.export_uwf_mpm_person
from stg_cdr.MPM_Person as a
inner join stg_cdr.uwf_kc_subset as b
on a.provideroneid = b.provideroneid;

--Prep REF_RaceEthnicityCode table
if object_id(N'stg_cdr.export_uwf_ref_raceeth_code', N'U') is not null drop table stg_cdr.export_uwf_ref_raceeth_code;
select distinct
code,
hca_description,
getdate() as apde_last_run
into stg_cdr.export_uwf_ref_raceeth_code
from stg_cdr.REF_RaceEthnicityCode;

--Prep CCD_Header table
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
getdate() as apde_last_run
into stg_cdr.export_uwf_ccd_header
from stg_cdr.CCD_Header as a
inner join (select distinct patientid from stg_cdr.uwf_kc_subset) as b
on a.patient_id = b.patientid;

--Prep CHR_Allergies table
if object_id(N'stg_cdr.export_uwf_chr_allergy', N'U') is not null drop table stg_cdr.export_uwf_chr_allergy;
select distinct
a.patient_id as patientid,
a.service_date,
a.available_or_deprecated_flag,
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
from stg_cdr.CHR_Allergies as a
inner join (select distinct patientid from stg_cdr.uwf_kc_subset) as b
on a.patient_id = b.patientid;

--Prep CHR_Labs table -- first create table shell with clustered index on patient_id, allowing nvarchar(max) for test_result

if object_id(N'stg_cdr.export_uwf_chr_lab', N'U') is not null drop table stg_cdr.export_uwf_chr_lab;
create table stg_cdr.export_uwf_chr_lab
(
  patientid int not null,
  service_date datetime null,
  ccd_available_deprecated_flag char(1) null,
  test_completion_status varchar(255) null,
  test_code varchar(255) null,
  test_code_system_oid varchar(255) null,
  test_code_description varchar(255) null,
  test_description_from_facility varchar(255) null,
  test_result nvarchar(max) null,
  measurement_units varchar(255) null,
  reference_range varchar(255) null,
  result_format_text varchar(255) null,
  test_result_system_oid varchar(255) null,
  cwe_code varchar(255) null,
  cwe_system_oid varchar(255) null,
  cwe_question varchar(255) null,
  cwe_answer_score varchar(255) null,
  apde_last_run datetime null
)
with (clustered index (patientid));

--load data to table shell
insert into stg_cdr.export_uwf_chr_lab
select distinct
a.patient_id as patientid,
a.service_date,
a.ccd_available_deprecated_flag,
a.test_completion_status,
a.test_code,
a.test_code_system_oid,
a.test_code_description,
a.test_description_from_facility,
a.test_result,
a.measurement_units,
a.reference_range,
a.result_format_text,
a.test_result_system_oid,
a.cwe_code,
a.cwe_system_oid,
a.cwe_question,
a.cwe_answer_score,
getdate() as apde_last_run
from stg_cdr.CHR_Labs_ek_test as a
inner join (select distinct patientid from stg_cdr.uwf_kc_subset) as b
on a.patient_id = b.patientid;


--NEXT: meds
select top 10 * from stg_cdr.CHR_Labs_ek_test;
select top 10 * from stg_cdr.export_uwf_chr_lab;