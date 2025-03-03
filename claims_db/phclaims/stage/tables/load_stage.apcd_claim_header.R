#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_HEADER
# Eli Kern, PHSKC (APDE)
#
# 2019-10
#
# 2024-04-29 update: Modified for HHSAW migration
# 2024-05-30 update: Modified to optimize query for efficient SQL pool workload in Synapse - insert into heap tables with label for all intermediate tables
# 2025-02-25 update: Adding CCS, BH and injury columns

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/07_apcd_create_analytic_tables.R

#### Load script ####
load_stage.apcd_claim_header_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "
------------------
--STEP 1: Do all line-level transformations that don't require ICD-CM, procedure, or provider information
--Exclude all denied and orphaned claim headers
--Acute inpatient stay defined through Susan Hernandez's work and dialogue with OnPoint
--Max of discharge dt grouped by claim header will take latest discharge date when >1 discharge dt
-------------------

--create table shell
if object_id(N'stg_claims.tmp_apcd_claim_header_temp1',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_temp1;
create table stg_claims.tmp_apcd_claim_header_temp1 (
id_apcd bigint,
claim_header_id bigint,
product_code_id bigint,
first_service_date date,
last_service_date date,
first_paid_date date,
last_paid_date date,
charge_amt numeric(38,2),
claim_status_id bigint,
type_of_bill_code varchar(4),
cardiac_imaging_and_tests_flag tinyint,
chiropractic_flag tinyint,
consultations_flag tinyint,
covid19_flag tinyint,
dialysis_flag tinyint,
durable_medical_equip_flag tinyint,
echography_flag tinyint,
endoscopic_procedure_flag tinyint,
evaluation_and_management_flag tinyint,
health_home_utilization_flag tinyint,
hospice_utilization_flag tinyint,
imaging_advanced_flag tinyint,
imaging_standard_flag tinyint,
inpatient_acute_flag tinyint,
inpatient_nonacute_flag tinyint,
lab_and_pathology_flag tinyint,
oncology_and_chemotherapy_flag tinyint,
physical_therapy_rehab_flag tinyint,
preventive_screenings_flag tinyint,
preventive_vaccinations_flag tinyint,
preventive_visits_flag tinyint,
psychiatric_visits_flag tinyint,
surgery_and_anesthesia_flag tinyint,
telehealth_flag tinyint,
claim_type_apcd_id varchar(100),
ed_perform_temp tinyint,
ed_pos_temp tinyint,
ed_revenue_code_temp tinyint,
ipt_flag tinyint,
discharge_date date
)
with (heap);

--insert data
insert into stg_claims.tmp_apcd_claim_header_temp1
select a.internal_member_id as id_apcd, 
a.medical_claim_header_id as claim_header_id,
case when a.product_code_id in (-1,-2) then null else a.product_code_id end as product_code_id,
a.first_service_dt as first_service_date,
a.last_service_dt as last_service_date,
a.first_paid_dt as first_paid_date,
a.last_paid_dt as last_paid_date,
a.charge_amt,
c.claim_status_id,
case when a.type_of_bill_code in (-1,-2) then null else a.type_of_bill_code end as type_of_bill_code,
    
--service type flags from OnPoint
a.cardiac_imaging_and_tests_flag,
a.chiropractic_flag,
a.consultations_flag,
a.covid19_flag,
a.dialysis_flag,
a.durable_medical_equip_flag,
a.echography_flag,
a.endoscopic_procedure_flag,
a.evaluation_and_management_flag,
a.health_home_utilization_flag,
a.hospice_utilization_flag,
a.imaging_advanced_flag,
a.imaging_standard_flag,
a.inpatient_acute_flag,
a.inpatient_nonacute_flag,
a.lab_and_pathology_flag,
a.oncology_and_chemotherapy_flag,
a.physical_therapy_rehab_flag,
a.preventive_screenings_flag,
a.preventive_vaccinations_flag,
a.preventive_visits_flag,
a.psychiatric_visits_flag,
a.surgery_and_anesthesia_flag,
a.telehealth_flag,

--concatenate claim type variables
cast(convert(varchar(100), a.claim_type_id)
    + '.' + convert(varchar(100), a.type_of_setting_id)
    + '.' + convert(varchar(100), case when a.place_of_setting_id in (-1,-2) then null else a.place_of_setting_id end)
as varchar(100)) as claim_type_apcd_id,
    
--ED performance temp flags (RDA measure)
cast(case when a.emergency_room_flag = 'Y' then 1 else 0 end as tinyint) as ed_perform_temp,
    
--ED population health temp flags (Yale measure)
b.ed_pos_temp,
b.ed_revenue_code_temp,
    
--inpatient visit
case when a.claim_type_id = '1' and a.type_of_setting_id = '1' and a.place_of_setting_id = '1'
    and c.claim_status_id in (-1, -2, 1, 5, 2, 6) -- only include primary and secondary claims
    and b.discharge_date is not null
then 1 else 0 end as ipt_flag,
b.discharge_date
    
from stg_claims.apcd_medical_claim_header as a
    
--join to claim_line table to grab place of service and revenue code for ED visit, and discharge date for IPT
left join (
    select claim_header_id, max(discharge_date) as discharge_date,
    max(case when place_of_service_code = '23' then 1 else 0 end) as ed_pos_temp,
    max(case when revenue_code like '045[01269]' or revenue_code = '0981' then 1 else 0 end) as ed_revenue_code_temp
    from stg_claims.stage_apcd_claim_line
    group by claim_header_id
    ) as b
on a.medical_claim_header_id = b.claim_header_id
    
--left join to claim status reference table to use numeric codes rather than varchar header_status variable
left join stg_claims.ref_apcd_claim_status as c
on a.header_status = c.claim_status_code
    
--exclude denined/orphaned claims
where a.denied_header_flag = 'N' and a.orphaned_header_flag = 'N'
option (label = 'apcd_claim_header_temp1');
    
    
------------------
--STEP 2: Procedure code query for ED visits
--Subset to relevant claims as last step to minimize temp table size
-------------------

--create table shell
if object_id(N'stg_claims.tmp_apcd_claim_header_ed_procedure_code',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_ed_procedure_code;
create table stg_claims.tmp_apcd_claim_header_ed_procedure_code (
claim_header_id bigint,
ed_procedure_code_temp tinyint
)
with (heap);

--insert data
insert into stg_claims.tmp_apcd_claim_header_ed_procedure_code
select x.medical_claim_header_id, x.ed_procedure_code_temp
from (
    select a.medical_claim_header_id,
    max(case when b.procedure_code like '9928[12345]' or b.procedure_code = '99291' then 1 else 0 end) as ed_procedure_code_temp
    from stg_claims.apcd_medical_claim_header as a
    --procedure code table
    left join stg_claims.stage_apcd_claim_procedure as b
    on a.medical_claim_header_id = b.claim_header_id
    --exclude denined/orphaned claims
    where a.denied_header_flag = 'N' and a.orphaned_header_flag = 'N'
    --cluster to claim header
    group by a.medical_claim_header_id
) as x
where x.ed_procedure_code_temp = 1
option (label = 'apcd_claim_header_ed_procedure_code');
    
    
------------------
--STEP 3: Primary care visit query
-------------------

--create table shell
if object_id(N'stg_claims.tmp_apcd_claim_header_pc_visit',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_pc_visit;
create table stg_claims.tmp_apcd_claim_header_pc_visit (
claim_header_id bigint,
pc_procedure_temp tinyint,
pc_zcode_temp tinyint,
pc_taxonomy_temp tinyint
)
with (heap);

--insert data
insert into stg_claims.tmp_apcd_claim_header_pc_visit
select x.medical_claim_header_id, x.pc_procedure_temp, x.pc_taxonomy_temp, x.pc_zcode_temp
from (
select a.medical_claim_header_id,
--primary care visit temp flags
max(case when b.code is not null then 1 else 0 end) as pc_procedure_temp,
max(case when c.code is not null then 1 else 0 end) as pc_zcode_temp,
max(case when d.code is not null then 1 else 0 end) as pc_taxonomy_temp
from stg_claims.apcd_medical_claim_header as a
    
--procedure codes
left join (
    select b1.claim_header_id, b2.code
    --procedure code table
    from stg_claims.stage_apcd_claim_procedure as b1
    --primary care-relevant procedure codes
    inner join (select code from stg_claims.ref_pc_visit_oregon where code_system in ('cpt', 'hcpcs')) as b2
    on b1.procedure_code = b2.code
) as b
on a.medical_claim_header_id = b.claim_header_id
    
--ICD-CM codes
left join (
    select c1.claim_header_id, c2.code
    --ICD-CM table
    from stg_claims.stage_apcd_claim_icdcm_header as c1
    --primary care-relevant ICD-10-CM codes
    inner join (select code from stg_claims.ref_pc_visit_oregon where code_system = 'icd10cm') as c2
    on (c1.icdcm_norm = c2.code) and (c1.icdcm_version = 10)
) as c
on a.medical_claim_header_id = c.claim_header_id
    
--provider taxonomies
left join (
    select d1.claim_header_id, d4.code
    --rendering and attending providers
    from (select * from stg_claims.stage_apcd_claim_provider where provider_type in ('rendering', 'attending')) as d1
    --NPIs for each provider
    inner join stg_claims.ref_apcd_provider_npi as d2
    on d1.provider_id_apcd = d2.provider_id_apcd
    --taxonomy codes for rendering and attending providers
    inner join stg_claims.ref_kc_provider_master as d3
    on d2.npi = d3.npi
    --primary care-relevant provider taxonomy codes
    inner join (select code from stg_claims.ref_pc_visit_oregon where code_system = 'provider_taxonomy') as d4
    on (d3.primary_taxonomy = d4.code) or (d3.secondary_taxonomy = d4.code)
) as d
on a.medical_claim_header_id = d.claim_header_id
    
--exclude denined/orphaned claims
    where a.denied_header_flag = 'N' and a.orphaned_header_flag = 'N'
    
--cluster to claim header
group by a.medical_claim_header_id
) as x
where (x.pc_procedure_temp = 1 or x.pc_zcode_temp = 1) and x.pc_taxonomy_temp = 1
option (label = 'apcd_claim_header_pc_visit');
    
    
------------------
--STEP 4: Extract primary diagnosis, take first ordered ICD-CM code when >1 primary per header
------------------

--create table shell
if object_id(N'stg_claims.tmp_apcd_claim_header_icd1',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_icd1;
create table stg_claims.tmp_apcd_claim_header_icd1 (
claim_header_id bigint,
primary_diagnosis varchar(255),
icdcm_version tinyint
)
with (heap);

--insert data
insert into stg_claims.tmp_apcd_claim_header_icd1
select claim_header_id,
min(icdcm_norm) as primary_diagnosis,
min(icdcm_version) as icdcm_version
from stg_claims.stage_apcd_claim_icdcm_header
where icdcm_number = '01'
group by claim_header_id
option (label = 'apcd_claim_header_icd1');


------------------
--STEP 5:  Join intermediate tables to prep for next step
------------------

--create table shell
if object_id(N'stg_claims.tmp_apcd_claim_header_temp1b',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_temp1b;
create table stg_claims.tmp_apcd_claim_header_temp1b (
	claim_header_id bigint,
	primary_diagnosis varchar(255),
	icdcm_version tinyint,
	ed_procedure_code_temp tinyint,
	pc_procedure_temp tinyint,
	pc_taxonomy_temp tinyint,
	pc_zcode_temp tinyint
)
with (heap);

--insert into table shell
insert into stg_claims.tmp_apcd_claim_header_temp1b
select
	a.claim_header_id,
	b.primary_diagnosis, b.icdcm_version,
	c.ed_procedure_code_temp,
	d.pc_procedure_temp, d.pc_taxonomy_temp, d.pc_zcode_temp
from (select distinct claim_header_id from stg_claims.tmp_apcd_claim_header_temp1) as a
left join stg_claims.tmp_apcd_claim_header_icd1 as b
on a.claim_header_id = b.claim_header_id
left join stg_claims.tmp_apcd_claim_header_ed_procedure_code as c
on a.claim_header_id = c.claim_header_id
left join stg_claims.tmp_apcd_claim_header_pc_visit as d
on a.claim_header_id = d.claim_header_id
option (label = 'apcd_claim_header_temp1b');

if object_id(N'stg_claims.tmp_apcd_claim_header_icd1',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_icd1;
if object_id(N'stg_claims.tmp_apcd_claim_header_ed_procedure_code',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_ed_procedure_code;
if object_id(N'stg_claims.tmp_apcd_claim_header_pc_visit',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_pc_visit;


------------------
--STEP 6: Prepare header-level concepts using analytic claim tables
--Add in CCS columns for primary diagnosis
-------------------

--create table shell
if object_id(N'stg_claims.tmp_apcd_claim_header_temp2',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_temp2;
create table stg_claims.tmp_apcd_claim_header_temp2 (
id_apcd bigint,
claim_header_id bigint,
product_code_id bigint,
first_service_date date,
last_service_date date,
first_paid_date date,
last_paid_date date,
charge_amt numeric(38,2),
primary_diagnosis varchar(255),
icdcm_version tinyint,
claim_status_id bigint,
claim_type_apcd_id varchar(10),
claim_type_id int,
type_of_bill_code varchar(4),
ccs_superlevel_desc varchar(255),
ccs_broad_desc varchar(255),
ccs_broad_code varchar(255),
ccs_midlevel_desc varchar(255),
ccs_detail_desc varchar(255),
ccs_detail_code varchar(255),
cardiac_imaging_and_tests_flag tinyint,
chiropractic_flag tinyint,
consultations_flag tinyint,
covid19_flag tinyint,
dialysis_flag tinyint,
durable_medical_equip_flag tinyint,
echography_flag tinyint,
endoscopic_procedure_flag tinyint,
evaluation_and_management_flag tinyint,
health_home_utilization_flag tinyint,
hospice_utilization_flag tinyint,
imaging_advanced_flag tinyint,
imaging_standard_flag tinyint,
inpatient_acute_flag tinyint,
inpatient_nonacute_flag tinyint,
lab_and_pathology_flag tinyint,
oncology_and_chemotherapy_flag tinyint,
physical_therapy_rehab_flag tinyint,
preventive_screenings_flag tinyint,
preventive_vaccinations_flag tinyint,
preventive_visits_flag tinyint,
psychiatric_visits_flag tinyint,
surgery_and_anesthesia_flag tinyint,
telehealth_flag tinyint,
ed_perform tinyint,
ed_yale_carrier tinyint,
ed_yale_opt tinyint,
ed_yale_ipt tinyint,
inpatient tinyint,
discharge_date date,
pc_visit tinyint
)
with (heap);

--insert into table shell
insert into stg_claims.tmp_apcd_claim_header_temp2
select distinct a.id_apcd, 
a.claim_header_id,
a.product_code_id,
a.first_service_date,
a.last_service_date,
a.first_paid_date,
a.last_paid_date,
a.charge_amt,
c.primary_diagnosis,
c.icdcm_version,
a.claim_status_id,
a.claim_type_apcd_id,
b.kc_clm_type_id as claim_type_id,
a.type_of_bill_code,
d.ccs_superlevel_desc,
d.ccs_broad_desc,
d.ccs_broad_code,
d.ccs_midlevel_desc,
d.ccs_detail_desc,
d.ccs_detail_code,
a.cardiac_imaging_and_tests_flag,
a.chiropractic_flag,
a.consultations_flag,
a.covid19_flag,
a.dialysis_flag,
a.durable_medical_equip_flag,
a.echography_flag,
a.endoscopic_procedure_flag,
a.evaluation_and_management_flag,
a.health_home_utilization_flag,
a.hospice_utilization_flag,
a.imaging_advanced_flag,
a.imaging_standard_flag,
a.inpatient_acute_flag,
a.inpatient_nonacute_flag,
a.lab_and_pathology_flag,
a.oncology_and_chemotherapy_flag,
a.physical_therapy_rehab_flag,
a.preventive_screenings_flag,
a.preventive_vaccinations_flag,
a.preventive_visits_flag,
a.psychiatric_visits_flag,
a.surgery_and_anesthesia_flag,
a.telehealth_flag,
    
--ED performance (RDA measure)
case when a.ed_perform_temp = 1 and b.kc_clm_type_id = 4 then 1 else 0 end as ed_perform,
    
--ED population health (Yale measure)
case when b.kc_clm_type_id = 5 and 
    ((c.ed_procedure_code_temp = 1 and a.ed_pos_temp = 1) or a.ed_revenue_code_temp = 1)
    then 1 else 0 end as ed_yale_carrier,
case when b.kc_clm_type_id = 4 and 
    (a.ed_revenue_code_temp = 1 or a.ed_pos_temp = 1 or c.ed_procedure_code_temp = 1)
    then 1 else 0 end as ed_yale_opt,
case when b.kc_clm_type_id = 1 and
    (a.ed_revenue_code_temp = 1 or a.ed_pos_temp = 1 or c.ed_procedure_code_temp = 1)
    then 1 else 0 end as ed_yale_ipt,
    
--Inpatient visit
ipt_flag as inpatient,
discharge_date,
    
--Primary care visit (Oregon)
case when (c.pc_procedure_temp = 1 or c.pc_zcode_temp = 1) and c.pc_taxonomy_temp = 1
	and a.claim_type_apcd_id not in ('1.1.1', '1.1.14', '1.1.2', '2.3.8', '2.3.2', '1.2.8') --exclude inpatient, swing bed, free-standing ambulatory
    and a.claim_status_id in (-1, -2, 1, 5, 2, 6) -- only include primary and secondary claim headers
    then 1 else 0
end as pc_visit
    
from stg_claims.tmp_apcd_claim_header_temp1 as a
left join (select * from stg_claims.ref_kc_claim_type_crosswalk where source_desc = 'apcd') as b
on a.claim_type_apcd_id = b.source_clm_type_id
left join stg_claims.tmp_apcd_claim_header_temp1b as c
on a.claim_header_id = c.claim_header_id
left join stg_claims.ref_icdcm_codes as d
on (c.icdcm_version = d.icdcm_version) and (c.primary_diagnosis = d.icdcm)
option (label = 'apcd_claim_header_temp2');
    
--drop other temp tables to make space
if object_id(N'stg_claims.tmp_apcd_claim_header_temp1',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_temp1;
if object_id(N'stg_claims.tmp_apcd_claim_header_temp1b',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_temp1b;
    

------------------
--STEP 7: Assign unique ID to healthcare utilization concepts that are grouped by person, service date
-------------------

--create table shell
if object_id(N'stg_claims.tmp_apcd_claim_header_temp3',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_temp3;
create table stg_claims.tmp_apcd_claim_header_temp3 (
id_apcd bigint,
claim_header_id bigint,
product_code_id bigint,
first_service_date date,
last_service_date date,
first_paid_date date,
last_paid_date date,
charge_amt numeric(38,2),
primary_diagnosis varchar(255),
icdcm_version tinyint,
claim_status_id bigint,
claim_type_apcd_id varchar(10),
claim_type_id int,
type_of_bill_code varchar(4),
ccs_superlevel_desc varchar(255),
ccs_broad_desc varchar(255),
ccs_broad_code varchar(255),
ccs_midlevel_desc varchar(255),
ccs_detail_desc varchar(255),
ccs_detail_code varchar(255),
cardiac_imaging_and_tests_flag tinyint,
chiropractic_flag tinyint,
consultations_flag tinyint,
covid19_flag tinyint,
dialysis_flag tinyint,
durable_medical_equip_flag tinyint,
echography_flag tinyint,
endoscopic_procedure_flag tinyint,
evaluation_and_management_flag tinyint,
health_home_utilization_flag tinyint,
hospice_utilization_flag tinyint,
imaging_advanced_flag tinyint,
imaging_standard_flag tinyint,
inpatient_acute_flag tinyint,
inpatient_nonacute_flag tinyint,
lab_and_pathology_flag tinyint,
oncology_and_chemotherapy_flag tinyint,
physical_therapy_rehab_flag tinyint,
preventive_screenings_flag tinyint,
preventive_vaccinations_flag tinyint,
preventive_visits_flag tinyint,
psychiatric_visits_flag tinyint,
surgery_and_anesthesia_flag tinyint,
telehealth_flag tinyint,
ed_perform tinyint,
ed_yale_carrier tinyint,
ed_yale_opt tinyint,
ed_yale_ipt tinyint,
inpatient tinyint,
discharge_date date,
pc_visit tinyint,
pc_visit_id bigint,
inpatient_id bigint,
ed_perform_id bigint
)
with (heap);

--insert data
--primary care visits
with pc as (
	select claim_header_id,
	dense_rank() over (order by id_apcd, first_service_date) as pc_visit_id
	from stg_claims.tmp_apcd_claim_header_temp2
	where pc_visit = 1
),
--inpatient stays
inpatient as (
	select claim_header_id,
	dense_rank() over (order by id_apcd, first_service_date) as inpatient_id
	from stg_claims.tmp_apcd_claim_header_temp2
	where inpatient = 1
),
--ED performance (RDA measure)
ed_perform as (
	select claim_header_id,
	dense_rank() over (order by id_apcd, first_service_date) as ed_perform_id
	from stg_claims.tmp_apcd_claim_header_temp2
	where ed_perform = 1
)
insert into stg_claims.tmp_apcd_claim_header_temp3
select a.*,
b.pc_visit_id,
c.inpatient_id,
d.ed_perform_id
from stg_claims.tmp_apcd_claim_header_temp2 as a
left join pc as b
on a.claim_header_id = b.claim_header_id
left join inpatient as c
on a.claim_header_id = c.claim_header_id
left join ed_perform as d
on a.claim_header_id = d.claim_header_id
option (label = 'apcd_claim_header_temp3');
    
--drop other temp tables to make space
if object_id(N'stg_claims.tmp_apcd_claim_header_temp2',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_temp2;
    

------------------
--STEP 8: RDA behavioral health diagnosis flags
-------------------

--create table shell
if object_id(N'stg_claims.tmp_apcd_claim_header_bh',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_bh;
create table stg_claims.tmp_apcd_claim_header_bh (
	claim_header_id varchar(255),
  mh_primary tinyint,
	mh_any tinyint,
	sud_primary tinyint,
	sud_any tinyint
)
with (heap);

--insert data
insert into stg_claims.tmp_apcd_claim_header_bh
select 
b.claim_header_id
,max(case when b.icdcm_number = '01' and a.mh_any = 1 then 1 else 0 end) as mh_primary
,max(case when a.mh_any = 1 then 1 else 0 end) as mh_any
,max(case when b.icdcm_number = '01' and a.sud_any = 1 then 1 else 0 end) as sud_primary
,max(case when a.sud_any = 1 then 1 else 0 end) as sud_any
from stg_claims.ref_icdcm_codes as a
inner join stg_claims.stage_apcd_claim_icdcm_header as b
on (a.icdcm_version = b.icdcm_version) and (a.icdcm = b.icdcm_norm)
group by b.claim_header_id
option (label = 'tmp_apcd_claim_header_bh');


------------------
--STEP 9: Injury cause and nature per CDC guidance
-------------------

--create table shell for final result
if object_id(N'stg_claims.tmp_apcd_claim_header_injury',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_injury;
create table stg_claims.tmp_apcd_claim_header_injury (
	claim_header_id varchar(255),
  ecode varchar(255),
	injury_narrow tinyint,
	injury_broad tinyint,
	intent varchar(255),
	mechanism varchar(255),
	icdcm_injury_nature varchar(255),
	icdcm_injury_nature_version tinyint,
	icdcm_injury_nature_type varchar(255)
)
with (heap);

--Step 9a: Create table of distinct icdcm codes

if object_id(N'stg_claims.tmp_apcd_icdcm_distinct',N'U') is not null drop table stg_claims.tmp_apcd_icdcm_distinct;
create table stg_claims.tmp_apcd_icdcm_distinct (
icdcm_norm varchar(255),
icdcm_version tinyint
)
with (heap);
insert into stg_claims.tmp_apcd_icdcm_distinct
select distinct icdcm_norm, icdcm_version
from stg_claims.stage_apcd_claim_icdcm_header;

--Step 9b: Flag nature of injury codes per CDC injury hospitalization surveillance definition for ICD-9-CM and ICD-10-CM
--Refer to 7/5/19 NHSR report for ICD-9-CM and ICD-10-CM surveillance case definition for injury hospitalizations
--ICD-9-CM definition is in 2nd paragraph of introduction
--ICD-10-CM definition is in Table C (note this is same as Table B in 2020 NHSR update to nature of injury body region classification)
--Tip - For using SQL between operator, the second parameter must be the last value in the list we want to include or it will miss values (e.g. 9949 not 994)

if object_id(N'stg_claims.tmp_apcd_injury_nature_ref',N'U') is not null drop table stg_claims.tmp_apcd_injury_nature_ref;
create table stg_claims.tmp_apcd_injury_nature_ref (
icdcm_norm varchar(255),
icdcm_version tinyint
)
with (heap);
insert into stg_claims.tmp_apcd_injury_nature_ref
select distinct *
from stg_claims.tmp_apcd_icdcm_distinct
--Apply CDC surveillance definition for ICD-9-CM codes
where (icdcm_version = 9 and 
(icdcm_norm between '800%' and '9949%' or icdcm_norm like '9955%' or icdcm_norm between '99580%' and '99585%') -- inclusion
and icdcm_norm not like '9093%' -- exclusion
and icdcm_norm not like '9095%' -- exclusion
)
--Apply CDC surveillance definition for ICD-10-CM codes
or (icdcm_version = 10 and (
(icdcm_norm like 'S%' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
or (icdcm_norm between 'T07%' and 'T3499XS' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
or (icdcm_norm between 'T36%' and 'T50996S' and substring(icdcm_norm,6,1) in ('1', '2', '3', '4') and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
or (icdcm_norm like 'T3[679]9%' and substring(icdcm_norm,5,1) in ('1', '2', '3', '4') and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
or (icdcm_norm like 'T414%' and substring(icdcm_norm,5,1) in ('1', '2', '3', '4') and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
or (icdcm_norm like 'T427%' and substring(icdcm_norm,5,1) in ('1', '2', '3', '4') and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion 
or (icdcm_norm like 'T4[3579]9%' and substring(icdcm_norm,5,1) in ('1', '2', '3', '4') and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
or (icdcm_norm between 'T51%' and 'T6594XS' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
or (icdcm_norm between 'T66%' and 'T7692XS' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
or (icdcm_norm like 'T79%' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
or (icdcm_norm between 'O9A2%' and 'O9A53' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
or (icdcm_norm like 'T8404%' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
or (icdcm_norm like 'M97%' and substring(icdcm_norm,7,1) in ('A', 'B', 'C', '')) -- inclusion
));

--Step 9c: Create flags for broad and narrrow injury surveillance definitions

if object_id(N'stg_claims.tmp_apcd_injury_nature',N'U') is not null drop table stg_claims.tmp_apcd_injury_nature;
create table stg_claims.tmp_apcd_injury_nature (
claim_header_id bigint,
icdcm_norm varchar(255),
icdcm_version tinyint,
icdcm_number varchar(255),
injury_narrow tinyint,
injury_broad tinyint
)
with (heap);
insert into stg_claims.tmp_apcd_injury_nature
select distinct a.claim_header_id, a.icdcm_norm, a.icdcm_version, a.icdcm_number, --remove distinct once icdcm_header issue is addressed
case when a.icdcm_number = '01' then 1 else 0 end as injury_narrow,
1 as injury_broad
from stg_claims.stage_apcd_claim_icdcm_header as a
inner join stg_claims.tmp_apcd_injury_nature_ref as b
on (a.icdcm_norm = b.icdcm_norm) and (a.icdcm_version = b.icdcm_version);

--Step 9d: Identify external cause-of-injury codes for intent and mechanism

--LIKE join distinct ICD-10-CM codes to ICD-10-CM external cause-of-injury code reference table

if object_id(N'stg_claims.tmp_apcd_injury_cause_icd10cm_ref',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_icd10cm_ref;
create table stg_claims.tmp_apcd_injury_cause_icd10cm_ref (
icdcm_norm varchar(255),
icdcm_version tinyint,
intent varchar(255),
mechanism varchar(255)
)
with (heap);
insert into stg_claims.tmp_apcd_injury_cause_icd10cm_ref
select distinct a.icdcm_norm, a.icdcm_version, b.intent, b.mechanism
from (select * from stg_claims.tmp_apcd_icdcm_distinct where icdcm_version = 10) as a
inner join (
select icdcm, icdcm + '%' as icdcm_like, icdcm_version, intent, mechanism
from stg_claims.ref_icdcm_codes
where icdcm_version = 10 and intent is not null
) as b
on (a.icdcm_norm like b.icdcm_like) and (a.icdcm_version = b.icdcm_version);

--LIKE join distinct ICD-9-CM codes to ICD-9-CM external cause-of-injury code reference table

if object_id(N'stg_claims.tmp_apcd_injury_cause_icd9cm_ref',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_icd9cm_ref;
create table stg_claims.tmp_apcd_injury_cause_icd9cm_ref (
icdcm_norm varchar(255),
icdcm_version tinyint,
intent varchar(255),
mechanism varchar(255)
)
with (heap);
insert into stg_claims.tmp_apcd_injury_cause_icd9cm_ref
select distinct a.icdcm_norm, a.icdcm_version, b.intent, b.mechanism
from (select * from stg_claims.tmp_apcd_icdcm_distinct where icdcm_version = 9) as a
inner join (
select icdcm, icdcm + '%' as icdcm_like, icdcm_version, intent, mechanism
from stg_claims.ref_icdcm_codes
where icdcm_version = 9 and intent is not null
) as b
on (a.icdcm_norm like b.icdcm_like) and (a.icdcm_version = b.icdcm_version);

--UNION ICD-10-CM and ICD-9-CM reference tables

if object_id(N'stg_claims.tmp_apcd_injury_cause_ref',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_ref;
create table stg_claims.tmp_apcd_injury_cause_ref (
icdcm_norm varchar(255),
icdcm_version tinyint,
intent varchar(255),
mechanism varchar(255)
)
with (heap);
insert into stg_claims.tmp_apcd_injury_cause_ref
select *
from stg_claims.tmp_apcd_injury_cause_icd9cm_ref
union
select *
from stg_claims.tmp_apcd_injury_cause_icd10cm_ref;

--EXACT join of above table to claims data with injury flags

if object_id(N'stg_claims.tmp_apcd_injury_cause',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause;
create table stg_claims.tmp_apcd_injury_cause (
claim_header_id bigint,
icdcm_norm varchar(255),
icdcm_version tinyint,
icdcm_number varchar(255),
intent varchar(255),
mechanism varchar(255),
ecode_flag tinyint
)
with (heap);
insert into stg_claims.tmp_apcd_injury_cause
select distinct a.claim_header_id, a.icdcm_norm, a.icdcm_version, a.icdcm_number, b.intent, b.mechanism, --remove distinct once icdcm_header issue resolved
1 as ecode_flag
from stg_claims.tmp_apcd_injury_nature as a
inner join stg_claims.tmp_apcd_injury_cause_ref as b
on (a.icdcm_norm = b.icdcm_norm) and (a.icdcm_version = b.icdcm_version);

--Create rank variables for valid nature-of-injury codes

if object_id(N'stg_claims.tmp_apcd_injury_nature_ranks',N'U') is not null drop table stg_claims.tmp_apcd_injury_nature_ranks;
create table stg_claims.tmp_apcd_injury_nature_ranks (
claim_header_id bigint,
icdcm_norm varchar(255),
icdcm_version tinyint,
icdcm_number varchar(255),
injury_narrow tinyint,
injury_broad tinyint,
injury_nature_rank int
)
with (heap);
insert into stg_claims.tmp_apcd_injury_nature_ranks
select *,
row_number() over (partition by claim_header_id, injury_broad order by icdcm_number) as injury_nature_rank
from stg_claims.tmp_apcd_injury_nature;

--Create rank variables for valid external cause-of-injury codes

if object_id(N'stg_claims.tmp_apcd_injury_cause_ranks',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_ranks;
create table stg_claims.tmp_apcd_injury_cause_ranks (
claim_header_id bigint,
icdcm_norm varchar(255),
icdcm_version tinyint,
icdcm_number varchar(255),
intent varchar(255),
mechanism varchar(255),
ecode_flag tinyint,
ecode_rank int
)
with (heap);
insert into stg_claims.tmp_apcd_injury_cause_ranks
select *,
row_number() over (partition by claim_header_id, ecode_flag order by icdcm_number) as ecode_rank
from stg_claims.tmp_apcd_injury_cause;

--Step 9e: Aggregate to claim header level

--Create some aggregated fields

if object_id(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_header_level_tmp;
create table stg_claims.tmp_apcd_injury_cause_header_level_tmp (
claim_header_id bigint,
icdcm_norm varchar(255),
injury_narrow tinyint,
injury_broad tinyint
)
with (heap);
insert into stg_claims.tmp_apcd_injury_cause_header_level_tmp
select claim_header_id,
icdcm_norm,
max(injury_narrow) over (partition by claim_header_id) as injury_narrow,
max(injury_broad) over (partition by claim_header_id) as injury_broad
from stg_claims.tmp_apcd_injury_nature_ranks;

if object_id(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp2',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_header_level_tmp2;
create table stg_claims.tmp_apcd_injury_cause_header_level_tmp2 (
claim_header_id bigint,
icdcm_norm varchar(255),
intent varchar(255),
mechanism varchar(255),
ecode_flag_max tinyint,
ecode_rank int
)
with (heap);
insert into stg_claims.tmp_apcd_injury_cause_header_level_tmp2
select claim_header_id,
icdcm_norm,
intent,
mechanism,
max(ecode_flag) over (partition by claim_header_id) as ecode_flag_max,
ecode_rank
from stg_claims.tmp_apcd_injury_cause_ranks;

--Collapse to claim header level

if object_id(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp3',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_header_level_tmp3;
create table stg_claims.tmp_apcd_injury_cause_header_level_tmp3 (
claim_header_id bigint,
ecode varchar(255),
injury_narrow tinyint,
injury_broad tinyint,
intent varchar(255),
mechanism varchar(255)
)
with (heap);
insert into stg_claims.tmp_apcd_injury_cause_header_level_tmp3
select distinct a.claim_header_id, 
case when b.ecode_rank = 1 then b.icdcm_norm else null end as ecode,
a.injury_narrow, a.injury_broad, b.intent, b.mechanism
from stg_claims.tmp_apcd_injury_cause_header_level_tmp as a
left join (select * from stg_claims.tmp_apcd_injury_cause_header_level_tmp2 where (ecode_flag_max = 1 and ecode_rank = 1)) as b
on a.claim_header_id = b.claim_header_id;

--Add back first-ranked diagnosis with a nature-of-injury code

if object_id(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp4',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_header_level_tmp4;
create table stg_claims.tmp_apcd_injury_cause_header_level_tmp4 (
claim_header_id bigint,
ecode varchar(255),
injury_narrow tinyint,
injury_broad tinyint,
intent varchar(255),
mechanism varchar(255),
icdcm_injury_nature varchar(255),
icdcm_injury_nature_version tinyint
)
with (heap);
insert into stg_claims.tmp_apcd_injury_cause_header_level_tmp4
select a.*, b.icdcm_norm as icdcm_injury_nature, b.icdcm_version as icdcm_injury_nature_version
from stg_claims.tmp_apcd_injury_cause_header_level_tmp3 as a
left join (select * from stg_claims.tmp_apcd_injury_nature_ranks where injury_nature_rank = 1) as b
on a.claim_header_id = b.claim_header_id;

--Step 9f: Create reference table to categorize type of nature of injury

--First join to ref.icdcm_codes to grab CCS detail description, removing [initial encounter] phrase

if object_id(N'stg_claims.tmp_apcd_distinct_injury_nature_icdcm_tmp1',N'U') is not null drop table stg_claims.tmp_apcd_distinct_injury_nature_icdcm_tmp1;
create table stg_claims.tmp_apcd_distinct_injury_nature_icdcm_tmp1 (
icdcm_injury_nature varchar(255),
icdcm_injury_nature_version tinyint,
ccs_detail_desc varchar(255)
)
with (heap);
insert into stg_claims.tmp_apcd_distinct_injury_nature_icdcm_tmp1
select distinct icdcm_injury_nature, icdcm_injury_nature_version,
case
  	when b.ccs_detail_desc like '%; initial encounter%' then replace(b.ccs_detail_desc, '; initial encounter', '')
  	when b.ccs_detail_desc like '%, initial encounter%' then replace(b.ccs_detail_desc, ', initial encounter', '')
  	else b.ccs_detail_desc
end as ccs_detail_desc
from stg_claims.tmp_apcd_injury_cause_header_level_tmp4 as a
left join stg_claims.ref_icdcm_codes as b
on (a.icdcm_injury_nature = b.icdcm) and (a.icdcm_injury_nature_version = b.icdcm_version)
where a.icdcm_injury_nature is not null;

--Normalize type of injury categories

if object_id(N'stg_claims.tmp_apcd_distinct_injury_nature_icdcm_final',N'U') is not null drop table stg_claims.tmp_apcd_distinct_injury_nature_icdcm_final;
create table stg_claims.tmp_apcd_distinct_injury_nature_icdcm_final (
icdcm_injury_nature varchar(255),
icdcm_injury_nature_version tinyint,
ccs_detail_desc varchar(255)
)
with (heap);
insert into stg_claims.tmp_apcd_distinct_injury_nature_icdcm_final
select icdcm_injury_nature, icdcm_injury_nature_version,
case
when ccs_detail_desc in ('Other specified injury', 'Other unspecified injury') then 'Other injuries'
when ccs_detail_desc in ('Spinal cord injury (SCI)') then 'Spinal cord injury'
when ccs_detail_desc in ('Effect of other external causes',
  	'External cause codes: other specified, classifiable and NEC',
  	'External cause codes: unspecified mechanism',
  	'Other injuries and conditions due to external causes')
  	then 'Other injuries and conditions due to external causes'
when ccs_detail_desc in ('Crushing injury', 'Crushing injury or internal injury') then 'Crushing injury or internal injury'
when ccs_detail_desc in ('Burns', 'Burn and corrosion') then 'Burn and corrosion'
else ccs_detail_desc
end as ccs_detail_desc
from stg_claims.tmp_apcd_distinct_injury_nature_icdcm_tmp1;

--Step 9g: Add broad type categories to nature of injury ICD-CM codes

insert into stg_claims.tmp_apcd_claim_header_injury
select
a.*,
b.ccs_detail_desc as icdcm_injury_nature_type
from stg_claims.tmp_apcd_injury_cause_header_level_tmp4 as a
left join stg_claims.tmp_apcd_distinct_injury_nature_icdcm_final as b
on (a.icdcm_injury_nature = b.icdcm_injury_nature) and (a.icdcm_injury_nature_version = b.icdcm_injury_nature_version)
option (label = 'tmp_apcd_claim_header_injury');

--Clean up temp tables
if object_id(N'stg_claims.tmp_apcd_icdcm_distinct',N'U') is not null drop table stg_claims.tmp_apcd_icdcm_distinct;
if object_id(N'stg_claims.tmp_apcd_injury_nature_ref',N'U') is not null drop table stg_claims.tmp_apcd_injury_nature_ref;
if object_id(N'stg_claims.tmp_apcd_injury_nature',N'U') is not null drop table stg_claims.tmp_apcd_injury_nature;
if object_id(N'stg_claims.tmp_apcd_injury_cause_icd10cm_ref',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_icd10cm_ref;
if object_id(N'stg_claims.tmp_apcd_injury_cause_icd9cm_ref',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_icd9cm_ref;
if object_id(N'stg_claims.tmp_apcd_injury_cause_ref',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_ref;
if object_id(N'stg_claims.tmp_apcd_injury_nature',N'U') is not null drop table stg_claims.tmp_apcd_injury_nature;
if object_id(N'stg_claims.tmp_apcd_injury_cause',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause;
if object_id(N'stg_claims.tmp_apcd_injury_nature_ranks',N'U') is not null drop table stg_claims.tmp_apcd_injury_nature_ranks;
if object_id(N'stg_claims.tmp_apcd_injury_cause_ranks',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_ranks;
if object_id(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_header_level_tmp;
if object_id(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp2',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_header_level_tmp2;
if object_id(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp3',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_header_level_tmp3;
if object_id(N'stg_claims.tmp_apcd_injury_cause_header_level_tmp4',N'U') is not null drop table stg_claims.tmp_apcd_injury_cause_header_level_tmp4;
if object_id(N'stg_claims.tmp_apcd_distinct_injury_nature_icdcm_tmp1',N'U') is not null drop table stg_claims.tmp_apcd_distinct_injury_nature_icdcm_tmp1;
if object_id(N'stg_claims.tmp_apcd_distinct_injury_nature_icdcm_final',N'U') is not null drop table stg_claims.tmp_apcd_distinct_injury_nature_icdcm_final;


------------------
--STEP 10: Conduct overlap and clustering for ED population health measure (Yale measure)
--Adaptation of Philip's Medicaid code, which is adaptation of Eli's original code
-------------------

--create table shell
if object_id(N'stg_claims.tmp_apcd_claim_header_ed_pophealth',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_ed_pophealth;
create table stg_claims.tmp_apcd_claim_header_ed_pophealth (
	claim_header_id bigint,
	ed_pophealth_id bigint
)
with (heap);

--Set date of service matching window
declare @match_window int;
set @match_window = 1;
    
--insert data
with increment_stays_by_person as
(
  select
  id_apcd,
  claim_header_id,
  first_service_date,
  last_service_date,
  --create chronological (0, 1) indicator column.
  --if 0, it is the first ED visit for the person OR the ED visit appears to be a duplicate (overlapping service dates) of the prior visit.
  --if 1, the prior ED visit appears to be distinct from the following stay.
  --this indicator column will be summed to create an episode_id.
  case
    when row_number() over(partition by id_apcd order by first_service_date, last_service_date, claim_header_id) = 1 then 0
    when datediff(day, lag(first_service_date) over(partition by id_apcd
      order by first_service_date, last_service_date, claim_header_id), first_service_date) <= @match_window then 0
    when datediff(day, lag(first_service_date) over(partition by id_apcd
      order by first_service_date, last_service_date, claim_header_id), first_service_date) > @match_window then 1
  end as increment
  from stg_claims.tmp_apcd_claim_header_temp3
  where ed_yale_carrier = 1 or ed_yale_opt = 1 or ed_yale_ipt = 1
),
    
--Sum [increment] column (Cumulative Sum) within person to create an stay_id that combines duplicate/overlapping ED visits.
create_within_person_stay_id AS
(
  select
  id_apcd,
  claim_header_id,
  sum(increment) over(partition by id_apcd order by first_service_date, last_service_date, claim_header_id rows unbounded preceding) + 1 as within_person_stay_id
  from increment_stays_by_person
)

insert into stg_claims.tmp_apcd_claim_header_ed_pophealth
select
claim_header_id,
dense_rank() over(order by id_apcd, within_person_stay_id) as ed_pophealth_id
from create_within_person_stay_id
option (label = 'apcd_claim_header_ed_pophealth');
    
    
------------------
--STEP 11: Join back ed_pophealth, CCS, BH, and injury tables with header table on claim header ID
-------------------
insert into stg_claims.stage_apcd_claim_header
select distinct
a.id_apcd,
a.claim_header_id,
a.product_code_id,
a.first_service_date,
a.last_service_date,
a.first_paid_date,
a.last_paid_date,
a.charge_amt,
a.primary_diagnosis,
a.icdcm_version,
a.claim_status_id,
a.claim_type_apcd_id,
a.claim_type_id,
a.type_of_bill_code,
a.ccs_superlevel_desc,
a.ccs_broad_desc,
a.ccs_broad_code,
a.ccs_midlevel_desc,
a.ccs_detail_desc,
a.ccs_detail_code,
case when c.mh_primary = 1 then 1 else 0 end as 'mh_primary',
case when c.mh_any = 1 then 1 else 0 end as 'mh_any',
case when c.sud_primary = 1 then 1 else 0 end as 'sud_primary',
case when c.sud_any = 1 then 1 else 0 end as 'sud_any',
case when d.injury_narrow = 1 then 1 else 0 end as injury_nature_narrow,
case when d.injury_broad = 1 then 1 else 0 end as injury_nature_broad,
d.icdcm_injury_nature_type as injury_nature_type,
d.icdcm_injury_nature as injury_nature_icdcm,
d.ecode as injury_ecode,
d.intent as injury_intent,
d.mechanism as injury_mechanism,
a.cardiac_imaging_and_tests_flag,
a.chiropractic_flag,
a.consultations_flag,
a.covid19_flag,
a.dialysis_flag,
a.durable_medical_equip_flag,
a.echography_flag,
a.endoscopic_procedure_flag,
a.evaluation_and_management_flag,
a.health_home_utilization_flag,
a.hospice_utilization_flag,
a.imaging_advanced_flag,
a.imaging_standard_flag,
a.inpatient_acute_flag,
a.inpatient_nonacute_flag,
a.lab_and_pathology_flag,
a.oncology_and_chemotherapy_flag,
a.physical_therapy_rehab_flag,
a.preventive_screenings_flag,
a.preventive_vaccinations_flag,
a.preventive_visits_flag,
a.psychiatric_visits_flag,
a.surgery_and_anesthesia_flag,
a.telehealth_flag,
a.ed_perform_id,
b.ed_pophealth_id,
a.inpatient_id,
a.discharge_date,
a.pc_visit_id,
getdate() as last_run
from stg_claims.tmp_apcd_claim_header_temp3 as a
left join stg_claims.tmp_apcd_claim_header_ed_pophealth as b
on a.claim_header_id = b.claim_header_id
left join stg_claims.tmp_apcd_claim_header_bh as c
on a.claim_header_id = c.claim_header_id
left join stg_claims.tmp_apcd_claim_header_injury as d
on a.claim_header_id = d.claim_header_id
option (label = 'stage_apcd_claim_header');
    
if object_id(N'stg_claims.tmp_apcd_claim_header_temp3',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_temp3;
if object_id(N'stg_claims.tmp_apcd_claim_header_ed_pophealth',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_ed_pophealth;
if object_id(N'stg_claims.tmp_apcd_claim_header_bh',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_bh;
if object_id(N'stg_claims.tmp_apcd_claim_header_injury',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_injury;",
    .con = dw_inthealth))
}


#### Table-level QA script ####
qa_stage.apcd_claim_header_f <- function() {
  
  #confirm that claim header is distinct
  res1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of headers' as qa_type,
    count(*) as qa
    from stg_claims.stage_apcd_claim_header;",
    .con = dw_inthealth))
  
  res2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of distinct headers' as qa_type,
    count(distinct claim_header_id) as qa
    from stg_claims.stage_apcd_claim_header;",
    .con = dw_inthealth))
  
  #compare claim header counts with raw data
  res3 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.apcd_medical_claim_header' as 'table', '# of headers in raw table' as qa_type,
    count(*) as qa
    from stg_claims.apcd_medical_claim_header
    --exclude denined/orphaned claims
    where denied_header_flag = 'N' and orphaned_header_flag = 'N';",
    .con = dw_inthealth))
  
  #all members should be in elig_demo table
  res4 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of members not in elig_demo, expect 0' as qa_type,
    count(a.id_apcd) as qa
    from stg_claims.stage_apcd_claim_header as a
    left join stg_claims.stage_apcd_elig_demo as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = dw_inthealth))
  
  #all members should be in elig_timevar table
  res5 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of members not in elig_timevar, expect 0' as qa_type,
    count(a.id_apcd) as qa
    from stg_claims.stage_apcd_claim_header as a
    left join stg_claims.stage_apcd_elig_timevar as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = dw_inthealth))
  
  #count unmatched claim types
  res6 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of claims with unmatched claim type, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_apcd_claim_header
    where claim_type_id is null or claim_type_apcd_id is null;",
    .con = dw_inthealth))
  
  #verify that all inpatient stays have discharge date
  res7 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of ipt stays with no discharge date, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_apcd_claim_header
    where inpatient_id is not null and discharge_date is null;",
    .con = dw_inthealth))
  
  #verify that no ed_pophealth_id value is used for more than one person
  res8 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of ed_pophealth_id values used for >1 person, expect 0' as qa_type,
    count(a.ed_pophealth_id) as qa
    from (
      select ed_pophealth_id, count(distinct id_apcd) as id_dcount
      from stg_claims.stage_apcd_claim_header
      group by ed_pophealth_id
    ) as a
    where a.id_dcount > 1;",
    .con = dw_inthealth))
  
  #verify that ed_pophealth_id does not skip any values
  res9a <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of distinct ed_pophealth_id values' as qa_type,
    count(distinct ed_pophealth_id) as qa
    from stg_claims.stage_apcd_claim_header;",
    .con = dw_inthealth))
  
  res9b <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', 'max ed_pophealth_id - min + 1' as qa_type,
    cast(max(ed_pophealth_id) - min(ed_pophealth_id) + 1 as int) as qa
    from stg_claims.stage_apcd_claim_header;",
    .con = dw_inthealth))
  
  #verify that there are no rows with ed_perform_id without ed_pophealth_id
  res10 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_header' as 'table', '# of ed_perform rows with no ed_pophealth, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_apcd_claim_header
    where ed_perform_id is not null and ed_pophealth_id is null;",
    .con = dw_inthealth))
  
  #verify that 1-day overlap window was implemented correctly with ed_pophealth_id
  res11 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "with cte as
    (
    select * 
    ,lag(ed_pophealth_id) over(partition by id_apcd, ed_pophealth_id order by first_service_date) as lag_ed_pophealth_id
    ,lag(first_service_date) over(partition by id_apcd, ed_pophealth_id order by first_service_date) as lag_first_service_date
    from stg_claims.stage_apcd_claim_header
    where [ed_pophealth_id] is not null
    )
    select 'stg_claims.stage_apcd_claim_header' as 'table', '# of ed_pophealth visits where the overlap date is greater than 1 day, expect 0' as 'qa_type',
      count(*) as qa
    from stg_claims.stage_apcd_claim_header
    where [ed_pophealth_id] in (select ed_pophealth_id from cte where abs(datediff(day, lag_first_service_date, first_service_date)) > 1);",
    .con = dw_inthealth))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}