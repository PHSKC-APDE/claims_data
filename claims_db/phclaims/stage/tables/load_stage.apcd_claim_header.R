#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_HEADER
# Eli Kern, PHSKC (APDE)
#
# 2019-10
#
# 2024-04-29 update: Modified for HHSAW migration

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

if object_id(N'stg_claims.tmp_apcd_claim_header_temp1',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_temp1;
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
    
--adding in new (extract 10001) service type flags from OnPoint
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
    
into stg_claims.tmp_apcd_claim_header_temp1
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
where a.denied_header_flag = 'N' and a.orphaned_header_flag = 'N';
    
    
------------------
--STEP 2: Procedure code query for ED visits
--Subset to relevant claims as last step to minimize temp table size
-------------------
if object_id(N'stg_claims.tmp_apcd_claim_header_ed_procedure_code',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_ed_procedure_code;
select x.medical_claim_header_id, x.ed_procedure_code_temp
into stg_claims.tmp_apcd_claim_header_ed_procedure_code
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
where x.ed_procedure_code_temp = 1;
    
    
------------------
--STEP 3: Primary care visit query
-------------------
if object_id(N'stg_claims.tmp_apcd_claim_header_pc_visit',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_pc_visit;
select x.medical_claim_header_id, x.pc_procedure_temp, x.pc_taxonomy_temp, x.pc_zcode_temp
into stg_claims.tmp_apcd_claim_header_pc_visit
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
where (x.pc_procedure_temp = 1 or x.pc_zcode_temp = 1) and x.pc_taxonomy_temp = 1;
    
    
------------------
--STEP 5: Extract primary diagnosis, take first ordered ICD-CM code when >1 primary per header
------------------
if object_id(N'stg_claims.tmp_apcd_claim_header_icd1',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_icd1;
select claim_header_id,
min(icdcm_norm) as primary_diagnosis,
min(icdcm_version) as icdcm_version
into stg_claims.tmp_apcd_claim_header_icd1
from stg_claims.stage_apcd_claim_icdcm_header
where icdcm_number = '01'
group by claim_header_id;
    

------------------
--STEP 6:  Join intermediate tables to prep for next step
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
);

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
on a.claim_header_id = c.medical_claim_header_id
left join stg_claims.tmp_apcd_claim_header_pc_visit as d
on a.claim_header_id = d.medical_claim_header_id;

if object_id(N'stg_claims.tmp_apcd_claim_header_icd1',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_icd1;
if object_id(N'stg_claims.tmp_apcd_claim_header_ed_procedure_code',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_ed_procedure_code;
if object_id(N'stg_claims.tmp_apcd_claim_header_pc_visit',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_pc_visit;


------------------
--STEP 7: Prepare header-level concepts using analytic claim tables
--Add in charge amounts and principal diagnosis
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
);

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
on a.claim_header_id = c.claim_header_id;
    
--drop other temp tables to make space
if object_id(N'stg_claims.tmp_apcd_claim_header_temp1',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_temp1;
if object_id(N'stg_claims.tmp_apcd_claim_header_temp1b',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_temp1b;
    

------------------
--STEP 8: Assign unique ID to healthcare utilization concepts that are grouped by person, service date
-------------------
if object_id(N'stg_claims.tmp_apcd_claim_header_temp3',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_temp3;
select *,
--primary care visits
case when pc_visit = 0 then null
else dense_rank() over
    (order by case when pc_visit = 0 then 2 else 1 end, --sorts non-relevant claims to bottom
    id_apcd, first_service_date)
end as pc_visit_id,
    
--inpatient stays
case when inpatient = 0 then null
else dense_rank() over
    (order by case when inpatient = 0 then 2 else 1 end, --sorts non-relevant claims to bottom
    id_apcd, discharge_date)
end as inpatient_id,
    
--ED performance (RDA measure)
case when ed_perform = 0 then null
else dense_rank() over
    (order by case when ed_perform = 0 then 2 else 1 end, --sorts non-relevant claims to bottom
    id_apcd, first_service_date)
end as ed_perform_id
into stg_claims.tmp_apcd_claim_header_temp3
from stg_claims.tmp_apcd_claim_header_temp2;
    
--drop other temp tables to make space
if object_id(N'stg_claims.tmp_apcd_claim_header_temp2',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_temp2;
    
    
------------------
--STEP 9: Conduct overlap and clustering for ED population health measure (Yale measure)
--Adaptation of Philip's Medicaid code, which is adaptation of Eli's original code
-------------------

-----
--Union carrier, outpatient and inpatient ED visits
-----
--extract carrier ED visits and create left and right matching windows
if object_id(N'stg_claims.tmp_apcd_claim_header_ed_yale_1',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_ed_yale_1;
select id_apcd, claim_header_id, first_service_date, last_service_date, 'Carrier' as ed_type
into stg_claims.tmp_apcd_claim_header_ed_yale_1
from stg_claims.tmp_apcd_claim_header_temp3
where ed_yale_carrier = 1
    
union
select id_apcd, claim_header_id, first_service_date, last_service_date, 'Outpatient' as ed_type
from stg_claims.tmp_apcd_claim_header_temp3 where ed_yale_opt = 1
    
union
select id_apcd, claim_header_id, first_service_date, last_service_date, 'Inpatient' as ed_type
from stg_claims.tmp_apcd_claim_header_temp3 where ed_yale_ipt = 1;
    
-----
--label duplicate/adjacent visits with a single [ed_pophealth_id]
-----
    
--Set date of service matching window
declare @match_window int;
set @match_window = 1;
    
if object_id(N'stg_claims.tmp_apcd_claim_header_ed_yale_final',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_ed_yale_final;
WITH [increment_stays_by_person] AS
(
SELECT
    [id_apcd]
,[claim_header_id]
-- If [prior_first_service_date] IS NULL, then it is the first chronological [first_service_date] for the person
,LAG([first_service_date]) OVER(PARTITION BY [id_apcd] ORDER BY [first_service_date], [last_service_date], [claim_header_id]) AS [prior_first_service_date]
,[first_service_date]
,[last_service_date]
,[ed_type]
-- Number of days between consecutive rows
,DATEDIFF(DAY, LAG([first_service_date]) OVER(PARTITION BY [id_apcd] 
    ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) AS [date_diff]
/*
Create a chronological (0, 1) indicator column.
If 0, it is the first ED visit for the person OR the ED visit appears to be a duplicate
(overlapping service dates) of the prior visit.
If 1, the prior ED visit appears to be distinct from the following stay.
This indicator column will be summed to create an episode_id.
*/
,CASE WHEN ROW_NUMBER() OVER(PARTITION BY [id_apcd] 
        ORDER BY [first_service_date], [last_service_date], [claim_header_id]) = 1 THEN 0
        WHEN DATEDIFF(DAY, LAG([first_service_date]) OVER(PARTITION BY [id_apcd]
    	ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) <= @match_window THEN 0
    	WHEN DATEDIFF(DAY, LAG(first_service_date) OVER(PARTITION BY [id_apcd]
    	ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) > @match_window THEN 1
    END AS [increment]
FROM stg_claims.tmp_apcd_claim_header_ed_yale_1
--ORDER BY [id_apcd], [first_service_date], [last_service_date], [claim_header_id]
),
    
/*
Sum [increment] column (Cumulative Sum) within person to create an stay_id that
combines duplicate/overlapping ED visits.
*/
[create_within_person_stay_id] AS
(
SELECT
    id_apcd
,[claim_header_id]
,[prior_first_service_date]
,[first_service_date]
,[last_service_date]
,[ed_type]
,[date_diff]
,[increment]
,SUM([increment]) OVER(PARTITION BY [id_apcd] ORDER BY [first_service_date], [last_service_date], [claim_header_id] ROWS UNBOUNDED PRECEDING) + 1 AS [within_person_stay_id]
FROM [increment_stays_by_person]
--ORDER BY [id_apcd], [first_service_date], [last_service_date], [claim_header_id]
)
    
SELECT
    id_apcd
,[claim_header_id]
,[prior_first_service_date]
,[first_service_date]
,[last_service_date]
,[ed_type]
,[date_diff]
,[increment]
,[within_person_stay_id]
,DENSE_RANK() OVER(ORDER BY [id_apcd], [within_person_stay_id]) AS [ed_pophealth_id]
    
,FIRST_VALUE([first_service_date]) OVER(PARTITION BY [id_apcd], [within_person_stay_id] 
    ORDER BY [id_apcd], [within_person_stay_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_first_service_date]
,LAST_VALUE([last_service_date]) OVER(PARTITION BY [id_apcd], [within_person_stay_id] 
    ORDER BY [id_apcd], [within_person_stay_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_last_service_date]
    
INTO stg_claims.tmp_apcd_claim_header_ed_yale_final
FROM [create_within_person_stay_id]
ORDER BY id_apcd, [first_service_date], [last_service_date], [claim_header_id];
    
--drop other temp tables to make space
if object_id(N'stg_claims.tmp_apcd_claim_header_ed_yale_1',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_ed_yale_1;
    
    
------------------
--STEP 10: Join back Yale table with header table on claim header ID
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
left join stg_claims.tmp_apcd_claim_header_ed_yale_final as b
on a.claim_header_id = b.claim_header_id;
    
if object_id(N'stg_claims.tmp_apcd_claim_header_temp3',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_temp3;
if object_id(N'stg_claims.tmp_apcd_claim_header_ed_yale_final',N'U') is not null drop table stg_claims.tmp_apcd_claim_header_ed_yale_final;",
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