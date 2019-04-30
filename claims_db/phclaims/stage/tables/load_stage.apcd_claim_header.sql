--Code to load data to stage.apcd_claim_header table
--Distinct header-level claim variables (e.g. claim type). In other words elements for which there is only one distinct 
--value per claim header.
--Eli Kern (PHSKC-APDE)
--2019-4-26
--Run time: XX min

------------------
--STEP 1: Transform medical_claim_header table, add all fields that do not require table joins
--Note that all denied and orphaned claim headers are dropped at this stage
-------------------
if object_id('tempdb..#temp1') is not null drop table #temp1;
select
internal_member_id as id_apcd,
extract_id,
medical_claim_header_id as claim_header_id,
submitter_id,
cast(internal_provider_id as bigint) as provider_id_apcd,
product_code_id,
first_service_dt,
last_service_dt,
first_paid_dt,
last_paid_dt,
charge_amt,
diagnosis_code as primary_diagnosis,
case when icd_version_ind = '9' then 9 when icd_version_ind = '0' then 10 end as icdcm_version,
header_status,
cast(convert(varchar(100), claim_type_id) + '.' + convert(varchar(100), type_of_setting_id) + '.' +
	convert(varchar(100), place_of_setting_id) as varchar(100)) as claim_type_apcd_id,
type_of_bill_code,
cast(case when emergency_room_flag = 'Y' then 1 when emergency_room_flag = 'N' then 0 end as tinyint) as ed_flag,
cast(case when operating_room_flag = 'Y' then 1 when operating_room_flag = 'N' then 0 end as tinyint) as or_flag
into #temp1
from PHClaims.stage.apcd_medical_claim_header
where denied_header_flag = 'N' and orphaned_header_flag = 'N';

------------------
--STEP 2: Prepare header-level inpatient stay data elements using line-level table
--Note that is definition of an acute inpatient stay was defined through Susan Hernandez's work and dialogue with OnPoint
-------------------
if object_id('tempdb..#temp2') is not null drop table #temp2;
select b.medical_claim_header_id, max(a.ipt_flag) as ipt_flag, max(a.discharge_dt) as discharge_dt
into #temp2
from (
select medical_claim_service_line_id, 1 as ipt_flag, discharge_dt
from PHClaims.stage.apcd_medical_claim 
where claim_type_id = '1' and type_of_setting_id = '1' and place_of_setting_id = '1'
	and (denied_claim_flag = 'N' AND orphaned_adjustment_flag = 'N')
	and claim_status_id in (-1, -2, 1, 5, 2, 6)
	and discharge_dt is not null
) as a
left join PHClaims.stage.apcd_medical_crosswalk as b
on a.medical_claim_service_line_id = b.medical_claim_service_line_id
group by b.medical_claim_header_id;


------------------
--STEP 3: Join claim header temp table with new header-level inpatient flags
-------------------
if object_id('tempdb..#temp3') is not null drop table #temp3;
select 
a.id_apcd,
a.extract_id,
a.claim_header_id,
a.submitter_id,
a.provider_id_apcd,
a.product_code_id,
a.first_service_dt,
a.last_service_dt,
a.first_paid_dt,
a.last_paid_dt,
a.charge_amt,
a.primary_diagnosis,
a.icdcm_version,
a.header_status,
a.claim_type_apcd_id,
a.type_of_bill_code,
cast(b.ipt_flag as tinyint) as ipt_flag,
cast(b.discharge_dt as date) as discharge_dt,
a.ed_flag,
a.or_flag
into #temp3
from #temp1 as a
left join #temp2 as b
on a.claim_header_id = b.medical_claim_header_id;


------------------
--STEP 4: Join to King County claim type reference table and insert into table shell
-------------------
insert into PHClaims.stage.apcd_claim_header with (tablock)
select 
a.id_apcd,
a.extract_id,
a.claim_header_id,
a.submitter_id,
a.provider_id_apcd,
a.product_code_id,
a.first_service_dt,
a.last_service_dt,
a.first_paid_dt,
a.last_paid_dt,
a.charge_amt,
a.primary_diagnosis,
a.icdcm_version,
a.header_status,
a.claim_type_apcd_id,
cast(b.kc_clm_type_id as tinyint) as claim_type_id,
a.type_of_bill_code,
a.ipt_flag,
a.discharge_dt,
a.ed_flag,
a.or_flag
from #temp3 as a
left join (select * from PHClaims.ref.kc_claim_type_crosswalk where source_desc = 'WA-APCD') as b
on a.claim_type_apcd_id = b.source_clm_type_id;