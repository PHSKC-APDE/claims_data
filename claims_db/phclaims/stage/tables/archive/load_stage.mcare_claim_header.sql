--Code to load data to stage.mcare_claim_header table
--Distinct header-level claim variables (e.g. claim type). In other words elements for which there is only one distinct 
--value per claim header.
--Eli Kern (PHSKC-APDE)
--2020-02
--Run time: XX min

--For testing, find a single person to run it on, who has an ED visit, an inpatient stay, and a pc visit
--GGGGGGG6PhDHQPQ

------------------
--STEP 1: Union all claim types to grab header-level concepts not currently in other analytic tables
--Exclude all denied claims
--Acute inpatient stay defined as NCH claim type 60
--Max of discharge date, min of admission and hospice_from_date
-------------------
if object_id('tempdb..#temp1') is not null drop table #temp1;
select
a.id_mcare collate SQL_Latin1_General_Cp1_CS_AS as id_mcare,
a.claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id,
a.first_service_date,
a.last_service_date,
a.claim_type_mcare_id,
b.kc_clm_type_id as claim_type_id,
a.facility_type_code,
a.service_type_code,
a.patient_status,
a.patient_status_code,
case when a.claim_type_mcare_id = '60' then 1 else 0 end as inpatient_flag,
min(a.admission_date) over(partition by a.claim_header_id) as admission_date,
max(a.discharge_date) over(partition by a.claim_header_id) as discharge_date,
a.ipt_admission_type,
a.ipt_admission_source,
a.drg_code,
min(a.hospice_from_date) over(partition by a.claim_header_id) as hospice_from_date,
a.filetype_mcare
into #temp1
from (
--bcarrier
	select 
	id_mcare,
	claim_header_id,
	first_service_date,
	last_service_date,
	claim_type as claim_type_mcare_id,
	facility_type_code = null,
	service_type_code = null,
	patient_status = null,
	patient_status_code = null,
	admission_date = null,
	discharge_date = null,
	ipt_admission_type = null,
	ipt_admission_source = null,
	drg_code = null,
	hospice_from_date = null,
	'carrier' as filetype_mcare
	from PHClaims.stage.mcare_bcarrier_claims
	where denial_code in ('1','2','3','4','5','6','7','8','9')
	--testing
	and id_mcare = 'GGGGGGG6PhDHQPQ'

	--dme
	union
	select
	id_mcare,
	claim_header_id,
	first_service_date,
	last_service_date,
	claim_type as claim_type_mcare_id,
	facility_type_code = null,
	service_type_code = null,
	patient_status = null,
	patient_status_code = null,
	admission_date = null,
	discharge_date = null,
	ipt_admission_type = null,
	ipt_admission_source = null,
	drg_code = null,
	hospice_from_date = null,
	'dme' as filetype_mcare
	from PHClaims.stage.mcare_dme_claims
	where denial_code in ('1','2','3','4','5','6','7','8','9')
	--testing
	and id_mcare = 'GGGGGGG6PhDHQPQ'

	--hha
	union
	select
	id_mcare,
	claim_header_id,
	first_service_date,
	last_service_date,
	claim_type as claim_type_mcare_id,
	facility_type_code,
	service_type_code,
	patient_status = null,
	patient_status_code,
	admission_date,
	discharge_date,
	ipt_admission_type = null,
	ipt_admission_source = null,
	drg_code = null,
	hospice_from_date = null,
	'hha' as filetype_mcare
	from PHClaims.stage.mcare_hha_base_claims
	where (denial_code_facility = '' or denial_code_facility is null)
	--testing
	and id_mcare = 'GGGGGGG6PhDHQPQ'

	--hospice
	union
	select
	id_mcare,
	claim_header_id,
	first_service_date,
	last_service_date,
	claim_type as claim_type_mcare_id,
	facility_type_code,
	service_type_code,
	patient_status,
	patient_status_code,
	admission_date = null,
	discharge_date,
	ipt_admission_type = null,
	ipt_admission_source = null,
	drg_code = null,
	hospice_from_date,
	'hospice' as filetype_mcare
	from PHClaims.stage.mcare_hospice_base_claims
	where (denial_code_facility = '' or denial_code_facility is null)
	--testing
	and id_mcare = 'GGGGGGG6PhDHQPQ'

	--inpatient
	union
	select
	id_mcare,
	claim_header_id,
	first_service_date,
	last_service_date,
	claim_type as claim_type_mcare_id,
	facility_type_code,
	service_type_code,
	patient_status,
	patient_status_code,
	admission_date,
	discharge_date,
	ipt_admission_type,
	ipt_admission_source,
	drg_code,
	hospice_from_date = null,
	'inpatient' as filetype_mcare
	from PHClaims.stage.mcare_inpatient_base_claims
	where (denial_code_facility = '' or denial_code_facility is null)
	--testing
	and id_mcare = 'GGGGGGG6PhDHQPQ'

	--outpatient
	union
	select
	id_mcare,
	claim_header_id,
	first_service_date,
	last_service_date,
	claim_type as claim_type_mcare_id,
	facility_type_code,
	service_type_code,
	patient_status = null,
	patient_status_code,
	admission_date = null,
	discharge_date = null,
	ipt_admission_type = null,
	ipt_admission_source = null,
	drg_code = null,
	hospice_from_date = null,
	'outpatient' as filetype_mcare
	from PHClaims.stage.mcare_outpatient_base_claims
	where (denial_code_facility = '' or denial_code_facility is null)
	--testing
	and id_mcare = 'GGGGGGG6PhDHQPQ'

	--snf
	union
	select
	id_mcare,
	claim_header_id,
	first_service_date,
	last_service_date,
	claim_type as claim_type_mcare_id,
	facility_type_code,
	service_type_code,
	patient_status,
	patient_status_code,
	admission_date,
	discharge_date,
	ipt_admission_type,
	ipt_admission_source,
	drg_code,
	hospice_from_date = null,
	'snf' as filetype_mcare
	from PHClaims.stage.mcare_snf_base_claims
	where (denial_code_facility = '' or denial_code_facility is null)
	--testing
	and id_mcare = 'GGGGGGG6PhDHQPQ'
) as a

--add in KC claim type
left join (select * from PHClaims.ref.kc_claim_type_crosswalk where source_desc = 'mcare') as b
on a.claim_type_mcare_id = b.source_clm_type_id

--exclude claims among people who have no eligibility data
left join PHClaims.final.mcare_elig_demo as c
on a.id_mcare = c.id_mcare
where c.id_mcare is not null;


------------------
--STEP 2: Do all line-level transformations
-------------------
if object_id('tempdb..#line') is not null drop table #line;
select
claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id,
--ED place of service flag
max(case when place_of_service_code = '23' then 1 else 0 end) as ed_pos,
--ED performance temp flags (RDA measure)
max(case when revenue_code like '045[01269]' then 1 else 0 end) as ed_rev_code_perform,
--ED population health temp flags (Yale measure)
max(case when revenue_code like '045[01269]' or revenue_code = '0981' then 1 else 0 end) as ed_rev_code_pophealth
into #line
from PHClaims.final.mcare_claim_line
--testing
where id_mcare = 'GGGGGGG6PhDHQPQ'
--grouping statement for consolidation to claim header level
group by claim_header_id;


------------------
--STEP 3: Procedure code query for ED visits
--Subset to relevant claims as last step to minimize temp table size
-------------------
if object_id('tempdb..#ed_procedure_code') is not null drop table #ed_procedure_code;
select a.claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id,
	a.ed_procedure_code_perform, a.ed_procedure_code_pophealth
into #ed_procedure_code
from (
select claim_header_id,
	max(case when procedure_code like '9928[123458]' then 1 else 0 end) as ed_procedure_code_perform,
	max(case when procedure_code like '9928[12345]' or procedure_code = '99291' then 1 else 0 end) as ed_procedure_code_pophealth
from PHClaims.final.mcare_claim_procedure
--testing
where id_mcare = 'GGGGGGG6PhDHQPQ'
group by claim_header_id
) as a
where a.ed_procedure_code_perform = 1 or a.ed_procedure_code_pophealth = 1;


------------------
--STEP 4: Primary care visit query
-------------------
if object_id('tempdb..#pc_visit') is not null drop table #pc_visit;
select x.claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id,
	x.pc_procedure_temp, x.pc_taxonomy_temp, x.pc_zcode_temp
into #pc_visit
from (
select a.claim_header_id,
--primary care visit temp flags
max(case when a.code is not null then 1 else 0 end) as pc_procedure_temp,
max(case when b.code is not null then 1 else 0 end) as pc_zcode_temp,
max(case when c.code is not null then 1 else 0 end) as pc_taxonomy_temp

--procedure codes
from (
	select a1.id_mcare, a1.claim_header_id, a2.code
	--procedure code table
	from PHClaims.final.mcare_claim_procedure as a1
	--primary care-relevant procedure codes
	inner join (select code from PHClaims.ref.pc_visit_oregon where code_system in ('cpt', 'hcpcs')) as a2
	on a1.procedure_code = a2.code
) as a

--ICD-CM codes
left join (
	select b1.claim_header_id, b2.code
	--ICD-CM table
	from PHClaims.final.mcare_claim_icdcm_header as b1
	--primary care-relevant ICD-10-CM codes
	inner join (select code from PHClaims.ref.pc_visit_oregon where code_system = 'icd10cm') as b2
	on (b1.icdcm_norm = b2.code) and (b1.icdcm_version = 10)
) as b
on a.claim_header_id = b.claim_header_id

--provider taxonomies
left join (
	select c1.claim_header_id, c3.code
	--rendering and attending providers
	from (select * from PHClaims.final.mcare_claim_provider where provider_type in ('rendering', 'attending')) as c1
	--taxonomy codes for rendering and attending providers
	inner join PHClaims.ref.kc_provider_master as c2
	on c1.provider_npi = c2.npi
	--primary care-relevant provider taxonomy codes
	inner join (select code from PHClaims.ref.pc_visit_oregon where code_system = 'provider_taxonomy') as c3
	on (c2.primary_taxonomy = c3.code) or (c2.secondary_taxonomy = c3.code)
) as c
on a.claim_header_id = c.claim_header_id

--testing
where a.id_mcare = 'GGGGGGG6PhDHQPQ'
--cluster to claim header
group by a.claim_header_id
) as x
where (x.pc_procedure_temp = 1 or x.pc_zcode_temp = 1) and x.pc_taxonomy_temp = 1;


------------------
--STEP 5: Extract primary diagnosis, take first ordered ICD-CM code when >1 primary per header
------------------
if object_id('tempdb..#icd1') is not null drop table #icd1;
select claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id,
min(icdcm_norm) as primary_diagnosis,
min(icdcm_version) as icdcm_version
into #icd1
from PHClaims.final.mcare_claim_icdcm_header
where icdcm_number = '01'
--testing
and id_mcare = 'GGGGGGG6PhDHQPQ'
group by claim_header_id;


------------------
--STEP 6: Prepare header-level concepts using analytic claim tables
--Add in principal diagnosis
-------------------
if object_id('tempdb..#temp2') is not null drop table #temp2;
select distinct a.id_mcare collate SQL_Latin1_General_Cp1_CS_AS as id_mcare, 
a.claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id,
a.first_service_date,
a.last_service_date,
b.primary_diagnosis,
b.icdcm_version,
a.claim_type_mcare_id,
a.claim_type_id,
a.facility_type_code,
a.service_type_code,
a.patient_status,
a.patient_status_code,
a.inpatient_flag,
a.admission_date,
a.discharge_date,
a.ipt_admission_type,
a.ipt_admission_source,
a.drg_code,
a.hospice_from_date,
a.filetype_mcare,

--ED performance (RDA measure)
case when a.claim_type_id = 4 and
	(d.ed_rev_code_perform = 1 or e.ed_procedure_code_perform = 1 or d.ed_pos = 1)
then 1 else 0 end as ed_perform,

--ED population health (Yale measure)
case when a.claim_type_id = 5 and 
	((e.ed_procedure_code_pophealth = 1 and d.ed_pos = 1) or d.ed_rev_code_pophealth = 1)
	then 1 else 0 end as ed_yale_carrier,
case when a.claim_type_id = 4 and 
	(d.ed_rev_code_pophealth = 1 or d.ed_pos = 1 or e.ed_procedure_code_pophealth = 1)
	then 1 else 0 end as ed_yale_opt,
case when a.claim_type_id = 1 and
	(d.ed_rev_code_pophealth = 1 or d.ed_pos = 1 or e.ed_procedure_code_pophealth = 1)
	then 1 else 0 end as ed_yale_ipt,

--Primary care visit (Oregon)
case when (f.pc_procedure_temp = 1 or f.pc_zcode_temp = 1) and f.pc_taxonomy_temp = 1
	and a.claim_type_mcare_id not in ('60', '30') --exclude inpatient, swing bed SNF
	then 1 else 0
end as pc_visit

into #temp2
from #temp1 as a
left join #icd1 as b
on a.claim_header_id = b.claim_header_id
left join #line as d
on a.claim_header_id = d.claim_header_id
left join #ed_procedure_code as e
on a.claim_header_id = e.claim_header_id
left join #pc_visit as f
on a.claim_header_id = f.claim_header_id;

--drop other temp tables to make space
if object_id('tempdb..#temp1') is not null drop table #temp1;
if object_id('tempdb..#line') is not null drop table #line;
if object_id('tempdb..#icd1') is not null drop table #icd1;
if object_id('tempdb..#ed_procedure_code') is not null drop table #ed_procedure_code;
if object_id('tempdb..#pc_visit') is not null drop table #pc_visit;


------------------
--STEP 7: Assign unique ID to healthcare utilization concepts that are grouped by person, service date
-------------------
if object_id('tempdb..#temp3') is not null drop table #temp3;
select 
id_mcare collate SQL_Latin1_General_Cp1_CS_AS as id_mcare, 
claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id,
first_service_date,
last_service_date,
primary_diagnosis,
icdcm_version,
claim_type_mcare_id,
claim_type_id,
facility_type_code,
service_type_code,
patient_status,
patient_status_code,
admission_date,
discharge_date,
ipt_admission_type,
ipt_admission_source,
drg_code,
hospice_from_date,
filetype_mcare,
ed_yale_carrier,
ed_yale_opt,
ed_yale_ipt,

--primary care visits
case when pc_visit = 0 then null
else dense_rank() over
	(order by case when pc_visit = 0 then 2 else 1 end, --sorts non-relevant claims to bottom
	id_mcare, first_service_date)
end as pc_visit_id,

--inpatient stays
case when inpatient_flag = 0 then null
else dense_rank() over
	(order by case when inpatient_flag = 0 then 2 else 1 end, --sorts non-relevant claims to bottom
	id_mcare, discharge_date)
end as inpatient_id,

--ED performance (RDA measure)
case when ed_perform = 0 then null
else dense_rank() over
	(order by case when ed_perform = 0 then 2 else 1 end, --sorts non-relevant claims to bottom
	id_mcare, first_service_date)
end as ed_perform_id
into #temp3
from #temp2;

--drop other temp tables to make space
if object_id('tempdb..#temp2') is not null drop table #temp2;


------------------
--STEP 8: Conduct overlap and clustering for ED population health measure (Yale measure)
--Adaptation of Philip's Medicaid code, which is adaptation of Eli's original code
--Run time: 12 min
-------------------

-----
--Union carrier, outpatient and inpatient ED visits
-----
--extract carrier ED visits and create left and right matching windows
if object_id('tempdb..#ed_yale_1') is not null drop table #ed_yale_1;
select id_mcare collate SQL_Latin1_General_Cp1_CS_AS as id_mcare,
claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id,
first_service_date, last_service_date, 'Carrier' as ed_type
into #ed_yale_1
from #temp3
where ed_yale_carrier = 1

union
select id_mcare collate SQL_Latin1_General_Cp1_CS_AS as id_mcare,
claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id,
first_service_date, last_service_date, 'Outpatient' as ed_type
from #temp3 where ed_yale_opt = 1

union
select id_mcare collate SQL_Latin1_General_Cp1_CS_AS as id_mcare,
claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id,
first_service_date, last_service_date, 'Inpatient' as ed_type
from #temp3 where ed_yale_ipt = 1;

-----
--label duplicate/adjacent visits with a single [ed_pophealth_id]
-----

--Set date of service matching window
declare @match_window int;
set @match_window = 1;

if object_id('tempdb..#ed_yale_final') is not null 
drop table #ed_yale_final;
WITH [increment_stays_by_person] AS
(
SELECT
id_mcare collate SQL_Latin1_General_Cp1_CS_AS as id_mcare
,claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id
-- If [prior_first_service_date] IS NULL, then it is the first chronological [first_service_date] for the person
,LAG([first_service_date]) OVER(PARTITION BY [id_mcare] ORDER BY [first_service_date], [last_service_date], [claim_header_id]) AS [prior_first_service_date]
,[first_service_date]
,[last_service_date]
,[ed_type]
-- Number of days between consecutive rows
,DATEDIFF(DAY, LAG([first_service_date]) OVER(PARTITION BY [id_mcare] 
 ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) AS [date_diff]
/*
Create a chronological (0, 1) indicator column.
If 0, it is the first ED visit for the person OR the ED visit appears to be a duplicate
(overlapping service dates) of the prior visit.
If 1, the prior ED visit appears to be distinct from the following stay.
This indicator column will be summed to create an episode_id.
*/
,CASE WHEN ROW_NUMBER() OVER(PARTITION BY [id_mcare] 
      ORDER BY [first_service_date], [last_service_date], [claim_header_id]) = 1 THEN 0
      WHEN DATEDIFF(DAY, LAG([first_service_date]) OVER(PARTITION BY [id_mcare]
	  ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) <= @match_window THEN 0
	  WHEN DATEDIFF(DAY, LAG(first_service_date) OVER(PARTITION BY [id_mcare]
	  ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) > @match_window THEN 1
 END AS [increment]
FROM #ed_yale_1
--ORDER BY [id_mcare], [first_service_date], [last_service_date], [claim_header_id]
),

/*
Sum [increment] column (Cumulative Sum) within person to create an stay_id that
combines duplicate/overlapping ED visits.
*/
[create_within_person_stay_id] AS
(
SELECT
id_mcare collate SQL_Latin1_General_Cp1_CS_AS as id_mcare
,claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id
,[prior_first_service_date]
,[first_service_date]
,[last_service_date]
,[ed_type]
,[date_diff]
,[increment]
,SUM([increment]) OVER(PARTITION BY [id_mcare] ORDER BY [first_service_date], [last_service_date], [claim_header_id] ROWS UNBOUNDED PRECEDING) + 1 AS [within_person_stay_id]
FROM [increment_stays_by_person]
--ORDER BY [id_mcare], [first_service_date], [last_service_date], [claim_header_id]
)

SELECT
id_mcare collate SQL_Latin1_General_Cp1_CS_AS as id_mcare
,claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id
,[prior_first_service_date]
,[first_service_date]
,[last_service_date]
,[ed_type]
,[date_diff]
,[increment]
,[within_person_stay_id]
,DENSE_RANK() OVER(ORDER BY [id_mcare], [within_person_stay_id]) AS [ed_pophealth_id]

,FIRST_VALUE([first_service_date]) OVER(PARTITION BY [id_mcare], [within_person_stay_id] 
 ORDER BY [id_mcare], [within_person_stay_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_first_service_date]
,LAST_VALUE([last_service_date]) OVER(PARTITION BY [id_mcare], [within_person_stay_id] 
 ORDER BY [id_mcare], [within_person_stay_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_last_service_date]

INTO #ed_yale_final
FROM [create_within_person_stay_id]
ORDER BY id_mcare, [first_service_date], [last_service_date], [claim_header_id];

--drop other temp tables to make space
if object_id('tempdb..#ed_yale_1') is not null drop table #ed_yale_1;


------------------
--STEP 9: Join back Yale table with header table on claim header ID
-------------------
insert into PHClaims.stage.mcare_claim_header with (tablock)
select distinct
rtrim(a.id_mcare) as id_mcare,
rtrim(a.claim_header_id) as id_mcare,
a.first_service_date,
a.last_service_date,
a.primary_diagnosis,
a.icdcm_version,
a.claim_type_mcare_id,
a.claim_type_id,
a.facility_type_code,
a.service_type_code,
a.patient_status,
a.patient_status_code,
a.ed_perform_id,
b.ed_pophealth_id,
a.inpatient_id,
a.admission_date,
a.discharge_date,
a.ipt_admission_type,
a.ipt_admission_source,
a.drg_code,
a.hospice_from_date,
a.pc_visit_id,
a.filetype_mcare,
getdate() as last_run
from #temp3 as a
left join #ed_yale_final as b
on a.claim_header_id = b.claim_header_id;