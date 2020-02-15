--Code to load data to stage.mcare_claim_header table
--Union of mcaid and mcare claim header tables
--Eli Kern (PHSKC-APDE)
--2020-02
--Run time: X min


------------------
--STEP 1: Union mcaid and mcare tables to prepare for re-assignment of unique IDs for health care event concepts
-------------------

if object_id('tempdb..#temp1') is not null drop table #temp1;

--Medicaid claims
select
top 100
b.id_apde
,'mcaid' as source_desc
,cast(a.claim_header_id as varchar(255)) --because mcare uses alpha characters
,a.clm_type_mcaid_id as claim_type_mcaid_id
,claim_type_mcare_id = null
,a.claim_type_id
,a.first_service_date
,a.last_service_date
,a.patient_status
,patient_status_code = null
,a.place_of_service_code
,a.type_of_bill_code
,facility_type_code = null
,service_type_code = null
,a.clm_status_code as claim_status_code
,a.billing_provider_npi
,a.primary_diagnosis
,a.icdcm_version
,a.ed_perform_id
,a.ed_pophealth_id
,a.inpatient as inpatient_id
,a.admsn_source as admission_source
,admission_type = null
,a.admsn_date as admission_date
,a.admsn_time as admission_time
,a.dschrg_date as discharge_date
,a.drvd_drg_code as drg_code
,hospice_from_date = null
,pc_visit_id = null
,a.ccs
,a.ccs_description
,a.ccs_description_plain_lang
,a.ccs_mult1
,a.ccs_mult1_description
,a.ccs_mult2
,a.ccs_mult2_description
,a.ccs_mult2_plain_lang
,a.ccs_final_description
,a.ccs_final_plain_lang
,filetype_mcare = null
,getdate() as last_run
into #temp1
from PHClaims.final.mcaid_claim_header as a
left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
on a.id_mcaid = b.id_mcaid

union

--Medicare claims
select
top 100
b.id_apde
,'mcare' as source_desc
,a.claim_header_id
,clm_type_mcaid_id = null
,a.claim_type_mcare_id
,a.claim_type_id
,a.first_service_date
,a.last_service_date
,a.patient_status
,a.patient_status_code
,place_of_service_code = null
,type_of_bill_code = null
,a.facility_type_code 
,a.service_type_code
,claim_status_code = null
,billing_provider_npi = null
,a.primary_diagnosis
,a.icdcm_version
,a.ed_perform_id
,a.ed_pophealth_id
,a.inpatient_id
,a.ipt_admission_source as admission_source
,a.ipt_admission_type as admission_type
,a.admission_date
,admission_time = null
,a.discharge_date
,a.drg_code
,a.hospice_from_date
,a.pc_visit_id
,ccs = null
,ccs_description = null
,ccs_description_plain_lang = null
,ccs_mult1 = null
,ccs_mult1_description = null
,ccs_mult2 = null
,ccs_mult2_description = null
,ccs_mult2_plain_lang = null
,ccs_final_description = null
,ccs_final_plain_lang = null
,a.filetype_mcare
,getdate() as last_run
from PHClaims.final.mcare_claim_header as a
left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
on a.id_mcare = b.id_mcare;











/*


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