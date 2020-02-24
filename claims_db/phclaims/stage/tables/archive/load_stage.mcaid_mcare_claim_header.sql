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
,cast(a.claim_header_id as varchar(255)) as claim_header_id --because mcare uses alpha characters
,a.clm_type_mcaid_id as claim_type_mcaid_id
,claim_type_mcare_id = null
,a.claim_type_id
,filetype_mcare = null
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
,a.pc_visit_id
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
,a.mental_dx1
,a.mental_dxany
,a.mental_dx_rda_any
,a.sud_dx_rda_any
,a.maternal_dx1
,a.maternal_broad_dx1
,a.newborn_dx1
,a.ipt_medsurg
,a.ipt_bh
,a.ipt_sdoh
,a.ed_sdoh
,a.sdoh_any
,a.intent
,a.mechanism
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
,claim_type_mcaid_id = null
,a.claim_type_mcare_id
,a.claim_type_id
,a.filetype_mcare
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
,c.ccs
,c.ccs_description
,c.ccs_description_plain_lang
,c.multiccs_lv1 as ccs_mult1
,c.multiccs_lv1_description as ccs_mult1_description
,c.multiccs_lv2 as ccs_mult2
,c.multiccs_lv2_description as ccs_mult2_description
,c.multiccs_lv2_plain_lang as ccs_mult2_plain_lang
,c.ccs_final_description
,c.ccs_final_plain_lang
,mental_dx1 = null
,mental_dxany = null
,mental_dx_rda_any = null
,sud_dx_rda_any = null
,maternal_dx1 = null
,maternal_broad_dx1 = null
,newborn_dx1 = null
,ipt_medsurg = null
,ipt_bh = null
,ipt_sdoh = null
,ed_sdoh = null
,sdoh_any = null
,intent = null
,mechanism = null
from PHClaims.final.mcare_claim_header as a
left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
on a.id_mcare = b.id_mcare
--join to ICD-CM lookup table to create some columns
left join PHClaims.ref.dx_lookup as c
on (a.primary_diagnosis = c.dx) and (a.icdcm_version = c.dx_ver);


----------------
--STEP 2: Assign unique ID to healthcare utilization concepts that are grouped by person, service date
-------------------
if object_id('tempdb..#temp2') is not null drop table #temp2;
select 
id_apde
,source_desc
,claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id
,claim_type_mcaid_id
,claim_type_mcare_id
,claim_type_id
,filetype_mcare
,first_service_date
,last_service_date
,patient_status
,patient_status_code
,place_of_service_code
,type_of_bill_code
,facility_type_code 
,service_type_code
,claim_status_code
,billing_provider_npi
,primary_diagnosis
,icdcm_version

--ED performance (RDA measure)
,case when ed_perform_id is null then null
else dense_rank() over
	(order by case when ed_perform_id is null then 2 else 1 end, --sorts non-relevant claims to bottom
	id_apde, first_service_date)
end as ed_perform_id

--Recreate Yale ED carrier, outpatient and inpatient flags
,case when ed_pophealth_id is not null and claim_type_id = 5 then 1 else 0 end as ed_yale_carrier
,case when ed_pophealth_id is not null and claim_type_id = 4 then 1 else 0 end as ed_yale_opt
,case when ed_pophealth_id is not null and claim_type_id = 1 then 1 else 0 end as ed_yale_ipt

--inpatient stays
,case when (inpatient_id = 0 or inpatient_id is null) then null
else dense_rank() over
	(order by case when (inpatient_id = 0 or inpatient_id is null) then 2 else 1 end, --sorts non-relevant claims to bottom
	id_apde, discharge_date)
end as inpatient_id

,admission_source
,admission_type
,admission_date
,admission_time
,discharge_date
,drg_code
,hospice_from_date

--primary care visits
,case when pc_visit_id is null then null
else dense_rank() over
	(order by case when pc_visit_id is null then 2 else 1 end, --sorts non-relevant claims to bottom
	id_apde, first_service_date)
end as pc_visit_id

,ccs
,ccs_description
,ccs_description_plain_lang
,ccs_mult1
,ccs_mult1_description
,ccs_mult2
,ccs_mult2_description
,ccs_mult2_plain_lang
,ccs_final_description
,ccs_final_plain_lang
,mental_dx1
,mental_dxany
,mental_dx_rda_any
,sud_dx_rda_any
,maternal_dx1
,maternal_broad_dx1
,newborn_dx1
,ipt_medsurg
,ipt_bh
,ipt_sdoh
,ed_sdoh
,sdoh_any
,intent
,mechanism

into #temp2
from #temp1;

--drop other temp tables to make space
if object_id('tempdb..#temp1') is not null drop table #temp1;


------------------
--STEP 3: Conduct overlap and clustering for ED population health measure (Yale measure)
--Adaptation of Philip's Medicaid code, which is adaptation of Eli's original code
-------------------

-----
--Union carrier, outpatient and inpatient ED visits
-----
--extract carrier ED visits and create left and right matching windows
if object_id('tempdb..#ed_yale_1') is not null drop table #ed_yale_1;
select id_apde,
claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id,
first_service_date, last_service_date, 'Carrier' as ed_type
into #ed_yale_1
from #temp2
where ed_yale_carrier = 1

union
select id_apde,
claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id,
first_service_date, last_service_date, 'Outpatient' as ed_type
from #temp2 where ed_yale_opt = 1

union
select id_apde,
claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id,
first_service_date, last_service_date, 'Inpatient' as ed_type
from #temp2 where ed_yale_ipt = 1;

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
id_apde
,claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id
-- If [prior_first_service_date] IS NULL, then it is the first chronological [first_service_date] for the person
,LAG([first_service_date]) OVER(PARTITION BY [id_apde] ORDER BY [first_service_date], [last_service_date], [claim_header_id]) AS [prior_first_service_date]
,[first_service_date]
,[last_service_date]
,[ed_type]
-- Number of days between consecutive rows
,DATEDIFF(DAY, LAG([first_service_date]) OVER(PARTITION BY [id_apde] 
 ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) AS [date_diff]
/*
Create a chronological (0, 1) indicator column.
If 0, it is the first ED visit for the person OR the ED visit appears to be a duplicate
(overlapping service dates) of the prior visit.
If 1, the prior ED visit appears to be distinct from the following stay.
This indicator column will be summed to create an episode_id.
*/
,CASE WHEN ROW_NUMBER() OVER(PARTITION BY [id_apde] 
      ORDER BY [first_service_date], [last_service_date], [claim_header_id]) = 1 THEN 0
      WHEN DATEDIFF(DAY, LAG([first_service_date]) OVER(PARTITION BY [id_apde]
	  ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) <= @match_window THEN 0
	  WHEN DATEDIFF(DAY, LAG(first_service_date) OVER(PARTITION BY [id_apde]
	  ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) > @match_window THEN 1
 END AS [increment]
FROM #ed_yale_1
--ORDER BY [id_apde], [first_service_date], [last_service_date], [claim_header_id]
),

/*
Sum [increment] column (Cumulative Sum) within person to create an stay_id that
combines duplicate/overlapping ED visits.
*/
[create_within_person_stay_id] AS
(
SELECT
id_apde
,claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id
,[prior_first_service_date]
,[first_service_date]
,[last_service_date]
,[ed_type]
,[date_diff]
,[increment]
,SUM([increment]) OVER(PARTITION BY [id_apde] ORDER BY [first_service_date], [last_service_date], [claim_header_id] ROWS UNBOUNDED PRECEDING) + 1 AS [within_person_stay_id]
FROM [increment_stays_by_person]
--ORDER BY [id_apde], [first_service_date], [last_service_date], [claim_header_id]
)

SELECT
id_apde
,claim_header_id collate SQL_Latin1_General_Cp1_CS_AS as claim_header_id
,[prior_first_service_date]
,[first_service_date]
,[last_service_date]
,[ed_type]
,[date_diff]
,[increment]
,[within_person_stay_id]
,DENSE_RANK() OVER(ORDER BY [id_apde], [within_person_stay_id]) AS [ed_pophealth_id]

,FIRST_VALUE([first_service_date]) OVER(PARTITION BY [id_apde], [within_person_stay_id] 
 ORDER BY [id_apde], [within_person_stay_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_first_service_date]
,LAST_VALUE([last_service_date]) OVER(PARTITION BY [id_apde], [within_person_stay_id] 
 ORDER BY [id_apde], [within_person_stay_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_last_service_date]

INTO #ed_yale_final
FROM [create_within_person_stay_id]
ORDER BY id_apde, [first_service_date], [last_service_date], [claim_header_id];

--drop other temp tables to make space
if object_id('tempdb..#ed_yale_1') is not null drop table #ed_yale_1;


------------------
--STEP 9: Join back Yale table with header table on claim header ID
-------------------
insert into PHClaims.stage.mcaid_mcare_claim_header with (tablock)
select distinct
a.id_apde
,a.source_desc
,a.claim_header_id
,a.claim_type_mcaid_id
,a.claim_type_mcare_id
,a.claim_type_id
,a.filetype_mcare
,a.first_service_date
,a.last_service_date
,a.patient_status
,a.patient_status_code
,a.place_of_service_code
,a.type_of_bill_code
,a.facility_type_code
,a.service_type_code
,a.claim_status_code
,a.billing_provider_npi
,a.primary_diagnosis
,a.icdcm_version
,a.ed_perform_id
,b.ed_pophealth_id
,a.inpatient_id
,a.admission_source
,a.admission_type
,a.admission_date
,a.admission_time
,a.discharge_date
,a.drg_code
,a.hospice_from_date
,a.pc_visit_id
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
,a.mental_dx1
,a.mental_dxany
,a.mental_dx_rda_any
,a.sud_dx_rda_any
,a.maternal_dx1
,a.maternal_broad_dx1
,a.newborn_dx1
,a.ipt_medsurg
,a.ipt_bh
,a.ipt_sdoh
,a.ed_sdoh
,a.sdoh_any
,a.intent
,a.mechanism
,getdate() as last_run
from #temp2 as a
left join #ed_yale_final as b
on a.claim_header_id = b.claim_header_id;