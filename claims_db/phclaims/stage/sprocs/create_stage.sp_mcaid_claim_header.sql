
USE [PHClaims];
GO

IF OBJECT_ID('[stage].[sp_mcaid_claim_header]','P') IS NOT NULL
DROP PROCEDURE [stage].[sp_mcaid_claim_header];
GO
CREATE PROCEDURE [stage].[sp_mcaid_claim_header]
AS
SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN
/*
This code creates table ([tmp].[mcaid_claim_header]) to hold DISTINCT 
header-level claim information in long format for Medicaid claims data

SQL script created by: Eli Kern, APDE, PHSKC, 2018-03-21
R functions created by: Alastair Matheson, PHSKC (APDE), 2019-05
Modified by: Philip Sylling, 2019-06-13

Data Pull Run time: XX min
Create Index Run Time: XX min

Returns
[stage].[mcaid_claim_header]

/* Header-level columns from [stage].[mcaid_claim] */
 [id_mcaid]
,[claim_header_id]
,[clm_type_mcaid_id]
,[claim_type_id]
,[first_service_date]
,[last_service_date]
,[patient_status]
,[admsn_source]
,[admsn_date]
,[admsn_time]
,[dschrg_date]
,[place_of_service_code]
,[type_of_bill_code]
,[clm_status_code]
,[billing_provider_npi]
,[drvd_drg_code]
,[insrnc_cvrg_code]
,[last_pymnt_date]
,[bill_date]
,[system_in_date]
,[claim_header_id_date]

/* Derived claim event flag columns (formerly columns from [mcaid_claim_summary]) */

,[primary_diagnosis]
,[icdcm_version]
,[primary_diagnosis_poa]
,[mental_dx1]
,[mental_dxany]
,[mental_dx_rda_any]
,[sud_dx_rda_any]
,[maternal_dx1]
,[maternal_broad_dx1]
,[newborn_dx1]
,[ed]
,[ed_nohosp]
,[ed_bh]
,[ed_avoid_ca]
,[ed_avoid_ca_nohosp]
,[ed_ne_nyu]
,[ed_pct_nyu]
,[ed_pa_nyu]
,[ed_npa_nyu]
,[ed_mh_nyu]
,[ed_sud_nyu]
,[ed_alc_nyu]
,[ed_injury_nyu]
,[ed_unclass_nyu]
,[ed_emergent_nyu]
,[ed_nonemergent_nyu]
,[ed_intermediate_nyu]
,[inpatient]
,[ipt_medsurg]
,[ipt_bh]
,[intent]
,[mechanism]
,[sdoh_any]
,[ed_sdoh]
,[ipt_sdoh]
,[ccs]
,[ccs_description]
,[ccs_description_plain_lang]
,[ccs_mult1]
,[ccs_mult1_description]
,[ccs_mult2]
,[ccs_mult2_description]
,[ccs_mult2_plain_lang]
,[ccs_final_description]
,[ccs_final_plain_lang]

,[last_run]
*/

if object_id('[tmp].[mcaid_claim_header]', 'U') is not null
drop table [tmp].[mcaid_claim_header];

select distinct 
--top(1000)
 cast([MEDICAID_RECIPIENT_ID] as varchar(255)) as id_mcaid
,cast([TCN] as bigint) as claim_header_id
,cast([CLM_TYPE_CID] as varchar(20)) as clm_type_mcaid_id
,cast(ref.[kc_clm_type_id] as tinyint) as claim_type_id
,cast([FROM_SRVC_DATE] as date) as first_service_date
,cast([TO_SRVC_DATE] as date) as last_service_date
,cast([PATIENT_STATUS_LKPCD] as varchar(255)) as patient_status
,cast([ADMSN_SOURCE_LKPCD] as varchar(255)) as admsn_source
,cast([ADMSN_DATE] as date) as admsn_date
,cast(timefromparts([ADMSN_HOUR] / 100, [ADMSN_HOUR] % 100, 0, 0, 0) as time(0)) as admsn_time
,cast([DSCHRG_DATE] as date) as dschrg_date
,cast([FCLTY_TYPE_CODE] as varchar(255)) as place_of_service_code
,cast([TYPE_OF_BILL] as varchar(255)) as type_of_bill_code
,cast([CLAIM_STATUS] as tinyint) as clm_status_code
,cast(case when [CLAIM_STATUS] = 71 then [BLNG_NATIONAL_PRVDR_IDNTFR] 
           when ([CLAIM_STATUS] = 83 and [NPI] is not null) then [NPI] 
		   when ([CLAIM_STATUS] = 83 and [NPI] is null) then [BLNG_NATIONAL_PRVDR_IDNTFR] 
 end as bigint) as billing_provider_npi
,cast([DRVD_DRG_CODE] as varchar(255)) as drvd_drg_code
,cast([PRIMARY_DIAGNOSIS_POA_LKPCD] as varchar(255)) as primary_diagnosis_poa
,cast([INSRNC_CVRG_CODE] as varchar(255)) as insrnc_cvrg_code
,cast([LAST_PYMNT_DATE] as date) as last_pymnt_date
,cast([BILL_DATE] as date) as bill_date
,cast([SYSTEM_IN_DATE] as date) as system_in_date
,cast([TCN_DATE] as date) as claim_header_id_date

into [tmp].[mcaid_claim_header]
from [stage].[mcaid_claim] as clm
left join [ref].[kc_claim_type_crosswalk] as ref
on cast(clm.[CLM_TYPE_CID] as varchar(20)) = ref.[source_clm_type_id];

--------------------------------------
--STEP 0: Remove one duplicate row from [PHClaims].[ref].[dx_lookup]
--------------------------------------
if object_id('tempdb..#dx_lookup') is not null
drop table #dx_lookup;
with cte as
(
SELECT *
	  ,ROW_NUMBER() OVER(PARTITION BY [dx_ver], [dx] ORDER BY [dx_ver], [dx]) AS [row_num]
FROM [PHClaims].[ref].[dx_lookup]
)
SELECT *
into #dx_lookup
from cte
where [row_num] = 1;
create unique clustered index idx_cl_dx_lookup on #dx_lookup(dx_ver, dx);

--------------------------------------
--STEP 1: select header-level information needed for event flags
--------------------------------------
if object_id('tempdb..#header') is not null 
drop table #header;
select 
 id_mcaid
,claim_header_id
,clm_type_mcaid_id
,claim_type_id
,first_service_date
,last_service_date
,patient_status
,admsn_source
,admsn_date
,admsn_time
,dschrg_date
,place_of_service_code
,type_of_bill_code
,clm_status_code
,billing_provider_npi
,drvd_drg_code
,primary_diagnosis_poa
,insrnc_cvrg_code
,last_pymnt_date
,bill_date
,system_in_date
,claim_header_id_date

--inpatient stay
,case when clm_type_mcaid_id in (31,33) then 1 else 0 end as 'inpatient'
--mental health-related DRG
,case when drvd_drg_code between '876' and '897' or drvd_drg_code between '945' and '946' then 1 else 0 end as 'mh_drg'
--newborn/liveborn infant-related DRG
,case when drvd_drg_code between '789' and '795' then 1 else 0 end as 'newborn_drg'
--maternity-related DRG or type of bill
,case when type_of_bill_code in 
('840','841','842','843','844','845','847','848','84F','84G','84H','84I','84J','84K','84M','84O','84X','84Y','84Z') or drvd_drg_code between '765' and '782' 
 then 1 else 0 end as 'maternal_drg_tob'
into #header
from [tmp].[mcaid_claim_header];
create clustered index idx_cl_#header on #header(claim_header_id);

--------------------------------------
--STEP 2: select line-level information needed for event flags
--------------------------------------
if object_id('tempdb..#line') is not null 
drop table #line;
select 
 claim_header_id
--ed visits sub-flags
,max(case when rev_code like '045[01269]' or rev_code like '0981' 
          then 1 else 0 end) as 'ed_rev_code'
--maternity revenue codes
,max(case when rev_code in ('0112','0122','0132','0142','0152','0720','0721','0722','0724')
          then 1 else 0 end) as 'maternal_rev_code'
into #line
from [stage].[mcaid_claim_line]
group by claim_header_id;
create clustered index idx_cl_#line on #line(claim_header_id);

--------------------------------------
--STEP 3: select diagnosis code information needed for event flags
--------------------------------------
if object_id('tempdb..#diag') is not null 
drop table #diag;
select claim_header_id
--primary diagnosis code with version
,max(case when icdcm_number = '01' then icdcm_norm else null end) as primary_diagnosis
,max(case when icdcm_number = '01' then icdcm_version else null end) as icdcm_version
--mental health-related primary diagnosis (HEDIS 2017)
,max(case when icdcm_number = '01'
               and ((icdcm_norm between '290' and '316' and icdcm_version = 9)
			   or (icdcm_norm between 'F03' and 'F0391' and icdcm_version = 10)
			   or (icdcm_norm between 'F10' and 'F69' and icdcm_version = 10)
			   or (icdcm_norm between 'F80' and 'F99' and icdcm_version = 10))
		  then 1 else 0 end) as 'dx1_mental'
--mental health-related, any diagnosis (HEDIS 2017)
,max(case when ((icdcm_norm between '290' and '316' and icdcm_version = 9)
               or (icdcm_norm between 'F03' and 'F0391' and icdcm_version = 10)
			   or (icdcm_norm between 'F10' and 'F69' and icdcm_version = 10)
			   or (icdcm_norm between 'F80' and 'F99' and icdcm_version = 10))
	      then 1 else 0 end) as 'dxany_mental'
--newborn-related primary diagnosis (HEDIS 2017)
,max(case when icdcm_number = '01'
               and ((icdcm_norm between 'V30' and 'V39' and icdcm_version = 9)
			   or (icdcm_norm between 'Z38' and 'Z389' and icdcm_version = 10))
		  then 1 else 0 end) as 'dx1_newborn'
--maternity-related primary diagnosis (HEDIS 2017)
,max(case when icdcm_number = '01'
               	and ((icdcm_norm between '630' and '679' and icdcm_version = 9)
				or (icdcm_norm between 'V24' and 'V242' and icdcm_version = 9)
				or (icdcm_norm between 'O00' and 'O9279' and icdcm_version = 10)
				or (icdcm_norm between 'O98' and 'O9989' and icdcm_version = 10)
				or (icdcm_norm between 'O9A' and 'O9A53' and icdcm_version = 10)
				or (icdcm_norm between 'Z0371' and 'Z0379' and icdcm_version = 10)
				or (icdcm_norm between 'Z332' and 'Z3329' and icdcm_version = 10)
				or (icdcm_norm between 'Z39' and 'Z3909' and icdcm_version = 10))
		  then 1 else 0 end) as 'dx1_maternal'
--maternity-related primary diagnosis (broader)
,max(case when icdcm_number = '01'
               and ((icdcm_norm between '630' and '679' and icdcm_version = 9)
			   or (icdcm_norm between 'V20' and 'V29' and icdcm_version = 9) /*broader*/
			   or (icdcm_norm between 'O00' and 'O9279' and icdcm_version = 10)
			   or (icdcm_norm between 'O94' and 'O9989' and icdcm_version = 10) /*broader*/
			   or (icdcm_norm between 'O9A' and 'O9A53' and icdcm_version = 10)
			   or (icdcm_norm between 'Z0371' and 'Z0379' and icdcm_version = 10)
			   or (icdcm_norm between 'Z30' and 'Z392' and icdcm_version = 10) /*broader*/
			   or (icdcm_norm between 'Z3A0' and 'Z3A49' and icdcm_version = 10)) /*broader*/
	      then 1 else 0 end) as 'dx1_maternal_broad'
--SDOH-related (any diagnosis)
,max(case when icdcm_norm between 'Z55' and 'Z659' and icdcm_version = 10
          then 1 else 0 end) as 'sdoh_any'
into #diag
from [stage].[mcaid_claim_icdcm_header]
group by claim_header_id;
create clustered index idx_cl_#diag on #diag(claim_header_id);

--------------------------------------
--STEP 4: select procedure code information needed for event flags
--------------------------------------
if object_id('tempdb..#procedure_code') is not null 
drop table #procedure_code;
select 
 claim_header_id
--ed visits sub-flags
,max(case when procedure_code like '9928[123458]' then 1 else 0 end) as 'ed_pcode1'
,max(case when procedure_code between '10021' and '69990' then 1 else 0 end) as 'ed_pcode2'
into #procedure_code
from [stage].[mcaid_claim_procedure]
group by claim_header_id;
create clustered index idx_cl_#procedure_code on #procedure_code(claim_header_id);

--------------------------------------
--STEP 5: create temp summary claims table with event-based flags
--------------------------------------
if object_id('tempdb..#temp1') is not null 
drop table #temp1;
select 
 header.id_mcaid
,header.claim_header_id
,header.clm_type_mcaid_id
,header.claim_type_id
,header.first_service_date
,header.last_service_date
,header.patient_status
,header.admsn_source
,admsn_date
,admsn_time
,dschrg_date
,insrnc_cvrg_code
,last_pymnt_date
,bill_date
,system_in_date
,claim_header_id_date
,header.place_of_service_code
,header.type_of_bill_code
,header.clm_status_code
,header.billing_provider_npi
,header.drvd_drg_code
--Mental health-related primary diagnosis
,case when header.mh_drg = 1 or diag.dx1_mental = 1 then 1 else 0 end as 'mental_dx1'
--Mental health-related, any diagnosis
,case when header.mh_drg = 1 or diag.dxany_mental = 1 then 1 else 0 end as 'mental_dxany'
--Maternity-related care (primary diagnosis only)
,case when header.maternal_drg_tob = 1 or line.maternal_rev_code = 1 or diag.dx1_maternal = 1 then 1 else 0 end as 'maternal_dx1'
--Maternity-related care (primary diagnosis only), broader definition for diagnosis codes
,case when header.maternal_drg_tob = 1 or line.maternal_rev_code = 1 or diag.dx1_maternal_broad = 1 then 1 else 0 end as 'maternal_broad_dx1'
--Newborn-related care (prim. diagnosis only)
,case when header.newborn_drg = 1 or diag.dx1_newborn = 1 then 1 else 0 end as 'newborn_dx1'
--Inpatient stay flag
,header.inpatient
--ED visit (broad definition)
,case when header.clm_type_mcaid_id in (3,26,34)
           and (line.ed_rev_code = 1
		   or procedure_code.ed_pcode1 = 1
		   or (header.place_of_service_code = '23' and ed_pcode2 = 1)) then 1 else 0 end as 'ed'
--Primary diagnosis and version

,diag.primary_diagnosis
,diag.icdcm_version
,header.primary_diagnosis_poa
--SDOH flags
,diag.sdoh_any

into #temp1
from #header as header
left join #line as line 
on header.claim_header_id = line.claim_header_id
left join #diag as diag 
on header.claim_header_id = diag.claim_header_id
left join #procedure_code as procedure_code 
on header.claim_header_id = procedure_code.claim_header_id;

--------------------------------------
--STEP 6: Avoidable ED visit flag, California algorithm
--------------------------------------
if object_id('tempdb..#avoid_ca') is not null 
drop table #avoid_ca;
select 
 b.claim_header_id
,max(a.ed_avoid_ca) as 'ed_avoid_ca'
into #avoid_ca
from (select dx, dx_ver, ed_avoid_ca from [ref].[dx_lookup] where ed_avoid_ca = 1) as a
inner join (select claim_header_id, icdcm_norm, icdcm_version from [stage].[mcaid_claim_icdcm_header] where icdcm_number = '01') as b
on (a.dx_ver = b.icdcm_version) and (a.dx = b.icdcm_norm)
group by b.claim_header_id;
create clustered index [idx_cl_#avoid_ca] on #avoid_ca(claim_header_id);

--------------------------------------
--STEP 7: ED visit classification, NYU algorithm
--------------------------------------
if object_id('tempdb..#avoid_nyu') is not null 
drop table #avoid_nyu;
select 
 b.claim_header_id
,a.ed_needed_unavoid_nyu
,a.ed_needed_avoid_nyu
,a.ed_pc_treatable_nyu
,a.ed_nonemergent_nyu
,a.ed_mh_nyu
,a.ed_sud_nyu
,a.ed_alc_nyu
,a.ed_injury_nyu
,a.ed_unclass_nyu
into #avoid_nyu
--from [ref].[dx_lookup] as a
from #dx_lookup as a
inner join (select claim_header_id, icdcm_norm, icdcm_version from [stage].[mcaid_claim_icdcm_header] where icdcm_number = '01') as b
on (a.dx_ver = b.icdcm_version) and (a.dx = b.icdcm_norm);
create clustered index [idx_cl_#avoid_nyu] on #avoid_nyu(claim_header_id);

--------------------------------------
--STEP 8: CCS groupings (CCS, CCS-level 1, CCS-level 2), primary diagnosis, final categorization
--------------------------------------
if object_id('tempdb..#ccs') is not null 
drop table #ccs;
select 
 b.claim_header_id
,a.ccs
,a.ccs_description
,a.ccs_description_plain_lang
,a.multiccs_lv1
,a.multiccs_lv1_description
,a.multiccs_lv2
,a.multiccs_lv2_description
,a.multiccs_lv2_plain_lang
,a.ccs_final_code
,a.ccs_final_description
,a.ccs_final_plain_lang
into #ccs
--from [ref].[dx_lookup] as a
from #dx_lookup as a
inner join (select claim_header_id, icdcm_norm, icdcm_version from [stage].[mcaid_claim_icdcm_header] where icdcm_number = '01') as b
on (a.dx_ver = b.icdcm_version) and (a.dx = b.icdcm_norm);
create clustered index [idx_cl_#ccs] on #ccs(claim_header_id);

--------------------------------------
--STEP 9: RDA Mental health and Substance use disorder diagnosis flags, any diagnosis
--------------------------------------
if object_id('tempdb..#rda') is not null 
drop table #rda;
select 
 b.claim_header_id
,max(a.mental_dx_rda) as 'mental_dx_rda_any'
,max(a.sud_dx_rda) as 'sud_dx_rda_any'
into #rda
from [ref].[dx_lookup] as a
inner join [stage].[mcaid_claim_icdcm_header] as b
on (a.dx_ver = b.icdcm_version) and (a.dx = b.icdcm_norm)
group by b.claim_header_id;
create clustered index [idx_cl_#rda] on #rda(claim_header_id);

--------------------------------------
--STEP 10: Injury intent and mechanism, ICD9-CM
--------------------------------------
if object_id('tempdb..#injury9cm') is not null 
drop table #injury9cm;
select 
 c.claim_header_id
,c.intent
,c.mechanism
into #injury9cm
from 
(
--find external cause codes (ICD9-CM) for each TCN, then rank by diagnosis number
select 
 b.claim_header_id
,intent
,mechanism
,row_number() over (partition by b.claim_header_id order by b.icdcm_number) as 'diag_rank'
from (select dx, intent, mechanism from [ref].[dx_lookup] where intent is not null and dx_ver = 9) as a
inner join (select claim_header_id, icdcm_norm, icdcm_number from [stage].[mcaid_claim_icdcm_header] where icdcm_version = 9) as b
on (a.dx = b.icdcm_norm)
) as c
--only keep the highest ranked external cause code per claim
where c.diag_rank = 1;

--------------------------------------
--STEP 11: Injury intent and mechanism, ICD10-CM
--------------------------------------
--first identify all injury claims (primary diagnosis only)
if object_id('tempdb..#inj10_temp1') is not null 
drop table #inj10_temp1;
select 
 b.claim_header_id
into #inj10_temp1
from (select dx, injury_icd10cm from [ref].[dx_lookup] where injury_icd10cm = 1 and dx_ver = 10) as a
inner join (select claim_header_id, icdcm_norm from [stage].[mcaid_claim_icdcm_header] where icdcm_number = '01' and icdcm_version = 10) as b
on a.dx = b.icdcm_norm;

--grab the full list of diagnosis codes for these injury claims
if object_id('tempdb..#inj10_temp2') is not null 
drop table #inj10_temp2;
select 
 b.claim_header_id
,b.icdcm_norm
,b.icdcm_number
into #inj10_temp2
from #inj10_temp1 as a
inner join (select claim_header_id, icdcm_norm, icdcm_number from [stage].[mcaid_claim_icdcm_header] where icdcm_version = 10) as b
on a.claim_header_id = b.claim_header_id;

--grab the highest ranked external cause code for each injury claim
if object_id('tempdb..#injury10cm') is not null 
drop table #injury10cm;
select 
 c.claim_header_id
,c.intent
,c.mechanism
into #injury10cm
from 
(
select 
 b.claim_header_id
,intent
,mechanism
,row_number() over (partition by b.claim_header_id order by b.icdcm_number) as 'diag_rank'
from (select dx, dx_ver, intent, mechanism from [ref].[dx_lookup] where intent is not null and dx_ver = 10) as a
inner join #inj10_temp2 as b
on a.dx = b.icdcm_norm
) as c
where c.diag_rank = 1;

--------------------------------------
--STEP 12: Union ICD9-CM and ICD10-CM injury tables
--------------------------------------
if object_id('tempdb..#injury') is not null 
drop table #injury;
select 
 claim_header_id
,intent
,mechanism 
into #injury 
from #injury9cm

union

select 
 claim_header_id
,intent
,mechanism 
from #injury10cm;
create clustered index [idx_cl_#injury] on #injury(claim_header_id);

/*
--------------------------------------
--STEP 13: create flags that require comparison of previously created event-based flags across time
--------------------------------------
if object_id('tempdb..#temp2test') is not null 
drop table #temp2test;

with [ed_hosp_difference_in_days] as
(
select distinct 
 e.id_mcaid
,ed_date = e.first_service_date
,hosp_date = h.first_service_date
,claim_header_id

-- Calculate difference in days between each ED visit and ALL following inpatient stays
-- Set to NULL if inpatient stay is prior to ED visit
,case when datediff(dd, e.first_service_date, h.first_service_date) >= 0 
      then datediff(dd, e.first_service_date, h.first_service_date)
	  else null
 end as 'eh_ddiff'

from #temp1 as e
left join (select distinct id_mcaid, first_service_date from #temp1 where inpatient = 1) as h
on e.id_mcaid = h.id_mcaid
where e.ed = 1
),

[ed_nohosp] as
(
-- Select ED visits with NO inpatient stay within 1 day
-- Closest inpatient stay is > 1 day
select 
 y.id_mcaid
,y.claim_header_id
,ed_nohosp = 1
from 
(
-- Calculate difference in days between each ED visit and CLOSEST following inpatient stay
select distinct 
 x.id_mcaid
,x.claim_header_id
,min(x.eh_ddiff) as 'eh_ddiff_pmin'
from [ed_hosp_difference_in_days] as x
group by x.id_mcaid, x.claim_header_id
) as y

where y.eh_ddiff_pmin > 1 or y.eh_ddiff_pmin is null
)

select 
 temp1.*
--ED flag that rules out visits with an inpatient stay within 24hrs
,case when ed_nohosp.ed_nohosp = 1 then 1 else 0 end as 'ed_nohosp'
into #temp2test
from #temp1 as temp1
left join [ed_nohosp]
on temp1.claim_header_id = ed_nohosp.claim_header_id;
*/

--------------------------------------
--STEP 13: create flags that require comparison of previously created event-based flags across time
--------------------------------------
if object_id('tempdb..#temp2') is not null 
drop table #temp2;
select temp1.*, case when ed_nohosp.ed_nohosp = 1 then 1 else 0 end as 'ed_nohosp'
into #temp2
from #temp1 as temp1
--ED flag that rules out visits with an inpatient stay within 24hrs
left join (
	select y.id_mcaid, y.claim_header_id, ed_nohosp = 1
	from (
		--group by ID and ED visit date and take minimum difference to get closest inpatient stay
		select distinct x.id_mcaid, x.claim_header_id, min(x.eh_ddiff) as 'eh_ddiff_pmin'
		from (
			select distinct e.id_mcaid, ed_date = e.first_service_date, hosp_date = h.first_service_date, claim_header_id,
				--create field that calculates difference in days between each ED visit and following inpatient stay
				--set to null when comparison is between ED visits and PRIOR inpatient stays
				case
					when datediff(dd, e.first_service_date, h.first_service_date) >=0 then datediff(dd, e.first_service_date, h.first_service_date)
					else null
				end as 'eh_ddiff'
			from #temp1 as e
			left join (
				select distinct id_mcaid, first_service_date
				from #temp1
				where inpatient = 1
			) as h
			on e.id_mcaid = h.id_mcaid
			where e.ed = 1
		) as x
		group by x.id_mcaid, x.claim_header_id
	) as y
	where y.eh_ddiff_pmin > 1 or y.eh_ddiff_pmin is null
) ed_nohosp
on temp1.claim_header_id = ed_nohosp.claim_header_id;
create clustered index [idx_cl_#temp2] on #temp2(claim_header_id);

--------------------------------------
--STEP 14: create final table structure
--------------------------------------
IF object_id('[stage].[mcaid_claim_header]', 'U') is not null 
drop table [stage].[mcaid_claim_header]
create table [stage].[mcaid_claim_header]
(id_mcaid varchar(255)
,claim_header_id bigint
,clm_type_mcaid_id varchar(20)
,claim_type_id tinyint
,first_service_date date
,last_service_date date
,patient_status varchar(255)
,admsn_source varchar(255)
,admsn_date date
,admsn_time time(0)
,dschrg_date date
,place_of_service_code varchar(255)
,type_of_bill_code varchar(255)
,clm_status_code tinyint
,billing_provider_npi bigint
,drvd_drg_code varchar(255)
,insrnc_cvrg_code varchar(255)
,last_pymnt_date date
,bill_date date
,system_in_date date
,claim_header_id_date date
,primary_diagnosis varchar(255)
,icdcm_version tinyint
,primary_diagnosis_poa varchar(255)
,mental_dx1 tinyint
,mental_dxany tinyint
,mental_dx_rda_any tinyint
,sud_dx_rda_any tinyint
,maternal_dx1 tinyint
,maternal_broad_dx1 tinyint
,newborn_dx1 tinyint
,ed tinyint
,ed_nohosp tinyint
,ed_bh tinyint
,ed_avoid_ca tinyint
,ed_avoid_ca_nohosp tinyint
,ed_ne_nyu tinyint
,ed_pct_nyu tinyint
,ed_pa_nyu tinyint
,ed_npa_nyu tinyint
,ed_mh_nyu tinyint
,ed_sud_nyu tinyint
,ed_alc_nyu tinyint
,ed_injury_nyu tinyint
,ed_unclass_nyu tinyint
,ed_emergent_nyu tinyint
,ed_nonemergent_nyu tinyint
,ed_intermediate_nyu tinyint
,inpatient tinyint
,ipt_medsurg tinyint
,ipt_bh tinyint
,intent varchar(255)
,mechanism varchar(255)
,sdoh_any tinyint
,ed_sdoh tinyint
,ipt_sdoh tinyint
,ccs varchar(255)
,ccs_description varchar(500)
,ccs_description_plain_lang varchar(500)
,ccs_mult1 varchar(255)
,ccs_mult1_description varchar(500)
,ccs_mult2 varchar(255)
,ccs_mult2_description varchar(500)
,ccs_mult2_plain_lang varchar(500)
,ccs_final_description varchar(500)
,ccs_final_plain_lang varchar(500)
,last_run datetime)
ON [PRIMARY];

--------------------------------------
--STEP 15: create final summary claims table with all event-based flags (temp table stage)
--------------------------------------
--if object_id('tempdb..#temp_final') is not null 
--drop table #temp_final;
-- Using a temp table because you don't seem to be able to cast a not null variable
with [temp_final] as
(
select 
 a.*
--ED-related flags
,case when a.ed = 1 and a.mental_dxany = 1 then 1 else 0 end as 'ed_bh'
,case when a.ed = 1 and b.ed_avoid_ca = 1 then 1 else 0 end as 'ed_avoid_ca'
,case when a.ed_nohosp = 1 and b.ed_avoid_ca = 1 then 1 else 0 end as 'ed_avoid_ca_nohosp'

--original nine categories of NYU ED algorithm
,case when a.ed = 1 and c.ed_nonemergent_nyu > 0.50 then 1 else 0 end as 'ed_ne_nyu'
,case when a.ed = 1 and c.ed_pc_treatable_nyu > 0.50 then 1 else 0 end as 'ed_pct_nyu'
,case when a.ed = 1 and c.ed_needed_avoid_nyu > 0.50 then 1 else 0 end as 'ed_pa_nyu'
,case when a.ed = 1 and c.ed_needed_unavoid_nyu > 0.50 then 1 else 0 end as 'ed_npa_nyu'
,case when a.ed = 1 and c.ed_mh_nyu > 0.50 then 1 else 0 end as 'ed_mh_nyu'
,case when a.ed = 1 and c.ed_sud_nyu > 0.50 then 1 else 0 end as 'ed_sud_nyu'
,case when a.ed = 1 and c.ed_alc_nyu > 0.50 then 1 else 0 end as 'ed_alc_nyu'
,case when a.ed = 1 and c.ed_injury_nyu > 0.50 then 1 else 0 end as 'ed_injury_nyu'

,case when a.ed = 1 and ((c.ed_unclass_nyu > 0.50)  or (c.ed_nonemergent_nyu <= 0.50 and c.ed_pc_treatable_nyu <= 0.50
           and c.ed_needed_avoid_nyu <= 0.50 and c.ed_needed_unavoid_nyu <= 0.50 and c.ed_mh_nyu <= 0.50 and c.ed_sud_nyu <= 0.50
		   and c.ed_alc_nyu <= 0.50 and c.ed_injury_nyu <= 0.50 and c.ed_unclass_nyu <= 0.50))
	  then 1 else 0 end as 'ed_unclass_nyu'

--collapsed 3 categories of NYU ED algorithm based on Ghandi et al.
,case when a.ed = 1 and (c.ed_needed_unavoid_nyu + c.ed_needed_avoid_nyu) > 0.50 then 1 else 0 end as 'ed_emergent_nyu'
,case when a.ed = 1 and (c.ed_pc_treatable_nyu + c.ed_nonemergent_nyu) > 0.50 then 1 else 0 end as 'ed_nonemergent_nyu'
,case when a.ed = 1 and (((c.ed_needed_unavoid_nyu + c.ed_needed_avoid_nyu) = 0.50) or 
			((c.ed_pc_treatable_nyu + c.ed_nonemergent_nyu) = 0.50)) then 1 else 0 end as 'ed_intermediate_nyu'

--Inpatient-related flags
,case when a.inpatient = 1 and a.mental_dx1 = 0 and a.newborn_dx1 = 0 and a.maternal_dx1 = 0 then 1 else 0 end as 'ipt_medsurg'
,case when a.inpatient = 1 and a.mental_dxany = 1 then 1 else 0 end as 'ipt_bh'

--Injuries
,f.intent
,f.mechanism

--CCS
,d.ccs
,d.ccs_description
,d.ccs_description_plain_lang
,d.multiccs_lv1 as 'ccs_mult1'
,d.multiccs_lv1_description as 'ccs_mult1_description'
,d.multiccs_lv2 as 'ccs_mult2'
,d.multiccs_lv2_description as 'ccs_mult2_description'
,d.multiccs_lv2_plain_lang as 'ccs_mult2_plain_lang'
,d.ccs_final_description
,d.ccs_final_plain_lang

--RDA MH and SUD flags
,case when e.mental_dx_rda_any = 1 then 1 else 0 end as 'mental_dx_rda_any'
,case when e.sud_dx_rda_any = 1 then 1 else 0 end as 'sud_dx_rda_any'

--SDOH ED and IPT flags
,case when a.ed = 1 and a.sdoh_any = 1 then 1 else 0 end as 'ed_sdoh'
,case when a.inpatient = 1 and a.sdoh_any = 1 then 1 else 0 end as 'ipt_sdoh'

--into #temp_final
from #temp2 as a
left join #avoid_ca as b
on a.claim_header_id = b.claim_header_id
left join #avoid_nyu as c
on a.claim_header_id = c.claim_header_id
left join #ccs as d
on a.claim_header_id = d.claim_header_id
left join #rda as e
on a.claim_header_id = e.claim_header_id
left join #injury as f
on a.claim_header_id = f.claim_header_id
)

--------------------------------------
--STEP 16: copy final temp table into summary claims table
--------------------------------------

insert into [stage].[mcaid_claim_header] with (tablock)
([id_mcaid]
,[claim_header_id]
,[clm_type_mcaid_id]
,[claim_type_id]
,[first_service_date]
,[last_service_date]
,[patient_status]
,[admsn_source]
,[admsn_date]
,[admsn_time]
,[dschrg_date]
,[place_of_service_code]
,[type_of_bill_code]
,[clm_status_code]
,[billing_provider_npi]
,[drvd_drg_code]
,[insrnc_cvrg_code]
,[last_pymnt_date]
,[bill_date]
,[system_in_date]
,[claim_header_id_date]
,[primary_diagnosis]
,[icdcm_version]
,[primary_diagnosis_poa]
,[mental_dx1]
,[mental_dxany]
,[mental_dx_rda_any]
,[sud_dx_rda_any]
,[maternal_dx1]
,[maternal_broad_dx1]
,[newborn_dx1]
,[ed]
,[ed_nohosp]
,[ed_bh]
,[ed_avoid_ca]
,[ed_avoid_ca_nohosp]
,[ed_ne_nyu]
,[ed_pct_nyu]
,[ed_pa_nyu]
,[ed_npa_nyu]
,[ed_mh_nyu]
,[ed_sud_nyu]
,[ed_alc_nyu]
,[ed_injury_nyu]
,[ed_unclass_nyu]
,[ed_emergent_nyu]
,[ed_nonemergent_nyu]
,[ed_intermediate_nyu]
,[inpatient]
,[ipt_medsurg]
,[ipt_bh]
,[intent]
,[mechanism]
,[sdoh_any]
,[ed_sdoh]
,[ipt_sdoh]
,[ccs]
,[ccs_description]
,[ccs_description_plain_lang]
,[ccs_mult1]
,[ccs_mult1_description]
,[ccs_mult2]
,[ccs_mult2_description]
,[ccs_mult2_plain_lang]
,[ccs_final_description]
,[ccs_final_plain_lang]
,[last_run])

select
--top(100) *

 [id_mcaid]
,[claim_header_id]
,[clm_type_mcaid_id]
,[claim_type_id]
,[first_service_date]
,[last_service_date]
,[patient_status]
,[admsn_source]
,[admsn_date]
,[admsn_time]
,[dschrg_date]
,[place_of_service_code]
,[type_of_bill_code]
,[clm_status_code]
,[billing_provider_npi]
,[drvd_drg_code]
,[insrnc_cvrg_code]
,[last_pymnt_date]
,[bill_date]
,[system_in_date]
,[claim_header_id_date]
,[primary_diagnosis]
,[icdcm_version]
,[primary_diagnosis_poa]
,[mental_dx1]
,[mental_dxany]
,[mental_dx_rda_any]
,[sud_dx_rda_any]
,[maternal_dx1]
,[maternal_broad_dx1]
,[newborn_dx1]
,[ed]
,[ed_nohosp]
,[ed_bh]
,[ed_avoid_ca]
,[ed_avoid_ca_nohosp]
,[ed_ne_nyu]
,[ed_pct_nyu]
,[ed_pa_nyu]
,[ed_npa_nyu]
,[ed_mh_nyu]
,[ed_sud_nyu]
,[ed_alc_nyu]
,[ed_injury_nyu]
,[ed_unclass_nyu]
,[ed_emergent_nyu]
,[ed_nonemergent_nyu]
,[ed_intermediate_nyu]
,[inpatient]
,[ipt_medsurg]
,[ipt_bh]
,[intent]
,[mechanism]
,[sdoh_any]
,[ed_sdoh]
,[ipt_sdoh]
,[ccs]
,[ccs_description]
,[ccs_description_plain_lang]
,[ccs_mult1]
,[ccs_mult1_description]
,[ccs_mult2]
,[ccs_mult2_description]
,[ccs_mult2_plain_lang]
,[ccs_final_description]
,[ccs_final_plain_lang]
,getdate() as [last_run]

--from #temp_final;
from [temp_final];
END
GO