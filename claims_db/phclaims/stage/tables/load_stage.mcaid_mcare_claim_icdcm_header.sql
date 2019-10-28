--Code to load data to stage.mcaid_mcare_claim_icdcm_header
--Union of mcaid and mcare claim ICD-CM header tables
--Eli Kern (PHSKC-APDE)
--2019-10
--Run time: X min

-------------------
--STEP 1: Union mcaid and mcare claim ICD-CM header tables and insert into table shell
-------------------
insert into PHClaims.stage.mcaid_mcare_claim_icdcm_header with (tablock)

--Medicaid claim ICD-CM header
select
b.id_apde
,'mcaid' as source_desc
,cast(a.claim_header_id as varchar(255)) --because mcare uses alpha characters
,a.first_service_date
,a.last_service_date
,a.icdcm_raw
,a.icdcm_norm
,a.icdcm_version
,a.icdcm_number
,file_type_mcare = null
,getdate() as last_run
from PHClaims.final.mcaid_claim_icdcm_header as a
left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
on a.id_mcaid = b.id_mcaid

union

--Medicare claim ICD-CM header
select
b.id_apde
,'mcare' as source_desc
,a.claim_header_id
,first_service_date = null
,last_service_date = null
,a.icdcm_raw
,a.icdcm_norm
,a.icdcm_version
,cast(a.icdcm_number as varchar(255)) collate SQL_Latin1_General_CP1_CI_AS --resolve collation conflict
,a.filetype as file_type_mcare
,getdate() as last_run
from PHClaims.final.mcare_claim_icdcm_header as a
left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
on a.id_mcare = b.id_mcare;