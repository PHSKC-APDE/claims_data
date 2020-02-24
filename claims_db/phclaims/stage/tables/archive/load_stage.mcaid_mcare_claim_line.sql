--Code to load data to stage.mcaid_mcare_claim_line
--Union of mcaid and mcare claim line tables
--Eli Kern (PHSKC-APDE)
--2020-02
--Run time: X min

-------------------
--STEP 1: Union mcaid and mcare tables and insert into table shell
-------------------
insert into PHClaims.stage.mcaid_mcare_claim_line with (tablock)

--Medicaid claim ICD-CM header
select
top 100
b.id_apde
,'mcaid' as source_desc
,cast(a.claim_header_id as varchar(255)) --because mcare uses alpha characters
,a.claim_line_id
,a.first_service_date
,a.last_service_date
,a.rev_code as revenue_code
,place_of_service_code = null
,type_of_service = null
,a.rac_code_line
,filetype_mcare = null
,getdate() as last_run
from PHClaims.final.mcaid_claim_line as a
left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
on a.id_mcaid = b.id_mcaid

union

--Medicare claim ICD-CM header
select
top 100
b.id_apde
,'mcare' as source_desc
,a.claim_header_id
,a.claim_line_id
,first_service_date
,last_service_date
,a.revenue_code
,a.place_of_service_code
,a.type_of_service
,rac_code_line = null
,a.filetype_mcare
,getdate() as last_run
from PHClaims.final.mcare_claim_line as a
left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
on a.id_mcare = b.id_mcare;