--Code to load data to stage.mcaid_mcare_claim_procedure
--Union of mcaid and mcare claim tables
--Eli Kern (PHSKC-APDE)
--2020-02
--Run time: X min

-------------------
--STEP 1: Union mcaid and mcare tables and insert into table shell
-------------------
insert into PHClaims.stage.mcaid_mcare_claim_procedure with (tablock)

--Medicaid claims
select
top 100
b.id_apde
,'mcaid' as source_desc
,cast(a.claim_header_id as varchar(255)) --because mcare uses alpha characters
,a.first_service_date
,a.last_service_date
,a.procedure_code
,a.procedure_code_number
,a.modifier_1
,a.modifier_2
,a.modifier_3
,a.modifier_4
,filetype_mcare = null
,getdate() as last_run
from PHClaims.final.mcaid_claim_procedure as a
left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
on a.id_mcaid = b.id_mcaid

union

--Medicare claims
select
top 100
b.id_apde
,'mcare' as source_desc
,a.claim_header_id
,first_service_date
,last_service_date
,a.procedure_code
,a.procedure_code_number
,a.modifier_1
,a.modifier_2
,a.modifier_3
,a.modifier_4
,a.filetype_mcare
,getdate() as last_run
from PHClaims.final.mcare_claim_procedure as a
left join PHClaims.final.xwalk_apde_mcaid_mcare_pha as b
on a.id_mcare = b.id_mcare;