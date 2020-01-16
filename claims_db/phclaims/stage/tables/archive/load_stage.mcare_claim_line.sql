--Code to load data to stage.mcare_claim_line table
--Distinct line-level claim variables that do not have a dedicated table (e.g. revenue code). In other words elements for which there is only one distinct 
--value per claim line.
--Eli Kern (PHSKC-APDE)
--2020-01
--Run time: XX min

------------------
--STEP 1: Select (distinct) desired columns from multi-year claim tables on stage schema
--Exclude all denied claims using proposed approach per ResDAC 01-2020 consult
-------------------
insert into PHClaims.stage.mcare_claim_line with (tablock)

--bcarrier
select
top 100
a.id_mcare,
a.claim_header_id,
a.claim_line_id,
b.first_service_date,
b.last_service_date,
revenue_code = null,
a.place_of_service_code,
a.type_of_service,
getdate() as last_run
from PHClaims.stage.mcare_bcarrier_line as a
left join PHClaims.stage.mcare_bcarrier_claims as b
on a.claim_header_id = b.claim_header_id
--exclude denined claims using carrier/dme claim method
where b.denial_code in ('1','2','3','4','5','6','7','8','9')

--dme
union
select
top 100
a.id_mcare,
a.claim_header_id,
a.claim_line_id,
b.first_service_date,
b.last_service_date,
revenue_code = null,
a.place_of_service_code,
a.type_of_service,
getdate() as last_run
from PHClaims.stage.mcare_dme_line as a
left join PHClaims.stage.mcare_dme_claims as b
on a.claim_header_id = b.claim_header_id
--exclude denined claims using carrier/dme claim method
where b.denial_code in ('1','2','3','4','5','6','7','8','9')

--hha
--placeholder once we receive HHA revenue center tables

--hospice
union
select
top 100
a.id_mcare,
a.claim_header_id,
a.claim_line_id,
b.first_service_date,
b.last_service_date,
a.revenue_code,
place_of_service_code = null,
type_of_service = null,
getdate() as last_run
from PHClaims.stage.mcare_hospice_revenue_center as a
left join PHClaims.stage.mcare_hospice_base_claims as b
on a.claim_header_id = b.claim_header_id
--exclude denined claims using carrier/dme claim method
where b.denial_code_facility = '' or b.denial_code_facility is null

--inpatient
union
select
top 100
a.id_mcare,
a.claim_header_id,
a.claim_line_id,
b.first_service_date,
b.last_service_date,
a.revenue_code,
place_of_service_code = null,
type_of_service = null,
getdate() as last_run
from PHClaims.stage.mcare_inpatient_revenue_center as a
left join PHClaims.stage.mcare_inpatient_base_claims as b
on a.claim_header_id = b.claim_header_id
--exclude denined claims using carrier/dme claim method
where b.denial_code_facility = '' or b.denial_code_facility is null

--outpatient
union
select
top 100
a.id_mcare,
a.claim_header_id,
a.claim_line_id,
b.first_service_date,
b.last_service_date,
a.revenue_code,
place_of_service_code = null,
type_of_service = null,
getdate() as last_run
from PHClaims.stage.mcare_outpatient_revenue_center as a
left join PHClaims.stage.mcare_outpatient_base_claims as b
on a.claim_header_id = b.claim_header_id
--exclude denined claims using carrier/dme claim method
where b.denial_code_facility = '' or b.denial_code_facility is null

--snf
union
select
top 100
a.id_mcare,
a.claim_header_id,
a.claim_line_id,
b.first_service_date,
b.last_service_date,
a.revenue_code,
place_of_service_code = null,
type_of_service = null,
getdate() as last_run
from PHClaims.stage.mcare_snf_revenue_center as a
left join PHClaims.stage.mcare_snf_base_claims as b
on a.claim_header_id = b.claim_header_id
--exclude denined claims using carrier/dme claim method
where b.denial_code_facility = '' or b.denial_code_facility is null;


