--Code to load data to stage.apcd_claim_provider
--Distinct header-level provider variables for medical claims (billing, rendering, attending, referring)
--Eli Kern (PHSKC-APDE)
--2019-6-29 
--Run time: XX min

------------------
--STEP 1: Exract header-level provider variables and insert into table shell
-------------------
insert into PHClaims.stage.apcd_claim_provider with (tablock)
select distinct
a.internal_member_id as id_apcd,
a.extract_id,
b.medical_claim_header_id as claim_header_id, 
cast(a.billing_internal_provider_id as bigint) as billing_provider_id_apcd,
cast(a.rendering_internal_provider_id as bigint) as rendering_provider_id_apcd,
cast(a.attending_internal_provider_id as bigint) as attending_provider_id_apcd,
cast(a.referring_internal_provider_id as bigint) as referring_provider_id_apcd
from PHClaims.stage.apcd_medical_claim as a
left join PHClaims.stage.apcd_medical_crosswalk as b
on a.medical_claim_service_line_id = b.medical_claim_service_line_id;



