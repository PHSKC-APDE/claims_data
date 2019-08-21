--Code to load data to final.apcd_claim_provider
--Eli Kern (PHSKC-APDE)
--2019-6-29 

------------------
--STEP 1: Insert data that has passed QA in stage schema table
-------------------
insert into PHClaims.final.apcd_claim_provider with (tablock)
select
id_apcd,
extract_id int,
claim_header_id,
first_service_date,
last_service_date,
billing_provider_id_apcd,
rendering_provider_id_apcd,
attending_provider_id_apcd,
referring_provider_id_apcd
from PHClaims.stage.apcd_claim_provider;


------------------
--STEP 2: Create clustered columnstore index (20 min)
-------------------
create clustered columnstore index idx_ccs_final_apcd_claim_provider on phclaims.final.apcd_claim_provider;



