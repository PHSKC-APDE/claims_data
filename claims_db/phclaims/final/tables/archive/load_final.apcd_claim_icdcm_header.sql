--Code to load data to final.apcd_claim_icdcm_header table
--ICD-CM diagnosis codes in long format at claim header level
--Eli Kern (PHSKC-APDE)
--2019-5-7
--Run time: 7 min


------------------
--STEP 1: Insert data that has passed QA in stage schema table
-------------------
insert into PHClaims.final.apcd_claim_icdcm_header with (tablock)
select
id_apcd,
extract_id,
claim_header_id,
first_service_date,
last_service_date,
icdcm_raw,
icdcm_norm,
icdcm_version,
icdcm_number
from PHClaims.stage.apcd_claim_icdcm_header;


------------------
--STEP 2: Create clustered columnstore index (53 min)
-------------------
create clustered columnstore index idx_ccs_final_apcd_claim_icdcm_header on phclaims.final.apcd_claim_icdcm_header;

