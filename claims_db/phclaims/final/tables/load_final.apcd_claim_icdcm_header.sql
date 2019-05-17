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
icdcm_raw,
icdcm_norm,
icdcm_version,
icdcm_number
from PHClaims.stage.apcd_claim_icdcm_header;



