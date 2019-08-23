--Code to load data to final.apcd_claim_header
--Distinct header-level claim variables (e.g. claim type). In other words elements for which there is only one distinct 
--value per claim header.
--Eli Kern (PHSKC-APDE)
--2019-4-26
------------------
--STEP 1: Insert data that has passed QA in stage schema table
-------------------
insert into PHClaims.final.apcd_claim_header with (tablock)
select
id_apcd,
extract_id,
claim_header_id,
submitter_id,
billing_provider_id_apcd,
product_code_id,
first_service_date,
last_service_date,
first_paid_date,
last_paid_date,
charge_amt,
primary_diagnosis,
icdcm_version,
header_status,
claim_type_apcd_id,
claim_type_id,
type_of_bill_code,
ipt_flag,
discharge_date,
ed_flag,
or_flag
from PHClaims.stage.apcd_claim_header;


------------------
--STEP 2: Create clustered columnstore index (18 min)
-------------------
create clustered columnstore index idx_ccs_final_apcd_claim_header on phclaims.final.apcd_claim_header;
