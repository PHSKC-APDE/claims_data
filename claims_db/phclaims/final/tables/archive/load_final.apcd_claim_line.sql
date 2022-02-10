--Code to load data to final.apcd_claim_line
--Distinct line-level claim variables that do not have a dedicated table (e.g. revenue code). In other words elements for which there is only one distinct 
--value per claim line.
--Eli Kern (PHSKC-APDE)
--2019-8-23
------------------
--STEP 1: Insert data that has passed QA in stage schema table
-------------------
insert into PHClaims.final.apcd_claim_line with (tablock)
select
id_apcd,
extract_id,
claim_header_id,
claim_line_id,
line_counter,
first_service_date,
last_service_date,
charge_amt,
revenue_code,
place_of_service_code
from PHClaims.stage.apcd_claim_line;


------------------
--STEP 2: Create clustered columnstore index (34 min)
-------------------
create clustered columnstore index idx_ccs_final_apcd_claim_line on phclaims.final.apcd_claim_line;


