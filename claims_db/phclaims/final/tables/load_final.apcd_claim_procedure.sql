--Code to load data to final.apcd_claim_procedure
--Procedure codes in long format at claim header level
--Eli Kern (PHSKC-APDE)
--2019-8-22

------------------
--STEP 1: Insert data that has passed QA in stage schema table
-------------------
insert into PHClaims.final.apcd_claim_procedure with (tablock)
select
id_apcd,
extract_id,
claim_header_id,
first_service_date,
last_service_date,
procedure_code,
procedure_code_number,
modifier_1,
modifier_2,
modifier_3,
modifier_4
from PHClaims.stage.apcd_claim_procedure;


------------------
--STEP 2: Create clustered columnstore index (30 min)
-------------------
create clustered columnstore index idx_ccs_final_apcd_claim_procedure on phclaims.final.apcd_claim_procedure;
