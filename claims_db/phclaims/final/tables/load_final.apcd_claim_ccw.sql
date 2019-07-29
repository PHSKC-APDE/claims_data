--Code to load data to final.apcd_claim_ccw table
--Person-level CCW condition status by time period
--Eli Kern (PHSKC-APDE)
--2019-5-17
------------------
--STEP 1: Insert data that has passed QA in stage schema table
-------------------
insert into PHClaims.final.apcd_claim_ccw with (tablock)
select
id_apcd,
from_date,
to_date,
ccw_code,
ccw_desc
from PHClaims.stage.apcd_claim_ccw;



