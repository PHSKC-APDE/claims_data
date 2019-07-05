--Code to load data to stage.apcd_medical_crosswalk
--Eli Kern (PHSKC-APDE)
--2019-6-27

------------------
--STEP 1: Insert data into table shell
-------------------
insert into PHClaims.stage.apcd_medical_crosswalk with (tablock)
--archived rows
select
[medical_claim_service_line_id]
,[extract_id]
,[inpatient_discharge_id]
,[medical_claim_header_id]
from PHclaims.archive.apcd_medical_crosswalk
--new rows from new extract
union
select
[medical_claim_service_line_id]
,[extract_id]
,[inpatient_discharge_id]
,[medical_claim_header_id]
from PHclaims.load_raw.apcd_medical_crosswalk;



