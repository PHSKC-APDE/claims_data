--Code to load data to stage.apcd_claim_line table
--Distinct line-level claim variables that do not have a dedicated table (e.g. revenue code). In other words elements for which there is only one distinct 
--value per claim line.
--Eli Kern (PHSKC-APDE)
--2019-8-22 
--Run time: XX min

------------------
--STEP 1: Select (distinct) desired columns from claim line table
--Exclude all denied/orphaned claim lines
-------------------
insert into PHClaims.stage.apcd_claim_line with (tablock)
select distinct
internal_member_id as id_apcd,
medical_claim_header_id as claim_header_id,
medical_claim_service_line_id as claim_line_id,
line_counter,
first_service_dt as first_service_date,
last_service_dt as last_service_date,
charge_amt,
revenue_code,
place_of_service_code,
getdate() as last_run
from PHClaims.stage.apcd_medical_claim
--exclude denined/orphaned claims
where denied_claim_flag = 'N' and orphaned_adjustment_flag = 'N';