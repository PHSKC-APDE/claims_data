--Code to load data to stage.apcd_claim_line table
--Distinct line-level claim variables that do not have a dedicated table (e.g. revenue code). In other words elements for which there is only one distinct 
--value per claim line.
--Eli Kern (PHSKC-APDE)
--2019-8-22 
--Run time: 33 min

------------------
--STEP 1: Select (distinct) desired columns from claim line table
--Exclude all members with no eligibility information using ref.apcd_claim_no_elig
--Use line-level denied/orphaned flags to exclude claim lines
--Include claim header ID
-------------------
insert into PHClaims.stage.apcd_claim_line with (tablock)
select distinct
a.internal_member_id as id_apcd,
a.extract_id,
c.medical_claim_header_id as claim_header_id,
a.medical_claim_service_line_id as claim_line_id,
a.line_counter,
a.first_service_dt as first_service_date,
a.last_service_dt as last_service_date,
a.charge_amt,
a.revenue_code,
a.place_of_service_code
from PHClaims.stage.apcd_medical_claim as a
left join PHClaims.ref.apcd_claim_no_elig as b
on a.internal_member_id = b.id_apcd
left join PHClaims.stage.apcd_medical_crosswalk as c
on a.medical_claim_service_line_id = c.medical_claim_service_line_id
--exclude members with no elig information
where b.id_apcd is null
--exclude denined/orphaned claims
and denied_claim_flag = 'N' and orphaned_adjustment_flag = 'N';