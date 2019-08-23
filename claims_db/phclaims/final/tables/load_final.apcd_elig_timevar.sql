--Code to load data to final.apcd_elig_timevar table
--Member characteristics for which we are primarily interested in variance over time. 
--Includes program/eligibility coverage dates, residential address, and all geographic information (integer if possible). 
--Member ID is not distinct, but member ID-from_date is distinct.
--Eli Kern (PHSKC-APDE)
--2019-4-12
------------------
--STEP 1: Insert data that has passed QA in stage schema table
-------------------
insert into PHClaims.final.apcd_elig_timevar with (tablock)
select
id_apcd,
from_date,
to_date,
contiguous,
med_covgrp,
pharm_covgrp,
med_medicaid,
med_medicare,
med_commercial,
pharm_medicaid,
pharm_medicare,
pharm_commercial,
dual,
rac_code,
geo_zip_code,
geo_county_code,
geo_county,
geo_ach_code,
geo_ach,
cov_time_day
from PHClaims.stage.apcd_elig_timevar;


------------------
--STEP 2: Create clustered columnstore index (1.5 min)
-------------------
create clustered columnstore index idx_ccs_final_apcd_elig_timevar on phclaims.final.apcd_elig_timevar;

