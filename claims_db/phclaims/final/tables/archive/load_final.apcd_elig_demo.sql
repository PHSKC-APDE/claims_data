-- Code to load data to final.apcd_elig_demo table
--A historical record of each person's non time-varying demographics (e.g. date of birth, gender)
-- Eli Kern (PHSKC-APDE)
-- 2019-3-28
-- Takes <1 min to run

------------------
--STEP 1: Insert data that has passed QA in stage schema table
-------------------
insert into PHClaims.final.apcd_elig_demo with (tablock)
select
	id_apcd,
	dob,
	ninety_only,
	gender_female,
	gender_male,
	gender_me,
	gender_recent
from PHClaims.stage.apcd_elig_demo;


------------------
--STEP 2: Create clustered columnstore index (0.5 min)
-------------------
create clustered columnstore index idx_ccs_final_apcd_elig_demo on phclaims.final.apcd_elig_demo;


