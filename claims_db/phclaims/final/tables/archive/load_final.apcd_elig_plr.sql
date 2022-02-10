-- Code to load data to final.apcd_elig_plr_DATE table
-- Creates a person level table with coverage stats, geographic information for member residence, and member demographics for requested date range
--Will become obsolete when claims package is extended to APCD data
--Eli Kern (PHSKC-APDE)
--2019-4-23

-------------------
--CHANGE YEAR/DATE suffix in table name as needed
-------------------

------------------
--STEP 1: Insert data that has passed QA in stage schema table
-------------------
insert into PHClaims.final.apcd_elig_plr_2018 with (tablock)
select id_apcd
,geo_wa_resident
,overall_mcaid
,overall_mcaid_med
,overall_mcaid_pharm
,performance_11_wa
,performance_7_wa
,performance_11_ach
,performance_7_ach
,geo_zip_code
,geo_county
,geo_ach
,geo_ach_covd
,geo_ach_covper
,age
,age_grp7
,gender_me
,gender_recent
,gender_female
,gender_male
,med_total_covd
,med_total_covper
,dsrip_full_covd
,dsrip_full_covper
,dual_covd
,dual_covper
,dual
,med_medicaid_covd
,rac_covd
,med_medicare_covd
,med_commercial_covd
,med_medicaid_covper
,med_medicare_covper
,med_commercial_covper
,med_total_ccovd_max
,med_medicaid_ccovd_max
,med_medicare_ccovd_max
,med_commercial_ccovd_max
,med_total_covgap_max
,med_medicaid_covgap_max
,med_medicare_covgap_max
,med_commercial_covgap_max
,pharm_total_covd
,pharm_total_covper
,pharm_medicaid_covd
,pharm_medicare_covd
,pharm_commercial_covd
,pharm_medicaid_covper
,pharm_medicare_covper
,pharm_commercial_covper
,pharm_total_ccovd_max
,pharm_medicaid_ccovd_max
,pharm_medicare_ccovd_max
,pharm_commercial_ccovd_max
,pharm_total_covgap_max
,pharm_medicaid_covgap_max
,pharm_medicare_covgap_max
,pharm_commercial_covgap_max
from PHClaims.stage.apcd_elig_plr_2018;



