--Line-level QA of stage.apcd_elig_plr_DATE table
--2019-10
--Eli Kern

--NOTES:
--Given that this table is computed for the entire APCD member population it will assign a member geographically in this context. 
--This means that if a member lives in King longer than Snohomish during the requested date range, this member assigned to King. 
--Once assigned to an ACH, this script will link back to the apcd_elig_timevar table and find all of the eligibility segments
--where this person lived in the assigned ACH and then calculate the ach_covd and ach_covper variables. 
--If a member lived longest outside of WA state they will not be assigned to an ACH.

----------------
--LINE-LEVEL QA
----------------

--person with pregap
--end date of mcaid gap (e.g. 4/30/17) - from_date of PLR table (1/1/2017) = med_medicaid_covgap_max
select * from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 13124461305;

select * from phclaims.final.apcd_elig_timevar
where id_apcd = 13124461305
order by from_date;

--person with several coverage types, pre and post gaps in any type of coverage 
--contiguous=> starts right after the previous coverage segment. all 2014-01-01 from dates will be contiguous. 
------Usually there is a new segment because one of the time varying concepts changes, such as coverage type.  
--look at codebook for the med_covgrp variables. can also look the columns

select * from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11057446763;

select * from phclaims.final.apcd_elig_timevar
where id_apcd = 11057446763
order by from_date;

----------------
--CHECK DEMOGRAPHIC VARIABLES
----------------

--age 0
select id_apcd, age, age_grp7 from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11050887863;

select * from phclaims.final.apcd_elig_demo
where id_apcd = 11050887863;

--age 90+, dob null
select id_apcd, age, age_grp7 from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11057428447;

select * from phclaims.final.apcd_elig_demo
where id_apcd = 11057428447;

--other ages (23-year old)
select id_apcd, age, age_grp7 from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11050747029;

select * from phclaims.final.apcd_elig_demo
where id_apcd = 11050747029;

--multiple gender
select id_apcd, gender_female, gender_male, gender_me, gender_recent from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11050747626;

select * from phclaims.final.apcd_elig_demo
where id_apcd = 11050747626;

--male only gender
select id_apcd, gender_female, gender_male, gender_me, gender_recent from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11050747038;

select * from phclaims.final.apcd_elig_demo
where id_apcd = 11050747038;

--female only gender
select id_apcd, gender_female, gender_male, gender_me, gender_recent from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11050747069;

select * from phclaims.final.apcd_elig_demo
where id_apcd = 11050747069;

--unknown gender
select id_apcd, gender_female, gender_male, gender_me, gender_recent from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 18262562602;

select * from phclaims.final.apcd_elig_demo
where id_apcd = 18262562602;


----------------
--CHECK GEOGRAPHIC VARIABLES
----------------

--Multiple regions
select id_apcd, geo_zip, geo_county, geo_ach, geo_ach_covd, geo_ach_covper
from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11990445526;

select id_apcd, from_date, to_date, geo_zip, geo_county, geo_ach from phclaims.final.apcd_elig_timevar
where id_apcd = 11990445526
order by from_date;

--Majority of time out of state (examine 2017 only)
select id_apcd, geo_zip, geo_county, geo_ach, geo_ach_covd, geo_ach_covper
from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 12280336477;

select id_apcd, from_date, to_date, geo_zip, geo_county, geo_ach from phclaims.final.apcd_elig_timevar
where id_apcd = 12280336477
order by from_date;

--Multiple ZIPs
select id_apcd, geo_zip, geo_county, geo_ach, geo_ach_covd, geo_ach_covper
from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11387171732;

select id_apcd, from_date, to_date, geo_zip, geo_county, geo_ach from phclaims.final.apcd_elig_timevar
where id_apcd = 11387171732
order by from_date;

--Partial year coverage in GCACH
select id_apcd, geo_zip, geo_county, geo_ach, geo_ach_covd, geo_ach_covper
from phclaims.stage.apcd_elig_plr_2016
where id_apcd = 11268594176;

select id_apcd, from_date, to_date, geo_zip, geo_county, geo_ach from phclaims.final.apcd_elig_timevar
where id_apcd = 11268594176
order by from_date;


----------------
--CHECK COVERAGE COHORT VARIABLES
----------------

--QA person with DSRIP full benefit cov per less than Medicaid covper
--women who went from family planning benefit to MCO
select * from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11060927580;

select * from phclaims.final.apcd_elig_timevar
where id_apcd = 11060927580
order by id_apcd, from_date;

--person was ruled out of 11-month cohort because of commerical medical covper
select * from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11057425379;

select * from phclaims.final.apcd_elig_timevar
where id_apcd = 11057425379
order by id_apcd, from_date;

--person was ruled out of 11-month cohort because of low medical coverage
select * from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11057425387;

select * from phclaims.final.apcd_elig_timevar
where id_apcd = 11057425387
order by id_apcd, from_date;

--person was ruled out of 11-month cohort because dual covper was too high
select * from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11268915668;

select * from phclaims.final.apcd_elig_timevar
where id_apcd = 11268915668
order by id_apcd, from_date;