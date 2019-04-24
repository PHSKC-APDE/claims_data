--QA of stage.apcd_elig_plr_DATE table
--4/23/19
--Eli Kern

--NOTES:
--Given that this table is computed for the entire APCD member population it will assign a member geographically in this context. 
--This means that if a member lives in King longer than Snohomish during the requested date range, this member assigned to King. 
--Once assigned to an ACH, this script will link back to the apcd_elig_timevar table and find all of the eligibility segments
--where this person lived in the assigned ACH and then calculate the ach_covd and ach_covper variables. 
--If a member lived longest outside of WA state they will not be assigned to an ACH.

----------------
--All members distinct
----------------
select count(a.id_apcd) as id_dup_cnt
from (
select id_apcd, count(id_apcd) as id_cnt
from phclaims.stage.apcd_elig_plr_2017
group by id_apcd
) as a
where a.id_cnt > 1;

----------------
--CHECK COVERAGE VARIABLES
----------------

--person with pregap
select * from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 13124461305;

select * from phclaims.final.apcd_elig_timevar
where id_apcd = 13124461305
order by from_date;

select top 1000 *
from phclaims.stage.apcd_elig_plr_2017
where med_medicaid_covd > 0 and med_medicare_covd >0 and med_commercial_covd >0

--person with all coverage types, pre and post gaps
--FIND A NEW PERSON
select * from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11057446763;

select * from phclaims.final.apcd_elig_timevar
where id_apcd = 11057446763
order by from_date;

--No coverage days, continuous period, coverage gaps greater than 365
select * from phclaims.stage.apcd_elig_plr_2017
where med_total_covd > 365 or med_medicaid_covd > 365 or med_commercial_covd > 365 or
  med_medicare_covd > 365 or dual_covd > 365 or geo_ach_covd > 365 or pharm_total_covd > 365 or
  pharm_medicaid_covd > 365 or pharm_medicare_covd > 365 or pharm_commercial_covd > 365;
  
--No percents greater than 100
select * from phclaims.stage.apcd_elig_plr_2017
where med_total_covper > 100 or med_medicaid_covper > 100 or med_commercial_covper > 100 or
  med_medicare_covper > 100 or dual_covper > 100 or geo_ach_covper > 100 or pharm_total_covper > 100 or
  pharm_medicaid_covper > 100 or pharm_medicare_covper > 100 or pharm_commercial_covper > 100;

----------------
--CHECK DEMOGRAPHIC VARIABLES
----------------
--age (<1, 90+, in between)
select id_apcd, age, age_grp7 from phclaims.stage.apcd_elig_plr_2017
where age < 0;

--age 0
select id_apcd, age, age_grp7 from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11050887863;

select internal_member_id, year_month, age
from phclaims.stage.apcd_member_month_detail
where internal_member_id = 11050887863
order by year_month;

select * from phclaims.final.apcd_elig_demo
where id_apcd = 11050887863;

--age 90+
select id_apcd, age, age_grp7 from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11057428447;

select internal_member_id, year_month, age
from phclaims.stage.apcd_member_month_detail
where internal_member_id = 11057428447
order by year_month;

select * from phclaims.final.apcd_elig_demo
where id_apcd = 11057428447;

--other ages (23-year old)
select id_apcd, age, age_grp7 from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11050747029;

select internal_member_id, year_month, age
from phclaims.stage.apcd_member_month_detail
where internal_member_id = 11050747029
order by year_month;

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
where id_apcd = 11057776891;

select * from phclaims.final.apcd_elig_demo
where id_apcd = 11057776891;


----------------
--CHECK GEOGRAPHIC VARIABLES
----------------

--Multiple regions
select id_apcd, geo_zip_code, geo_county, geo_ach, geo_ach_covd, geo_ach_covper
from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11990445526;

select id_apcd, from_date, to_date, geo_zip_code, geo_county, geo_ach from phclaims.final.apcd_elig_timevar
where id_apcd = 11990445526
order by from_date;

--Majority of time out of state
select id_apcd, geo_zip_code, geo_county, geo_ach, geo_ach_covd, geo_ach_covper
from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 12280336477;

select id_apcd, from_date, to_date, geo_zip_code, geo_county, geo_ach from phclaims.final.apcd_elig_timevar
where id_apcd = 12280336477
order by from_date;

--Fully in King, multiple ZIPs
select id_apcd, geo_zip_code, geo_county, geo_ach, geo_ach_covd, geo_ach_covper
from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11386939889;

select id_apcd, from_date, to_date, geo_zip_code, geo_county, geo_ach from phclaims.final.apcd_elig_timevar
where id_apcd = 11386939889
order by from_date;

--Partial year coverage in GCACH
select id_apcd, geo_zip_code, geo_county, geo_ach, geo_ach_covd, geo_ach_covper
from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11268594176;

select id_apcd, from_date, to_date, geo_zip_code, geo_county, geo_ach from phclaims.final.apcd_elig_timevar
where id_apcd = 11268594176
order by from_date;


----------------
--CHECK COVERAGE COHORT VARIABLES
----------------

--There should be no one in overall or performance cohorts that are out of state
select count(id_apcd)
from phclaims.stage.apcd_elig_plr_2017
where overall_mcaid = 1 and geo_county is null;

select count(id_apcd)
from phclaims.stage.apcd_elig_plr_2017
where performance_11_wa = 1 and geo_county is null;

select count(id_apcd)
from phclaims.stage.apcd_elig_plr_2017
where performance_7_wa = 1 and geo_county is null;

--QA person with DSRIP full benefit cov per less than Medicaid covper
--women who went from family planning benefit to pregnant women's benefit
select * from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11060927580;

select * from phclaims.final.apcd_elig_timevar
where id_apcd = 11060927580
order by id_apcd, from_date;

--No one should be in ACH and not in WA cohort
select top 1000 *
from phclaims.stage.apcd_elig_plr_2017
where performance_7_wa = 0 and performance_7_ach = 1;

select top 1000 *
from phclaims.stage.apcd_elig_plr_2017
where performance_11_wa = 0 and performance_11_ach = 1;

--person was ruled out of 11-month cohort because of commerical medical covper
select * from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11057425379;

select * from phclaims.final.apcd_elig_timevar
where id_apcd = 11057425379
order by id_apcd, from_date;

--person was ruled out of 11-month cohort because of low medicaid coverage
select * from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11057425387;

select * from phclaims.final.apcd_elig_timevar
where id_apcd = 11057425387
order by id_apcd, from_date;

--person was ruled out of 11-month cohort because dual covper was too high
select * from phclaims.stage.apcd_elig_plr_2017
where id_apcd = 11057428541;

select * from phclaims.final.apcd_elig_timevar
where id_apcd = 11057428541
order by id_apcd, from_date;

--check to make sure WA vs ACH covper cutoffs make sense
select max(geo_ach_covper)
from phclaims.stage.apcd_elig_plr_2017
where performance_11_wa = 1 and performance_11_ach = 0;

select max(geo_ach_covper)
from phclaims.stage.apcd_elig_plr_2017
where performance_7_wa = 1 and performance_7_ach = 0;

--Check out of state flag
select count(id_apcd)
from phclaims.stage.apcd_elig_plr_2017
where geo_wa_resident = 1 and geo_county is null;

select count(id_apcd)
from phclaims.stage.apcd_elig_plr_2017
where geo_wa_resident = 0 and geo_county is not null;

--Check medical versus pharmacy coverage
select count(id_apcd)
from phclaims.stage.apcd_elig_plr_2017
where overall_mcaid = 1;

select count(id_apcd)
from phclaims.stage.apcd_elig_plr_2017
where overall_mcaid_med = 1 and overall_mcaid_pharm = 0;

select count(id_apcd)
from phclaims.stage.apcd_elig_plr_2017
where overall_mcaid_med = 0 and overall_mcaid_pharm = 1;