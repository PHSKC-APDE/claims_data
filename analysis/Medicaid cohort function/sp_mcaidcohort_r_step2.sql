--Eli Kern
--Assessment, Policy Development & Evaluation, Public Health - Seattle & King County
--2/27/18
--Code to return a demographic subset of the King County Medicaid member population for a specific time period
--This script creates a stored procedure for use within R (only difference is that this does not create a temp table)

--Refer to README file on GitHub to understand parameters below
--https://github.com/PHSKC-APDE/Medicaid/tree/master/analysis/Medicaid%20cohort%20function

--select database
use PHClaims
go

--drop stored procedure before creating new
drop procedure dbo.sp_mcaidcohort_r_step2
go

--create stored procedure
create proc dbo.sp_mcaidcohort_r_step2
as
begin

--------------------------
--STEP 6: Join all tables
--------------------------

select cov.id, 
	case
		when cov.covgap_max <= 30 and dual.dual_flag = 0 then 'small gap, nondual'
		when cov.covgap_max > 30 and dual.dual_flag = 0 then 'large gap, nondual'
		when cov.covgap_max <= 30 and dual.dual_flag = 1 then 'small gap, dual'
		when cov.covgap_max > 30 and dual.dual_flag = 1 then 'large gap, dual'
	end as 'cov_cohort',

	cov.covd, cov.covper, cov.ccovd_max, cov.covgap_max, dual.duald, dual.dualper, dual.dual_flag, demo.dobnew, demo.age, demo.age_grp7, demo.gender_mx, demo.male, demo.female, 
	demo.male_t, demo.female_t, demo.gender_unk, demo.race_eth_mx, demo.race_mx, demo.aian, demo.asian, demo.black, demo.nhpi, demo.white, demo.latino, demo.aian_t, demo.asian_t, demo.black_t, 
	demo.nhpi_t, demo.white_t, demo.latino_t, demo.race_unk, geo.tractce10, geo.zip_new, geo.hra_id, geo.hra, geo.region_id, geo.region, demo.maxlang, demo.english, demo.spanish, demo.vietnamese, demo.chinese, demo.somali, 
	demo.russian, demo.arabic, demo.korean, demo.ukrainian, demo.amharic, demo.english_t, demo.spanish_t, demo.vietnamese_t, demo.chinese_t, demo.somali_t, demo.russian_t,
	demo.arabic_t, demo.korean_t, demo.ukrainian_t, demo.amharic_t, demo.lang_unk

--1st table - coverage
from (
	select id, covd, covper, ccovd_max, covgap_max from ##cov
)as cov

--2nd table - dual eligibility duration
inner join (
	select id, duald, dualper, dual_flag from ##dual
) as dual
on cov.id = dual.id

--3rd table - sub-county areas
inner join (
	select id, tractce10, zip_new, hra_id, hra, region_id, region from ##geo
) as geo
--join on ID
on cov.id = geo.id

--4th table - age, gender, race, and language
inner join (
	select id, dobnew, age, age_grp7, gender_mx, male, female, 
		male_t, female_t, gender_unk, race_eth_mx, race_mx, aian, asian, black, nhpi, white, latino, aian_t, asian_t, black_t, 
		nhpi_t, white_t, latino_t, race_unk, maxlang, english, spanish, vietnamese, chinese, somali, 
		russian, arabic, korean, ukrainian, amharic, english_t, spanish_t, vietnamese_t, chinese_t, somali_t, russian_t,
		arabic_t, korean_t, ukrainian_t, amharic_t, lang_unk
	from ##demo
) as demo
--join on ID
on cov.id = demo.id

end