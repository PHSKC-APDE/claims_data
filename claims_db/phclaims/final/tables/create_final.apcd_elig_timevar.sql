--Code to create final.apcd_elig_timevar table
--Member characteristics for which we are primarily interested in variance over time. 
--Includes program/eligibility coverage dates, residential address, and all geographic information (integer if possible). 
--Member ID is not distinct, but member ID-from_date is distinct.
--Eli Kern (PHSKC-APDE)
--2019-4-12


IF object_id('PHClaims.final.apcd_elig_timevar', 'U') is not null DROP TABLE PHClaims.final.apcd_elig_timevar;
CREATE TABLE PHClaims.final.apcd_elig_timevar (
	id_apcd bigint,
	from_date date,
	to_date date,
	contiguous tinyint,
	med_covgrp tinyint,
	pharm_covgrp tinyint,
	med_medicaid tinyint,
	med_medicare tinyint,
	med_commercial tinyint,
	pharm_medicaid tinyint,
	pharm_medicare tinyint,
	pharm_commercial tinyint,
	dual tinyint,
	rac_code varchar(10),
	geo_zip_code varchar(10),
	geo_county_code varchar(20),
	geo_county varchar(100),
	geo_ach_code varchar(20),
	geo_ach varchar(100),
	cov_time_day bigint
);



