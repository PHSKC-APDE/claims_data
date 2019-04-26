--Code to create stage.apcd_elig_demo table
--A historical record of each person's non time-varying demographics (e.g. date of birth, gender)
--Eli Kern (PHSKC-APDE)
--2019-4-11

IF object_id('PHClaims.stage.apcd_elig_demo', 'U') is not null DROP TABLE PHClaims.stage.apcd_elig_demo;
CREATE TABLE PHClaims.stage.apcd_elig_demo (
	id_apcd bigint,
	dob date,
	ninety_only tinyint,
	gender_female tinyint,
	gender_male tinyint,
	gender_me varchar(8),
	gender_recent varchar(8)
);



