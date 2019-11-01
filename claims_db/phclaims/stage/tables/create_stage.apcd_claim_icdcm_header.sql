--Code to create stage.apcd_claim_icdcm_header table
--ICD-CM diagnosis codes in long format at claim header level
--Eli Kern (PHSKC-APDE)
--2019-5-3

if object_id('PHClaims.stage.apcd_claim_icdcm_header', 'U') is not null drop table PHClaims.stage.apcd_claim_icdcm_header;
create table PHClaims.stage.apcd_claim_icdcm_header (
	id_apcd bigint,
	claim_header_id bigint,
	first_service_date date,
	last_service_date date,
	icdcm_raw varchar(200),
	icdcm_norm varchar(200),
	icdcm_version tinyint,
	icdcm_number varchar(200),
	last_run datetime
);



