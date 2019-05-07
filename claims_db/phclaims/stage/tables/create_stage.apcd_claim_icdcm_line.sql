--Code to create stage.apcd_claim_icdcm_line table
--ICD-CM diagnosis codes in long format at claim line level
--Eli Kern (PHSKC-APDE)
--2019-5-3

if object_id('PHClaims.stage.apcd_claim_icdcm_line', 'U') is not null drop table PHClaims.stage.apcd_claim_icdcm_line;
create table PHClaims.stage.apcd_claim_icdcm_line (
	id_apcd bigint,
	extract_id int,
	claim_header_id bigint,
	claim_line_id bigint,
	icdcm_raw varchar(200),
	icdcm_norm varchar(200),
	icdcm_version tinyint,
	icdcm_number varchar(200)
);



