--Code to create final.apcd_claim_icdcm_header table
--ICD-CM diagnosis codes in long format at claim header level
--Eli Kern (PHSKC-APDE)
--2019-5-7

if object_id('PHClaims.final.apcd_claim_icdcm_header', 'U') is not null drop table PHClaims.final.apcd_claim_icdcm_header;
create table PHClaims.final.apcd_claim_icdcm_header (
	id_apcd bigint,
	extract_id int,
	claim_header_id bigint,
	icdcm_raw varchar(200),
	icdcm_norm varchar(200),
	icdcm_version tinyint,
	icdcm_number varchar(200)
);



