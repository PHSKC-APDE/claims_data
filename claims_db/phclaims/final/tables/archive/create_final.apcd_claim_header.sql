--Code to create final.apcd_claim_header table
--Distinct header-level claim variables (e.g. claim type). In other words elements for which there is only one distinct 
--value per claim header.
--Eli Kern (PHSKC-APDE)
--2019-4-26

if object_id('PHClaims.final.apcd_claim_header', 'U') is not null drop table PHClaims.final.apcd_claim_header;
create table PHClaims.final.apcd_claim_header (
	id_apcd bigint,
	extract_id int,
	claim_header_id bigint,
	submitter_id int,
	billing_provider_id_apcd bigint,
	product_code_id int,
	first_service_date date,
	last_service_date date,
	first_paid_date date,
	last_paid_date date,
	charge_amt numeric(38,2),
	primary_diagnosis varchar(20),
	icdcm_version int,
	header_status varchar(2),
	claim_type_apcd_id varchar(100),
	claim_type_id tinyint,
	type_of_bill_code varchar(4),
	ipt_flag tinyint,
	discharge_date date,
	ed_flag tinyint,
	or_flag tinyint
);



