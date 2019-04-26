--Code to create stage.apcd_claim_header table
--Distinct header-level claim variables (e.g. claim type). In other words elements for which there is only one distinct 
--value per claim header.
--Eli Kern (PHSKC-APDE)
--2019-4-26

IF object_id('PHClaims.stage.apcd_claim_header', 'U') is not null DROP TABLE PHClaims.stage.apcd_claim_header;
CREATE TABLE PHClaims.stage.apcd_claim_header (
	id_apcd bigint,
	claim_header_id bigint,
	submitter_id int,
	provider_id_apcd bigint,
	product_code_id int,
	first_service_dt date,
	last_service_dt date,
	first_paid_dt date,
	last_paid_dt date,
	charge_amt numeric(38,2),
	primary_diagnosis varchar(20),
	icdcm_version int,
	header_status varchar(2),
	claim_type_apcd_id varchar(100),
	claim_type_id tinyint,
	type_of_bill_code varchar(4),
	inpatient_flag tinyint,
	ed_flag tinyint,
	or_flag tinyint
);



