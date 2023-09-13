--Code to create final.apcd_claim_procedure table
--Procedure codes in long format at claim header level
--Eli Kern (PHSKC-APDE)
--2019-8-22

if object_id('PHClaims.final.apcd_claim_procedure', 'U') is not null drop table PHClaims.final.apcd_claim_procedure;
create table PHClaims.final.apcd_claim_procedure (
	id_apcd bigint,
	extract_id int,
	claim_header_id bigint,
	first_service_date date,
	last_service_date date,
	procedure_code varchar(255),
	procedure_code_number varchar(255),
	modifier_1 varchar(255),
	modifier_2 varchar(255),
	modifier_3 varchar(255),
	modifier_4 varchar(255)
);



