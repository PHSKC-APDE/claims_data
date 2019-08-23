--Code to create stage.apcd_claim_provider
--Distinct header-level provider variables for medical claims (billing, rendering, attending, referring)
--Reshaped to long
--Eli Kern (PHSKC-APDE)
--2019-6-29 

if object_id('PHClaims.stage.apcd_claim_provider', 'U') is not null drop table PHClaims.stage.apcd_claim_provider;
create table PHClaims.stage.apcd_claim_provider (
	id_apcd bigint,
	extract_id int,
	claim_header_id bigint,
	first_service_date date,
	last_service_date date,
	provider_id_apcd bigint,
	provider_type varchar(255)
);



