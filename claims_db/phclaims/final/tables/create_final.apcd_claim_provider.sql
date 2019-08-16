--Code to create final.apcd_claim_provider
--Distinct header-level provider variables for medical claims (billing, rendering, attending, referring)
--Eli Kern (PHSKC-APDE)
--2019-6-29 

if object_id('PHClaims.final.apcd_claim_provider', 'U') is not null drop table PHClaims.final.apcd_claim_provider;
create table PHClaims.final.apcd_claim_provider (
	id_apcd bigint,
	extract_id int,
	claim_header_id bigint,
	first_service_date date,
	last_service_date date,
	billing_provider_id_apcd bigint,
	rendering_provider_id_apcd bigint,
	attending_provider_id_apcd bigint,
	referring_provider_id_apcd bigint
);



