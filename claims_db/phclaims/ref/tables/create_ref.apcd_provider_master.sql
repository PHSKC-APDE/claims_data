--Code to create ref.apcd_provider_master
--A table holding APCD provider ID, NPI, entity type, ZIP code of practice, and primary and secondary specialties for all APCD providers in medical claims
--For providers that are in provider_master table, only take info from there
--For all other providers, take most common NPI, entity type, and ZIP code, and most common two specialties (sorting alphabetically for ties)
--Eli Kern (PHSKC-APDE)
--2019-9-12

if object_id('PHClaims.ref.apcd_provider_master', 'U') is not null drop table PHClaims.ref.apcd_provider_master;
create table PHClaims.ref.apcd_provider_master (
	npi bigint,
	entity_type varchar(255),
	geo_zip_practice varchar(255),
	primary_taxonomy varchar(255),
	secondary_taxonomy varchar(255),
	provider_master_flag tinyint
);



