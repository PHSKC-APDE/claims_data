--Code to create ref.apcd_provider_npi
--A crosswalk between APCD provider ID and most common NPI
--Eli Kern (PHSKC-APDE)
--2019-9-12

if object_id('PHClaims.ref.apcd_provider_npi', 'U') is not null drop table PHClaims.ref.apcd_provider_npi;
create table PHClaims.ref.apcd_provider_npi (
	provider_id_apcd bigint,
	npi bigint,
	provider_master_flag tinyint
);



