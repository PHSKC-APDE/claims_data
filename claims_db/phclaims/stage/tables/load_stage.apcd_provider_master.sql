--Code to load data to stage.apcd_provider_master
--Eli Kern (PHSKC-APDE)
--2019-6-27

------------------
--STEP 1: Insert data into table shell
-------------------
insert into PHClaims.stage.apcd_provider_master with (tablock)
select
[internal_provider_id]
,[extract_id]
,[entity_type]
,[organization_name_legal]
,[last_name_legal]
,[first_name_legal]
,[middle_name_legal]
,[organization_name_other]
,[organization_name_other_type]
,[last_name_other]
,[first_name_other]
,[middle_name_other]
,[generation_suffix]
,[professional_credential_code]
,[npi]
,[primary_taxonomy]
,[secondary_taxonomy]
,[city_physical]
,[state_physical]
,[zip_physical]
,[county_physical]
,[country_physical]
,[ach_region_physical]
from PHclaims.load_raw.apcd_provider_master;



