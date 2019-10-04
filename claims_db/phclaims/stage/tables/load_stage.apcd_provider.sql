--Code to load data to stage.apcd_provider
--Eli Kern (PHSKC-APDE)
--2019-6-27

------------------
--STEP 1: Insert data into table shell
-------------------
insert into PHClaims.stage.apcd_provider with (tablock)
--archived rows
select
[provider_id]
,[extract_id]
,[submitter_id]
,[internal_provider_id]
,[organization_name]
,[last_name]
,[first_name]
,[middle_name]
,[generation_suffix]
,[entity_type]
,[professional_credential_code]
,[orig_npi]
,[primary_specialty_id]
,[primary_specialty_code]
,[city]
,[state]
,[zip]
from PHclaims.archive.apcd_provider
--new rows from new extract
union
select
[provider_id]
,[extract_id]
,[submitter_id]
,[internal_provider_id]
,[organization_name]
,[last_name]
,[first_name]
,[middle_name]
,[generation_suffix]
,[entity_type]
,[professional_credential_code]
,[orig_npi]
,[primary_specialty_id]
,[primary_specialty_code]
,[city]
,[state]
,[zip]
from PHclaims.load_raw.apcd_provider;


------------------
--STEP 2: Create clustered columnstore index (2 min)
-------------------
create clustered columnstore index idx_ccs_stage_apcd_provider on phclaims.stage.apcd_provider;
