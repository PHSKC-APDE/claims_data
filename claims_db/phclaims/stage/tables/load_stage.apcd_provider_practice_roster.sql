--Code to load data to stage.apcd_provider_practice_roster
--Eli Kern (PHSKC-APDE)
--2019-6-27

------------------
--STEP 1: Insert data into table shell
-------------------
insert into PHClaims.stage.apcd_provider_practice_roster with (tablock)
select
[npi]
,[first_name]
,[middle_name]
,[last_name]
,[professional_credential]
,[practice_organization_id]
,[primary_organization_ind]
,[affiliation_begin_dt]
,[affiliation_end_dt]
,[organization_name]
,[organization_type]
,[practice_street_address1]
,[practice_street_address2]
,[practice_city]
,[practice_state]
,[practice_zip]
from PHclaims.load_raw.apcd_provider_practice_roster;



