--QA of ref.apcd_provider_npi
--9/12/19
--Eli Kern

--There should be no records with a provider ID less than or equal to the max of the provider master table that has a 0 value for provider_master_flag
select count(*) as row_count
FROM [PHClaims].[ref].[apcd_provider_npi]
where provider_id_apcd <= (select max(internal_provider_id) from phclaims.stage.apcd_provider_master) and provider_master_flag = 0;

--No provider ID should have more than one row
select count(*) as row_count
from (
	select provider_id_apcd, count(*) as row_count
	FROM [PHClaims].[ref].[apcd_provider_npi]
	group by provider_id_apcd
) as a
where a.row_count >1;

--No NPI should be any length other than 10 digits
select count(*)
from [PHClaims].[ref].[apcd_provider_npi]
where len(npi) != 10;