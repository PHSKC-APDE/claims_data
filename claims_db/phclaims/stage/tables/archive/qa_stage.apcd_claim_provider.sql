--QA of stage.apcd_claim_provider
--7/7/19
--Eli Kern
--Run time: 5 min

--Compare min/max of provider ID variables with medical_claim table
select min(provider_id_apcd) as rendering_min, max(provider_id_apcd) as rendering_max
from PHClaims.stage.apcd_claim_provider
where provider_type = 'rendering';

select min(cast(rendering_internal_provider_id as bigint)) as rendering_min_raw,
	max(cast(rendering_internal_provider_id as bigint)) as rendering_max_raw
from PHClaims.stage.apcd_medical_claim;

--Check claim header ID for a single row
select * from PHClaims.stage.apcd_claim_provider
where claim_header_id = 629250074914541;

select medical_claim_service_line_id, billing_provider_internal_id, rendering_internal_provider_id, 
	attending_internal_provider_id, referring_internal_provider_id, first_service_dt, last_service_dt
from PHClaims.stage.apcd_medical_claim
where medical_claim_header_id = 629250074914541;


