--QA of stage.apcd_claim_provider
--7/7/19
--Eli Kern
--Run time: XX min

--Compare min/max of provider ID variables with medical_claim table (12 min)
select min(provider_id_apcd) as rendering_min, max(provider_id_apcd) as rendering_max
from PHClaims.stage.apcd_claim_provider
where provider_type = 'rendering';

select min(cast(rendering_internal_provider_id as bigint)) as rendering_min_raw,
	max(cast(rendering_internal_provider_id as bigint)) as rendering_max_raw
from PHClaims.stage.apcd_medical_claim;

--Check claim header ID for a single row
select * from PHClaims.stage.apcd_claim_provider
where claim_header_id = 629250074914541;

select a.medical_claim_service_line_id, a.billing_internal_provider_id, a.rendering_internal_provider_id, 
	a.attending_internal_provider_id, a.referring_internal_provider_id, a.first_service_dt, a.last_service_dt
from PHClaims.stage.apcd_medical_claim as a
left join PHClaims.stage.apcd_medical_crosswalk as b
on a.medical_claim_service_line_id = b.medical_claim_service_line_id
where b.medical_claim_header_id = 629250074914541;


