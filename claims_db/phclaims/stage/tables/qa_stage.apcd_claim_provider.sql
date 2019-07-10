--QA of stage.apcd_claim_provider
--7/7/19
--Eli Kern
--Run time: XX min

--Compare min/max of provider ID variables with medical_claim table
select min(billing_provider_id_apcd) as billing_min, min(rendering_provider_id_apcd) as rendering_min,
	min(attending_provider_id_apcd) as attending_min, min(referring_provider_id_apcd) as referring_min,
	max(billing_provider_id_apcd) as billing_max, max(rendering_provider_id_apcd) as rendering_max,
	max(attending_provider_id_apcd) as attending_max, max(referring_provider_id_apcd) as referring_max
from PHClaims.stage.apcd_claim_provider;

select min(cast(billing_internal_provider_id as bigint)) as billing_min_raw,
	min(cast(rendering_internal_provider_id as bigint)) as rendering_min_raw,
	min(cast(attending_internal_provider_id as bigint)) as attending_min_raw,
	min(cast(referring_internal_provider_id as bigint)) as referring_min_raw,
	max(cast(billing_internal_provider_id as bigint)) as billing_max_raw,
	max(cast(rendering_internal_provider_id as bigint)) as rendering_max_raw,
	max(cast(attending_internal_provider_id as bigint)) as attending_max_raw,
	max(cast(referring_internal_provider_id as bigint)) as referring_max_raw
from PHClaims.stage.apcd_medical_claim;

--Check claim header ID for a single row
select * from PHClaims.stage.apcd_claim_provider
where claim_header_id = 629250074914541;

select a.medical_claim_service_line_id, a.billing_internal_provider_id, a.rendering_internal_provider_id, 
	a.attending_internal_provider_id, a.referring_internal_provider_id
from PHClaims.stage.apcd_medical_claim as a
left join PHClaims.stage.apcd_medical_crosswalk as b
on a.medical_claim_service_line_id = b.medical_claim_service_line_id
where b.medical_claim_header_id = 629250074914541;


