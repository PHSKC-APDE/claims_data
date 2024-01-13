--Line-level QA of stage.apcd_claim_provider
--7/7/19
--Eli Kern
--Run time: 2 min

--Check claim header ID for a single row
select * from PHClaims.stage.apcd_claim_provider
where claim_header_id = 629250074914541;

select medical_claim_header_id, medical_claim_service_line_id, billing_provider_internal_id, rendering_internal_provider_id, 
	attending_internal_provider_id, referring_internal_provider_id, first_service_dt, last_service_dt
from PHClaims.stage.apcd_medical_claim
where medical_claim_header_id = 629250074914541;


