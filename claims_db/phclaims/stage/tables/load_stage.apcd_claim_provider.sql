--Code to load data to stage.apcd_claim_provider
--Distinct header-level provider variables for medical claims (billing, rendering, attending, referring)
--Eli Kern (PHSKC-APDE)
--2019-6-29 
--Run time: 65 min

------------------
--STEP 1: Extract header-level provider variables and insert into table shell
-------------------
insert into PHClaims.stage.apcd_claim_provider with (tablock)
select distinct
b.id_apcd,
b.extract_id,
d.medical_claim_header_id as claim_header_id, 
e.first_service_dt as first_service_date,
e.last_service_dt as last_service_date,
b.provider_id_apcd,
b.provider_type
from (
	--reshape provider ID columns to single column
	select id_apcd, extract_id, medical_claim_service_line_id, cast(providers as bigint) as provider_id_apcd,
		cast(provider_type as varchar(255)) as provider_type
	from (
	select internal_member_id as id_apcd, extract_id, medical_claim_service_line_id,
		billing_internal_provider_id as billing, rendering_internal_provider_id as rendering,
		attending_internal_provider_id as attending, referring_internal_provider_id as referring
	from PHClaims.stage.apcd_medical_claim
	where denied_claim_flag = 'N' and orphaned_adjustment_flag = 'N' 
	) as a
	unpivot(providers for provider_type in(billing, rendering, attending, referring)) as providers
	where providers != '-1'
) as b
--remove claims with no eligibility info
left join PHClaims.ref.apcd_claim_no_elig as c
on b.id_apcd = c.id_apcd
--grab claim header ID and service dates
left join PHClaims.stage.apcd_medical_crosswalk as d
on b.medical_claim_service_line_id = d.medical_claim_service_line_id
left join PHClaims.stage.apcd_medical_claim_header as e
on d.medical_claim_header_id = e.medical_claim_header_id
where c.id_apcd is null;



