--Code to load data to ref.apcd_mcare_carrier_billing_npi
--Crosswalk between APCD claim header ID and RESDAC claim ID for carrier claims
--Eli Kern (PHSKC-APDE)
--2019-11
--Run time: XX min


------------------
--STEP 1: Create table shell
-------------------
if object_id('PHClaims.ref.apcd_mcare_carrier_billing_npi', 'U') is not null drop table PHClaims.ref.apcd_mcare_carrier_billing_npi;
create table PHClaims.ref.apcd_mcare_carrier_billing_npi (
	claim_header_id bigint,
	carr_clm_blg_npi_num bigint,
	last_run datetime
);

------------------
--STEP 2: Grab NPIs for billing providers on Medicare FFS carrier claims using in-house data
-------------------
insert into PHClaims.ref.apcd_mcare_carrier_billing_npi with (tablock)
select a.medical_claim_header_id as claim_header_id, cast(b.carr_clm_blg_npi_num as bigint) as carr_clm_blg_npi_num, getdate() as last_run
from (
	select submitter_clm_control_num collate SQL_Latin1_General_CP1_CS_AS as submitter_clm_control_num,
		medical_claim_header_id
	from PHClaims.stage.apcd_medical_claim
	where submitted_claim_type_id in (24,25)
) as a
left join PHClaims.stage.mcare_bcarrier_claims as b
on a.submitter_clm_control_num = b.clm_id;

------------------
--QA: Verify that there are no claims that have submitted claim type other than Medicare carrier
--Note that this turns out to not be true because OnPoint clusters multiple Resdac headers into a single header in Enclave
--As a temporary solution until OnPoint fixes their data issue, this is good enough
-------------------
select 'ref.apcd_mcare_carrier_billing_npi' as 'table', '# claims not having carrier claim type, expect 0' as 'qa_type',
	count(a.claim_header_id) as qa
from phclaims.ref.apcd_mcare_carrier_billing_npi as a
left join PHClaims.stage.apcd_medical_claim as b
on a.claim_header_id = b.medical_claim_header_id
where b.submitted_claim_type_id not in (24,25);