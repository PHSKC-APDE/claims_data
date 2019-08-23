--QA of stage.apcd_claim_header table
--7/9/19
--Eli Kern
--Run time: ~28 min

--Confirm that claim header is distinct (5 min)
select count(distinct claim_header_id) as header_cnt
from PHClaims.stage.apcd_claim_header;

select count(claim_header_id) as header_cnt
from PHClaims.stage.apcd_claim_header;

----investigate if any above sums do not equal
--select a.*
--from (
--select claim_header_id, count(claim_header_id) as row_count
--from PHClaims.stage.apcd_claim_header
--group by claim_header_id
--) as a
--where a.row_count > 1

--known cases:
--header ID 629252324839878 in extracts 149 and 159 (inpatient stay that cuts across two extracts)

--Compare min and max member ID, claim header ID and provider ID, excluding members with no claims from raw header table (3 min)
select min(id_apcd) as member_min, max(id_apcd) as member_max, min(claim_header_id) as header_min,
	max(claim_header_id) as header_max, min(billing_provider_id_apcd) as provider_min,
	max(billing_provider_id_apcd) as provider_max
from PHClaims.stage.apcd_claim_header;

select min(a.internal_member_id) as member_min, max(a.internal_member_id) as member_max,
	min(a.medical_claim_header_id) as header_min, max(a.medical_claim_header_id) as header_max,
	min(a.internal_provider_id) as provider_min,
	max(a.internal_provider_id) as provider_max
from PHClaims.stage.apcd_medical_claim_header as a
left join PHClaims.ref.apcd_claim_no_elig as b
on a.internal_member_id = b.id_apcd
where b.id_apcd is null;

--All members should be in elig_demo and elig_timevar tables
select count(a.id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_header as a
left join PHClaims.final.apcd_elig_demo as b
on a.id_apcd = b.id_apcd
where b.id_apcd is null;

select count(a.id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_header as a
left join PHClaims.final.apcd_elig_timevar as b
on a.id_apcd = b.id_apcd
where b.id_apcd is null;

--Check an individual inpatient stay to make sure discharge date has come through correctly (13 min)
select id_apcd, claim_header_id, ipt_flag, discharge_date
from PHClaims.stage.apcd_claim_header
where claim_header_id = 629250577489368;

select a.*
from PHClaims.stage.apcd_medical_claim as a
left join PHClaims.stage.apcd_medical_crosswalk as b
on a.medical_claim_service_line_id = b.medical_claim_service_line_id
where b.medical_claim_header_id = 629250577489368;

--Verify that an example of a denied and orphaned claim header is excluded (1 min)
select * 
from PHClaims.stage.apcd_claim_header
where claim_header_id = 629246595882276;

select * 
from PHClaims.stage.apcd_claim_header
where claim_header_id = 629250026243188;

--Query distinct APCD claim types by KC claim type (<1 min)
select distinct claim_type_id, claim_type_apcd_id
from PHClaims.stage.apcd_claim_header;

--Should be no claims where ipt_flag = 1 and discharge_date is null (1 min)
select count(*)
from PHClaims.stage.apcd_claim_header
where ipt_flag = 1 and discharge_date is null;