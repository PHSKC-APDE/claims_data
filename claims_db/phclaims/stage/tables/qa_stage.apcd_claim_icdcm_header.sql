--QA of stage.apcd_claim_icdcm_header table
--5/7/19
--Eli Kern
--Run time: 75 min


--All members should be in elig_demo and elig_timevar tables
select count(distinct a.id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_icdcm_header as a
left join PHClaims.stage.apcd_elig_demo as b
on a.id_apcd = b.id_apcd
where b.id_apcd is null;

select count(distinct a.id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_icdcm_header as a
left join PHClaims.stage.apcd_elig_timevar as b
on a.id_apcd = b.id_apcd
where b.id_apcd is null;

--Check that length of all ICD-9-CM is 5
select min(len(icdcm_norm)) as min_len, max(len(icdcm_norm)) as max_len
from PHClaims.stage.apcd_claim_icdcm_header
where icdcm_version = 9;

--Check to make sure no -1 and -2 diagnoses made it through
select count(*)
from PHClaims.stage.apcd_claim_icdcm_header
where icdcm_raw = '-1' or icdcm_raw = '-2';

--Count distinct claim header IDs that have a 25th diagnosis code
--Note that second code block appropriately excludes members with no elig information
select count(distinct claim_header_id) as header_cnt
from PHClaims.stage.apcd_claim_icdcm_header
where icdcm_number = '25';

select count(distinct b.medical_claim_header_id) as header_cnt
from (
select medical_claim_service_line_id
from PHClaims.stage.apcd_medical_claim
where diagnosis_code_other_24 != '-1' and diagnosis_code_other_24 != '-2'
	and denied_claim_flag = 'N' and orphaned_adjustment_flag = 'N'
	and internal_member_id not in (select id_apcd from PHClaims.ref.apcd_claim_no_elig)
) as a
left join phclaims.stage.apcd_medical_crosswalk as b
on a.medical_claim_service_line_id = b.medical_claim_service_line_id;

--Count diagnosis codes that do not join to ICD-CM reference table
select distinct a.icdcm_norm, a.icdcm_version
from PHClaims.stage.apcd_claim_icdcm_header as a
left join PHClaims.ref.dx_lookup as b
on a.icdcm_norm = b.dx and a.icdcm_version = b.dx_ver
where b.dx is null;

--Assess min and max length of ICD-10-CM codes
select a.icd_len, count(a.icdcm_norm) as icd_count
from (
select icdcm_norm, len(icdcm_norm) icd_len
from PHClaims.stage.apcd_claim_icdcm_header
where icdcm_version = 10
) as a
group by a.icd_len;

--Compare number of people with apcd_claim_header table
select count(distinct id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_icdcm_header;

select count(distinct id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_header;

