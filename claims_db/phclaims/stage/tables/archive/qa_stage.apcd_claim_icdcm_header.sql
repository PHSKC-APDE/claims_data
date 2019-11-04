--QA of stage.apcd_claim_icdcm_header table
--5/7/19
--Eli Kern
--Run time: 39 min


--All members should be in elig_demo and elig_timevar tables
select count(distinct a.id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_icdcm_header as a
left join PHClaims.final.apcd_elig_demo as b
on a.id_apcd = b.id_apcd
where b.id_apcd is null;

select count(distinct a.id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_icdcm_header as a
left join PHClaims.final.apcd_elig_timevar as b
on a.id_apcd = b.id_apcd
where b.id_apcd is null;

--Check that length of all ICD-9-CM is 5
select min(len(icdcm_norm)) as min_len, max(len(icdcm_norm)) as max_len
from PHClaims.stage.apcd_claim_icdcm_header
where icdcm_version = 9;

--Check to make sure no null diagnoses made it through
select count(*)
from PHClaims.stage.apcd_claim_icdcm_header
where icdcm_raw is null;

--Count distinct claim header IDs that have a 25th diagnosis code
select count(distinct claim_header_id) as header_cnt
from PHClaims.stage.apcd_claim_icdcm_header
where icdcm_number = '25';

select count(distinct medical_claim_header_id) as header_cnt
from PHClaims.stage.apcd_medical_claim
where diagnosis_code_other_24 is not null and denied_claim_flag = 'N' and orphaned_adjustment_flag = 'N';

--Count diagnosis codes that do not join to ICD-CM reference table
select distinct a.icdcm_norm, a.icdcm_version
from PHClaims.stage.apcd_claim_icdcm_header as a
left join PHClaims.ref.dx_lookup as b
on a.icdcm_norm = b.dx and a.icdcm_version = b.dx_ver
where b.dx is null;

--Assess min and max length of ICD-10-CM codes
--Should be between 3 and 7
select a.icd_len, count(a.icdcm_norm) as icd_count
from (
select icdcm_norm, len(icdcm_norm) icd_len
from PHClaims.stage.apcd_claim_icdcm_header
where icdcm_version = 10
) as a
group by a.icd_len;

