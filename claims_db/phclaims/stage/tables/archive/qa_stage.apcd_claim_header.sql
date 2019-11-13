--QA of stage.apcd_claim_header table
--2019-11
--Eli Kern
--Run time: ~54 min

--Confirm that claim header is distinct (4.5 min), expect 0
select count(a.claim_header_id) as qa
from (
select claim_header_id, count(*) as header_cnt
from PHClaims.stage.apcd_claim_header
group by claim_header_id
) as a
where a.header_cnt > 1;

--Compare member and claim header counts with raw data (35 min!!), expect match
select count(distinct id_apcd) as id_dcount, count(distinct claim_header_id) as header_dcount
from PHClaims.stage.apcd_claim_header;

select count(distinct internal_member_id) as id_dcount, count(distinct medical_claim_header_id) as header_dcount
from PHClaims.stage.apcd_medical_claim
where denied_claim_flag = 'N' and orphaned_adjustment_flag = 'N';

--All members should be in elig_demo and elig_timevar tables (run time: 2 min), expect 0
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

--Count unmatched claim types, expect 0 (1 min)
select count(*) as qa
from PHClaims.stage.apcd_claim_header
where claim_type_id is null or claim_type_apcd_id is null;

--Should be no claims where ipt_flag = 1 and discharge_date is null (1 min), expect 0
select count(*) as qa
from PHClaims.stage.apcd_claim_header
where inpatient_id is not null and discharge_date is null;

--Verify that no ed_pophealth_id value is used for more than one person, expect 0 (run-time: 2 min)
select count(a.ed_pophealth_id) as qa
from (
select ed_pophealth_id, count(distinct id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_header
group by ed_pophealth_id
) as a
where a.id_dcount > 1;

--Verify that ed_pophealth_id does not skip any values (run-time: 2 min)
select count(distinct ed_pophealth_id) as qa1, min(ed_pophealth_id) as qa2,
	max(ed_pophealth_id) as qa3, max(ed_pophealth_id) - min(ed_pophealth_id) + 1 as q4
from PHClaims.stage.apcd_claim_header;