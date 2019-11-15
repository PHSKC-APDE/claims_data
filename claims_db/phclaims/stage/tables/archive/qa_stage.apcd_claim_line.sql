--QA of stage.apcd_claim_line table
--8/22/19
--Eli Kern
--Run time: XX min

--Confirm that most claim lines are distinct (11 min) [don't need to check as part of regular QA]
select count(a.claim_line_id) as qa
from (
select claim_line_id, count(*) as row_count
from phclaims.stage.apcd_claim_line
group by claim_line_id
) as a
where a.row_count > 1;

--Compare sum of member ID and claim line ID (3 min)
select sum(cast(id_apcd as decimal(38,0))) as id_sum, sum(cast(claim_line_id as decimal(38,0))) as line_sum
from PHClaims.stage.apcd_claim_line;

select sum(cast(internal_member_id as decimal(38,0))) as id_sum, sum(cast(medical_claim_service_line_id as decimal(38,0))) as line_sum
from PHClaims.stage.apcd_medical_claim
where denied_claim_flag = 'N' and orphaned_adjustment_flag = 'N';

--All members should be in elig_demo and elig_timevar tables (6 min)
select count(a.id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_line as a
left join PHClaims.final.apcd_elig_demo as b
on a.id_apcd = b.id_apcd
where b.id_apcd is null;

select count(a.id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_line as a
left join PHClaims.final.apcd_elig_timevar as b
on a.id_apcd = b.id_apcd
where b.id_apcd is null;