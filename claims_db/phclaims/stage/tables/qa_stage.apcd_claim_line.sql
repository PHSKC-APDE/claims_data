--QA of stage.apcd_claim_line table
--8/22/19
--Eli Kern
--Run time: XX min

--Confirm that most (18 min)
select count(distinct claim_line_id) as line_cnt
from PHClaims.stage.apcd_claim_line;

select count(claim_line_id) as line_cnt
from PHClaims.stage.apcd_claim_line;

----investigate if any above sums do not equal (X min)
--select a.*
--from (
--select claim_line_id, count(claim_line_id) as row_count
--from PHClaims.stage.apcd_claim_line
--group by claim_line_id
--) as a
--where a.row_count > 1;

--known cases:

--Compare sum of member ID and claim line ID, excluding members with no claims from raw line table (3 min)
select sum(cast(id_apcd as decimal(38,0))) as id_sum, sum(cast(claim_line_id as decimal(38,0))) as line_sum
from PHClaims.stage.apcd_claim_line;

select sum(cast(a.internal_member_id as decimal(38,0))) as id_sum, sum(cast(a.medical_claim_service_line_id as decimal(38,0))) as line_sum
from PHClaims.stage.apcd_medical_claim as a
left join PHClaims.ref.apcd_claim_no_elig as b
on a.internal_member_id = b.id_apcd
where b.id_apcd is null
and a.denied_claim_flag = 'N' and a.orphaned_adjustment_flag = 'N';

--All members should be in elig_demo and elig_timevar tables (X min)
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