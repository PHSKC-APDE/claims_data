--QA of stage.apcd_claim_procedure table
--8/23/19
--Eli Kern
--Run time: XX min


--All members should be in elig_demo and elig_timevar tables
select count(distinct a.id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_procedure as a
left join PHClaims.final.apcd_elig_demo as b
on a.id_apcd = b.id_apcd
where b.id_apcd is null;

select count(distinct a.id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_procedure as a
left join PHClaims.final.apcd_elig_timevar as b
on a.id_apcd = b.id_apcd
where b.id_apcd is null;

--Check to make sure no -1 and -2 values made it through
select count(*)
from PHClaims.stage.apcd_claim_procedure
where procedure_code = '-1' or procedure_code = '-2';

--Count distinct claim header IDs that have a 25th procedure code
--Note that second code block appropriately excludes members with no elig information
select count(distinct claim_header_id) as header_cnt
from PHClaims.stage.apcd_claim_procedure
where procedure_code_number = '25';

select count(distinct b.medical_claim_header_id) as header_cnt
from (
select medical_claim_service_line_id
from PHClaims.stage.apcd_medical_claim
where icd_procedure_code_24 != '-1' and icd_procedure_code_24 != '-2'
	and denied_claim_flag = 'N' and orphaned_adjustment_flag = 'N'
	and internal_member_id not in (select id_apcd from PHClaims.ref.apcd_claim_no_elig)
) as a
left join phclaims.stage.apcd_medical_crosswalk as b
on a.medical_claim_service_line_id = b.medical_claim_service_line_id;

--Compare number of people with apcd_claim_header table
select count(distinct id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_procedure;

select count(distinct id_apcd) as id_dcount
from PHClaims.stage.apcd_claim_header;

