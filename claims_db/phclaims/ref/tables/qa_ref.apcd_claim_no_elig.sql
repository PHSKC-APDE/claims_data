--QA of ref.apcd_claim_no_elig table
--5/14/19
--Eli Kern

--Check to make sure that there are no records in member_month_detail for IDs in this table
select a.internal_member_id, a.year_month
from PHClaims.stage.apcd_member_month_detail as a
inner join PHClaims.ref.apcd_claim_no_elig as b
on a.internal_member_id = b.id_apcd;

