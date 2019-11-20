--Code to load data to ref.apcd_denied_orphaned_header
--Contains the claim header-level minimum and maximum value of the line-level denied and orphaned claim flags
--Eli Kern (PHSKC-APDE)
--2019-11

insert into PHClaims.ref.apcd_denied_orphaned_header with (tablock)
select medical_claim_header_id as claim_header_id,
  min(case when denied_claim_flag = 'Y' then 1 else 0 end) as denied_min,
  max(case when denied_claim_flag = 'Y' then 1 else 0 end) as denied_max,
  min(case when orphaned_adjustment_flag = 'Y' then 1 else 0 end) as orphaned_min,
  max(case when orphaned_adjustment_flag = 'Y' then 1 else 0 end) as orphaned_max
from PHClaims.stage.apcd_medical_claim
group by medical_claim_header_id;


