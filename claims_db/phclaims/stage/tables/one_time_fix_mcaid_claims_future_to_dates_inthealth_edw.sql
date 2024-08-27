--Eli Kern, PHSKC, APDE
--2024-08-27
--One-time fix to correct incorrect to service dates (in future)

--find to_service_dates where year is in future (error)
select distinct from_srvc_date, to_srvc_date
from stg_claims.mcaid_claims_incr
where year(to_srvc_date) > year(sysdatetime())
order by from_srvc_date, to_srvc_date;

--one-time fix using UPDATE
UPDATE stg_claims.mcaid_claims_incr
SET to_srvc_date = '2024-04-25'
WHERE from_srvc_date = '2024-04-15' and to_srvc_date = '2025-04-25';

--find to_service_dates where year is in future (error)
select distinct from_srvc_date, to_srvc_date
from stg_claims.stage_mcaid_claims
where year(to_srvc_date) > year(sysdatetime())
order by from_srvc_date, to_srvc_date;

--one-time fix using UPDATE
UPDATE stg_claims.stage_mcaid_claims
SET to_srvc_date = '2024-04-25'
WHERE from_srvc_date = '2024-04-15' and to_srvc_date = '2025-04-25';

UPDATE stg_claims.stage_mcaid_claims
SET to_srvc_date = '2017-01-06'
WHERE from_srvc_date = '2017-01-06' and to_srvc_date = '2107-01-06';

