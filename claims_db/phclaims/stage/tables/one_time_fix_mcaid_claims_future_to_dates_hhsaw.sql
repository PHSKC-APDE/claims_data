--Eli Kern, PHSKC, APDE
--2024-08-27
--One-time fix to correct incorrect to service dates (in future)

--Claim header table
select distinct first_service_date, last_service_date
from claims.final_mcaid_claim_header
where year(last_service_date) > year(sysdatetime())
order by first_service_date, last_service_date;

UPDATE claims.final_mcaid_claim_header
SET last_service_date = '2024-04-25'
WHERE first_service_date = '2024-04-15' and last_service_date = '2025-04-25';

UPDATE claims.final_mcaid_claim_header
SET last_service_date = '2017-01-06'
WHERE first_service_date = '2017-01-06' and last_service_date = '2107-01-06';

--ICD-CM table
select distinct first_service_date, last_service_date
from claims.final_mcaid_claim_icdcm_header
where year(last_service_date) > year(sysdatetime())
order by first_service_date, last_service_date;

UPDATE claims.final_mcaid_claim_icdcm_header
SET last_service_date = '2024-04-25'
WHERE first_service_date = '2024-04-15' and last_service_date = '2025-04-25';

--Claim line table
select distinct first_service_date, last_service_date
from claims.final_mcaid_claim_line
where year(last_service_date) > year(sysdatetime())
order by first_service_date, last_service_date;

UPDATE claims.final_mcaid_claim_line
SET last_service_date = '2024-04-25'
WHERE first_service_date = '2024-04-15' and last_service_date = '2025-04-25';

UPDATE claims.final_mcaid_claim_line
SET last_service_date = '2017-01-06'
WHERE first_service_date = '2017-01-06' and last_service_date = '2107-01-06';

--Procedure code table
select distinct first_service_date, last_service_date
from claims.final_mcaid_claim_procedure
where year(last_service_date) > year(sysdatetime())
order by first_service_date, last_service_date;

UPDATE claims.final_mcaid_claim_procedure
SET last_service_date = '2024-04-25'
WHERE first_service_date = '2024-04-15' and last_service_date = '2025-04-25';

UPDATE claims.final_mcaid_claim_procedure
SET last_service_date = '2017-01-06'
WHERE first_service_date = '2017-01-06' and last_service_date = '2107-01-06';