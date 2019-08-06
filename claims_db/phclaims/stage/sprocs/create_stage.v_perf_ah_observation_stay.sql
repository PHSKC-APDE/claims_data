
USE PHClaims;
GO

IF OBJECT_ID('[stage].[v_perf_ah_observation_stay]', 'V') IS NOT NULL
DROP VIEW [stage].[v_perf_ah_observation_stay];
GO
CREATE VIEW [stage].[v_perf_ah_observation_stay]
AS
/*
HEDIS Guideline
If an observation stay results in an acute inpatient stay, include only the acute 
inpatient stay discharge. When an observation visit and an inpatient stay are billed 
on separate claims, the visit results in an inpatient stay when the admission date 
for the inpatient stay occurs on the observation date of service or one calendar day 
after. An observation visit billed on the same claim as an inpatient stay is 
considered a visit that resulted in an inpatient stay.

SELECT [value_set_name]
      ,[code_system]
      ,[code]
      ,[definition]
      ,[value_set_version]
      ,[code_system_version]
      ,[value_set_oid]
      ,[code_system_oid]
FROM [ref].[hedis_code_system]
WHERE [value_set_name] = 'Observation';

The UNION operator removes duplicates 
*/

-- Get Observation Stays
SELECT DISTINCT 
 hd.[id_mcaid]
,hd.[claim_header_id]
,hd.[first_service_date]
,hd.[last_service_date]
,hd.[patient_status]
,CASE WHEN [patient_status] = '20' THEN 1 ELSE 0 END AS [death_during_stay]
,1 AS [observation_stay]
FROM [final].[mcaid_claim_header] AS hd
INNER JOIN [final].[mcaid_claim_procedure] AS pr
ON hd.[claim_header_id] = pr.[claim_header_id]
INNER JOIN [ref].[hedis_code_system] AS hed 
 ON hed.[value_set_name] = 'Observation'
AND hed.[code_system] = 'CPT'
AND pr.[procedure_code] = hed.[code]

/* Now, exclude observation stays that result in an inpatient stay */
WHERE hd.[claim_header_id] NOT IN
(
/* These are claims where an observation stay occurs on the same day as an acute inpatient 
stay or where an observation stay occurs one day prior to an acute inpatient stay (1,688 rows) */
SELECT b.[claim_header_id]

FROM [final].[mcaid_claim_header] AS a
INNER JOIN
(
SELECT
 pr.[id_mcaid]
,pr.[claim_header_id]
,pr.[first_service_date]
,pr.[last_service_date]
FROM [final].[mcaid_claim_procedure] AS pr
INNER JOIN [ref].[hedis_code_system] AS hed 
 ON hed.[value_set_name] = 'Observation'
AND hed.[code_system] = 'CPT'
AND pr.[procedure_code] = hed.[code]
) AS b
ON a.[id_mcaid] = b.[id_mcaid]
AND DATEDIFF(DAY, b.[last_service_date], a.[first_service_date]) IN (0, 1)
WHERE a.[clm_type_mcaid_id] IN (31, 33)

UNION 

(
/* There were no Medicaid claims with an Acute Inpatient Stay and
an Observation Stay on the same [claim_header_id] (0 rows) */
SELECT [claim_header_id]
FROM [final].[mcaid_claim_header]
WHERE [clm_type_mcaid_id] IN (31, 33)

INTERSECT

SELECT pr.[claim_header_id]
FROM [final].[mcaid_claim_procedure] AS pr
INNER JOIN [ref].[hedis_code_system] AS hed 
 ON hed.[value_set_name] = 'Observation'
AND hed.[code_system] = 'CPT'
AND pr.[procedure_code] = hed.[code]
));
GO

/*
SELECT COUNT(*) FROM [stage].[v_perf_ah_observation_stay];
*/