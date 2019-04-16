
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

The UNION operator removes duplicates 
*/

-- Get Observation Stays
SELECT DISTINCT 
 hd.[id]
,hd.[tcn]
,[from_date]
,[to_date]
,[patient_status]
,CASE WHEN [patient_status] = 'Expired' THEN 1 ELSE 0 END AS [death_during_stay]
,1 AS [observation_stay]
FROM [dbo].[mcaid_claim_header] AS hd
INNER JOIN [dbo].[mcaid_claim_proc] AS pr
ON hd.[tcn] = pr.[tcn]
INNER JOIN [ref].[hedis_code_system] AS hed 
 ON hed.[value_set_name] = 'Observation'
AND hed.[code_system] = 'CPT'
AND pr.[pcode] = hed.[code]

/* Now, exclude observation stays that result in an inpatient stay */
WHERE hd.[tcn] NOT IN
(
/* These are claims where an observation stay occurs on the same day as an acute inpatient 
stay or where an observation stay occurs one day prior to an acute inpatient stay */
-- 1,273 Rows
SELECT b.[tcn]

FROM [dbo].[mcaid_claim_header] AS a
INNER JOIN
(
SELECT
 hd.[id]
,hd.[tcn]
,hd.[from_date]
,hd.[to_date]
FROM [dbo].[mcaid_claim_header] AS hd
INNER JOIN [dbo].[mcaid_claim_proc] AS pr
ON hd.[tcn] = pr.[tcn]
INNER JOIN [ref].[hedis_code_system] AS hed 
 ON hed.[value_set_name] = 'Observation'
AND hed.[code_system] = 'CPT'
AND pr.[pcode] = hed.[code]
) AS b
ON a.[id] = b.[id]
AND DATEDIFF(DAY, b.[to_date], a.[from_date]) IN (0, 1)
WHERE a.[clm_type_code] IN (31, 33)

UNION 

(
/* There were no Medicaid claims with an Acute Inpatient Stay and
an Observation Stay on the same [tcn] (0 rows) */
SELECT [tcn]
FROM [dbo].[mcaid_claim_header]
WHERE [clm_type_code] IN (31, 33)

INTERSECT

SELECT pr.[tcn]
FROM [dbo].[mcaid_claim_proc] AS pr
INNER JOIN [ref].[hedis_code_system] AS hed 
 ON hed.[value_set_name] = 'Observation'
AND hed.[code_system] = 'CPT'
AND pr.[pcode] = hed.[code]
));
GO

/*
SELECT COUNT(*) FROM [stage].[v_perf_ah_observation_stay];
*/