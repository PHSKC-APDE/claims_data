

USE [PHClaims];
GO

IF OBJECT_ID('[stage].[v_mcaid_pqi_inpatient]', 'V') IS NOT NULL
DROP VIEW [stage].[v_mcaid_pqi_inpatient];
GO
CREATE VIEW [stage].[v_mcaid_pqi_inpatient]
AS

WITH [get_pqi_claims] AS
(
/*
Prevention Quality Indicator 01 (PQI 01) Diabetes Short-Term Complications
*/
SELECT 
 b.[value_set_group]
,a.[claim_header_id]
,a.[inpatient_id]
,1 AS [flag]

FROM [stage].[mcaid_claim_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[primary_diagnosis] = b.[code]
/*
Discharges, for patients ages 18 years and older, with a principal ICD-10-CM 
diagnosis code for diabetes short-term complications (ketoacidosis, 
hyperosmolarity, or coma) (ACDIASD)
*/
AND b.[value_set_name] IN ('ACDIASD')

WHERE a.[inpatient_id] IS NOT NULL
AND (a.[admsn_source] IS NULL OR a.[admsn_source] NOT IN ('4', '5', '6', 'A', 'B', 'C', 'D', 'E', 'F'))

UNION

/*
Prevention Quality Indicator 03 (PQI 03) Diabetes Long-Term Complications
*/
SELECT 
 b.[value_set_group]
,a.[claim_header_id]
,a.[inpatient_id]
,1 AS [flag]

FROM [stage].[mcaid_claim_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[primary_diagnosis] = b.[code]
/*
Discharges, for patients ages 18 years and older, with a principal ICD-10-CM 
diagnosis code for diabetes with long-term complications (renal, eye, 
neurological, circulatory, or complications not otherwise specified) (ACDIALD)
*/
AND b.[value_set_name] IN ('ACDIALD')

WHERE a.[inpatient_id] IS NOT NULL
AND (a.[admsn_source] IS NULL OR a.[admsn_source] NOT IN ('4', '5', '6', 'A', 'B', 'C', 'D', 'E', 'F'))

UNION

/*
Prevention Quality Indicator 05 (PQI 05) Chronic Obstructive Pulmonary 
Disease (COPD) or Asthma in Older Adults
*/
SELECT 
 CASE 
 WHEN b.[value_set_group] = 'PQI 05/PQI 15' THEN 'PQI 05' 
 WHEN b.[value_set_group] = 'PQI 05' THEN b.[value_set_group] 
 END AS [value_set_group]
,a.[claim_header_id]
,a.[inpatient_id]
,1 AS [flag]

FROM [stage].[mcaid_claim_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[primary_diagnosis] = b.[code]
/*
Discharges, for patients ages 40 years and older, with either a principal 
ICD-10-CM diagnosis code for COPD (ACCOPDD) (excluding acute bronchitis) or a 
principal ICD-10-CM diagnosis code for asthma (ACSASTD)
*/
AND b.[value_set_name] IN ('ACCOPDD', 'ACSASTD')

WHERE a.[inpatient_id] IS NOT NULL
AND (a.[admsn_source] IS NULL OR a.[admsn_source] NOT IN ('4', '5', '6', 'A', 'B', 'C', 'D', 'E', 'F'))

/*
Exclude cases with any-listed ICD-10-CM diagnosis codes for cystic fibrosis and
anomalies of the respiratory system (RESPAN)
*/
AND [claim_header_id] NOT IN 
(
SELECT DISTINCT [claim_header_id]
FROM [final].[mcaid_claim_icdcm_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[icdcm_version] = 10
AND a.[icdcm_norm] = b.[code]
AND b.[value_set_name] IN ('RESPAN')
)

UNION

-- Prevention Quality Indicator 07 (PQI 07) Hypertension
SELECT 
 b.[value_set_group]
,a.[claim_header_id]
,a.[inpatient_id]
,1 AS [flag]

FROM [stage].[mcaid_claim_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[primary_diagnosis] = b.[code]
/*
Discharges, for patients ages 18 years and older, with a principal ICD-10-CM 
diagnosis code for hypertension (ACSHYPD)
*/
AND b.[value_set_name] IN ('ACSHYPD')

WHERE a.[inpatient_id] IS NOT NULL
AND (a.[admsn_source] IS NULL OR a.[admsn_source] NOT IN ('4', '5', '6', 'A', 'B', 'C', 'D', 'E', 'F'))

/*
Exclude cases with any-listed ICD-10-PCS procedure codes for cardiac procedure 
(ACSCARP)
*/
AND [claim_header_id] NOT IN 
(
SELECT DISTINCT [claim_header_id]
FROM [final].[mcaid_claim_procedure] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[procedure_code] = b.[code]
AND b.[value_set_name] IN ('ACSCARP')
)

/*
Exclude cases with any-listed ICD-10-CM diagnosis codes of Stage I-IV kidney 
disease (ACSHY2D), only if accompanied by any-listed ICD-10-PCS procedure codes
for dialysis access (DIALY2P)
*/
AND [claim_header_id] NOT IN 
(
SELECT [claim_header_id]
FROM [final].[mcaid_claim_icdcm_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[icdcm_version] = 10
AND a.[icdcm_norm] = b.[code]
AND b.[value_set_name] IN ('ACSHY2D')

INTERSECT

SELECT [claim_header_id]
FROM [final].[mcaid_claim_procedure] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[procedure_code] = b.[code]
AND b.[value_set_name] IN ('DIALY2P')
)

UNION

-- Prevention Quality Indicator 08 (PQI 08) Heart Failure
SELECT 
 b.[value_set_group]
,a.[claim_header_id]
,a.[inpatient_id]
,1 AS [flag]

FROM [stage].[mcaid_claim_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[primary_diagnosis] = b.[code]
/*
Discharges, for patients ages 18 years and older, with a principal ICD-10-CM 
diagnosis code for heart failure (MRTCHFD)
*/
AND b.[value_set_name] IN ('MRTCHFD')

WHERE a.[inpatient_id] IS NOT NULL
AND (a.[admsn_source] IS NULL OR a.[admsn_source] NOT IN ('4', '5', '6', 'A', 'B', 'C', 'D', 'E', 'F'))

/*
Exclude cases with any-listed ICD-10-PCS procedure codes for cardiac procedure 
(ACSCARP)
*/
AND [claim_header_id] NOT IN 
(
SELECT DISTINCT [claim_header_id]
FROM [final].[mcaid_claim_procedure] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[procedure_code] = b.[code]
AND b.[value_set_name] IN ('ACSCARP')
)

UNION

/*
Prevention Quality Indicator 11 (PQI 11) Community-Acquired Pneumonia
*/
SELECT 
 b.[value_set_group]
,a.[claim_header_id]
,a.[inpatient_id]
,1 AS [flag]

FROM [stage].[mcaid_claim_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[primary_diagnosis] = b.[code]

/*
Discharges, for patients ages 18 years and older, with a principal ICD-10-CM 
diagnosis code for bacterial pneumonia (ACSBACD)
*/
AND b.[value_set_name] IN ('ACSBACD')

WHERE a.[inpatient_id] IS NOT NULL
AND (a.[admsn_source] IS NULL OR a.[admsn_source] NOT IN ('4', '5', '6', 'A', 'B', 'C', 'D', 'E', 'F'))

/*
Exclude cases with any-listed ICD-10-CM diagnosis codes for sickle cell anemia 
or HB-S disease (ACSBA2D)
*/
AND [claim_header_id] NOT IN 
(
SELECT DISTINCT [claim_header_id]
FROM [final].[mcaid_claim_icdcm_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[icdcm_version] = 10
AND a.[icdcm_norm] = b.[code]
AND b.[value_set_name] IN ('ACSBA2D')
)

/*
Exclude cases with any-listed ICD-10-CM diagnosis codes for immunocompromised 
state (IMMUNID)
*/
AND [claim_header_id] NOT IN 
(
SELECT DISTINCT [claim_header_id]
FROM [final].[mcaid_claim_icdcm_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[icdcm_version] = 10
AND a.[icdcm_norm] = b.[code]
AND b.[value_set_name] IN ('IMMUNID')
)

/*
Exclude cases with any-listed ICD-10-PCS procedure codes for immunocompromised 
state (IMMUNIP)
*/
AND [claim_header_id] NOT IN 
(
SELECT DISTINCT [claim_header_id]
FROM [final].[mcaid_claim_procedure] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[procedure_code] = b.[code]
AND b.[value_set_name] IN ('IMMUNIP')
)

UNION

-- Prevention Quality Indicator 12 (PQI 12) Urinary Tract Infection
SELECT 
 b.[value_set_group]
,a.[claim_header_id]
,a.[inpatient_id]
,1 AS [flag]

FROM [stage].[mcaid_claim_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[primary_diagnosis] = b.[code]
/*
Discharges, for patients ages 18 years and older, with a principal ICD-10-CM 
diagnosis code for urinary tract infection (ACSUTID)
*/
AND b.[value_set_name] IN ('ACSUTID')

WHERE a.[inpatient_id] IS NOT NULL
AND (a.[admsn_source] IS NULL OR a.[admsn_source] NOT IN ('4', '5', '6', 'A', 'B', 'C', 'D', 'E', 'F'))

/*
Exclude cases with any-listed ICD-10-CM diagnosis codes for kidney/urinary 
tract disorder (KIDNEY)
*/
AND [claim_header_id] NOT IN 
(
SELECT DISTINCT [claim_header_id]
FROM [final].[mcaid_claim_icdcm_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[icdcm_version] = 10
AND a.[icdcm_norm] = b.[code]
AND b.[value_set_name] IN ('KIDNEY')
)

/*
Exclude cases with any-listed ICD-10-CM diagnosis codes for immunocompromised 
state (IMMUNID)
*/
AND [claim_header_id] NOT IN 
(
SELECT DISTINCT [claim_header_id]
FROM [final].[mcaid_claim_icdcm_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[icdcm_version] = 10
AND a.[icdcm_norm] = b.[code]
AND b.[value_set_name] IN ('IMMUNID')
)

/*
Exclude cases with any-listed ICD-10-PCS procedure codes for immunocompromised 
state (IMMUNIP)
*/
AND [claim_header_id] NOT IN 
(
SELECT DISTINCT [claim_header_id]
FROM [final].[mcaid_claim_procedure] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[procedure_code] = b.[code]
AND b.[value_set_name] IN ('IMMUNIP')
)

UNION

-- Prevention Quality Indicator 14 (PQI 14) Uncontrolled Diabetes
SELECT 
 b.[value_set_group]
,a.[claim_header_id]
,a.[inpatient_id]
,1 AS [flag]

FROM [stage].[mcaid_claim_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[primary_diagnosis] = b.[code]

/*
Discharges, for patients ages 18 years and older, with a principal ICD-10-CM 
diagnosis code for uncontrolled diabetes without mention of a short-term or 
long-term complication (ACDIAUD)
*/
AND b.[value_set_name] IN ('ACDIAUD')

WHERE a.[inpatient_id] IS NOT NULL
AND (a.[admsn_source] IS NULL OR a.[admsn_source] NOT IN ('4', '5', '6', 'A', 'B', 'C', 'D', 'E', 'F'))

UNION

-- Prevention Quality Indicator 15 (PQI 15) Asthma in Younger Adults
SELECT 
 CASE 
 WHEN b.[value_set_group] = 'PQI 05/PQI 15' THEN 'PQI 15' 
 END AS [value_set_group]
,a.[claim_header_id]
,a.[inpatient_id]
,1 AS [flag]

FROM [stage].[mcaid_claim_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[primary_diagnosis] = b.[code]

/*
Discharges, for patients ages 18 through 39 years, with a principal ICD-10-CM 
diagnosis code for asthma (ACSASTD)
*/
AND b.[value_set_name] IN ('ACSASTD')

WHERE a.[inpatient_id] IS NOT NULL
AND (a.[admsn_source] IS NULL OR a.[admsn_source] NOT IN ('4', '5', '6', 'A', 'B', 'C', 'D', 'E', 'F'))

/*
Exclude cases with any-listed ICD-10-CM diagnosis codes for cystic fibrosis and
anomalies of the respiratory system (RESPAN)
*/
AND [claim_header_id] NOT IN 
(
SELECT DISTINCT [claim_header_id]
FROM [final].[mcaid_claim_icdcm_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[icdcm_version] = 10
AND a.[icdcm_norm] = b.[code]
AND b.[value_set_name] IN ('RESPAN')
)

UNION

/*
Prevention Quality Indicator 16 (PQI 16) Lower-Extremity Amputation Among 
Patients with Diabetes
*/
SELECT
 a.[value_set_group]
,a.[claim_header_id]
,b.[inpatient_id]
,1 AS [flag]
FROM
(
/*
Discharges, for patients ages 18 years and older, with any-listed ICD-10-PCS 
procedure codes for lower-extremity amputation (ACSLEAP) and any-listed 
ICD-10-CM diagnosis codes for diabetes (ACSLEAD)
*/
SELECT 
 b.[value_set_group]
,a.[claim_header_id]
FROM [final].[mcaid_claim_procedure] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[procedure_code] = b.[code]
AND b.[value_set_name] IN ('ACSLEAP')

INTERSECT

SELECT 
 b.[value_set_group]
,a.[claim_header_id]
FROM [final].[mcaid_claim_icdcm_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[icdcm_version] = 10
AND a.[icdcm_norm] = b.[code]
AND b.[value_set_name] IN ('ACSLEAD')
) AS a

-- Claim has to be a valid inpatient claim that is not a transfer (by [admsn_source])
INNER JOIN [stage].[mcaid_claim_header] AS b
ON a.[claim_header_id] = b.[claim_header_id]
AND b.[inpatient_id] IS NOT NULL
AND (b.[admsn_source] IS NULL OR b.[admsn_source] NOT IN ('4', '5', '6', 'A', 'B', 'C', 'D', 'E', 'F'))

/*
Exclude cases with any-listed ICD-10-CM diagnosis codes for traumatic 
amputation of the lower extremity (ACLEA2D)
*/
WHERE a.[claim_header_id] NOT IN 
(
SELECT DISTINCT [claim_header_id]
FROM [final].[mcaid_claim_icdcm_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[icdcm_version] = 10
AND a.[icdcm_norm] = b.[code]
AND b.[value_set_name] IN ('ACLEA2D')
)

/*
Exclude cases with a major diagnositic category for pregnancy, childbirth and 
puerperium (MDC 14)
*/
AND a.[claim_header_id] NOT IN 
(
SELECT DISTINCT [claim_header_id]
FROM [final].[mcaid_claim_header] AS a
INNER JOIN [ref].[ahrq_value_set] AS b
ON a.[drvd_drg_code] = b.[code]
AND b.[value_set_name] IN ('MDC 14')
)
)

SELECT
 [claim_header_id]
,[inpatient_id]
,ISNULL([PQI 01], 0) AS [pqi_01]
,ISNULL([PQI 03], 0) AS [pqi_03]
,ISNULL([PQI 05], 0) AS [pqi_05]
,ISNULL([PQI 07], 0) AS [pqi_07]
,ISNULL([PQI 08], 0) AS [pqi_08]
,ISNULL([PQI 11], 0) AS [pqi_11]
,ISNULL([PQI 12], 0) AS [pqi_12]
,ISNULL([PQI 14], 0) AS [pqi_14]
,ISNULL([PQI 15], 0) AS [pqi_15]
,ISNULL([PQI 16], 0) AS [pqi_16]

,ISNULL([PQI 01], 0) +
 ISNULL([PQI 03], 0) +
 ISNULL([PQI 05], 0) +
 ISNULL([PQI 07], 0) +
 ISNULL([PQI 08], 0) +
 ISNULL([PQI 11], 0) +
 ISNULL([PQI 12], 0) +
 ISNULL([PQI 14], 0) +
 ISNULL([PQI 15], 0) +
 ISNULL([PQI 16], 0) AS [pqi_composite]

FROM [get_pqi_claims]
PIVOT(MAX([flag]) FOR [value_set_group] IN 
([PQI 01]
,[PQI 03]
,[PQI 05]
,[PQI 07]
,[PQI 08]
,[PQI 11]
,[PQI 12]
,[PQI 14]
,[PQI 15]
,[PQI 16])) AS P;
GO

/*
SELECT * FROM [stage].[v_mcaid_pqi_inpatient];
*/