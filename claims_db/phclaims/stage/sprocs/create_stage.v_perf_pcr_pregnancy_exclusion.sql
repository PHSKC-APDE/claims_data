
USE PHClaims;
GO

IF OBJECT_ID('[stage].[v_perf_pcr_pregnancy_exclusion]', 'V') IS NOT NULL
DROP VIEW [stage].[v_perf_pcr_pregnancy_exclusion];
GO
CREATE VIEW [stage].[v_perf_pcr_pregnancy_exclusion]
AS
/*
SELECT [value_set_name]
      ,[code_system]
      ,COUNT([code])
FROM [archive].[hedis_code_system]
WHERE [value_set_name] IN
('Inpatient Stay'
,'Nonacute Inpatient Stay'
,'Pregnancy'
,'Perinatal Conditions')
GROUP BY [value_set_name], [code_system]
ORDER BY [code_system], [value_set_name];
*/
WITH [get_claims] AS
(
SELECT 
 ln.[id_mcaid]
,ln.[claim_header_id]
,ln.[first_service_date]
,ln.[last_service_date]
FROM [final].[mcaid_claim_line] AS ln
INNER JOIN [archive].[hedis_code_system] AS hed
ON hed.[value_set_name] IN 
('Inpatient Stay')
AND hed.[code_system] = 'UBREV'
AND ln.[rev_code] = hed.[code]

INTERSECT

SELECT 
 dx.[id_mcaid]
,dx.[claim_header_id]
,dx.[first_service_date]
,dx.[last_service_date]
FROM [final].[mcaid_claim_icdcm_header] AS dx
INNER JOIN [archive].[hedis_code_system] AS hed
ON hed.[value_set_name] IN 
('Perinatal Conditions'
,'Pregnancy')
AND hed.[code_system] = 'ICD10CM'
AND dx.[icdcm_version] = 10
-- Principal Diagnosis
AND dx.[icdcm_number] = '01'
AND dx.[icdcm_norm] = hed.[code]

EXCEPT

(
SELECT 
 ln.[id_mcaid]
,ln.[claim_header_id]
,ln.[first_service_date]
,ln.[last_service_date]

FROM [final].[mcaid_claim_line] AS ln
INNER JOIN [archive].[hedis_code_system] AS hed
ON hed.[value_set_name] IN 
('Nonacute Inpatient Stay')
AND hed.[code_system] = 'UBREV'
AND ln.[rev_code] = hed.[code]

UNION

SELECT 
 hd.[id_mcaid]
,hd.[claim_header_id]
,hd.[first_service_date]
,hd.[last_service_date]

FROM [final].[mcaid_claim_header] AS hd
INNER JOIN [archive].[hedis_code_system] AS hed
ON [value_set_name] IN 
('Nonacute Inpatient Stay')
AND hed.[code_system] = 'UBTOB' 
AND hd.[type_of_bill_code] = hed.[code]
))

SELECT
 [id_mcaid]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,1 AS [flag]
FROM [get_claims];
GO

/*
SELECT * FROM [stage].[v_perf_pcr_pregnancy_exclusion];
*/