
USE [PHClaims];
GO

IF OBJECT_ID('[stage].[v_mcaid_mh_aod_ed]', 'V') IS NOT NULL
DROP VIEW [stage].[v_mcaid_mh_aod_ed];
GO
CREATE VIEW [stage].[v_mcaid_mh_aod_ed]
AS

WITH [ed_pophealth_id] AS
(
SELECT
 hed.[value_set_name]
,hd.[ed_pophealth_id]
--,hd.[claim_header_id]
,1 AS [flag]
FROM [final].[mcaid_claim_header] AS hd
INNER JOIN [archive].[hedis_code_system] AS hed
ON hed.[value_set_name] IN
('Mental Illness')
AND hd.[icdcm_version] = 10
AND hd.[primary_diagnosis] = hed.[code]
WHERE [ed_pophealth_id] IS NOT NULL

UNION

SELECT 
 hed.[value_set_name]
,hd.[ed_pophealth_id]
--,hd.[claim_header_id]
,1 AS [flag]
FROM [final].[mcaid_claim_header] AS hd
INNER JOIN [archive].[hedis_code_system] AS hed
ON hed.[value_set_name] IN
('AOD Abuse and Dependence')
AND hd.[icdcm_version] = 10
AND hd.[primary_diagnosis] = hed.[code]
WHERE [ed_pophealth_id] IS NOT NULL
)

SELECT
 [ed_pophealth_id]
--,[claim_header_id]
,ISNULL([Mental Illness], 0) AS [mental_illness]
,ISNULL([AOD Abuse and Dependence], 0) AS [aod_abuse_dependence]

FROM [ed_pophealth_id]
PIVOT(MAX([flag]) FOR [value_set_name] IN 
([Mental Illness]
,[AOD Abuse and Dependence])) AS P;
GO

/*
SELECT * FROM [stage].[v_mcaid_mh_aod_ed];

SELECT * FROM [stage].[v_mcaid_mh_aod_ed]
WHERE [mental_illness] = 1 AND [aod_abuse_dependence] = 1;
*/