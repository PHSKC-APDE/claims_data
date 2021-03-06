
/*
This view gets follow-up visits for the FUH measure:
Follow-up Hospitalization for Mental Illness: 7 days
Follow-up Hospitalization for Mental Illness: 30 days

Author: Philip Sylling
Created: 2019-04-24
Modified: 2019-08-09 | Point to new [final] analytic tables

Returns:
SELECT 
 [id_mcaid]
,[claim_header_id]
,[service_date] = [first_service_date]
,[flag] = 1 for claim meeting FUH follow-up visits criteria
,[only_30_day_fu], if 'Y' claim only meets requirement for 30-day follow-up, if 'N' claim meets requirement for 7-day and 30-day follow-up
*/

USE [PHClaims];
GO

IF OBJECT_ID('[stage].[v_perf_fuh_follow_up_visit]', 'V') IS NOT NULL
DROP VIEW [stage].[v_perf_fuh_follow_up_visit];
GO
CREATE VIEW [stage].[v_perf_fuh_follow_up_visit]
AS
/*
SELECT [measure_id]
      ,[value_set_name]
      ,[value_set_oid]
FROM [archive].[hedis_value_set]
WHERE [measure_id] = 'FUH';

SELECT [value_set_name]
      ,[code_system]
      ,COUNT([code])
FROM [archive].[hedis_code_system]
WHERE [value_set_name] IN
('TCM 14 Day'
,'TCM 7 Day')
GROUP BY [value_set_name], [code_system]
ORDER BY [value_set_name], [code_system];

SELECT 'FUH' AS [measure_id]
      ,[value_set_name]
      ,[code_system]
      ,[code]
FROM [archive].[hedis_code_system]
WHERE [value_set_name] IN
('TCM 14 Day'
,'TCM 7 Day')
ORDER BY [value_set_name], [code_system], [code];
*/

WITH [get_claims] AS
(
/*
Condition 1:
A visit (FUH Stand Alone Visits Value Set) with a mental health practitioner, 
with or without a telehealth modifier (Telehealth Modifier Value Set).
*/
(
SELECT 
 [id_mcaid]
,[claim_header_id]
,[service_date]
,1 AS [flag]
,'N' AS [only_30_day_fu]

--SELECT COUNT(*)
FROM [stage].[mcaid_claim_value_set]
WHERE 1 = 1
AND [value_set_group] = 'HEDIS'
AND [value_set_name] IN 
('FUH Stand Alone Visits')
AND [code_set] IN ('CPT', 'HCPCS')
)

UNION
/*
Condition 2:
A visit (FUH Visits Group 1 Value Set with FUH POS Group 1 Value Set) with a 
mental health practitioner, with or without a telehealth modifier (Telehealth 
Modifier Value Set).
*/
(
SELECT 
 [id_mcaid]
,[claim_header_id]
,[service_date]
,1 AS [flag]
,'N' AS [only_30_day_fu]

--SELECT COUNT(*)
FROM [stage].[mcaid_claim_value_set]
WHERE 1 = 1
AND [value_set_group] = 'HEDIS'
AND [value_set_name] IN 
('FUH Visits Group 1')
AND [code_set] IN ('CPT')

INTERSECT

SELECT 
 [id_mcaid]
,[claim_header_id]
,[service_date]
,1 AS [flag]
,'N' AS [only_30_day_fu]

--SELECT COUNT(*)
FROM [stage].[mcaid_claim_value_set]
WHERE 1 = 1
AND [value_set_group] = 'HEDIS'
AND [value_set_name] IN 
('FUH POS Group 1')
AND [code_set] = 'POS'
)

UNION
/*
Condition 3:
A visit (FUH Visits Group 2 Value Set with FUH POS Group 2 Value Set) with a 
mental health practitioner, with or without a telehealth modifier (Telehealth 
Modifier Value Set).
*/
(
SELECT 
 [id_mcaid]
,[claim_header_id]
,[service_date]
,1 AS [flag]
,'N' AS [only_30_day_fu]

--SELECT COUNT(*)
FROM [stage].[mcaid_claim_value_set]
WHERE 1 = 1
AND [value_set_group] = 'HEDIS'
AND [value_set_name] IN 
('FUH Visits Group 2')
AND [code_set] IN ('CPT')

INTERSECT

SELECT 
 [id_mcaid]
,[claim_header_id]
,[service_date]
,1 AS [flag]
,'N' AS [only_30_day_fu]

--SELECT COUNT(*)
FROM [stage].[mcaid_claim_value_set]
WHERE 1 = 1
AND [value_set_group] = 'HEDIS'
AND [value_set_name] IN 
('FUH POS Group 2')
AND [code_set] = 'POS'
)

UNION
/*
Condition 4:
A visit in a behavioral healthcare setting (FUH RevCodes Group 1 Value Set).
*/
(
SELECT 
 [id_mcaid]
,[claim_header_id]
,[service_date]
,1 AS [flag]
,'N' AS [only_30_day_fu]

--SELECT COUNT(*)
FROM [stage].[mcaid_claim_value_set]
WHERE 1 = 1
AND [value_set_group] = 'HEDIS'
AND [value_set_name] IN 
('FUH RevCodes Group 1')
AND [code_set] = 'UBREV'
)

UNION
/*
Condition 5:
A visit in a nonbehavioral healthcare setting (FUH RevCodes Group 2 Value Set) 
with a mental health practitioner.
*/
(
SELECT 
 [id_mcaid]
,[claim_header_id]
,[service_date]
,1 AS [flag]
,'N' AS [only_30_day_fu]

--SELECT COUNT(*)
FROM [stage].[mcaid_claim_value_set]
WHERE 1 = 1
AND [value_set_group] = 'HEDIS'
AND [value_set_name] IN 
('FUH RevCodes Group 2')
AND [code_set] = 'UBREV'
)

UNION
/*
Condition 7:
Transitional care management services (TCM 7 Day Value Set), with or without a 
telehealth modifier (Telehealth Modifier Value Set).
*/
(
SELECT 
 [id_mcaid]
,[claim_header_id]
,[service_date]
,1 AS [flag]
,'N' AS [only_30_day_fu]

--SELECT COUNT(*)
FROM [stage].[mcaid_claim_value_set]
WHERE 1 = 1
AND [value_set_group] = 'HEDIS'
AND [value_set_name] IN 
('TCM 7 Day')
AND [code_set] IN ('CPT')
)

UNION
/*
Condition 8:
Transitional care management services (TCM 14 Day Value Set), with or without a
telehealth modifier (Telehealth Modifier Value Set).
*/
(
SELECT 
 [id_mcaid]
,[claim_header_id]
,[service_date]
,1 AS [flag]
,'Y' AS [only_30_day_fu]

--SELECT COUNT(*)
FROM [stage].[mcaid_claim_value_set]
WHERE 1 = 1
AND [value_set_group] = 'HEDIS'
AND [value_set_name] IN 
('TCM 14 Day')
AND [code_set] IN ('CPT')
)
)

SELECT 
 [id_mcaid]
,[claim_header_id]
,[service_date]
,[flag]
,MAX([only_30_day_fu]) AS [only_30_day_fu] 

FROM [get_claims]
GROUP BY [id_mcaid], [claim_header_id], [service_date], [flag]
GO

/*
-- 8,098,388
IF OBJECT_ID('tempdb..#temp') IS NOT NULL
DROP TABLE #temp;
SELECT * 
INTO #temp
FROM [stage].[v_perf_fuh_follow_up_visit]
WHERE [service_date] BETWEEN '2016-01-01' AND '2018-12-31';

-- 2,617,306
IF OBJECT_ID('tempdb..#temp') IS NOT NULL
DROP TABLE #temp;
SELECT * 
INTO #temp
FROM [stage].[v_perf_fuh_follow_up_visit]
WHERE [service_date] BETWEEN '2017-01-01' AND '2017-12-31';
*/