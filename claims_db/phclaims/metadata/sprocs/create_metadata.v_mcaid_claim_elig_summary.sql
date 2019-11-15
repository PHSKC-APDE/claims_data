
USE [PHClaims];
GO

IF OBJECT_ID('[metadata].[v_mcaid_claim_elig_summary]', 'V') IS NOT NULL
DROP VIEW [metadata].[v_mcaid_claim_elig_summary];
GO
CREATE VIEW [metadata].[v_mcaid_claim_elig_summary]
AS

SELECT 
 '[stage].[mcaid_claim]' AS [table]
,a.[etl_batch_id]
,MIN([CLNDR_YEAR_MNTH]) AS [first_calendar_month]
,MAX([CLNDR_YEAR_MNTH]) AS [last_calendar_month]
,COUNT(*) AS [row_count]
,[batch_type]
,[data_source]
,[date_min]
,[date_max]
,[delivery_date]
,[note]

FROM [stage].[mcaid_claim] AS a
INNER JOIN [metadata].[etl_log] AS b
ON a.[etl_batch_id] = b.[etl_batch_id]

GROUP BY
 a.[etl_batch_id]
,[batch_type]
,[data_source]
,[date_min]
,[date_max]
,[delivery_date]
,[note]

UNION ALL

SELECT 
 '[stage].[mcaid_elig]' AS [table]
,a.[etl_batch_id]
,MIN([CLNDR_YEAR_MNTH]) AS [first_calendar_month]
,MAX([CLNDR_YEAR_MNTH]) AS [last_calendar_month]
,COUNT(*) AS [row_count]
,[batch_type]
,[data_source]
,[date_min]
,[date_max]
,[delivery_date]
,[note]

FROM [stage].[mcaid_elig] AS a
INNER JOIN [metadata].[etl_log] AS b
ON a.[etl_batch_id] = b.[etl_batch_id]

GROUP BY
 a.[etl_batch_id]
,[batch_type]
,[data_source]
,[date_min]
,[date_max]
,[delivery_date]
,[note]
GO

/*
SELECT * FROM [metadata].[v_mcaid_claim_elig_summary]
ORDER BY 
 [table]
,[etl_batch_id];
*/