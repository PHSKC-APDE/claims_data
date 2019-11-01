
USE PHClaims;
GO

IF OBJECT_ID('[metadata].[v_perf_measure_summary]', 'V') IS NOT NULL
DROP VIEW [metadata].[v_perf_measure_summary];
GO
CREATE VIEW [metadata].[v_perf_measure_summary]
AS

SELECT 
 '[stage].[perf_elig_member_month]' AS [Table]
,'N/A' AS [Measure Name]
,'N/A' AS [Component]
,NULL AS [Load Date]
,CAST(MIN([CLNDR_YEAR_MNTH]) AS VARCHAR(6)) AS [First Measurement Period]
,CAST(MAX([CLNDR_YEAR_MNTH]) AS VARCHAR(6)) AS [Last Measurement Period]
,COUNT(*) AS [Row Count]
FROM [stage].[perf_elig_member_month]

UNION ALL

SELECT 
 '[stage].[perf_enroll_denom]' AS [Table]
,'N/A' AS [Measure Name]
,'N/A' AS [Component]
,NULL AS [Load Date]
,CAST(MIN([year_month]) AS VARCHAR(6)) AS [First Measurement Period]
,CAST(MAX([year_month]) AS VARCHAR(6)) AS [Last Measurement Period]
,COUNT(*) AS [Row Count]
FROM [stage].[perf_enroll_denom]

UNION ALL

SELECT 
 '[stage].[mcaid_perf_measure]' AS [Table]
,[measure_name] AS [Measure Name]
,'Measure' AS [Component]
,[load_date] AS [Load Date]
,CAST(MIN([beg_year_month]) AS VARCHAR(6)) + ' - ' + CAST(MIN([end_year_month]) AS VARCHAR(6)) AS [First Measurement Period]
,CAST(MAX([beg_year_month]) AS VARCHAR(6)) + ' - ' + CAST(MAX([end_year_month]) AS VARCHAR(6)) AS [Last Measurement Period]
,COUNT(*) AS [Row Count]
FROM [stage].[mcaid_perf_measure] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
GROUP BY b.[measure_id], [measure_name], [load_date]

UNION ALL

SELECT 
 '[stage].[perf_staging]' AS [Table]
,[measure_name] AS [Measure Name]
,'Denominator' AS [Component]
,[load_date] AS [Load Date]
,CAST(MIN([year_month]) AS VARCHAR(6)) AS [First Measurement Period]
,CAST(MAX([year_month]) AS VARCHAR(6)) AS [Last Measurement Period]
,COUNT(*)
FROM [stage].[perf_staging] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE [num_denom] = 'D'
GROUP BY b.[measure_id], [measure_name], [load_date]

UNION ALL

SELECT 
 '[stage].[perf_staging_event_date]' AS [Table]
,[measure_name] AS [Measure Name]
,'Measure' AS [Component]
,[load_date] AS [Load Date]
,CAST(MIN([year_month]) AS VARCHAR(6)) AS [First Measurement Period]
,CAST(MAX([year_month]) AS VARCHAR(6)) AS [Last Measurement Period]
,COUNT(*)
FROM [stage].[perf_staging_event_date] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
GROUP BY b.[measure_id], [measure_name], [load_date]

GO

/*
SELECT * FROM [metadata].[v_perf_measure_summary]
ORDER BY 
 [Table]
,[Measure Name]
,[Component]
,[Load Date];
*/