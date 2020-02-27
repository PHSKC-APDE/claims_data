
USE [PHClaims];
GO

/*
Preliminary Stored Procedures
*/
EXEC [stage].[sp_mcaid_perf_elig_member_month];
GO

EXEC [stage].[sp_mcaid_perf_enroll_denom] @start_date_int = 201601, @end_date_int = 201912
GO

EXEC [stage].[sp_mcaid_perf_distinct_member];
GO

EXEC [stage].[sp_mcaid_perf_enroll_provider] @start_date_int = 201502, @end_date_int = 201912;
GO

/*
See summary of person-month-level table of enrollment and provider criteria
*/
SELECT 
 [year_month]
,[end_quarter]
,COUNT(*)
FROM [stage].[mcaid_perf_enroll_denom]
GROUP BY [year_month], [end_quarter]
ORDER BY [year_month], [end_quarter];

SELECT 
 [year_month]
,[end_quarter]
,COUNT(*)
FROM [stage].[mcaid_perf_enroll_provider]
GROUP BY [year_month], [end_quarter]
ORDER BY [year_month], [end_quarter];

/*
There should be no duplicate rows at the person-measurement period-level.
Check if [num_rows] = 1 below.
*/
SELECT
 [num_rows]
,COUNT(*)
FROM
(
SELECT 
 [CLNDR_YEAR_MNTH]
,[MEDICAID_RECIPIENT_ID]
,COUNT(*) AS [num_rows]
FROM (SELECT DISTINCT [CLNDR_YEAR_MNTH], [MEDICAID_RECIPIENT_ID] FROM [stage].[mcaid_perf_elig_member_month]) AS a
GROUP BY
 [CLNDR_YEAR_MNTH]
,[MEDICAID_RECIPIENT_ID]
) AS SubQuery
GROUP BY [num_rows]
ORDER BY [num_rows];

SELECT
 [num_rows]
,COUNT(*)
FROM
(
SELECT 
 [year_month]
,[id_mcaid]
,COUNT(*) AS [num_rows]
FROM (SELECT DISTINCT [year_month], [id_mcaid] FROM [stage].[mcaid_perf_enroll_denom]) AS a
--FROM (SELECT DISTINCT [year_month], [id_mcaid] FROM [stage].[mcaid_perf_enroll_provider]) AS a
GROUP BY
 [year_month]
,[id_mcaid]
) AS SubQuery
GROUP BY [num_rows]
ORDER BY [num_rows];