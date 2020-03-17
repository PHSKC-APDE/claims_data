
USE [PHClaims];
GO

/*
Preliminary Stored Procedures
*/

/*
(1) This has no parameters - Table is dropped/created
*/
EXEC [stage].[sp_mcaid_perf_elig_member_month];
GO

/*
(2) This step does have parameters. Guidance: Run the queries below and find 
the last twelve months. If the last twelve months are 201901 to 201912, set 
@start_date_int = 201901, @end_date_int = 201912.

SELECT 
 [CLNDR_YEAR_MNTH]
,COUNT(*)
FROM [stage].[mcaid_perf_elig_member_month]
GROUP BY [CLNDR_YEAR_MNTH]
ORDER BY [CLNDR_YEAR_MNTH];

SELECT 
 [year_month]
,[end_quarter]
,[load_date]
,COUNT(*)
FROM [stage].[mcaid_perf_enroll_denom]
GROUP BY [year_month], [end_quarter], [load_date]
ORDER BY [year_month], [end_quarter], [load_date];
*/

EXEC [stage].[sp_mcaid_perf_enroll_denom] @start_date_int = 201901, @end_date_int = 201912
GO

/*
(3) This has no parameters - Table is dropped/created
*/
EXEC [stage].[sp_mcaid_perf_distinct_member];
GO

/*
(4) This step does have parameters. Guidance: Run the queries below and find 
the last twelve months. If the last twelve months are 201901 to 201912, set 
@start_date_int = 201901, @end_date_int = 201912.

SELECT 
 [CLNDR_YEAR_MNTH]
,COUNT(*)
FROM [stage].[mcaid_perf_elig_member_month]
GROUP BY [CLNDR_YEAR_MNTH]
ORDER BY [CLNDR_YEAR_MNTH];

SELECT 
 [year_month]
,[end_quarter]
,[load_date]
,COUNT(*)
FROM [stage].[mcaid_perf_enroll_provider]
GROUP BY [year_month], [end_quarter], [load_date]
ORDER BY [year_month], [end_quarter], [load_date];
*/

EXEC [stage].[sp_mcaid_perf_enroll_provider] @start_date_int = 201901, @end_date_int = 201912;
GO

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
GROUP BY
 [year_month]
,[id_mcaid]
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
FROM (SELECT DISTINCT [year_month], [id_mcaid] FROM [stage].[mcaid_perf_enroll_provider]) AS a
GROUP BY
 [year_month]
,[id_mcaid]
) AS SubQuery
GROUP BY [num_rows]
ORDER BY [num_rows];