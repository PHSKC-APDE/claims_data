
USE [PHClaims];
GO

IF OBJECT_ID('[stage].[sp_perf_enroll_provider]','P') IS NOT NULL
DROP PROCEDURE [stage].[sp_perf_enroll_provider];
GO
CREATE PROCEDURE [stage].[sp_perf_enroll_provider]
 @start_date_int INT = 201701
,@end_date_int INT = 201712
AS
SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN

SET @SQL = @SQL + N'

IF OBJECT_ID(''tempdb..#perf_elig_member_month'') IS NOT NULL
DROP TABLE #perf_elig_member_month;
SELECT 
 [CLNDR_YEAR_MNTH] AS [year_month]
,[MEDICAID_RECIPIENT_ID] AS [id_mcaid]
,CASE WHEN [COVERAGE_TYPE_IND] = ''FFS'' THEN ''FFS'' ELSE [MC_PRVDR_NAME] END AS [mco_or_ffs]
,1 AS [flag]
INTO #perf_elig_member_month
FROM [stage].[perf_elig_member_month]
WHERE 1 = 1
AND ([CLNDR_YEAR_MNTH] BETWEEN ' + CAST(@start_date_int AS VARCHAR(6)) + ' AND ' + CAST(@end_date_int AS VARCHAR(6)) + ')
AND ([COVERAGE_TYPE_IND] = ''FFS'' OR ([COVERAGE_TYPE_IND] = ''MC'' AND [MC_PRVDR_NAME] IS NOT NULL));

CREATE CLUSTERED INDEX [idx_cl_#perf_elig_member_month] ON #perf_elig_member_month([id_mcaid], [mco_or_ffs], [year_month]);

IF OBJECT_ID(''tempdb..#perf_elig_member'') IS NOT NULL
DROP TABLE #perf_elig_member;
SELECT DISTINCT
 [id_mcaid]
,[mco_or_ffs]
INTO #perf_elig_member
FROM #perf_elig_member_month;

CREATE CLUSTERED INDEX [idx_cl_#perf_elig_member] ON #perf_elig_member([id_mcaid], [mco_or_ffs]);

IF OBJECT_ID(''tempdb..#year_month'') IS NOT NULL
DROP TABLE #year_month;
SELECT 
 [year_month]
,[month]
,ROW_NUMBER() OVER(ORDER BY [year_month]) AS [row_num]
INTO #year_month
FROM [ref].[perf_year_month]
WHERE ([year_month] BETWEEN ' + CAST(@start_date_int AS VARCHAR(6)) + ' AND ' + CAST(@end_date_int AS VARCHAR(6)) + ');

CREATE CLUSTERED INDEX [idx_cl_#year_month] ON #year_month([year_month]);

IF OBJECT_ID(''tempdb..#cross_join'') IS NOT NULL
DROP TABLE #cross_join;
SELECT 
 a.[year_month]
,a.[month]
,a.[row_num]
,b.[id_mcaid]
,b.[mco_or_ffs]
,c.[flag]
INTO #temp
FROM #year_month AS a
CROSS JOIN #perf_elig_member AS b
LEFT JOIN #perf_elig_member_month AS c
ON b.[id_mcaid] = c.[id_mcaid]
AND b.[mco_or_ffs] = c.[mco_or_ffs]
AND a.[year_month] = c.[year_month];

CREATE CLUSTERED INDEX [idx_cl_#cross_join] ON #cross_join([id_mcaid], [mco_or_ffs], [year_month]);

IF OBJECT_ID(''tempdb..#coverage_months_t_12_m'') IS NOT NULL
DROP TABLE #coverage_months_t_12_m;
SELECT
 [year_month]
,[month]
,[row_num]
,[id_mcaid]
,[mco_or_ffs]
,[flag]
,SUM([flag]) OVER(PARTITION BY [id_mcaid], [mco_or_ffs] ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [coverage_months_t_12_m]
INTO #coverage_months_t_12_m
FROM #cross_join;

CREATE CLUSTERED INDEX [idx_cl_#coverage_months_t_12_m] ON #coverage_months_t_12_m([id_mcaid], [year_month], [coverage_months_t_12_m]);

IF OBJECT_ID(''[stage].[perf_enroll_provider]'') IS NOT NULL
DROP TABLE [stage].[perf_enroll_provider];
WITH CTE AS
(
SELECT
 [year_month]
,CASE WHEN [month] IN (3, 6, 9, 12) THEN 1 ELSE 0 END AS [end_quarter]
,[row_num]
,[id_mcaid]
,[mco_or_ffs]
,[flag]
,[coverage_months_t_12_m]
,ROW_NUMBER() OVER(PARTITION BY [id_mcaid], [year_month] ORDER BY [coverage_months_t_12_m] DESC, [flag] DESC) AS [tie_breaker] 
FROM #coverage_months_t_12_m
)
SELECT
 [year_month]
,[end_quarter]
,[id_mcaid]
,[mco_or_ffs]
,[coverage_months_t_12_m]
INTO [stage].[perf_enroll_provider]
FROM CTE
WHERE [coverage_months_t_12_m] >= 1 
AND [tie_breaker] = 1;


















IF OBJECT_ID(''tempdb..#temp'', ''U'') IS NOT NULL
DROP TABLE #temp;
SELECT *
INTO #temp
FROM [stage].[fn_perf_enroll_member_month](' + CAST(@start_date_int AS NVARCHAR(20)) + ', ' + CAST(@end_date_int AS NVARCHAR(20)) + ');

CREATE CLUSTERED INDEX [idx_cl_#temp_id_year_month] ON #temp([id_mcaid], [year_month]);

IF OBJECT_ID(''[stage].[perf_enroll_denom]'',''U'') IS NOT NULL
DROP TABLE [stage].[perf_enroll_denom];

WITH CTE AS
(
SELECT
 [year_month]
,[month]
,[id_mcaid]
,[dob]
,[end_month_age]
,CASE WHEN [end_month_age] BETWEEN 0 AND 20 THEN [age_in_months] ELSE NULL END AS [age_in_months]

,[enrolled_any]
,SUM([enrolled_any]) OVER(PARTITION BY [id_mcaid] ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [enrolled_any_t_12_m]

,[full_benefit]
,SUM([full_benefit]) OVER(PARTITION BY [id_mcaid] ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [full_benefit_t_12_m]

,[dual]
,SUM([dual]) OVER(PARTITION BY [id_mcaid] ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [dual_t_12_m]

,[tpl]
,SUM([tpl]) OVER(PARTITION BY [id_mcaid] ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [tpl_t_12_m]

,[hospice]
,SUM([hospice]) OVER(PARTITION BY [id_mcaid] ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [hospice_t_12_m]
,SUM([hospice]) OVER(PARTITION BY [id_mcaid] ORDER BY [year_month] ROWS BETWEEN 23 PRECEDING AND 12 PRECEDING) AS [hospice_prior_t_12_m]
,SUM([hospice]) OVER(PARTITION BY [id_mcaid] ORDER BY [year_month] ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS [hospice_p_2_m]

,[full_criteria]
,SUM([full_criteria]) OVER(PARTITION BY [id_mcaid] ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [full_criteria_t_12_m]
,SUM([full_criteria]) OVER(PARTITION BY [id_mcaid] ORDER BY [year_month] ROWS BETWEEN 23 PRECEDING AND 12 PRECEDING) AS [full_criteria_prior_t_12_m]
,SUM([full_criteria]) OVER(PARTITION BY [id_mcaid] ORDER BY [year_month] ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS [full_criteria_p_2_m]

,[full_criteria_without_tpl]
,SUM([full_criteria_without_tpl]) OVER(PARTITION BY [id_mcaid] ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [full_criteria_without_tpl_t_12_m]
,SUM([full_criteria_without_tpl]) OVER(PARTITION BY [id_mcaid] ORDER BY [year_month] ROWS BETWEEN 23 PRECEDING AND 12 PRECEDING) AS [full_criteria_without_tpl_prior_t_12_m]
,SUM([full_criteria_without_tpl]) OVER(PARTITION BY [id_mcaid] ORDER BY [year_month] ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS [full_criteria_without_tpl_p_2_m]

,[row_num]
FROM #temp
)
SELECT
 [year_month]
,CASE WHEN [month] IN (3, 6, 9, 12) THEN 1 ELSE 0 END AS [end_quarter]
,[id_mcaid]
,[dob]
,[end_month_age]
,[age_in_months]

,[enrolled_any]
,[enrolled_any_t_12_m]

,[full_benefit]
,[full_benefit_t_12_m]

,[dual]
,[dual_t_12_m]

,[tpl]
,[tpl_t_12_m]

,[hospice]
,[hospice_t_12_m]
,[hospice_prior_t_12_m]
,[hospice_p_2_m]

,[full_criteria]
,[full_criteria_t_12_m]
,[full_criteria_prior_t_12_m]
,[full_criteria_p_2_m]

,[full_criteria_without_tpl]
,[full_criteria_without_tpl_t_12_m]
,[full_criteria_without_tpl_prior_t_12_m]
,[full_criteria_without_tpl_p_2_m]

INTO [stage].[perf_enroll_denom]
FROM CTE
WHERE 1 = 1
-- Months with at least 23 prior months
--AND [row_num] >= 24
-- Months with at least 11 prior months
AND [row_num] >= 12
-- Include members enrolled at least one month
AND [enrolled_any_t_12_m] >= 1;

CREATE CLUSTERED INDEX [idx_cl_perf_enroll_denom_id_mcaid_year_month] ON [stage].[perf_enroll_denom]([id_mcaid], [year_month]);
CREATE NONCLUSTERED INDEX [idx_nc_perf_enroll_denom_end_month_age] ON [stage].[perf_enroll_denom]([end_month_age]);
CREATE NONCLUSTERED INDEX [idx_nc_perf_enroll_denom_age_in_months] ON [stage].[perf_enroll_denom]([age_in_months]);'

PRINT @SQL;
END

EXEC sp_executeSQL @statement=@SQL, 
                   @params=N'@start_date_int INT, @end_date_int INT',
				   @start_date_int=@start_date_int, @end_date_int=@end_date_int;

GO

/*
If the first 12-month period ends 201303
@start_date_int = 201204 to get the full 12 months
If the last 12-month period ends 201712
@end_date_int = 201712
THESE PARAMETERS ARE INTEGERS
This procedure will index the [stage].[perf_enroll_denom] table

EXEC [stage].[sp_perf_enroll_denom] @start_date_int = 201202, @end_date_int = 201812;

SELECT 
 [year_month]
,COUNT(*)
FROM [stage].[perf_enroll_denom]
GROUP BY [year_month]
ORDER BY [year_month];

-- Check Duplicates
SELECT NumRows
      ,COUNT(*)
FROM
(
SELECT [id_mcaid]
      ,[year_month]
      ,COUNT(*) AS NumRows
FROM [stage].[perf_enroll_denom]
GROUP BY [id_mcaid], [year_month]
) AS SubQuery
GROUP BY NumRows
ORDER BY NumRows;

SELECT
 [end_month_age]
,[age_in_months]
,COUNT(*)
FROM [PHClaims].[stage].[perf_enroll_denom]
GROUP BY [end_month_age], [age_in_months]
ORDER BY [end_month_age], [age_in_months];
*/