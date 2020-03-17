
USE [PHClaims];
GO

IF OBJECT_ID('[stage].[sp_mcaid_perf_enroll_provider]','P') IS NOT NULL
DROP PROCEDURE [stage].[sp_mcaid_perf_enroll_provider];
GO
CREATE PROCEDURE [stage].[sp_mcaid_perf_enroll_provider]
 @start_date_int INT = 201701
,@end_date_int INT = 201712
AS
SET NOCOUNT ON;
DECLARE @look_back_date_int INT;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN

DELETE FROM [stage].[mcaid_perf_enroll_provider]
WHERE [year_month] >= @start_date_int
AND [year_month] <= @end_date_int;

IF EXISTS(SELECT * FROM sys.indexes WHERE [name] = 'idx_cl_mcaid_perf_enroll_provider_id_mcaid_year_month')
DROP INDEX [idx_cl_mcaid_perf_enroll_provider_id_mcaid_year_month] ON [stage].[mcaid_perf_enroll_provider];

SET @look_back_date_int = (SELECT YEAR([12_month_prior]) * 100 + MONTH([12_month_prior]) FROM [ref].[perf_year_month] WHERE [year_month] = @start_date_int);

SET @SQL = @SQL + N'

IF OBJECT_ID(''tempdb..#mcaid_perf_elig_member_month'') IS NOT NULL
DROP TABLE #mcaid_perf_elig_member_month;
SELECT 
 [CLNDR_YEAR_MNTH] AS [year_month]
,[MEDICAID_RECIPIENT_ID] AS [id_mcaid]
,CASE WHEN [COVERAGE_TYPE_IND] = ''FFS'' THEN ''FFS'' ELSE [MC_PRVDR_NAME] END AS [mco_or_ffs]
,1 AS [flag]

INTO #mcaid_perf_elig_member_month
FROM [stage].[mcaid_perf_elig_member_month]
WHERE 1 = 1
AND ([CLNDR_YEAR_MNTH] BETWEEN ' + CAST(@look_back_date_int AS VARCHAR(6)) + ' AND ' + CAST(@end_date_int AS VARCHAR(6)) + ');

CREATE CLUSTERED INDEX [idx_cl_#mcaid_perf_elig_member_month] ON #mcaid_perf_elig_member_month([id_mcaid], [mco_or_ffs], [year_month]);

IF OBJECT_ID(''tempdb..#mcaid_perf_elig_member'') IS NOT NULL
DROP TABLE #mcaid_perf_elig_member;
SELECT DISTINCT
 [id_mcaid]
,[mco_or_ffs]

INTO #mcaid_perf_elig_member
FROM #mcaid_perf_elig_member_month;

CREATE CLUSTERED INDEX [idx_cl_#mcaid_perf_elig_member] ON #mcaid_perf_elig_member([id_mcaid], [mco_or_ffs]);

IF OBJECT_ID(''tempdb..#year_month'') IS NOT NULL
DROP TABLE #year_month;
SELECT 
 [year_month]
,[month]
,ROW_NUMBER() OVER(ORDER BY [year_month]) AS [row_num]

INTO #year_month
FROM [ref].[perf_year_month]
WHERE ([year_month] BETWEEN ' + CAST(@look_back_date_int AS VARCHAR(6)) + ' AND ' + CAST(@end_date_int AS VARCHAR(6)) + ');

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

INTO #cross_join
FROM #year_month AS a
CROSS JOIN #mcaid_perf_elig_member AS b
LEFT JOIN #mcaid_perf_elig_member_month AS c
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
INSERT INTO [stage].[mcaid_perf_enroll_provider]
([year_month]
,[end_quarter]
,[id_mcaid]
,[mco_or_ffs]
,[coverage_months_t_12_m]
,[load_date])

SELECT
 [year_month]
,[end_quarter]
,[id_mcaid]
,[mco_or_ffs]
,[coverage_months_t_12_m]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM CTE
WHERE 1 = 1
AND [row_num] >= 12
AND [coverage_months_t_12_m] >= 1 
AND [tie_breaker] = 1;

CREATE CLUSTERED INDEX [idx_cl_mcaid_perf_enroll_provider_id_mcaid_year_month] ON [stage].[mcaid_perf_enroll_provider]([id_mcaid], [year_month]);'

PRINT @SQL;
END

EXEC sp_executeSQL @statement=@SQL, 
                   @params=N'@start_date_int INT, @end_date_int INT',
				   @start_date_int=@start_date_int, @end_date_int=@end_date_int;

GO

/*
EXEC [stage].[sp_mcaid_perf_enroll_provider] @start_date_int = 201901, @end_date_int = 201912;

-- Check Duplicates
SELECT NumRows
      ,COUNT(*)
FROM
(
SELECT [id_mcaid]
      ,[year_month]
      ,COUNT(*) AS NumRows
FROM [stage].[mcaid_perf_enroll_provider]
GROUP BY [id_mcaid], [year_month]
) AS SubQuery
GROUP BY NumRows
ORDER BY NumRows;
*/