
USE [PHClaims];
GO

IF OBJECT_ID('[stage].[sp_mcaid_perf_enroll_denom]','P') IS NOT NULL
DROP PROCEDURE [stage].[sp_mcaid_perf_enroll_denom];
GO
CREATE PROCEDURE [stage].[sp_mcaid_perf_enroll_denom]
 @start_date_int INT = 201701
,@end_date_int INT = 201712
AS
SET NOCOUNT ON;
DECLARE @look_back_date_int INT;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN

DELETE FROM [stage].[mcaid_perf_enroll_denom]
WHERE [year_month] >= @start_date_int
AND [year_month] <= @end_date_int;

IF EXISTS(SELECT * FROM sys.indexes WHERE [name] = 'idx_nc_mcaid_perf_enroll_denom_age_in_months')
DROP INDEX [idx_nc_mcaid_perf_enroll_denom_age_in_months] ON [stage].[mcaid_perf_enroll_denom];
IF EXISTS(SELECT * FROM sys.indexes WHERE [name] = 'idx_nc_mcaid_perf_enroll_denom_end_month_age')
DROP INDEX [idx_nc_mcaid_perf_enroll_denom_end_month_age] ON [stage].[mcaid_perf_enroll_denom];
IF EXISTS(SELECT * FROM sys.indexes WHERE [name] = 'idx_cl_mcaid_perf_enroll_denom_id_mcaid_year_month')
DROP INDEX [idx_cl_mcaid_perf_enroll_denom_id_mcaid_year_month] ON [stage].[mcaid_perf_enroll_denom];

SET @look_back_date_int = (SELECT YEAR([24_month_prior]) * 100 + MONTH([24_month_prior]) FROM [ref].[perf_year_month] WHERE [year_month] = @start_date_int);

SET @SQL = @SQL + N'

IF OBJECT_ID(''tempdb..#temp'', ''U'') IS NOT NULL
DROP TABLE #temp;
SELECT *
INTO #temp
FROM [stage].[fn_mcaid_perf_enroll_member_month](' + CAST(@look_back_date_int AS VARCHAR(20)) + ', ' + CAST(@end_date_int AS VARCHAR(20)) + ');

CREATE CLUSTERED INDEX [idx_cl_#temp_id_mcaid_year_month] ON #temp([id_mcaid], [year_month]);

IF OBJECT_ID(''tempdb..#mcaid_perf_enroll_denom'',''U'') IS NOT NULL
DROP TABLE #mcaid_perf_enroll_denom;
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

,[zip_code]
,[row_num]
INTO #mcaid_perf_enroll_denom
FROM #temp;

CREATE CLUSTERED INDEX [idx_cl_#mcaid_perf_enroll_denom_id_mcaid_year_month] ON #mcaid_perf_enroll_denom([id_mcaid], [year_month]);

IF OBJECT_ID(''tempdb..#last_year_month'') IS NOT NULL
DROP TABLE #last_year_month;
SELECT
 [year_month]
,[month]
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
,[zip_code]
,[relevant_year_month]
,MAX([relevant_year_month]) OVER(PARTITION BY [id_mcaid] ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [last_year_month]
,[row_num]

INTO #last_year_month
FROM #mcaid_perf_enroll_denom
CROSS APPLY(VALUES(CASE WHEN [zip_code] IS NOT NULL THEN [year_month] END)) AS a([relevant_year_month]);

CREATE CLUSTERED INDEX idx_cl_#last_year_month ON #last_year_month([id_mcaid], [last_year_month]);

WITH CTE AS
(
SELECT
 [year_month]
,CASE WHEN [month] IN (3, 6, 9, 12) THEN 1 ELSE 0 END AS [end_quarter]
,[id_mcaid]
,[dob]
,[end_month_age]
,[age_in_months]
,MAX([zip_code]) OVER(PARTITION BY [id_mcaid], [last_year_month]) AS [last_zip_code]
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
,CAST(GETDATE() AS DATE) AS [load_date]
FROM #last_year_month
)
INSERT INTO [stage].[mcaid_perf_enroll_denom]
SELECT *
FROM CTE
WHERE 1 = 1
AND [year_month] >= ' + CAST(@start_date_int AS VARCHAR(20)) + '
AND [year_month] <= ' + CAST(@end_date_int AS VARCHAR(20)) + '
AND [enrolled_any_t_12_m] >= 1
ORDER BY [id_mcaid], [year_month];

CREATE CLUSTERED INDEX [idx_cl_mcaid_perf_enroll_denom_id_mcaid_year_month] ON [stage].[mcaid_perf_enroll_denom]([id_mcaid], [year_month]);
CREATE NONCLUSTERED INDEX [idx_nc_mcaid_perf_enroll_denom_end_month_age] ON [stage].[mcaid_perf_enroll_denom]([end_month_age]);
CREATE NONCLUSTERED INDEX [idx_nc_mcaid_perf_enroll_denom_age_in_months] ON [stage].[mcaid_perf_enroll_denom]([age_in_months]);'

PRINT @SQL;
END

EXEC sp_executeSQL @statement=@SQL, 
                   @params=N'@start_date_int INT, @end_date_int INT',
				   @start_date_int=@start_date_int, @end_date_int=@end_date_int;

GO

/*
EXEC [stage].[sp_mcaid_perf_enroll_denom] @start_date_int = 201601, @end_date_int = 201911;

SELECT 
 [year_month]
,[load_date]
,COUNT(*)
FROM [stage].[mcaid_perf_enroll_denom]
GROUP BY [year_month], [load_date]
ORDER BY [year_month], [load_date];

SELECT COUNT(DISTINCT [MEDICAID_RECIPIENT_ID])
FROM [stage].[mcaid_perf_elig_member_month] AS a
INNER JOIN [final].[mcaid_elig_demo] AS b
ON a.[MEDICAID_RECIPIENT_ID] = b.[id_mcaid]
WHERE a.[CLNDR_YEAR_MNTH] BETWEEN 201702 AND 201801;

-- Check Duplicates
SELECT 
 NumRows
,COUNT(*)
FROM
(
SELECT 
 [id_mcaid]
,[year_month]
,COUNT(*) AS NumRows
FROM [stage].[mcaid_perf_enroll_denom]
GROUP BY [id_mcaid], [year_month]
) AS SubQuery
GROUP BY NumRows
ORDER BY NumRows;
*/