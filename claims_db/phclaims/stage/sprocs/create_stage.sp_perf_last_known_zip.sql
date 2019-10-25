
USE [PHClaims];
GO

IF OBJECT_ID('[stage].[sp_perf_last_known_zip]', 'P') IS NOT NULL
DROP PROCEDURE [stage].[sp_perf_last_known_zip];
GO
CREATE PROCEDURE [stage].[sp_perf_last_known_zip]
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
 a.[id_mcaid]
,b.[year_month]
,c.[RSDNTL_POSTAL_CODE] AS [zip_code]

INTO #perf_elig_member_month
FROM [final].[mcaid_elig_demo] AS a

CROSS JOIN 
(
SELECT [year_month] FROM [ref].[perf_year_month]
WHERE [year_month] BETWEEN ' + CAST(@start_date_int AS VARCHAR(6)) + ' AND ' + CAST(@end_date_int AS VARCHAR(6)) + '
) AS b

LEFT JOIN [stage].[perf_elig_member_month] AS c
ON a.[id_mcaid] = c.[MEDICAID_RECIPIENT_ID]
AND b.[year_month] = c.[CLNDR_YEAR_MNTH];

CREATE CLUSTERED INDEX idx_cl_#perf_elig_member_month ON #perf_elig_member_month([id_mcaid], [year_month]);

IF OBJECT_ID(''tempdb..#last_year_month_by_member'') IS NOT NULL
DROP TABLE #last_year_month_by_member;

SELECT
 [id_mcaid]
,[year_month]
,[zip_code]
,[relevant_year_month]
,MAX([relevant_year_month]) OVER(PARTITION BY [id_mcaid] 
 ORDER BY [year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [last_year_month]
INTO #last_year_month_by_member
FROM #perf_elig_member_month
CROSS APPLY(VALUES(CASE WHEN [zip_code] IS NOT NULL THEN [year_month] END)) AS a([relevant_year_month]);

CREATE CLUSTERED INDEX idx_cl_#last_year_month_by_member ON #last_year_month_by_member([id_mcaid], [last_year_month]);

SELECT
 [id_mcaid]
,[year_month]
,[zip_code]
,[relevant_year_month]
,[last_year_month]
,MAX([zip_code]) OVER(PARTITION BY [id_mcaid], [last_year_month]) AS [last_zip_code]
FROM #last_year_month_by_member;'

PRINT @SQL;
END

EXEC sp_executeSQL 
 @statement=@SQL
,@params=N'@start_date_int INT, @end_date_int INT'
,@start_date_int=@start_date_int, @end_date_int=@end_date_int;

GO

/*
IF OBJECT_ID('tempdb..#last_zip_code_by_member') IS NOT NULL
DROP TABLE #last_zip_code_by_member;
CREATE TABLE #last_zip_code_by_member
([id_mcaid] VARCHAR(200)
,[year_month] INT
,[zip_code] VARCHAR(200)
,[relevant_year_month] INT
,[last_year_month] INT
,[last_zip_code] VARCHAR(200));

INSERT INTO #last_zip_code_by_member
EXEC [stage].[sp_perf_last_known_zip] @start_date_int = 201201, @end_date_int = 201212;

CREATE CLUSTERED INDEX idx_cl_#last_zip_code_by_member ON #last_zip_code_by_member([id_mcaid], [year_month]);

SELECT TOP(1000) *
FROM #last_zip_code_by_member
ORDER BY [id_mcaid], [year_month];
*/