
USE PHClaims;
GO

IF OBJECT_ID('[stage].[sp_perf_staging]','P') IS NOT NULL
DROP PROCEDURE [stage].[sp_perf_staging];
GO
CREATE PROCEDURE [stage].[sp_perf_staging]
 @start_date_int INT = 201701
,@end_date_int INT = 201712
,@measure_name VARCHAR(200) = NULL
AS
SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN
IF @measure_name = 'All-Cause ED Visits'
BEGIN
SET @SQL = @SQL + N'
/*
Get qualifying ED encounters and attach from_date to year_month.
Later, use window function to create 12-month window sum
for measurement year.
*/
INSERT INTO [stage].[perf_staging]
([year_month]
,[id]
,[measure_id]
,[num_denom]
,[measure_value]
,[load_date])

SELECT
 [year_month]
,[id]
,b.[measure_id]
,''N'' AS [num_denom]
,SUM([ed_visit_num]) AS [measure_value]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM [stage].[v_perf_ed_visit_num] AS a
LEFT JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
--WHERE [year_month] BETWEEN 201701 AND 201702
WHERE [year_month] BETWEEN ' + CAST(@start_date_int AS CHAR(6)) + ' AND ' + CAST(@end_date_int AS CHAR(6)) + '
GROUP BY [year_month], [id], b.[measure_id];'
END

IF @measure_name = 'Child and Adolescent Access to Primary Care'
BEGIN
SET @SQL = @SQL + N'
/*
Get qualifying ambulatory visits and join to year_month.
Later, use window function to create 12-month or 24-month 
window sum (depending on age) for measurement year.
*/
INSERT INTO [stage].[perf_staging]
([year_month]
,[id]
,[measure_id]
,[num_denom]
,[measure_value]
,[load_date])

SELECT
 [year_month]
,[id]
,b.[measure_id]
,''N'' AS [num_denom]
,SUM([flag]) AS [measure_value]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM [stage].[v_perf_cap_ambulatory_visit] AS a
LEFT JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
--WHERE [year_month] BETWEEN 201701 AND 201702
WHERE [year_month] BETWEEN ' + CAST(@start_date_int AS CHAR(6)) + ' AND ' + CAST(@end_date_int AS CHAR(6)) + '
GROUP BY [year_month], [id], b.[measure_id];'
END
PRINT @SQL;
END

EXEC sp_executeSQL @statement=@SQL, 
                   @params=N'@start_date_int INT, @end_date_int INT, @measure_name VARCHAR(200)',
				   @start_date_int=@start_date_int, @end_date_int=@end_date_int, @measure_name=@measure_name;
GO