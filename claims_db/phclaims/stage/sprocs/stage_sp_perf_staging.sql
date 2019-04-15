
USE PHClaims;
GO

IF OBJECT_ID('[stage].[sp_perf_staging]','P') IS NOT NULL
DROP PROCEDURE [stage].[sp_perf_staging];
GO
CREATE PROCEDURE [stage].[sp_perf_staging]
 @start_month_int INT = 201701
,@end_month_int INT = 201712
,@measure_name VARCHAR(200) = NULL
AS
SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN
IF @measure_name = 'All-Cause ED Visits'
BEGIN

DELETE FROM [stage].[perf_staging]
FROM [stage].[perf_staging] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [year_month] >= @start_month_int
AND [year_month] <= @end_month_int;

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
WHERE [year_month] BETWEEN ' + CAST(@start_month_int AS CHAR(6)) + ' AND ' + CAST(@end_month_int AS CHAR(6)) + '
GROUP BY [year_month], [id], b.[measure_id];'
END

IF @measure_name = 'Acute Hospital Utilization'
BEGIN

DELETE FROM [stage].[perf_staging]
FROM [stage].[perf_staging] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [year_month] >= @start_month_int
AND [year_month] <= @end_month_int;

SET @SQL = @SQL + N'
/*
Sum discharges within member-month
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
,SUM([total_discharges]) AS [measure_value]
--,SUM([medicine]) AS [medicine]
--,SUM([surgery]) AS [surgery]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM [stage].[v_perf_ah_inpatient_numerator]
LEFT JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
WHERE [year_month] BETWEEN ' + CAST(@start_month_int AS CHAR(6)) + ' AND ' + CAST(@end_month_int AS CHAR(6)) + '
GROUP BY [year_month], [id], b.[measure_id];'
END

IF @measure_name = 'Child and Adolescent Access to Primary Care'
BEGIN

DELETE FROM [stage].[perf_staging]
FROM [stage].[perf_staging] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [year_month] >= @start_month_int
AND [year_month] <= @end_month_int;

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
WHERE [year_month] BETWEEN ' + CAST(@start_month_int AS CHAR(6)) + ' AND ' + CAST(@end_month_int AS CHAR(6)) + '
GROUP BY [year_month], [id], b.[measure_id];'
END
PRINT @SQL;
END

EXEC sp_executeSQL @statement=@SQL, 
                   @params=N'@start_month_int INT, @end_month_int INT, @measure_name VARCHAR(200)',
				   @start_month_int=@start_month_int, @end_month_int=@end_month_int, @measure_name=@measure_name;
GO