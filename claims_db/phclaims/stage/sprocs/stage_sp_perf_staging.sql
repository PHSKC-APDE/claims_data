
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
DECLARE 
 @start_month_date DATE
,@end_month_date DATE;

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

IF @measure_name = 'Follow-up ED visit for Alcohol/Drug Abuse'
BEGIN

DELETE FROM [stage].[perf_staging_event_date]
FROM [stage].[perf_staging_event_date] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] LIKE @measure_name + '%'
AND [year_month] >= @start_month_int
AND [year_month] <= @end_month_int;

SET @start_month_date = CAST(CAST(@start_month_int * 100 + 1 AS CHAR(8)) AS DATE);
SET @end_month_date = EOMONTH(CAST(CAST(@end_month_int * 100 + 1 AS CHAR(8)) AS DATE));

SET @SQL = @SQL + N'
IF OBJECT_ID(''tempdb..#temp'', ''U'') IS NOT NULL
DROP TABLE #temp;
CREATE TABLE #temp
([year_month] INT
,[id] VARCHAR(200)
,[age] INT
,[tcn] VARCHAR(200)
,[from_date] DATE
,[to_date] DATE
,[ed_index_visit] INT
,[ed_within_30_day] INT
,[inpatient_within_30_day] INT
,[need_1_month_coverage] INT
,[follow_up_7_day] INT
,[follow_up_30_day] INT);

INSERT INTO #temp
EXEC [stage].[sp_perf_fua_join_step]
 @measurement_start_date=''' + CAST(@start_month_date AS CHAR(10)) + '''
,@measurement_end_date=''' + CAST(@end_month_date AS CHAR(10)) + '''
,@age=13
,@dx_value_set_name=''AOD Abuse and Dependence'';

WITH CTE AS
(
--First, insert rows for 7-day measure
SELECT
 [year_month]
,[from_date] AS [event_date]
,[id]
,[measure_id]
,[ed_index_visit] AS [denominator]
,[follow_up_7_day] AS [numerator]
,CAST(GETDATE() AS DATE) AS [load_date]
FROM #temp AS a
LEFT JOIN [ref].[perf_measure] AS b
ON [measure_name] = ''Follow-up ED visit for Alcohol/Drug Abuse: 7 days''

UNION ALL

--Next, insert rows for 30-day measure
SELECT
 [year_month]
,[from_date] AS [event_date]
,[id]
,[measure_id]
,[ed_index_visit] AS [denominator]
,[follow_up_30_day] AS [numerator]
,CAST(GETDATE() AS DATE) AS [load_date]
FROM #temp AS a
LEFT JOIN [ref].[perf_measure] AS b
ON [measure_name] = ''Follow-up ED visit for Alcohol/Drug Abuse: 30 days''
)

INSERT INTO [stage].[perf_staging_event_date]
([year_month]
,[event_date]
,[id]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 [year_month]
,[event_date]
,[id]
,[measure_id]
,[denominator]
,[numerator]
,[load_date]
FROM [CTE]'
END

IF @measure_name = 'Follow-up ED visit for Mental Illness'
BEGIN

DELETE FROM [stage].[perf_staging_event_date]
FROM [stage].[perf_staging_event_date] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] LIKE @measure_name + '%'
AND [year_month] >= @start_month_int
AND [year_month] <= @end_month_int;

SET @start_month_date = CAST(CAST(@start_month_int * 100 + 1 AS CHAR(8)) AS DATE);
SET @end_month_date = EOMONTH(CAST(CAST(@end_month_int * 100 + 1 AS CHAR(8)) AS DATE));

SET @SQL = @SQL + N'
IF OBJECT_ID(''tempdb..#temp'', ''U'') IS NOT NULL
DROP TABLE #temp;
CREATE TABLE #temp
([year_month] INT
,[id] VARCHAR(200)
,[age] INT
,[tcn] VARCHAR(200)
,[from_date] DATE
,[to_date] DATE
,[ed_index_visit] INT
,[ed_within_30_day] INT
,[inpatient_within_30_day] INT
,[need_1_month_coverage] INT
,[follow_up_7_day] INT
,[follow_up_30_day] INT);

INSERT INTO #temp
EXEC [stage].[sp_perf_fum_join_step]
 @measurement_start_date=''' + CAST(@start_month_date AS CHAR(10)) + '''
,@measurement_end_date=''' + CAST(@end_month_date AS CHAR(10)) + '''
,@age=6
,@dx_value_set_name=''Mental Illness'';

WITH CTE AS
(
--First, insert rows for 7-day measure
SELECT
 [year_month]
,[from_date] AS [event_date]
,[id]
,[measure_id]
,[ed_index_visit] AS [denominator]
,[follow_up_7_day] AS [numerator]
,CAST(GETDATE() AS DATE) AS [load_date]
FROM #temp AS a
LEFT JOIN [ref].[perf_measure] AS b
ON [measure_name] = ''Follow-up ED visit for Mental Illness: 7 days''

UNION ALL

--Next, insert rows for 30-day measure
SELECT
 [year_month]
,[from_date] AS [event_date]
,[id]
,[measure_id]
,[ed_index_visit] AS [denominator]
,[follow_up_30_day] AS [numerator]
,CAST(GETDATE() AS DATE) AS [load_date]
FROM #temp AS a
LEFT JOIN [ref].[perf_measure] AS b
ON [measure_name] = ''Follow-up ED visit for Mental Illness: 30 days''
)

INSERT INTO [stage].[perf_staging_event_date]
([year_month]
,[event_date]
,[id]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 [year_month]
,[event_date]
,[id]
,[measure_id]
,[denominator]
,[numerator]
,[load_date]
FROM [CTE]'
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