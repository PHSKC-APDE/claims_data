
/*
This procedure aggregates numerator and denominator utilization for a 
@measure_name between @start_month_int and @end_month_int.

The structure is 
IF @measure_name = 'All-Cause ED Visits', run a SQL batch
IF @measure_name = 'Acute Hospital Utilization', run a SQL batch
...

Calls on:
[ref].[perf_measure], table of measure characteristics
[ref].[perf_year_month], month-by-month table of dates
...

Loads to:
[stage].[perf_staging]
OR
[stage].[perf_staging_event_date]
Measures which can be aggregated to month-level prior to final calculation are 
loaded to [stage].[perf_staging]. This includes the majority of measures.

Index-event-date-based measures (such as follow-up and readmissions) are loaded
to [stage].[perf_staging_event_date]. This is because the index-event-date must
be known when calculating the final measure. For example, an ED visit occuring 
on Dec. 15th is NOT eligible for 30-day follow-up in measurement year JAN-DEC
but this visit IS eligible for follow-up in measurement year APR-MAR.

Run for one measure at a time, for any @start_month_int and @end_month_int
(e.g., this gets all Mental Health Treatment Penetration numerator and 
denominator utilization for 2016-JAN to 2017-DEC (2016-01-01-2017-12-31) period.

EXEC [stage].[sp_perf_staging]
 @start_month_int = 201601
,@end_month_int = 201712
--,@measure_name = 'All-Cause ED Visits';
--,@measure_name = 'Acute Hospital Utilization';
--,@measure_name = 'Follow-up ED visit for Alcohol/Drug Abuse';
--,@measure_name = 'Follow-up ED visit for Mental Illness';
--,@measure_name = 'Follow-up Hospitalization for Mental Illness';
,@measure_name = 'Mental Health Treatment Penetration';
--,@measure_name = 'SUD Treatment Penetration';
--,@measure_name = 'Plan All-Cause Readmissions (30 days)';
--,@measure_name = 'Child and Adolescent Access to Primary Care';

Author: Philip Sylling
Modified: 2019-07-19: Modified to utilize new analytic tables
*/

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
DECLARE @start_month_date DATE;
DECLARE @end_month_date DATE;

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
,[id_mcaid]
,[measure_id]
,[num_denom]
,[measure_value]
,[load_date])

SELECT
 [year_month]
,[id_mcaid]
,b.[measure_id]
,''N'' AS [num_denom]
,SUM([ed_visit_num]) AS [measure_value]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM [stage].[v_perf_ed_visit_num] AS a
LEFT JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
WHERE [year_month] BETWEEN ' + CAST(@start_month_int AS CHAR(6)) + ' AND ' + CAST(@end_month_int AS CHAR(6)) + '
GROUP BY [year_month], [id_mcaid], b.[measure_id];'
END
PRINT @SQL;
END

EXEC sp_executeSQL @statement=@SQL, 
                   @params=N'@start_month_int INT, @end_month_int INT, @measure_name VARCHAR(200)',
				   @start_month_int=@start_month_int, @end_month_int=@end_month_int, @measure_name=@measure_name;
GO

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

IF @measure_name = 'Follow-up Hospitalization for Mental Illness'
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
,[inpatient_index_stay] INT
,[inpatient_within_30_day] INT
,[need_1_month_coverage] INT
,[follow_up_7_day] INT
,[follow_up_30_day] INT);

INSERT INTO #temp
EXEC [stage].[sp_perf_fuh_join_step]
 @measurement_start_date=''' + CAST(@start_month_date AS CHAR(10)) + '''
,@measurement_end_date=''' + CAST(@end_month_date AS CHAR(10)) + '''
,@age=6
,@dx_value_set_name=''Mental Illness''
,@exclusion_value_set_name=''Mental Health Diagnosis'';

WITH CTE AS
(
--First, insert rows for 7-day measure
SELECT
 [year_month]
,[from_date] AS [event_date]
,[id]
,[measure_id]
,[inpatient_index_stay] AS [denominator]
,[follow_up_7_day] AS [numerator]
,CAST(GETDATE() AS DATE) AS [load_date]
FROM #temp AS a
LEFT JOIN [ref].[perf_measure] AS b
ON [measure_name] = ''Follow-up Hospitalization for Mental Illness: 7 days''

UNION ALL

--Next, insert rows for 30-day measure
SELECT
 [year_month]
,[from_date] AS [event_date]
,[id]
,[measure_id]
,[inpatient_index_stay] AS [denominator]
,[follow_up_30_day] AS [numerator]
,CAST(GETDATE() AS DATE) AS [load_date]
FROM #temp AS a
LEFT JOIN [ref].[perf_measure] AS b
ON [measure_name] = ''Follow-up Hospitalization for Mental Illness: 30 days''
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

IF @measure_name = 'Mental Health Treatment Penetration'
BEGIN

DELETE FROM [stage].[perf_staging]
FROM [stage].[perf_staging] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [year_month] >= @start_month_int
AND [year_month] <= @end_month_int;

SET @start_month_date = CAST(CAST(@start_month_int * 100 + 1 AS CHAR(8)) AS DATE);
SET @end_month_date = EOMONTH(CAST(CAST(@end_month_int * 100 + 1 AS CHAR(8)) AS DATE));

SET @SQL = @SQL + N'

IF OBJECT_ID(''tempdb..#v_perf_tpm_numerator'') IS NOT NULL
DROP TABLE #v_perf_tpm_numerator;
SELECT *
INTO #v_perf_tpm_numerator
FROM [stage].[v_perf_tpm_numerator]
WHERE [from_date] BETWEEN ''' + CAST(@start_month_date AS CHAR(10)) + ''' AND ''' + CAST(@end_month_date AS CHAR(10)) + ''';
CREATE CLUSTERED INDEX idx_cl_#v_perf_tpm_numerator ON #v_perf_tpm_numerator([from_date]);

IF OBJECT_ID(''tempdb..#v_perf_tpm_denominator'') IS NOT NULL
DROP TABLE #v_perf_tpm_denominator;
SELECT *
INTO #v_perf_tpm_denominator
FROM [stage].[v_perf_tpm_denominator]
WHERE [from_date] BETWEEN ''' + CAST(@start_month_date AS CHAR(10)) + ''' AND ''' + CAST(@end_month_date AS CHAR(10)) + ''';
CREATE CLUSTERED INDEX idx_cl_#v_perf_tpm_denominator ON #v_perf_tpm_denominator([from_date]);

INSERT INTO [stage].[perf_staging]
([year_month]
,[id]
,[measure_id]
,[num_denom]
,[measure_value]
,[load_date])

SELECT 
 ym.[year_month]
,a.[id]
,b.[measure_id]
,''N'' AS [num_denom]
,MAX(a.[flag]) AS [measure_value]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM #v_perf_tpm_numerator AS a
INNER JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
INNER JOIN [ref].[perf_year_month] AS ym
ON a.[from_date] BETWEEN ym.[beg_month] AND ym.[end_month]
GROUP BY ym.[year_month], a.[id], b.[measure_id];

INSERT INTO [stage].[perf_staging]
([year_month]
,[id]
,[measure_id]
,[num_denom]
,[measure_value]
,[load_date])

SELECT 
 ym.[year_month]
,a.[id]
,b.[measure_id]
,''D'' AS [num_denom]
,MAX(a.[flag]) AS [measure_value]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM #v_perf_tpm_denominator AS a
INNER JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
INNER JOIN [ref].[perf_year_month] AS ym
ON a.[from_date] BETWEEN ym.[beg_month] AND ym.[end_month]
GROUP BY ym.[year_month], a.[id], b.[measure_id];'
END

IF @measure_name = 'SUD Treatment Penetration'
BEGIN

DELETE FROM [stage].[perf_staging]
FROM [stage].[perf_staging] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [year_month] >= @start_month_int
AND [year_month] <= @end_month_int;

SET @start_month_date = CAST(CAST(@start_month_int * 100 + 1 AS CHAR(8)) AS DATE);
SET @end_month_date = EOMONTH(CAST(CAST(@end_month_int * 100 + 1 AS CHAR(8)) AS DATE));

SET @SQL = @SQL + N'

IF OBJECT_ID(''tempdb..#v_perf_tps_numerator'') IS NOT NULL
DROP TABLE #v_perf_tps_numerator;
SELECT *
INTO #v_perf_tps_numerator
FROM [stage].[v_perf_tps_numerator]
WHERE [from_date] BETWEEN ''' + CAST(@start_month_date AS CHAR(10)) + ''' AND ''' + CAST(@end_month_date AS CHAR(10)) + ''';
CREATE CLUSTERED INDEX idx_cl_#v_perf_tps_numerator ON #v_perf_tps_numerator([from_date]);

IF OBJECT_ID(''tempdb..#v_perf_tps_denominator'') IS NOT NULL
DROP TABLE #v_perf_tps_denominator;
SELECT *
INTO #v_perf_tps_denominator
FROM [stage].[v_perf_tps_denominator]
WHERE [from_date] BETWEEN ''' + CAST(@start_month_date AS CHAR(10)) + ''' AND ''' + CAST(@end_month_date AS CHAR(10)) + ''';
CREATE CLUSTERED INDEX idx_cl_#v_perf_tps_denominator ON #v_perf_tps_denominator([from_date]);

INSERT INTO [stage].[perf_staging]
([year_month]
,[id]
,[measure_id]
,[num_denom]
,[measure_value]
,[load_date])

SELECT 
 ym.[year_month]
,a.[id]
,b.[measure_id]
,''N'' AS [num_denom]
,MAX(a.[flag]) AS [measure_value]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM #v_perf_tps_numerator AS a
INNER JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
INNER JOIN [ref].[perf_year_month] AS ym
ON a.[from_date] BETWEEN ym.[beg_month] AND ym.[end_month]
GROUP BY ym.[year_month], a.[id], b.[measure_id];

INSERT INTO [stage].[perf_staging]
([year_month]
,[id]
,[measure_id]
,[num_denom]
,[measure_value]
,[load_date])

SELECT 
 ym.[year_month]
,a.[id]
,b.[measure_id]
,''D'' AS [num_denom]
,MAX(a.[flag]) AS [measure_value]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM #v_perf_tps_denominator AS a
INNER JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
INNER JOIN [ref].[perf_year_month] AS ym
ON a.[from_date] BETWEEN ym.[beg_month] AND ym.[end_month]
GROUP BY ym.[year_month], a.[id], b.[measure_id];'
END

IF @measure_name = 'SUD Treatment Penetration (Opioid)'
BEGIN

DELETE FROM [stage].[perf_staging]
FROM [stage].[perf_staging] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [year_month] >= @start_month_int
AND [year_month] <= @end_month_int;

SET @start_month_date = CAST(CAST(@start_month_int * 100 + 1 AS CHAR(8)) AS DATE);
SET @end_month_date = EOMONTH(CAST(CAST(@end_month_int * 100 + 1 AS CHAR(8)) AS DATE));

SET @SQL = @SQL + N'

IF OBJECT_ID(''tempdb..#v_perf_tpo_numerator'') IS NOT NULL
DROP TABLE #v_perf_tpo_numerator;
SELECT *
INTO #v_perf_tpo_numerator
FROM [stage].[v_perf_tpo_numerator]
WHERE [from_date] BETWEEN ''' + CAST(@start_month_date AS CHAR(10)) + ''' AND ''' + CAST(@end_month_date AS CHAR(10)) + ''';
CREATE CLUSTERED INDEX idx_cl_#v_perf_tpo_numerator ON #v_perf_tpo_numerator([from_date]);

IF OBJECT_ID(''tempdb..#v_perf_tpo_denominator'') IS NOT NULL
DROP TABLE #v_perf_tpo_denominator;
SELECT *
INTO #v_perf_tpo_denominator
FROM [stage].[v_perf_tpo_denominator]
WHERE [from_date] BETWEEN ''' + CAST(@start_month_date AS CHAR(10)) + ''' AND ''' + CAST(@end_month_date AS CHAR(10)) + ''';
CREATE CLUSTERED INDEX idx_cl_#v_perf_tpo_denominator ON #v_perf_tpo_denominator([from_date]);

INSERT INTO [stage].[perf_staging]
([year_month]
,[id]
,[measure_id]
,[num_denom]
,[measure_value]
,[load_date])

SELECT 
 ym.[year_month]
,a.[id]
,b.[measure_id]
,''N'' AS [num_denom]
,MAX(a.[flag]) AS [measure_value]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM #v_perf_tpo_numerator AS a
INNER JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
INNER JOIN [ref].[perf_year_month] AS ym
ON a.[from_date] BETWEEN ym.[beg_month] AND ym.[end_month]
GROUP BY ym.[year_month], a.[id], b.[measure_id];

INSERT INTO [stage].[perf_staging]
([year_month]
,[id]
,[measure_id]
,[num_denom]
,[measure_value]
,[load_date])

SELECT 
 ym.[year_month]
,a.[id]
,b.[measure_id]
,''D'' AS [num_denom]
,MAX(a.[flag]) AS [measure_value]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM #v_perf_tpo_denominator AS a
INNER JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
INNER JOIN [ref].[perf_year_month] AS ym
ON a.[from_date] BETWEEN ym.[beg_month] AND ym.[end_month]
GROUP BY ym.[year_month], a.[id], b.[measure_id];'
END

IF @measure_name = 'Plan All-Cause Readmissions (30 days)'
BEGIN

DELETE FROM [stage].[perf_staging]
FROM [stage].[perf_staging] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [year_month] >= @start_month_int
AND [year_month] <= @end_month_int;

SET @SQL = @SQL + N'
IF OBJECT_ID(''tempdb..#temp'', ''U'') IS NOT NULL
DROP TABLE #temp;
CREATE TABLE #temp
([year_month] INT
,[id] VARCHAR(200)
,[age] INT
,[episode_id] INT
,[episode_from_date] DATE
,[episode_to_date] DATE
,[inpatient_index_stay] INT
,[readmission_from_date] DATE
,[readmission_to_date] DATE
,[readmission_flag] INT
,[date_diff] INT
,[planned_readmission] INT);

INSERT INTO #temp
EXEC [stage].[sp_perf_pcr_join_step];

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
,[episode_to_date] AS [event_date]
,[id]
,[measure_id]
,[inpatient_index_stay] AS [denominator]
,[readmission_flag] AS [numerator]
,CAST(GETDATE() AS DATE) AS [load_date]
FROM #temp AS a
LEFT JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
WHERE [year_month] BETWEEN ' + CAST(@start_month_int AS CHAR(6)) + ' AND ' + CAST(@end_month_int AS CHAR(6)) + ';'
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


--VERSION FOR BHO DATA
USE [DCHS_Analytics];
GO

IF OBJECT_ID('[stage].[sp_perf_bho_staging]','P') IS NOT NULL
DROP PROCEDURE [stage].[sp_perf_bho_staging];
GO
CREATE PROCEDURE [stage].[sp_perf_bho_staging]
 @start_month_int INT = 201701
,@end_month_int INT = 201712
,@measure_name VARCHAR(200) = NULL
AS
SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';
DECLARE @start_month_date DATE;
DECLARE @end_month_date DATE;

BEGIN

IF @measure_name = 'Mental Health Treatment Penetration'
BEGIN

DELETE FROM [stage].[perf_bho_staging]
FROM [stage].[perf_bho_staging] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [year_month] >= @start_month_int
AND [year_month] <= @end_month_int;

SET @start_month_date = CAST(CAST(@start_month_int * 100 + 1 AS CHAR(8)) AS DATE);
SET @end_month_date = EOMONTH(CAST(CAST(@end_month_int * 100 + 1 AS CHAR(8)) AS DATE));

SET @SQL = @SQL + N'

IF OBJECT_ID(''tempdb..#v_perf_bho_tpm_numerator'') IS NOT NULL
DROP TABLE #v_perf_bho_tpm_numerator;
SELECT *
INTO #v_perf_bho_tpm_numerator
FROM [stage].[v_perf_bho_tpm_numerator]
WHERE [event_date] BETWEEN ''' + CAST(@start_month_date AS CHAR(10)) + ''' AND ''' + CAST(@end_month_date AS CHAR(10)) + ''';
CREATE CLUSTERED INDEX idx_cl_#v_perf_bho_tpm_numerator ON #v_perf_bho_tpm_numerator([event_date]);

IF OBJECT_ID(''tempdb..#v_perf_bho_tpm_denominator'') IS NOT NULL
DROP TABLE #v_perf_bho_tpm_denominator;
SELECT *
INTO #v_perf_bho_tpm_denominator
FROM [stage].[v_perf_bho_tpm_denominator]
WHERE [event_date] BETWEEN ''' + CAST(@start_month_date AS CHAR(10)) + ''' AND ''' + CAST(@end_month_date AS CHAR(10)) + ''';
CREATE CLUSTERED INDEX idx_cl_#v_perf_bho_tpm_denominator ON #v_perf_bho_tpm_denominator([event_date]);

INSERT INTO [stage].[perf_bho_staging]
([year_month]
,[kcid]
,[p1_id]
,[measure_id]
,[num_denom]
,[measure_value]
,[load_date])

SELECT 
 ym.[year_month]
,a.[kcid]
,c.[p1_id]
,b.[measure_id]
,''N'' AS [num_denom]
,MAX(a.[flag]) AS [measure_value]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM #v_perf_bho_tpm_numerator AS a
INNER JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
INNER JOIN [ref].[perf_year_month] AS ym
ON a.[event_date] BETWEEN ym.[beg_month] AND ym.[end_month]
LEFT JOIN [php96].[client_id] AS c
ON a.[kcid] = c.[kcid]
GROUP BY ym.[year_month], a.[kcid], c.[p1_id], b.[measure_id];

INSERT INTO [stage].[perf_bho_staging]
([year_month]
,[kcid]
,[p1_id]
,[measure_id]
,[num_denom]
,[measure_value]
,[load_date])

SELECT 
 ym.[year_month]
,a.[kcid]
,c.[p1_id]
,b.[measure_id]
,''D'' AS [num_denom]
,MAX(a.[flag]) AS [measure_value]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM #v_perf_bho_tpm_denominator AS a
INNER JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
INNER JOIN [ref].[perf_year_month] AS ym
ON a.[event_date] BETWEEN ym.[beg_month] AND ym.[end_month]
LEFT JOIN [php96].[client_id] AS c
ON a.[kcid] = c.[kcid]
GROUP BY ym.[year_month], a.[kcid], c.[p1_id], b.[measure_id];'
END

IF @measure_name = 'SUD Treatment Penetration'
BEGIN

DELETE FROM [stage].[perf_bho_staging]
FROM [stage].[perf_bho_staging] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [year_month] >= @start_month_int
AND [year_month] <= @end_month_int;

SET @start_month_date = CAST(CAST(@start_month_int * 100 + 1 AS CHAR(8)) AS DATE);
SET @end_month_date = EOMONTH(CAST(CAST(@end_month_int * 100 + 1 AS CHAR(8)) AS DATE));

SET @SQL = @SQL + N'

IF OBJECT_ID(''tempdb..#v_perf_bho_tps_numerator'') IS NOT NULL
DROP TABLE #v_perf_bho_tps_numerator;
SELECT *
INTO #v_perf_bho_tps_numerator
FROM [stage].[v_perf_bho_tps_numerator]
WHERE [event_date] BETWEEN ''' + CAST(@start_month_date AS CHAR(10)) + ''' AND ''' + CAST(@end_month_date AS CHAR(10)) + ''';
CREATE CLUSTERED INDEX idx_cl_#v_perf_bho_tps_numerator ON #v_perf_bho_tps_numerator([event_date]);

IF OBJECT_ID(''tempdb..#v_perf_bho_tps_denominator'') IS NOT NULL
DROP TABLE #v_perf_bho_tps_denominator;
SELECT *
INTO #v_perf_bho_tps_denominator
FROM [stage].[v_perf_bho_tps_denominator]
WHERE [event_date] BETWEEN ''' + CAST(@start_month_date AS CHAR(10)) + ''' AND ''' + CAST(@end_month_date AS CHAR(10)) + ''';
CREATE CLUSTERED INDEX idx_cl_#v_perf_bho_tps_denominator ON #v_perf_bho_tps_denominator([event_date]);

INSERT INTO [stage].[perf_bho_staging]
([year_month]
,[kcid]
,[p1_id]
,[measure_id]
,[num_denom]
,[measure_value]
,[load_date])

SELECT 
 ym.[year_month]
,a.[kcid]
,c.[p1_id]
,b.[measure_id]
,''N'' AS [num_denom]
,MAX(a.[flag]) AS [measure_value]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM #v_perf_bho_tps_numerator AS a
INNER JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
INNER JOIN [ref].[perf_year_month] AS ym
ON a.[event_date] BETWEEN ym.[beg_month] AND ym.[end_month]
LEFT JOIN [php96].[client_id] AS c
ON a.[kcid] = c.[kcid]
GROUP BY ym.[year_month], a.[kcid], c.[p1_id], b.[measure_id];

INSERT INTO [stage].[perf_bho_staging]
([year_month]
,[kcid]
,[p1_id]
,[measure_id]
,[num_denom]
,[measure_value]
,[load_date])

SELECT 
 ym.[year_month]
,a.[kcid]
,c.[p1_id]
,b.[measure_id]
,''D'' AS [num_denom]
,MAX(a.[flag]) AS [measure_value]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM #v_perf_bho_tps_denominator AS a
INNER JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
INNER JOIN [ref].[perf_year_month] AS ym
ON a.[event_date] BETWEEN ym.[beg_month] AND ym.[end_month]
LEFT JOIN [php96].[client_id] AS c
ON a.[kcid] = c.[kcid]
GROUP BY ym.[year_month], a.[kcid], c.[p1_id], b.[measure_id];'
END

IF @measure_name = 'SUD Treatment Penetration (Opioid)'
BEGIN

DELETE FROM [stage].[perf_bho_staging]
FROM [stage].[perf_bho_staging] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [year_month] >= @start_month_int
AND [year_month] <= @end_month_int;

SET @start_month_date = CAST(CAST(@start_month_int * 100 + 1 AS CHAR(8)) AS DATE);
SET @end_month_date = EOMONTH(CAST(CAST(@end_month_int * 100 + 1 AS CHAR(8)) AS DATE));

SET @SQL = @SQL + N'

IF OBJECT_ID(''tempdb..#v_perf_bho_tpo_numerator'') IS NOT NULL
DROP TABLE #v_perf_bho_tpo_numerator;
SELECT *
INTO #v_perf_bho_tpo_numerator
FROM [stage].[v_perf_bho_tpo_numerator]
WHERE [event_date] BETWEEN ''' + CAST(@start_month_date AS CHAR(10)) + ''' AND ''' + CAST(@end_month_date AS CHAR(10)) + ''';
CREATE CLUSTERED INDEX idx_cl_#v_perf_bho_tpo_numerator ON #v_perf_bho_tpo_numerator([event_date]);

IF OBJECT_ID(''tempdb..#v_perf_bho_tpo_denominator'') IS NOT NULL
DROP TABLE #v_perf_bho_tpo_denominator;
SELECT *
INTO #v_perf_bho_tpo_denominator
FROM [stage].[v_perf_bho_tpo_denominator]
WHERE [event_date] BETWEEN ''' + CAST(@start_month_date AS CHAR(10)) + ''' AND ''' + CAST(@end_month_date AS CHAR(10)) + ''';
CREATE CLUSTERED INDEX idx_cl_#v_perf_bho_tpo_denominator ON #v_perf_bho_tpo_denominator([event_date]);

INSERT INTO [stage].[perf_bho_staging]
([year_month]
,[kcid]
,[p1_id]
,[measure_id]
,[num_denom]
,[measure_value]
,[load_date])

SELECT 
 ym.[year_month]
,a.[kcid]
,c.[p1_id]
,b.[measure_id]
,''N'' AS [num_denom]
,MAX(a.[flag]) AS [measure_value]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM #v_perf_bho_tpo_numerator AS a
INNER JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
INNER JOIN [ref].[perf_year_month] AS ym
ON a.[event_date] BETWEEN ym.[beg_month] AND ym.[end_month]
LEFT JOIN [php96].[client_id] AS c
ON a.[kcid] = c.[kcid]
GROUP BY ym.[year_month], a.[kcid], c.[p1_id], b.[measure_id];

INSERT INTO [stage].[perf_bho_staging]
([year_month]
,[kcid]
,[p1_id]
,[measure_id]
,[num_denom]
,[measure_value]
,[load_date])

SELECT 
 ym.[year_month]
,a.[kcid]
,c.[p1_id]
,b.[measure_id]
,''D'' AS [num_denom]
,MAX(a.[flag]) AS [measure_value]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM #v_perf_bho_tpo_denominator AS a
INNER JOIN [ref].[perf_measure] AS b
ON b.[measure_name] = ''' + @measure_name + '''
INNER JOIN [ref].[perf_year_month] AS ym
ON a.[event_date] BETWEEN ym.[beg_month] AND ym.[end_month]
LEFT JOIN [php96].[client_id] AS c
ON a.[kcid] = c.[kcid]
GROUP BY ym.[year_month], a.[kcid], c.[p1_id], b.[measure_id];'
END
PRINT @SQL;
END

EXEC sp_executeSQL @statement=@SQL, 
                   @params=N'@start_month_int INT, @end_month_int INT, @measure_name VARCHAR(200)',
				   @start_month_int=@start_month_int, @end_month_int=@end_month_int, @measure_name=@measure_name;
GO