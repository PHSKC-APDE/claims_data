
/*
This procedure aggregates numerator and denominator utilization from combined 
Medicaid and BHO data for a @measure_name between @start_month_int and @end_month_int.

The structure is 
IF @measure_name = 'Follow-up Hospitalization for Mental Illness', run a SQL batch
...

Calls on:
[PHClaims_RO].[PHClaims].[ref].[perf_measure], table of measure characteristics
[PHClaims_RO].[PHClaims].[ref].[perf_year_month], month-by-month table of dates
...

Because the person-level combined measures cannot be saved to a permanent 
table, results are loaded to global temporary tables:
##perf_cmb_staging
OR
##perf_cmb_staging_event_date

Measures which can be aggregated to month-level prior to final calculation are 
loaded to ##perf_cmb_staging.

Index-event-date-based measures (such as follow-up and readmissions) are loaded
to ##perf_cmb_staging_event_date. This is because the index-event-date must
be known when calculating the final measure. For example, an ED visit occuring 
on Dec. 15th is NOT eligible for 30-day follow-up in measurement year JAN-DEC
but this visit IS eligible for follow-up in measurement year APR-MAR.

Run for one measure at a time, for any @start_month_int and @end_month_int

IF OBJECT_ID('tempdb..##perf_cmb_staging_event_date') IS NOT NULL
DROP TABLE ##perf_cmb_staging_event_date;
CREATE TABLE ##perf_cmb_staging_event_date
([year_month] INT NOT NULL
,[event_date] DATE NOT NULL
,[id_mcaid] VARCHAR(255) NOT NULL
,[measure_id] SMALLINT NOT NULL
,[denominator] INT NOT NULL
,[numerator] INT NOT NULL
,[load_date] DATE NOT NULL
) ON [PRIMARY];
GO

EXEC [stage].[sp_cmb_perf_staging]
 @start_month_int = 201601
,@end_month_int = 201812
,@measure_name = 'Follow-up Hospitalization for Mental Illness';

Author: Philip Sylling
Modified: 2019-07-19: Modified to utilize new analytic tables
Modified: 2019-10-07: Modified to accomodate combined Medicaid/BHO measures
*/

USE [DCHS_Analytics];
GO

IF OBJECT_ID('[stage].[sp_perf_cmb_staging]','P') IS NOT NULL
DROP PROCEDURE [stage].[sp_perf_cmb_staging];
GO
CREATE PROCEDURE [stage].[sp_perf_cmb_staging]
 @start_month_int INT = NULL
,@end_month_int INT = NULL
,@measure_name VARCHAR(200) = NULL
AS
SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';
DECLARE @start_month_date DATE;
DECLARE @end_month_date DATE;

BEGIN

IF @measure_name = 'Follow-up Hospitalization for Mental Illness'
BEGIN

SET @start_month_date = CAST(CAST(@start_month_int * 100 + 1 AS CHAR(8)) AS DATE);
SET @end_month_date = EOMONTH(CAST(CAST(@end_month_int * 100 + 1 AS CHAR(8)) AS DATE));

SET @SQL = @SQL + N'
IF OBJECT_ID(''tempdb..#temp'', ''U'') IS NOT NULL
DROP TABLE #temp;
CREATE TABLE #temp
([year_month] INT
,[id_mcaid] VARCHAR(255)
,[age] INT
,[admit_date] DATE
,[discharge_date] DATE
,[inpatient_index_stay] INT
,[inpatient_within_30_day] INT
,[need_1_month_coverage] INT
,[follow_up_7_day] INT
,[follow_up_30_day] INT);

INSERT INTO #temp
EXEC [stage].[sp_perf_cmb_fuh_join_step]
 @measurement_start_date=''' + CAST(@start_month_date AS CHAR(10)) + '''
,@measurement_end_date=''' + CAST(@end_month_date AS CHAR(10)) + ''';

WITH CTE AS
(
--First, insert rows for 7-day measure
SELECT
 [year_month]
,[discharge_date] AS [event_date]
,[id_mcaid]
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
,[discharge_date] AS [event_date]
,[id_mcaid]
,[measure_id]
,[inpatient_index_stay] AS [denominator]
,[follow_up_30_day] AS [numerator]
,CAST(GETDATE() AS DATE) AS [load_date]
FROM #temp AS a
LEFT JOIN [ref].[perf_measure] AS b
ON [measure_name] = ''Follow-up Hospitalization for Mental Illness: 30 days''
)

INSERT INTO ##perf_cmb_staging_event_date
([year_month]
,[event_date]
,[id_mcaid]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 [year_month]
,[event_date]
,[id_mcaid]
,[measure_id]
,[denominator]
,[numerator]
,[load_date]
FROM [CTE]'
END

PRINT @SQL;
END

EXEC sp_executeSQL @statement=@SQL, 
                   @params=N'@start_month_int INT, @end_month_int INT, @measure_name VARCHAR(200)',
				   @start_month_int=@start_month_int, @end_month_int=@end_month_int, @measure_name=@measure_name;

GO

/*
IF OBJECT_ID('tempdb..##perf_cmb_staging_event_date') IS NOT NULL
DROP TABLE ##perf_cmb_staging_event_date;
CREATE TABLE ##perf_cmb_staging_event_date
([year_month] INT NOT NULL
,[event_date] DATE NOT NULL
,[id_mcaid] VARCHAR(255) NOT NULL
,[measure_id] SMALLINT NOT NULL
,[denominator] INT NOT NULL
,[numerator] INT NOT NULL
,[load_date] DATE NOT NULL
) ON [PRIMARY];
GO

EXEC [stage].[sp_perf_cmb_staging]
 @start_month_int = 201701
,@end_month_int = 201812
,@measure_name = 'Follow-up Hospitalization for Mental Illness';

IF OBJECT_ID('tempdb..##perf_cmb_staging_event_date') IS NOT NULL
DROP TABLE ##perf_cmb_staging_event_date;

SELECT
 [measure_name]
,SUM([denominator]) AS [denominator]
,SUM([numerator]) AS [numerator]
FROM ##perf_staging_event_date AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
GROUP BY
 [measure_name];
*/