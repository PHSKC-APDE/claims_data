
USE [PHClaims];
GO

IF OBJECT_ID('[stage].[sp_perf_staging_ALL_MEASURES]','P') IS NOT NULL
DROP PROCEDURE [stage].[sp_perf_staging_ALL_MEASURES];
GO
CREATE PROCEDURE [stage].[sp_perf_staging_ALL_MEASURES]
 @start_month_int INT = 201501
,@end_month_int INT = 201906
AS
SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN
SET @SQL = @SQL + N'

DECLARE
 @start_month_int_input INT
,@end_month_int_input AS INT
,@measure_name_input AS VARCHAR(255);

DECLARE sp_perf_staging_cursor CURSOR FAST_FORWARD FOR
SELECT ' +
 CAST(@start_month_int AS VARCHAR(255)) + ' AS [start_month_int]
,' + CAST(@end_month_int AS VARCHAR(255)) + ' AS [end_month_int]
,[measure_name]
FROM (VALUES
 (''Acute Hospital Utilization'')
,(''All-Cause ED Visits'')
,(''Child and Adolescent Access to Primary Care'')
,(''Follow-up ED visit for Alcohol/Drug Abuse'')
,(''Follow-up ED visit for Mental Illness'')
,(''Follow-up Hospitalization for Mental Illness'')
,(''Mental Health Treatment Penetration'')
,(''MH Treatment Penetration by Diagnosis'')
,(''Plan All-Cause Readmissions (30 days)'')
,(''SUD Treatment Penetration'')
,(''SUD Treatment Penetration (Opioid)'')
) AS a([measure_name]);

OPEN sp_perf_staging_cursor;
FETCH NEXT FROM sp_perf_staging_cursor INTO @start_month_int_input, @end_month_int_input, @measure_name_input;

WHILE @@FETCH_STATUS = 0
BEGIN

EXEC [stage].[sp_perf_staging]
 @start_month_int = @start_month_int_input
,@end_month_int = @end_month_int_input
,@measure_name = @measure_name_input;

FETCH NEXT FROM sp_perf_staging_cursor INTO @start_month_int_input, @end_month_int_input, @measure_name_input;
END

CLOSE sp_perf_staging_cursor;
DEALLOCATE sp_perf_staging_cursor;'
PRINT @SQL;
END;

EXEC sp_executeSQL 
 @statement=@SQL
,@params=N'@start_month_int INT, @end_month_int INT'
,@start_month_int=@start_month_int, @end_month_int=@end_month_int;
GO

/*
EXEC [stage].[sp_perf_staging_ALL_MEASURES]
 @start_month_int = 201801
,@end_month_int = 201906;
*/