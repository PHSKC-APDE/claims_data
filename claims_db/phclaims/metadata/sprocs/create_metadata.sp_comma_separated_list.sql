
USE [PHClaims];
GO

IF OBJECT_ID('[metadata].[sp_comma_separated_list]','P') IS NOT NULL
DROP PROCEDURE [metadata].[sp_comma_separated_list];
GO
CREATE PROCEDURE [metadata].[sp_comma_separated_list]
 @schema_name VARCHAR(255)
,@table_name VARCHAR(255)
,@column_name VARCHAR(255)
,@ordinal_position INT
AS
--SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN
/*
SET @SQL = @SQL + N'
SELECT STUFF(
(
SELECT '', '' + ' + @column_name + '
FROM
(
SELECT DISTINCT ' + @column_name + ' FROM ' + @table_name + '
) AS a
ORDER BY ' + @column_name + '
FOR XML PATH('''')), 1, 2, ''''
) AS a;';
*/
SET @SQL = @SQL + N'
SELECT
''' + @schema_name + '.' + @table_name + ''' AS [object_name]
,' + CAST(@ordinal_position AS VARCHAR(255)) + ' AS [ordinal_position]
,''' + @column_name + ''' AS [column_name]
,'''' AS [column_description]
,CASE 
 WHEN (SELECT COUNT(DISTINCT ' + @column_name + ') FROM ' + @schema_name + '.' + @table_name + ') > 20 
 --THEN ''Min Value: '' + CAST((SELECT MIN(' + @column_name + ') FROM ' + @schema_name + '.' + @table_name + ') AS VARCHAR(255)) + '', Max Value: '' + CAST((SELECT MAX(' + @column_name + ') FROM ' + @schema_name + '.' + @table_name + ') AS VARCHAR(255))
 THEN ''Range: {'' + CAST((SELECT MIN(' + @column_name + ') FROM ' + @schema_name + '.' + @table_name + ') AS VARCHAR(255)) + '', ..., '' + CAST((SELECT MAX(' + @column_name + ') FROM ' + @schema_name + '.' + @table_name + ') AS VARCHAR(255)) + ''}''
 ELSE ''Values: '' +
 (
 SELECT STUFF(
 (
 SELECT '', '' + CAST(' + @column_name + ' AS VARCHAR(255))
 FROM
 (
 SELECT DISTINCT ' + @column_name + ' FROM ' + @schema_name + '.' + @table_name + '
 ) AS a
 ORDER BY ' + @column_name + '
  FOR XML PATH('''')), 1, 2, ''''
 ) AS a
 ) 
 END AS [column_values]
,(SELECT COUNT(DISTINCT ' + @column_name + ') FROM ' + @schema_name + '.' + @table_name + ') AS [count_distinct_values]
,CAST(CAST(100 * (SELECT COUNT(*) FROM ' + @schema_name + '.' + @table_name + ' WHERE ' + @column_name + ' IS NULL) AS NUMERIC) / (SELECT COUNT(*) FROM ' + @schema_name + '.' + @table_name + ') AS NUMERIC(5,1)) AS [column_pct_null]
,(SELECT DATA_TYPE + CASE WHEN CHARACTER_MAXIMUM_LENGTH IS NULL THEN '''' ELSE ''('' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR(255)) + '')'' END FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''' + @schema_name + ''' AND TABLE_NAME = ''' + @table_name + ''' AND COLUMN_NAME = ''' + @column_name + ''') AS [column_data_type]
,(SELECT CASE WHEN IS_NULLABLE = ''YES'' THEN ''Yes'' ELSE ''No'' END FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''' + @schema_name + ''' AND TABLE_NAME = ''' + @table_name + ''' AND COLUMN_NAME = ''' + @column_name + ''') AS [column_is_nullable];';
PRINT @SQL;

END

EXEC sp_executeSQL 
 @statement=@SQL
,@params=N'@schema_name VARCHAR(255), @table_name VARCHAR(255), @column_name VARCHAR(255), @ordinal_position INT'
,@schema_name=@schema_name
,@table_name=@table_name
,@column_name=@column_name
,@ordinal_position=@ordinal_position;
GO

/*
EXEC [metadata].[sp_comma_separated_list] @schema_name = 'ref', @table_name = 'age_grp', @column_name = 'age_grp_0', @ordinal_position = 1;
EXEC [metadata].[sp_comma_separated_list] @schema_name = 'ref', @table_name = 'taxonomy', @column_name = 'specialty_desc', @ordinal_position = 1;
EXEC [metadata].[sp_comma_separated_list] @schema_name = 'ref', @table_name = 'hedis_code_system', @column_name = 'code_system_oid', @ordinal_position = 9;
*/

IF OBJECT_ID('tempdb..#temp') IS NOT NULL
DROP TABLE #temp;
CREATE TABLE #temp
([object_name] VARCHAR(255)
,[ordinal_position] INT
,[column_name] VARCHAR(255)
,[column_description] VARCHAR(MAX)
,[column_values] VARCHAR(MAX)
,[count_distinct_values] INT
,[column_pct_null] NUMERIC(5,1)
,[column_data_type] VARCHAR(255)
,[column_is_nullable] VARCHAR(255));
GO

DECLARE
 @schema_name_input VARCHAR(255)
,@table_name_input VARCHAR(255)
,@column_name_input VARCHAR(255)
,@ordinal_position_input INT;

DECLARE sp_profile_table_cursor CURSOR FAST_FORWARD FOR
SELECT
 [TABLE_SCHEMA]
,[TABLE_NAME]
,[COLUMN_NAME]
,[ORDINAL_POSITION]
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'final' AND TABLE_NAME = 'mcaid_claim_header';

OPEN sp_profile_table_cursor;
FETCH NEXT FROM sp_profile_table_cursor INTO @schema_name_input, @table_name_input, @column_name_input, @ordinal_position_input;

WHILE @@FETCH_STATUS = 0
BEGIN

INSERT INTO #temp
EXEC [metadata].[sp_comma_separated_list]
 @schema_name = @schema_name_input
,@table_name = @table_name_input
,@column_name = @column_name_input
,@ordinal_position = @ordinal_position_input;

FETCH NEXT FROM sp_profile_table_cursor INTO @schema_name_input, @table_name_input, @column_name_input, @ordinal_position_input;
END

CLOSE sp_profile_table_cursor;
DEALLOCATE sp_profile_table_cursor;

SELECT * FROM #temp ORDER BY [ordinal_position];