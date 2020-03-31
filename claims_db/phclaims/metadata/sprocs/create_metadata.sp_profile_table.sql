
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
,''Values: '' +
(
SELECT STUFF(
(
SELECT '', '' + ' + @column_name + '
FROM
(
SELECT DISTINCT ' + @column_name + ' FROM ' + @schema_name + '.' + @table_name + '
) AS a
ORDER BY ' + @column_name + '
FOR XML PATH('''')), 1, 2, ''''
) AS a
) AS [column_description];';
PRINT @SQL;

END

EXEC sp_executeSQL 
 @statement=@SQL
,@params=N'@table_name VARCHAR(255), @column_name VARCHAR(255)'
,@table_name=@table_name
,@column_name=@column_name;
GO

/*
EXEC [metadata].[sp_comma_separated_list] @schema_name = 'ref', @table_name = 'age_grp', @column_name = 'age_grp_0', @ordinal_position = 1;
*/