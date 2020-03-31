
USE [PHClaims];
GO

IF OBJECT_ID('[metadata].[sp_min_max_value]','P') IS NOT NULL
DROP PROCEDURE [metadata].[sp_min_max_value];
GO
CREATE PROCEDURE [metadata].[sp_min_max_value]
 @schema_name VARCHAR(255)
,@table_name VARCHAR(255)
,@column_name VARCHAR(255)
,@ordinal_position INT
AS
--SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN
SET @SQL = @SQL + N'
SELECT
''' + @schema_name + '.' + @table_name + ''' AS [object_name]
,' + CAST(@ordinal_position AS VARCHAR(255)) + ' AS [ordinal_position]
,''' + @column_name + ''' AS [column_name]
,''Min Value: '' + 
 CAST((SELECT MIN(' + @column_name + ') FROM ' + @schema_name + '.' + @table_name + ') AS VARCHAR(255)) + '', Max Value: '' + 
 CAST((SELECT MAX(' + @column_name + ') FROM ' + @schema_name + '.' + @table_name + ') AS VARCHAR(255)) AS [column_description];';
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
EXEC [metadata].[sp_min_max_value] @schema_name = 'ref', @table_name = 'age_grp', @column_name = 'age_grp_0', @ordinal_position = 1;
*/