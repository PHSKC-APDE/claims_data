
USE [PHClaims];
GO

IF OBJECT_ID('[metadata].[sp_comma_separated_list]','P') IS NOT NULL
DROP PROCEDURE [metadata].[sp_comma_separated_list];
GO
CREATE PROCEDURE [metadata].[sp_comma_separated_list]
 @table_name VARCHAR(255)
,@column_name VARCHAR(255)
AS
--SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN

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
PRINT @SQL;

END

EXEC sp_executeSQL 
 @statement=@SQL
,@params=N'@table_name VARCHAR(255), @column_name VARCHAR(255)'
,@table_name=@table_name
,@column_name=@column_name;
GO

/*
EXEC [metadata].[sp_comma_separated_list] @table_name = 'stage.daily_log_clean', @column_name = 'referral_source';
*/