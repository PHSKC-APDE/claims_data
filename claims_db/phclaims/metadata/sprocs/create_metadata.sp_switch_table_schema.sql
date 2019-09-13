
USE [PHClaims];
GO

IF OBJECT_ID('[metadata].[sp_switch_table_schema]', 'P') IS NOT NULL
DROP PROCEDURE [metadata].[sp_switch_table_schema];
GO
CREATE PROCEDURE [metadata].[sp_switch_table_schema]
 @from_schema VARCHAR(255)
,@from_table VARCHAR(255)
,@to_schema VARCHAR(255)
,@to_table VARCHAR(255)


AS
SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN

SET @SQL = @SQL + N'
BEGIN TRY
ALTER TABLE [' + @from_schema + '].[' + @from_table + ']
SWITCH PARTITION 1 TO [' + @to_schema + '].[' + @to_table + '] PARTITION 1
WITH (WAIT_AT_LOW_PRIORITY (MAX_DURATION = 1 MINUTES, ABORT_AFTER_WAIT = SELF));
END TRY
BEGIN CATCH
PRINT ''Process stopped after 1 minute. Table [' + @from_schema + '].[' + @from_table + '] is already in use.'';
END CATCH'

PRINT @SQL;
END

EXEC sp_executeSQL 
 @statement=@SQL
,@params=N'@from_schema VARCHAR(255), @from_table VARCHAR(255), @to_schema VARCHAR(255), @to_table VARCHAR(255)'
,@from_schema=@from_schema, @from_table=@from_table, @to_schema=@to_schema, @to_table=@to_table;
GO

/*
EXEC [metadata].[sp_switch_table_schema]
 @from_schema='stage'
,@from_table='mcaid_claim_header'
,@to_schema='final'
,@to_table='mcaid_claim_header';

SELECT DISTINCT [last_run] FROM [stage].[mcaid_claim_header];
SELECT DISTINCT [last_run] FROM [final].[mcaid_claim_header];
*/

--TRUNCATE TABLE [' + @to_schema + '].[' + @to_table + '];