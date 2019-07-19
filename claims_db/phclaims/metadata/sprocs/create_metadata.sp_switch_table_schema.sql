
USE [PHClaims];
GO

IF OBJECT_ID('[metadata].[sp_switch_table]', 'P') IS NOT NULL
DROP PROCEDURE [metadata].[sp_switch_table];
GO
CREATE PROCEDURE [metadata].[sp_switch_table]
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
TRUNCATE TABLE [' + @to_schema + '].[' + @to_table + '];
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
EXEC [metadata].[sp_switch_table]
 @from_schema='stage'
,@from_table='mcaid_claim_icdcm_header'
,@to_schema='final'
,@to_table='mcaid_claim_icdcm_header';

SELECT COUNT(*) FROM [stage].[mcaid_claim_icdcm_header];
SELECT COUNT(*) FROM [final].[mcaid_claim_icdcm_header];

EXEC [metadata].[sp_switch_table]
 @from_schema='final'
,@from_table='mcaid_claim_icdcm_header'
,@to_schema='stage'
,@to_table='mcaid_claim_icdcm_header';

SELECT COUNT(*) FROM [stage].[mcaid_claim_icdcm_header];
SELECT COUNT(*) FROM [final].[mcaid_claim_icdcm_header];
*/
