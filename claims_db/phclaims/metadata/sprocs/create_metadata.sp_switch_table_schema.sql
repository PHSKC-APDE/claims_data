
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
SELECT 
 sc.[name]
,ob.[name]
,CAST(ob.[create_date] AS DATE) AS [create_date]
,pt.[rows]
FROM sys.partitions AS pt
INNER JOIN sys.objects AS ob
ON pt.[object_id] = ob.[object_id]
INNER JOIN sys.schemas AS sc
ON ob.[schema_id] = sc.[schema_id]
WHERE ob.[name] IN
('mcaid_claim_icdcm_header'
,'mcaid_claim_line'
,'mcaid_claim_pharm'
,'mcaid_claim_procedure'
,'mcaid_claim_header')
AND pt.[index_id] = 1
ORDER BY ob.[name], sc.[name];
*/

DECLARE @from_schema_input AS VARCHAR(100) = 'stage';
DECLARE @to_schema_input AS VARCHAR(100) = 'final';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema=@from_schema_input
,@from_table='mcaid_claim_icdcm_header'
,@to_schema=@to_schema_input
,@to_table='mcaid_claim_icdcm_header';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema=@from_schema_input
,@from_table='mcaid_claim_line'
,@to_schema=@to_schema_input
,@to_table='mcaid_claim_line';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema=@from_schema_input
,@from_table='mcaid_claim_pharm'
,@to_schema=@to_schema_input
,@to_table='mcaid_claim_pharm';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema=@from_schema_input
,@from_table='mcaid_claim_procedure'
,@to_schema=@to_schema_input
,@to_table='mcaid_claim_procedure';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema=@from_schema_input
,@from_table='mcaid_claim_header'
,@to_schema=@to_schema_input
,@to_table='mcaid_claim_header';