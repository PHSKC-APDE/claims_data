
USE [PHClaims];
GO

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='stage'
,@from_table='mcaid_claim_procedure'
,@to_schema='final'
,@to_table='mcaid_claim_procedure';

if object_id('[stage].[mcaid_claim_procedure]', 'U') is not null
drop table [stage].[mcaid_claim_procedure];

SELECT * FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'mcaid_claim_procedure';

