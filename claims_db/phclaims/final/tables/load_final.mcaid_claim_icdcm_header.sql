
USE [PHClaims];
GO

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='stage'
,@from_table='mcaid_claim_icdcm_header'
,@to_schema='final'
,@to_table='mcaid_claim_icdcm_header';

if object_id('[stage].[mcaid_claim_icdcm_header]', 'U') is not null
drop table [stage].[mcaid_claim_icdcm_header];

