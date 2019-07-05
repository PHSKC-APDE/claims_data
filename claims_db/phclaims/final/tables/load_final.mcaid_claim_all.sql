
USE [PHClaims];
GO

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='stage'
,@from_table='mcaid_claim_icdcm_header'
,@to_schema='final'
,@to_table='mcaid_claim_icdcm_header';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='stage'
,@from_table='mcaid_claim_pharm'
,@to_schema='final'
,@to_table='mcaid_claim_pharm';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='stage'
,@from_table='mcaid_claim_procedure'
,@to_schema='final'
,@to_table='mcaid_claim_procedure';

if object_id('[stage].[mcaid_claim_icdcm_header]', 'U') is not null
drop table [stage].[mcaid_claim_icdcm_header];
if object_id('[stage].[mcaid_claim_pharm]', 'U') is not null
drop table [stage].[mcaid_claim_pharm];
if object_id('[stage].[mcaid_claim_procedure]', 'U') is not null
drop table [stage].[mcaid_claim_procedure];

/*
EXEC [metadata].[sp_switch_table_schema]
 @from_schema='final'
,@from_table='mcaid_claim_icdcm_header'
,@to_schema='stage'
,@to_table='mcaid_claim_icdcm_header';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='final'
,@from_table='mcaid_claim_pharm'
,@to_schema='stage'
,@to_table='mcaid_claim_pharm';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='final'
,@from_table='mcaid_claim_procedure'
,@to_schema='stage'
,@to_table='mcaid_claim_procedure';
*/