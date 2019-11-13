
USE [PHClaims];
GO

/*
[mcaid_claim_icdcm_header]

SELECT COUNT(*) FROM [archive].[mcaid_claim_icdcm_header];
SELECT COUNT(*) FROM [final].[mcaid_claim_icdcm_header];
SELECT COUNT(*) FROM [stage].[mcaid_claim_icdcm_header];
*/

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='final'
,@from_table='mcaid_claim_icdcm_header'
,@to_schema='archive'
,@to_table='mcaid_claim_icdcm_header';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='stage'
,@from_table='mcaid_claim_icdcm_header'
,@to_schema='final'
,@to_table='mcaid_claim_icdcm_header';

/*
[mcaid_claim_line]

SELECT COUNT(*) FROM [archive].[mcaid_claim_line];
SELECT COUNT(*) FROM [final].[mcaid_claim_line];
SELECT COUNT(*) FROM [stage].[mcaid_claim_line];
*/

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='final'
,@from_table='mcaid_claim_line'
,@to_schema='archive'
,@to_table='mcaid_claim_line';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='stage'
,@from_table='mcaid_claim_line'
,@to_schema='final'
,@to_table='mcaid_claim_line';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='stage'
,@from_table='mcaid_claim_line'
,@to_schema='final'
,@to_table='mcaid_claim_line';

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

select top(1) [last_run] from [final].[mcaid_claim_header];
select top(1) [last_run] from [stage].[mcaid_claim_header];

select top(1) [last_run] from [final].[mcaid_claim_icdcm_header];
select top(1) [last_run] from [stage].[mcaid_claim_icdcm_header];

select top(1) [last_run] from [final].[mcaid_claim_line];
select top(1) [last_run] from [stage].[mcaid_claim_line];

select top(1) [last_run] from [final].[mcaid_claim_pharm];
select top(1) [last_run] from [stage].[mcaid_claim_pharm];

select top(1) [last_run] from [final].[mcaid_claim_procedure];
select top(1) [last_run] from [stage].[mcaid_claim_procedure];

if object_id('[stage].[mcaid_claim_header]', 'U') is not null
drop table [stage].[mcaid_claim_header];
if object_id('[stage].[mcaid_claim_icdcm_header]', 'U') is not null
drop table [stage].[mcaid_claim_icdcm_header];
if object_id('[stage].[mcaid_claim_line]', 'U') is not null
drop table [stage].[mcaid_claim_line];
if object_id('[stage].[mcaid_claim_pharm]', 'U') is not null
drop table [stage].[mcaid_claim_pharm];
if object_id('[stage].[mcaid_claim_procedure]', 'U') is not null
drop table [stage].[mcaid_claim_procedure];

/*
EXEC [metadata].[sp_switch_table_schema]
 @from_schema='stage'
,@from_table='mcaid_claim_header'
,@to_schema='final'
,@to_table='mcaid_claim_header';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='stage'
,@from_table='mcaid_claim_icdcm_header'
,@to_schema='final'
,@to_table='mcaid_claim_icdcm_header';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='stage'
,@from_table='mcaid_claim_line'
,@to_schema='final'
,@to_table='mcaid_claim_line';

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

SELECT 
 OBJECT_SCHEMA_NAME(t.object_id) AS schema_name
,t.name AS table_name
,i.index_id
,i.name AS index_name
,ds.name AS filegroup_name
,FORMAT(p.rows, '#,###') AS rows
FROM sys.tables t
INNER JOIN sys.indexes i 
ON t.object_id=i.object_id
INNER JOIN sys.filegroups ds 
ON i.data_space_id=ds.data_space_id
INNER JOIN sys.partitions p 
ON i.object_id=p.object_id 
AND i.index_id=p.index_id
ORDER BY t.name, i.index_id;
*/