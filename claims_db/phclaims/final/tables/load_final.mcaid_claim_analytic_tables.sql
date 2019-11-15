
USE [PHClaims];
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
 @from_schema='final'
,@from_table='mcaid_claim_pharm'
,@to_schema='archive'
,@to_table='mcaid_claim_pharm';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='stage'
,@from_table='mcaid_claim_pharm'
,@to_schema='final'
,@to_table='mcaid_claim_pharm';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='final'
,@from_table='mcaid_claim_procedure'
,@to_schema='archive'
,@to_table='mcaid_claim_procedure';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='stage'
,@from_table='mcaid_claim_procedure'
,@to_schema='final'
,@to_table='mcaid_claim_procedure';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='final'
,@from_table='mcaid_claim_header'
,@to_schema='archive'
,@to_table='mcaid_claim_header';

EXEC [metadata].[sp_switch_table_schema]
 @from_schema='stage'
,@from_table='mcaid_claim_header'
,@to_schema='final'
,@to_table='mcaid_claim_header';

/*
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
WHERE t.name LIKE 'mcaid%'
ORDER BY OBJECT_SCHEMA_NAME(t.object_id), t.name, i.index_id;
*/