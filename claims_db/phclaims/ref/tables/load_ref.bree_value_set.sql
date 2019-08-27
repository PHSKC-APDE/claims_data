
USE [PHClaims];
GO

TRUNCATE TABLE [ref].[bree_value_set];

INSERT INTO [ref].[bree_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[code_set]
,[code]
,[drug_name]
,[drug_label_name]
,[drug_generic_short_name]
,[drug_route_desc]
,[cough_and_cold_flag])

SELECT 
 CAST('Opioid' AS VARCHAR(20)) AS [value_set_group]
,CAST('Opioid-Include' AS VARCHAR(100)) AS [value_set_name]
,CAST('Pharmacy' AS VARCHAR(50)) AS [data_source_type]
,CAST('NDC' AS VARCHAR(50)) AS [code_set]
,CAST(FORMAT(CAST([Drug.Key] AS BIGINT), '00000000000') AS VARCHAR(20)) AS [code]
,CAST([Drug.Name] AS VARCHAR(255)) AS [drug_name]
,CAST([Drug.Labl.Name] AS VARCHAR(255)) AS [drug_label_name]
,CAST([Drug.Gnrc.Short.Name] AS VARCHAR(255)) AS [drug_generic_short_name]
,CAST([Drug.Route.Admstr.Code.Desc] AS VARCHAR(255)) AS [drug_route_desc]
,NULL AS [cough_and_cold_flag]
FROM [tmp].[Bree_Opioid_NDC_2017_include];

WITH CTE AS
(
SELECT 
 CAST('Opioid' AS VARCHAR(20)) AS [value_set_group]
,CAST('Opioid-Exclude' AS VARCHAR(100)) AS [value_set_name]
,CAST('Pharmacy' AS VARCHAR(50)) AS [data_source_type]
,CAST('NDC' AS VARCHAR(50)) AS [code_set]
,CAST(FORMAT(CAST([Drug.Key] AS BIGINT), '00000000000') AS VARCHAR(20)) AS [code]
,CAST([Drug.Name] AS VARCHAR(255)) AS [drug_name]
,CAST([Drug.Labl.Name] AS VARCHAR(255)) AS [drug_label_name]
,CAST([Drug.Gnrc.Short.Name] AS VARCHAR(255)) AS [drug_generic_short_name]
,CAST([Drug.Route.Admstr.Code.Desc] AS VARCHAR(255)) AS [drug_route_desc]
,CAST([Cough.and.Cold.Flag] AS TINYINT) AS [cough_and_cold_flag]
,ROW_NUMBER() OVER(PARTITION BY [Drug.Key] ORDER BY [Cough.and.Cold.Flag] DESC) AS [row_num]
FROM [tmp].[Bree_Opioid_NDC_2017_exclude]
)

INSERT INTO [ref].[bree_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[code_set]
,[code]
,[drug_name]
,[drug_label_name]
,[drug_generic_short_name]
,[drug_route_desc]
,[cough_and_cold_flag])

SELECT 
 [value_set_group]
,[value_set_name]
,[data_source_type]
,[code_set]
,[code]
,[drug_name]
,[drug_label_name]
,[drug_generic_short_name]
,[drug_route_desc]
,[cough_and_cold_flag]
FROM [CTE]
WHERE [row_num] = 1;