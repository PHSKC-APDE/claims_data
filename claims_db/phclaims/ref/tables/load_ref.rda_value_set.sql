
USE [PHClaims];
GO

/*
SELECT *
FROM [metadata].[v_rda_value_set_summary]
ORDER BY
 [value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[active]
,[num_code];
*/

/*
SUD-Tx-Pen-Value-Set-1 (Line 239)

ALTER INDEX [pk_rda_value_set] 
ON [ref].[rda_value_set] REBUILD PARTITION = ALL;
*/

TRUNCATE TABLE [ref].[rda_value_set];

-- Mental Health Value Sets
INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('MH' AS VARCHAR(20)) AS [value_set_group]
,CAST('MH-Dx-value-set' AS VARCHAR(100)) AS [value_set_name]
,CAST('Diagnosis' AS VARCHAR(50)) AS [data_source_type]
,CAST([Category] AS VARCHAR(50)) AS [sub_group]
,CAST('ICD10CM' AS VARCHAR(50)) AS [code_set]
,CAST([Code] AS VARCHAR(20)) AS [code]
,CAST([Description] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [tmp].[tmp_MH-Dx-value-set-ICD9-10.xlsx]
WHERE [ICD] = 'ICD-10';

WITH CTE AS
(
SELECT 
 *
,ROW_NUMBER() OVER(PARTITION BY REPLACE(CAST(REPLACE([Code], '.', '') AS CHAR(5)), ' ', '0') ORDER BY [Code] DESC) AS [row_num]
FROM [tmp].[tmp_MH-Dx-value-set-ICD9-10.xlsx]
WHERE [ICD] = 'ICD-9'
)

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])

SELECT 
 CAST('MH' AS VARCHAR(20)) AS [value_set_group]
,CAST('MH-Dx-value-set' AS VARCHAR(100)) AS [value_set_name]
,CAST('Diagnosis' AS VARCHAR(50)) AS [data_source_type]
,CAST([Category] AS VARCHAR(50)) AS [sub_group]
,CAST('ICD9CM' AS VARCHAR(50)) AS [code_set]
-- ZERO-RIGHT-PADDED
,CAST(REPLACE(CAST(REPLACE([Code], '.', '') AS CHAR(5)), ' ', '0') AS VARCHAR(20)) AS [code]
,CAST([Description] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [CTE]
WHERE [row_num] = 1;

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('MH' AS VARCHAR(20)) AS [value_set_group]
,CAST('MH-procedure-value-set' AS VARCHAR(100)) AS [value_set_name]
,CAST('Procedure' AS VARCHAR(50)) AS [data_source_type]
,CAST('NA' AS VARCHAR(50)) AS [sub_group]
,CAST([CodeSet] AS VARCHAR(50)) AS [code_set]
,CAST([Code] AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [tmp].[tmp_MH-procedure-value-set_20180918.xlsx];

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('MH' AS VARCHAR(20)) AS [value_set_group]
,CAST('MH-procedure-with-Dx-value-set' AS VARCHAR(100)) AS [value_set_name]
,CAST('Procedure' AS VARCHAR(50)) AS [data_source_type]
,CAST('NA' AS VARCHAR(50)) AS [sub_group]
,CAST([CodeSet] AS VARCHAR(50)) AS [code_set]
,CAST([Code] AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [tmp].[tmp_MH-procedure-with-Dx-value-set_20180920.xlsx];

WITH CTE AS
(
SELECT [MetricCodeGroup]
      ,[GPI]
      ,[CodeDescription]
	  ,ROW_NUMBER() OVER(PARTITION BY [GPI] ORDER BY CASE WHEN [CodeDescription] IS NOT NULL THEN 1 ELSE 2 END) AS [row_num]
FROM [tmp].[tmp_MH-Rx-value-set-20180430.xlsx]
)

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])

SELECT 
 CAST('MH' AS VARCHAR(20)) AS [value_set_group]
,CAST('MH-Rx-value-set' AS VARCHAR(100)) AS [value_set_name]
,CAST('Pharmacy' AS VARCHAR(50)) AS [data_source_type]
,CAST([MetricCodeGroup] AS VARCHAR(50)) AS [sub_group]
,CAST('GPI' AS VARCHAR(50)) AS [code_set]
,CAST([GPI] AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM CTE
WHERE [row_num] = 1;

WITH CTE AS
(
SELECT [MetricCodeGroup]
      ,[CodeDescription]
      ,CAST(CAST([NDCExpansion] AS BIGINT) AS VARCHAR(255)) AS [NDCExpansion]
      ,[NDCLabel]
	  ,ROW_NUMBER() OVER(PARTITION BY CAST(CAST([NDCExpansion] AS BIGINT) AS VARCHAR(255)) ORDER BY CASE WHEN [NDCLabel] IS NOT NULL THEN 1 ELSE 2 END) AS [row_num]
FROM [tmp].[tmp_MH-Rx-value-set-20180430.xlsx]
)

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])

SELECT 
 CAST('MH' AS VARCHAR(20)) AS [value_set_group]
,CAST('MH-Rx-value-set' AS VARCHAR(100)) AS [value_set_name]
,CAST('Pharmacy' AS VARCHAR(50)) AS [data_source_type]
,CAST([MetricCodeGroup] AS VARCHAR(50)) AS [sub_group]
,CAST('NDC' AS VARCHAR(50)) AS [code_set]
,CAST(FORMAT(CAST([NDCExpansion] AS BIGINT), '00000000000') AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST([NDCLabel] AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM CTE
WHERE [row_num] = 1;

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('MH' AS VARCHAR(20)) AS [value_set_group]
,CAST('MH-taxonomy-value-set' AS VARCHAR(100)) AS [value_set_name]
,CAST('Provider' AS VARCHAR(50)) AS [data_source_type]
,CAST('NA' AS VARCHAR(50)) AS [sub_group]
,CAST('HPT' AS VARCHAR(50)) AS [code_set]
,CAST([Code] AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [tmp].[tmp_MH-taxonomy-value-set.xlsx];

/*
DELETE FROM [ref].[rda_value_set]
WHERE [value_set_name] = 'SUD-Tx-Pen-Value-Set-1';
SELECT * FROM [tmp].[SUD-Tx-Pen-Value-Set-1_xlsx];
*/
INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('SUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('SUD-Tx-Pen-Value-Set-1' AS VARCHAR(100)) AS [value_set_name]
,CAST('Diagnosis' AS VARCHAR(50)) AS [data_source_type]
,CAST(ISNULL([SubGroup], 'NA') AS VARCHAR(50)) AS [sub_group]
,CAST('ICD10CM' AS VARCHAR(50)) AS [code_set]
,CAST([Code] AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [tmp].[SUD-Tx-Pen-Value-Set-1_xlsx] WHERE CodeSet = 'ICD10';

WITH CTE AS
(
SELECT 
 *
,ROW_NUMBER() OVER(PARTITION BY REPLACE(CAST(REPLACE([Code], '.', '') AS CHAR(5)), ' ', '0') ORDER BY [Code] DESC) AS [row_num]
FROM [tmp].[SUD-Tx-Pen-Value-Set-1_xlsx]
WHERE [CodeSet] = 'ICD9'
)
INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])

SELECT 
 CAST('SUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('SUD-Tx-Pen-Value-Set-1' AS VARCHAR(100)) AS [value_set_name]
,CAST('Diagnosis' AS VARCHAR(50)) AS [data_source_type]
,CAST(ISNULL([SubGroup], 'NA') AS VARCHAR(50)) AS [sub_group]
,CAST('ICD9CM' AS VARCHAR(50)) AS [code_set]
-- ZERO-RIGHT-PADDED
,CAST(REPLACE(CAST(REPLACE([Code], '.', '') AS CHAR(5)), ' ', '0') AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [CTE]
WHERE [row_num] = 1;

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('SUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('SUD-Tx-Pen-Value-Set-2' AS VARCHAR(100)) AS [value_set_name]
,CAST(CASE WHEN [CodeSet] = 'DRG' THEN 'Diagnosis' ELSE 'Procedure' END AS VARCHAR(50)) AS [data_source_type]
,CAST('Inpatient' AS VARCHAR(50)) AS [sub_group]
,CAST(CASE WHEN [CodeSet] = 'ICD-9 procedure' THEN 'ICD9PCS' WHEN [CodeSet] = 'HCPC' THEN 'HCPCS' WHEN [CodeSet] = 'DRG' THEN 'DRG' ELSE [CodeSet] END AS VARCHAR(50)) AS [code_set]
,CAST([Code] AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [tmp].[tmp_SUD-Tx-Pen-Value-Set-2-1.xlsx];

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('SUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('SUD-Tx-Pen-Value-Set-2' AS VARCHAR(100)) AS [value_set_name]
,CAST('Procedure' AS VARCHAR(50)) AS [data_source_type]
,CAST('Outpatient' AS VARCHAR(50)) AS [sub_group]
,CAST(CASE WHEN [CodeSet] = 'HCPC' THEN 'HCPCS' ELSE [CodeSet] END AS VARCHAR(50)) AS [code_set]
,CAST([Code] AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [tmp].[tmp_SUD-Tx-Pen-Value-Set-2-2.xlsx];

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('SUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('SUD-Tx-Pen-Value-Set-3' AS VARCHAR(100)) AS [value_set_name]
,CAST('Pharmacy' AS VARCHAR(50)) AS [data_source_type]
,CAST('Buprenorphine' AS VARCHAR(50)) AS [sub_group]
,CAST('NDC' AS VARCHAR(50)) AS [code_set]
,CAST([DRUG_CODE] AS VARCHAR(20)) AS [code]
,CAST([GENERIC_NAME] AS VARCHAR(200)) AS [desc_1]
,CAST([LABEL_NAME] AS VARCHAR(200)) AS [desc_2]
,CAST(CASE WHEN [DRUG_STATUS_CODE] = 'I' THEN 'N' WHEN [DRUG_STATUS_CODE] = 'A' THEN 'Y' END AS VARCHAR(1)) AS [active]
,CAST([FROM_DATE] AS DATE) AS [from_date]
,CAST([TO_DATE] AS DATE) AS [to_date]
FROM [tmp].[tmp_SUD-Tx-Pen-Value-Set-3-1_20180928.xlsx];

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('SUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('SUD-Tx-Pen-Value-Set-3' AS VARCHAR(100)) AS [value_set_name]
,CAST('Pharmacy' AS VARCHAR(50)) AS [data_source_type]
,CAST('Naltrexone' AS VARCHAR(50)) AS [sub_group]
,CAST('NDC' AS VARCHAR(50)) AS [code_set]
,CAST([DRUG_CODE] AS VARCHAR(20)) AS [code]
,CAST([GENERIC_NAME] AS VARCHAR(200)) AS [desc_1]
,CAST([LABEL_NAME] AS VARCHAR(200)) AS [desc_2]
,CAST(CASE WHEN [DRUG_STATUS_CODE] = 'I' THEN 'N' WHEN [DRUG_STATUS_CODE] = 'A' THEN 'Y' END AS VARCHAR(1)) AS [active]
,CAST([FROM_DATE] AS DATE) AS [from_date]
,CAST([TO_DATE] AS DATE) AS [to_date]
FROM [tmp].[tmp_SUD-Tx-Pen-Value-Set-3-2_20180928.xlsx];

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('SUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('SUD-Tx-Pen-Value-Set-3' AS VARCHAR(100)) AS [value_set_name]
,CAST('Pharmacy' AS VARCHAR(50)) AS [data_source_type]
,CAST('Disulfiram' AS VARCHAR(50)) AS [sub_group]
,CAST('NDC' AS VARCHAR(50)) AS [code_set]
,CAST([DRUG_CODE] AS VARCHAR(20)) AS [Code_Char]
,CAST([GENERIC_NAME] AS VARCHAR(200)) AS [desc_1]
,CAST([LABEL_NAME] AS VARCHAR(200)) AS [desc_2]
,CAST(CASE WHEN [DRUG_STATUS_CODE] = 'I' THEN 'N' 
           WHEN [DRUG_STATUS_CODE] IS NULL THEN 'N' 
		   WHEN [DRUG_STATUS_CODE] = 'A' THEN 'Y' 
	  END AS VARCHAR(1)) AS [active]
,CAST([FROM_DATE] AS DATE) AS [from_date]
,CAST([TO_DATE] AS DATE) AS [to_date]
FROM [tmp].[tmp_SUD-Tx-Pen-Value-Set-3-3_20180928.xlsx];

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('SUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('SUD-Tx-Pen-Value-Set-4' AS VARCHAR(100)) AS [value_set_name]
,CAST('Procedure' AS VARCHAR(50)) AS [data_source_type]
,CAST('SBIRT' AS VARCHAR(50)) AS [sub_group]
,CAST(CASE WHEN [CodeSet] = 'CPT' THEN 'CPT' WHEN [CodeSet] = 'HCPC' THEN 'HCPCS' ELSE [CodeSet] END AS VARCHAR(50)) AS [code_set]
,CAST([Code] AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [tmp].[tmp_SUD-Tx-Pen-Value-Set-4.xlsx];

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('SUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('SUD-Tx-Pen-Value-Set-5' AS VARCHAR(100)) AS [value_set_name]
,CAST(CASE WHEN [CodeSet] = 'revenue code' THEN 'Billing' ELSE 'Procedure' END AS VARCHAR(50)) AS [data_source_type]
,CAST('Detox' AS VARCHAR(50)) AS [sub_group]
,CAST(CASE WHEN [CodeSet] = 'ICD-9 procedure code' THEN 'ICD9PCS' 
           WHEN [CodeSet] = 'ICD-10 procedure code' THEN 'ICD10PCS' 
		   WHEN [CodeSet] = 'HCPC' THEN 'HCPCS' 
		   WHEN [CodeSet] = 'revenue code' THEN 'UBREV' 
		   ELSE [CodeSet] END AS VARCHAR(50)) AS [code_set]
,CAST(CASE WHEN [CodeSet] = 'revenue code' THEN FORMAT(CAST([Code] AS INT), '0000') ELSE [Code] END AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [tmp].[tmp_SUD-Tx-Pen-Value-Set-5.xlsx];

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('SUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('SUD-Tx-Pen-Value-Set-6' AS VARCHAR(100)) AS [value_set_name]
,CAST('Procedure' AS VARCHAR(50)) AS [data_source_type]
,CAST('Outpatient' AS VARCHAR(50)) AS [sub_group]
,CAST(CASE WHEN [CodeSet] = 'HCPC' THEN 'HCPCS' ELSE [CodeSet] END AS VARCHAR(50)) AS [code_set]
,CAST([Code] AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [tmp].[tmp_SUD-Tx-Pen-Value-Set-6.xlsx];

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('SUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('SUD-Tx-Pen-Value-Set-7' AS VARCHAR(100)) AS [value_set_name]
,CAST('Taxonomy' AS VARCHAR(50)) AS [data_source_type]
,CAST('Outpatient' AS VARCHAR(50)) AS [sub_group]
,CAST('HPT' AS VARCHAR(50)) AS [code_set]
,CAST([Code] AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [tmp].[tmp_SUD-Tx-Pen-Value-Set-7.xlsx];

-- Opioid Use Disorder Value Sets
INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('OUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('OUD-Tx-Pen-Value-Set-1' AS VARCHAR(100)) AS [value_set_name]
,CAST('Diagnosis' AS VARCHAR(50)) AS [data_source_type]
,CAST('NA' AS VARCHAR(50)) AS [sub_group]
,CAST('ICD10CM' AS VARCHAR(50)) AS [code_set]
,CAST([Code] AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [tmp].[tmp_OUD-Tx-Pen-Value-Set-1.xlsx] WHERE [CodeSet] = 'ICD10';

WITH CTE AS
(
SELECT 
 *
,ROW_NUMBER() OVER(PARTITION BY REPLACE(CAST(REPLACE([Code], '.', '') AS CHAR(5)), ' ', '0') ORDER BY [Code] DESC) AS [row_num]
FROM [tmp].[tmp_OUD-Tx-Pen-Value-Set-1.xlsx]
WHERE [CodeSet] = 'ICD9'
)

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])

SELECT 
 CAST('OUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('OUD-Tx-Pen-Value-Set-1' AS VARCHAR(100)) AS [value_set_name]
,CAST('Diagnosis' AS VARCHAR(50)) AS [data_source_type]
,CAST('NA' AS VARCHAR(50)) AS [sub_group]
,CAST('ICD9CM' AS VARCHAR(50)) AS [code_set]
-- ZERO-RIGHT-PADDED
,CAST(REPLACE(CAST(REPLACE([Code], '.', '') AS CHAR(5)), ' ', '0') AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [CTE]
WHERE [row_num] = 1;

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('OUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('OUD-Tx-Pen-Value-Set-2' AS VARCHAR(100)) AS [value_set_name]
,CAST('Pharmacy' AS VARCHAR(50)) AS [data_source_type]
,CAST('Buprenorphine' AS VARCHAR(50)) AS [sub_group]
,CAST('NDC' AS VARCHAR(50)) AS [code_set]
,CAST([Code] AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [tmp].[tmp_OUD-Tx-Pen-Value-Set-2-1.xlsx];

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('OUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('OUD-Tx-Pen-Value-Set-2' AS VARCHAR(100)) AS [value_set_name]
,CAST('Pharmacy' AS VARCHAR(50)) AS [data_source_type]
,CAST('Naltrexone' AS VARCHAR(50)) AS [sub_group]
,CAST('NDC' AS VARCHAR(50)) AS [code_set]
,CAST([Code] AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [tmp].[tmp_OUD-Tx-Pen-Value-Set-2-2.xlsx];

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('OUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('OUD-Tx-Pen-Value-Set-2' AS VARCHAR(100)) AS [value_set_name]
,CAST('Pharmacy' AS VARCHAR(50)) AS [data_source_type]
,CAST('Buprenorphine-Naloxone' AS VARCHAR(50)) AS [sub_group]
,CAST('NDC' AS VARCHAR(50)) AS [code_set]
,CAST([Code] AS VARCHAR(20)) AS [code]
,CAST([CodeDescription] AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date]
FROM [tmp].[tmp_OUD-Tx-Pen-Value-Set-2-3.xlsx];

INSERT INTO [ref].[rda_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[sub_group]
,[code_set]
,[code]
,[desc_1]
,[desc_2]
,[active]
,[from_date]
,[to_date])
SELECT 
 CAST('OUD' AS VARCHAR(20)) AS [value_set_group]
,CAST('OUD-Tx-Pen-Receipt-of-MAT' AS VARCHAR(100)) AS [value_set_name]
,CAST('Procedure' AS VARCHAR(50)) AS [data_source_type]
,CAST('MAT' AS VARCHAR(50)) AS [sub_group]
,CAST('HCPCS' AS VARCHAR(50)) AS [code_set]
,CAST('H0020' AS VARCHAR(20)) AS [code]
,CAST('Alcohol and/or drug services; methadone administration and/or service (provision of the drug by a licensed program)' AS VARCHAR(200)) AS [desc_1]
,CAST(NULL AS VARCHAR(200)) AS [desc_2]
,CAST('Y' AS VARCHAR(1)) AS [active]
,CAST(NULL AS DATE) AS [from_date]
,CAST(NULL AS DATE) AS [to_date];
GO

/*
Cursor to Delete Intermediary Tables
*/
IF OBJECT_ID('[dbo].[sp_drop_table]', 'P') IS NOT NULL
DROP PROCEDURE [dbo].[sp_drop_table];
GO
CREATE PROCEDURE [dbo].[sp_drop_table]
 @SchemaName VARCHAR(128) = NULL
,@TableName  VARCHAR(128) = NULL
,@DBName     VARCHAR(128) = 'PHClaims'
AS
SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';
BEGIN
SET @SQL = @SQL + '
DROP TABLE ' +
'[' + @DBName + '].[' + @SchemaName + '].[' + @TableName + '];'
END

EXEC sp_executeSQL 
 @statement=@SQL
,@params=N'@SchemaName VARCHAR(128), @TableName VARCHAR(128), @DBName VARCHAR(128)'
,@SchemaName=@SchemaName, @TableName=@TableName, @DBName=@DBName;
GO

DECLARE 
 @InputSchemaName AS VARCHAR(128)
,@InputTableName AS VARCHAR(128)
,@InputDBName VARCHAR(128);
DECLARE DropTable_Cursor CURSOR FAST_FORWARD FOR
SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE 1 = 1
  AND TABLE_CATALOG = 'PHClaims'
  AND RIGHT(TABLE_NAME, 5) = '.xlsx'
ORDER BY TABLE_NAME;

OPEN DropTable_Cursor;
FETCH NEXT FROM DropTable_Cursor INTO @InputDBName, @InputSchemaName, @InputTableName;

WHILE @@FETCH_STATUS = 0
BEGIN 
EXEC [dbo].[sp_drop_table] @SchemaName=@InputSchemaName, @TableName=@InputTableName, @DBName=@InputDBName;

FETCH NEXT FROM DropTable_Cursor INTO @InputDBName, @InputSchemaName, @InputTableName;
END;
CLOSE DropTable_Cursor;
DEALLOCATE DropTable_Cursor;