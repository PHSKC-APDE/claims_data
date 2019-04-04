
USE [PHClaims];
GO

IF OBJECT_ID('[ref].[mcaid_prvdr_txnmy]', 'U') IS NOT NULL
DROP TABLE [ref].[mcaid_prvdr_txnmy];
CREATE TABLE [ref].[mcaid_prvdr_txnmy]
([taxonomy_code] VARCHAR(200) NOT NULL
,[desc] VARCHAR(1000) NULL
,[cms_taxonomy_group] VARCHAR(200) NULL
,[cms_specialty_code] VARCHAR(200) NULL
,CONSTRAINT [PK_ref_mcaid_prvdr_txnmy] PRIMARY KEY CLUSTERED([taxonomy_code]));
GO

TRUNCATE TABLE [ref].[mcaid_prvdr_txnmy];

INSERT INTO [ref].[mcaid_prvdr_txnmy]
([taxonomy_code]
,[desc]
,[cms_taxonomy_group]
,[cms_specialty_code])

SELECT 
 CAST([PROVIDER.TAXONOMY.CODE] AS VARCHAR(200)) AS [taxonomy_code]
,CAST([PROVIDER.TAXONOMY.DESCRIPTION] AS VARCHAR(1000)) AS [desc]
,CAST([MEDICARE.PROVIDER/SUPPLIER.TYPE.DESCRIPTION] AS VARCHAR(200)) AS [cms_taxonomy_group]
,CAST([MEDICARE.SPECIALTY.CODE] AS VARCHAR(200)) AS [cms_specialty_code]
FROM [PHClaims].[KC\psylling].[CROSSWALK_MEDICARE_PROVIDER_SUPPLIER_to_HEALTHCARE_PROVIDER_TAXONOMY_CLEANED.xlsx]
WHERE [DUPLICATE] IS NULL
GO

SELECT * 
FROM [ref].[mcaid_prvdr_txnmy];

IF OBJECT_ID('[PHClaims].[KC\psylling].[CROSSWALK_MEDICARE_PROVIDER_SUPPLIER_to_HEALTHCARE_PROVIDER_TAXONOMY_CLEANED.xlsx]') IS NOT NULL
DROP TABLE [PHClaims].[KC\psylling].[CROSSWALK_MEDICARE_PROVIDER_SUPPLIER_to_HEALTHCARE_PROVIDER_TAXONOMY_CLEANED.xlsx];