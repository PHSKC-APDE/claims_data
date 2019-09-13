
USE [PHClaims];
GO

/*
-- Version 1
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
FROM [tmp].[ref.mcaid_prvdr_txnmy.xlsx]
WHERE [DUPLICATE] IS NULL;
GO

SELECT * 
FROM [ref].[mcaid_prvdr_txnmy];

IF OBJECT_ID('[tmp].[ref.mcaid_prvdr_txnmy.xlsx]') IS NOT NULL
DROP TABLE [tmp].[ref.mcaid_prvdr_txnmy.xlsx];
*/

TRUNCATE TABLE [ref].[taxonomy];

INSERT INTO [ref].[taxonomy]
([taxonomy_code]
,[specialty_desc]
,[taxonomy_general_type]
,[taxonomy_provider_type]
,[taxonomy_specialization_area])

SELECT 
 [specialty_code]
,[specialty_desc]
,[taxonomy_general_type]
,[taxonomy_provider_type]
,[taxonomy_specialization_area]
/*
[ref].[apcd_specialty] appears to be virtually identical to 
NUCC Health Care Provider Taxonomy Code Set
http://nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40/csv-mainmenu-57
*/
FROM [ref].[apcd_specialty]
WHERE [specialty_type_desc] = 'Taxonomy'
AND [specialty_code] NOT IN ('-1', '-2')
ORDER BY [specialty_code];
GO

--SELECT * FROM [ref].[taxonomy];

/*
There are 4 HCA taxonomy codes
https://www.hca.wa.gov/assets/program/imc-seri-national-provider-id-and-taxonomies-questions.pdf
*/
INSERT INTO [ref].[taxonomy]
VALUES
 ('101Y99996L', 'Local HCA Taxonomy Code', NULL, NULL, NULL)
,('101Y99995L', 'Local HCA Taxonomy Code', NULL, NULL, NULL)
,('175T99994L', 'Local HCA Taxonomy Code', NULL, NULL, NULL)
,('101Y99993L', 'Local HCA Taxonomy Code', NULL, NULL, NULL)