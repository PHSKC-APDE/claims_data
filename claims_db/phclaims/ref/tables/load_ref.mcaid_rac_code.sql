
USE [PHClaims];
GO

/*
-- Create Backup
IF OBJECT_ID('[tmp].[mcaid_rac_code]') IS NOT NULL
DROP TABLE [tmp].[mcaid_rac_code];
SELECT *
INTO [tmp].[mcaid_rac_code]
FROM [ref].[mcaid_rac_code];

ALTER TABLE [tmp].[mcaid_rac_code] ADD CONSTRAINT PK_tmp_mcaid_rac_code PRIMARY KEY([rac_code]);
GO
*/

WITH [BSP_GROUP_CID] AS
(
SELECT DISTINCT
 [RPRTBL_RAC_CODE]
,[RPRTBL_BSP_GROUP_CID]
,'Y' AS [flag]
FROM [PHClaims].[stage].[mcaid_elig]
WHERE [RPRTBL_BSP_GROUP_CID] IN
(1003960, 1003956, 10066833, 1003962)
),

[PIVOT_BSP_GROUP_CID] AS
(
SELECT 
 [RPRTBL_RAC_CODE]
,[1003960] AS [BSP_GROUP_CID_1003960]
,[1003956] AS [BSP_GROUP_CID_1003956]
,[10066833] AS [BSP_GROUP_CID_10066833]
,[1003962] AS [BSP_GROUP_CID_1003962]
FROM [BSP_GROUP_CID]
PIVOT (MAX([flag]) FOR [RPRTBL_BSP_GROUP_CID] IN ([1003960], [1003956], [10066833], [1003962])) AS P
)

INSERT INTO [ref].[mcaid_rac_code]
([rac_code]
,[rac_name]
,[fund_source_code]
,[elig_value]
,[sub_elig_value]
,[category]
,[title_xix_full_benefit_1519_reporting]
,[title_xix_limited_benefit]
,[title_xxi_full_benefit]
,[legacy_mcs]
,[magi]
,[major_cov_grp_1519_reporting]
,[bsp_group_cid_1003960]
,[bsp_group_cid_1003956]
,[bsp_group_cid_10066833]
,[bsp_group_cid_1003962]
,[full_benefit_flag])

SELECT
 CAST(a.[RAC_CODE] AS INT) AS [rac_code]
,CAST(a.[RAC_DESC] AS VARCHAR(200)) AS [rac_name]
,CAST(a.[FUND_SOURCE_CODE] AS VARCHAR(200)) AS [fund_source_code]
,CAST(b.[Elig.Value] AS INT) AS [elig_value]
,CAST(b.[Subelig.Value] AS INT) AS [sub_elig_value]

,CAST(CASE WHEN b.[Elig.Value] = 14 THEN 'MN – Other (Family/Pregnancy)' ELSE c.[Category] END AS VARCHAR(100)) AS [category] 
,CAST(c.[Title.XIX.Full.Benefit.Included.in.1519.Public.Reporting] AS CHAR(1)) AS [title_xix_full_benefit_1519_reporting]
,CAST(c.[Title.XIX.Limited.Benefit] AS CHAR(1)) AS [title_xix_limited_benefit]
,CAST(c.[Title.XXI.Full.Benefit] AS CHAR(1)) AS [title_xxi_full_benefit]
,CAST(c.[Legacy.MCS] AS CHAR(1)) AS [legacy_mcs]
,CAST(CASE WHEN c.[MAGI] = '--' THEN NULL ELSE c.[MAGI] END AS CHAR(1)) AS [magi]
,CAST(c.[Major.Coverage.Group.for.1519.Reporting] AS VARCHAR(50)) AS [major_cov_grp_1519_reporting]
,ISNULL(d.[BSP_GROUP_CID_1003960], 'N') AS [BSP_GROUP_CID_1003960]
,ISNULL(d.[BSP_GROUP_CID_1003956], 'N') AS [BSP_GROUP_CID_1003956]
,ISNULL(d.[BSP_GROUP_CID_10066833], 'N') AS [BSP_GROUP_CID_10066833]
,ISNULL(d.[BSP_GROUP_CID_1003962], 'N') AS [BSP_GROUP_CID_1003962]

,CASE WHEN ([Title.XIX.Full.Benefit.Included.in.1519.Public.Reporting] = 'Y' OR [Title.XXI.Full.Benefit] = 'Y')
      THEN 'Y'
      ELSE 'N'
 END AS [full_benefit_flag]

FROM [tmp].[Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS-3] AS a

LEFT JOIN [tmp].[Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS-2] AS b
ON CAST(a.[RAC_CODE] AS INT) = CAST(b.[RAC_CODE] AS INT)

LEFT JOIN [tmp].[Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS-1] AS c
ON CAST(b.[Elig.Value] AS INT) = CAST(c.[Elig.Value] AS INT)

LEFT JOIN [PIVOT_BSP_GROUP_CID] AS d
ON CAST(a.[RAC_CODE] AS INT) = d.[RPRTBL_RAC_CODE];

/*
DROP TABLE [tmp].[Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS-1];
DROP TABLE [tmp].[Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS-2];
DROP TABLE [tmp].[Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS-3];
*/

/*
-- Validation Query
WITH CTE AS
(
SELECT [rac_code]
      ,[rac_name]
      ,[fund_source_code]
      ,[elig_value]
      ,[sub_elig_value]
      ,[category]
      ,[title_xix_full_benefit_1519_reporting]
      ,[title_xix_limited_benefit]
      ,[title_xxi_full_benefit]
      ,[legacy_mcs]
      ,[magi]
      ,[major_cov_grp_1519_reporting]
      ,[bsp_group_cid_1003960]
      ,[bsp_group_cid_1003956]
      ,[bsp_group_cid_10066833]
      ,[bsp_group_cid_1003962]
      ,[full_benefit_flag]
	  ,CASE WHEN [fund_source_code] IN ('Federal', 'Title XXI') AND ([bsp_group_cid_1003960] = 'Y' OR [bsp_group_cid_1003956] = 'Y' OR [bsp_group_cid_10066833] = 'Y' OR [bsp_group_cid_1003962] = 'Y')
	        THEN 'Y' ELSE 'N' 
	   END AS [new_full_benefit_flag]
FROM [PHClaims].[ref].[mcaid_rac_code]
)
SELECT [rac_code]
      ,[rac_name]
      ,[fund_source_code]
	  ,[elig_value]
      ,[title_xix_full_benefit_1519_reporting]
	  ,[title_xix_limited_benefit]
      ,[title_xxi_full_benefit]
      ,[bsp_group_cid_1003960]
      ,[bsp_group_cid_1003956]
      ,[bsp_group_cid_10066833]
      ,[bsp_group_cid_1003962]
      ,[full_benefit_flag]
	  ,[new_full_benefit_flag]
FROM [CTE]
WHERE 1 = 1
AND [elig_value] IS NOT NULL
AND [full_benefit_flag] = 'Y'
AND [new_full_benefit_flag] = 'N'
--AND [full_benefit_flag] = 'N'
--AND [new_full_benefit_flag] = 'Y'
--AND [full_benefit_flag] = 'Y'
--AND [new_full_benefit_flag] = 'Y'
--AND [full_benefit_flag] = 'N'
--AND [new_full_benefit_flag] = 'N';
*/