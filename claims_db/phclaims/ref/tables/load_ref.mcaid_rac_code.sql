
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

-- Verify 1-1 mapping
SELECT NumRows
	  ,COUNT(*)
FROM
(
SELECT [RAC.Code.4.Bytes], [BSP_GROUP_CID]
	  ,COUNT(*) AS NumRows
FROM (SELECT DISTINCT [RAC.Code.4.Bytes], [BSP_GROUP_CID] FROM [PHClaims].[tmp].[Medicaid_RAC_Codes_BSP_Group]) AS a
GROUP BY [RAC.Code.4.Bytes], [BSP_GROUP_CID]
) AS SubQuery
GROUP BY NumRows
ORDER BY NumRows;
*/

TRUNCATE TABLE [ref].[mcaid_rac_code];

WITH [BSP_GROUP_CID] AS
(
SELECT 
 DISTINCT [RAC.Code.4.Bytes]
,[BSP_GROUP_ABBREV]
,[BSP_GROUP_NAME]
,[BSP_GROUP_CID]
FROM [PHClaims].[tmp].[Medicaid_RAC_Codes_BSP_Group]
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
,[bsp_group_cid]
,[bsp_group_abbrev]
,[bsp_group_name]
,[full_benefit]
,[alternate_rda_full_benefit])

SELECT
 CAST(a.[RAC_CODE] AS INT) AS [rac_code]
,CAST(a.[RAC_DESC] AS VARCHAR(255)) AS [rac_name]
,CAST(a.[FUND_SOURCE_CODE] AS VARCHAR(255)) AS [fund_source_code]

,CAST(b.[Elig.Value] AS INT) AS [elig_value]
,CAST(b.[Subelig.Value] AS INT) AS [sub_elig_value]

-- Recode character MN â€“ Other (Family/Pregnancy)
,CAST(CASE WHEN b.[Elig.Value] = 14 THEN 'MN – Other (Family/Pregnancy)' ELSE c.[Category] END AS VARCHAR(255)) AS [category]
,CAST(c.[Title.XIX.Full.Benefit.Included.in.1519.Public.Reporting] AS CHAR(1)) AS [title_xix_full_benefit_1519_reporting]
,CAST(c.[Title.XIX.Limited.Benefit] AS CHAR(1)) AS [title_xix_limited_benefit]
,CAST(c.[Title.XXI.Full.Benefit] AS CHAR(1)) AS [title_xxi_full_benefit]
,CAST(c.[Legacy.MCS] AS CHAR(1)) AS [legacy_mcs]
,CAST(CASE WHEN c.[MAGI] = '--' THEN NULL ELSE c.[MAGI] END AS CHAR(1)) AS [magi]
,CAST(c.[Major.Coverage.Group.for.1519.Reporting] AS VARCHAR(255)) AS [major_cov_grp_1519_reporting]

,CAST(d.[BSP_GROUP_CID] AS INT) AS [bsp_group_cid]
,CAST(d.[BSP_GROUP_ABBREV] AS VARCHAR(255)) AS [bsp_group_abbrev]
,CAST(d.[BSP_GROUP_NAME] AS VARCHAR(255)) AS [bsp_group_name] 

,CASE WHEN CAST(d.[BSP_GROUP_CID] AS INT) IN (1003960, 1003956, 10066833, 1003962) AND a.[FUND_SOURCE_CODE] IN ('Federal', 'Title XXI') THEN 'Y'
      WHEN d.[RAC.Code.4.Bytes] IS NOT NULL AND (CAST(d.[BSP_GROUP_CID] AS INT) NOT IN (1003960, 1003956, 10066833, 1003962) OR a.[FUND_SOURCE_CODE] NOT IN ('Federal', 'Title XXI')) THEN 'N'
	  ELSE NULL
 END AS [full_benefit]
 
,CASE WHEN (c.[Title.XIX.Full.Benefit.Included.in.1519.Public.Reporting] = 'Y' OR c.[Title.XXI.Full.Benefit] = 'Y') THEN 'Y'
      WHEN (b.[RAC_CODE] IS NOT NULL AND c.[Title.XIX.Full.Benefit.Included.in.1519.Public.Reporting] IS NULL AND c.[Title.XXI.Full.Benefit] IS NULL) THEN 'N'
	  ELSE NULL
 END AS [alternate_rda_full_benefit]

FROM [tmp].[Medicaid_RAC_Codes_Fund_Source] AS a

LEFT JOIN [tmp].[Medicaid_RAC_Codes_Detailed_Codes] AS b
ON CAST(a.[RAC_CODE] AS INT) = CAST(b.[RAC_CODE] AS INT)

LEFT JOIN [tmp].[Medicaid_RAC_Codes_Grouping] AS c
ON CAST(b.[Elig.Value] AS INT) = CAST(c.[Elig.Value] AS INT)

LEFT JOIN [BSP_GROUP_CID] AS d
ON CAST(a.[RAC_CODE] AS INT) = CAST(d.[RAC.Code.4.Bytes] AS INT)

ORDER BY CAST(a.[RAC_CODE] AS INT);


-- Validation Query
SELECT 
 [rac_code]
,[fund_source_code]
,[rac_name]
,[elig_value]
,[title_xix_full_benefit_1519_reporting]
,[title_xix_limited_benefit]
,[title_xxi_full_benefit]
,[bsp_group_cid]
,[full_benefit]
,[alternate_rda_full_benefit]


FROM [PHClaims].[ref].[mcaid_rac_code]
WHERE 1 = 1
--AND [full_benefit] = 'Y' AND [alternate_rda_full_benefit] = 'Y'
--AND [full_benefit] = 'N' AND [alternate_rda_full_benefit] = 'N'
--AND ([full_benefit] = 'Y' AND [alternate_rda_full_benefit] = 'N') OR ([full_benefit] = 'N' AND [alternate_rda_full_benefit] = 'Y')
--AND [full_benefit] = 'Y' AND [alternate_rda_full_benefit] IS NULL
--AND [full_benefit] IS NULL AND [alternate_rda_full_benefit] = 'Y'
--AND [full_benefit] = 'N' AND [alternate_rda_full_benefit] IS NULL
--AND [full_benefit] IS NULL AND [alternate_rda_full_benefit] = 'N'
AND [full_benefit] IS NULL AND [alternate_rda_full_benefit] IS NULL
