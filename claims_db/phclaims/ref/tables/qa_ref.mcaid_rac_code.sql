
USE [PHClaims];
GO

-- Verify 1-1 mapping of [RAC.Code.4.Bytes] to [BSP_GROUP_CID] in source data
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

-- Verify 1-1 mapping of [BSP_GROUP_CID] to [BSP_GROUP_ABBREV] in source data
SELECT NumRows
	  ,COUNT(*)
FROM
(
SELECT [BSP_GROUP_CID], [BSP_GROUP_ABBREV]
	  ,COUNT(*) AS NumRows
FROM (SELECT DISTINCT [BSP_GROUP_CID], [BSP_GROUP_ABBREV] FROM [PHClaims].[tmp].[Medicaid_RAC_Codes_BSP_Group]) AS a
GROUP BY [BSP_GROUP_CID], [BSP_GROUP_ABBREV]
) AS SubQuery
GROUP BY NumRows
ORDER BY NumRows;

-- Verify 1-1 mapping of [BSP_GROUP_ABBREV] to [BSP_GROUP_NAME] in source data
SELECT NumRows
	  ,COUNT(*)
FROM
(
SELECT [BSP_GROUP_ABBREV], [BSP_GROUP_NAME]
	  ,COUNT(*) AS NumRows
FROM (SELECT DISTINCT [BSP_GROUP_ABBREV], [BSP_GROUP_NAME] FROM [PHClaims].[tmp].[Medicaid_RAC_Codes_BSP_Group]) AS a
GROUP BY [BSP_GROUP_ABBREV], [BSP_GROUP_NAME]
) AS SubQuery
GROUP BY NumRows
ORDER BY NumRows;

-- Verify 1-1 mapping of BSP_GROUP_CID to BSP_GROUP_ABBREV in Reference Table
SELECT DISTINCT
 [bsp_group_cid]
,[bsp_group_abbrev]
,[bsp_group_name]
FROM [ref].[mcaid_rac_code];