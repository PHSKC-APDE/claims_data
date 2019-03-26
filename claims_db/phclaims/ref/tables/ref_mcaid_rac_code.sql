
USE [PHClaims];
GO

/*
IF OBJECT_ID('[ref].[mcaid_rac_code]') IS NOT NULL
DROP TABLE [ref].[mcaid_rac_code];
SELECT [rac_code]
      ,[rac_name]
      ,[elig_value]
      ,[sub_elig_value]
      ,[category]
      ,[title_xix_full_benefit_1519_reporting]
      ,[title_xix_limited_benefit]
      ,[title_xxi_full_benefit]
      ,[legacy_mcs]
      ,[magi]
      ,[major_cov_grp_1519_reporting]
      ,[full_benefit_flag]
INTO [ref].[mcaid_rac_code]
FROM [PHClaims].[dbo].[ref_rac_code];

ALTER TABLE [ref].[mcaid_rac_code] ADD CONSTRAINT PK_ref_mcaid_rac_code PRIMARY KEY([rac_code]);
GO
*/

IF OBJECT_ID('[ref].[mcaid_rac_code]') IS NOT NULL
DROP TABLE [ref].[mcaid_rac_code];
CREATE TABLE [ref].[mcaid_rac_code]
([rac_code] INT NOT NULL
,[rac_name] VARCHAR(200)
,[elig_value] INT
,[sub_elig_value] INT
,[category] VARCHAR(100)
,[title_xix_full_benefit_1519_reporting] VARCHAR(1)
,[title_xix_limited_benefit] VARCHAR(1)
,[title_xxi_full_benefit] VARCHAR(1)
,[legacy_mcs] VARCHAR(1)
,[magi] VARCHAR(1)
,[major_cov_grp_1519_reporting] VARCHAR(50)
,[full_benefit_flag] VARCHAR(1)
,CONSTRAINT [PK_ref_mcaid_rac_code] PRIMARY KEY CLUSTERED ([rac_code])
);
GO

INSERT INTO [ref].[mcaid_rac_code]
([rac_code]
,[rac_name]
,[elig_value]
,[sub_elig_value]
,[category]
,[title_xix_full_benefit_1519_reporting]
,[title_xix_limited_benefit]
,[title_xxi_full_benefit]
,[legacy_mcs]
,[magi]
,[major_cov_grp_1519_reporting]
,[full_benefit_flag])

SELECT [RAC_CODE] AS [rac_code]
      ,[RAC_NAME] AS [rac_name]
      ,a.[Elig.Value] AS [elig_value]
      ,[Subelig.Value] AS [sub_elig_value]

      ,CASE WHEN a.[Elig.Value] = 14 THEN 'MN – Other (Family/Pregnancy)' ELSE [Category] END AS [category] 
      ,[Title.XIX.Full.Benefit.Included.in.1519.Public.Reporting] AS [title_xix_full_benefit_1519_reporting]
      ,[Title.XIX.Limited.Benefit] AS [title_xix_limited_benefit]
      ,[Title.XXI.Full.Benefit] AS [title_xxi_full_benefit]
      ,[Legacy.MCS] AS [legacy_mcs]
      ,CASE WHEN MAGI = '--' THEN NULL ELSE [MAGI] END AS [magi]
      ,[Major.Coverage.Group.for.1519.Reporting] AS [major_cov_grp_1519_reporting]
	  ,CASE WHEN ([Title.XIX.Full.Benefit.Included.in.1519.Public.Reporting] = 'Y' OR [Title.XXI.Full.Benefit] = 'Y')
	        THEN 'Y'
			ELSE 'N'
	   END AS [full_benefit_flag]

FROM [KC\psylling].[tmp_Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS-2] AS a
LEFT JOIN [KC\psylling].[tmp_Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS-1] AS b
ON a.[Elig.Value] = b.[Elig.Value];

DROP TABLE [KC\psylling].[tmp_Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS-2];
DROP TABLE [KC\psylling].[tmp_Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS-1];