
USE [PHClaims];
GO

IF OBJECT_ID('[ref].[mcaid_rac_code]') IS NOT NULL
DROP TABLE [ref].[mcaid_rac_code];
CREATE TABLE [ref].[mcaid_rac_code]
([rac_code] INT NOT NULL
,[rac_name] VARCHAR(255)
,[fund_source_code] VARCHAR(255)
,[elig_value] INT
,[sub_elig_value] INT
,[category] VARCHAR(255)
,[title_xix_full_benefit_1519_reporting] CHAR(1)
,[title_xix_limited_benefit] CHAR(1)
,[title_xxi_full_benefit] CHAR(1)
,[legacy_mcs] CHAR(1)
,[magi] CHAR(1)
,[major_cov_grp_1519_reporting] VARCHAR(255)
,[bsp_group_cid] INT
,[bsp_group_abbrev] VARCHAR(255)
,[bsp_group_name] VARCHAR(255)
,[full_benefit] CHAR(1)
,[alternate_rda_full_benefit] CHAR(1)
,CONSTRAINT [PK_ref_mcaid_rac_code] PRIMARY KEY CLUSTERED ([rac_code])
);
GO