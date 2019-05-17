
USE [PHClaims];
GO

IF OBJECT_ID('[ref].[mcaid_rac_code]') IS NOT NULL
DROP TABLE [ref].[mcaid_rac_code];
CREATE TABLE [ref].[mcaid_rac_code]
([rac_code] INT NOT NULL
,[rac_name] VARCHAR(200)
,[fund_source_code] VARCHAR(200)
,[elig_value] INT
,[sub_elig_value] INT
,[category] VARCHAR(100)
,[title_xix_full_benefit_1519_reporting] CHAR(1)
,[title_xix_limited_benefit] CHAR(1)
,[title_xxi_full_benefit] CHAR(1)
,[legacy_mcs] CHAR(1)
,[magi] CHAR(1)
,[major_cov_grp_1519_reporting] VARCHAR(50)
,[bsp_group_cid_1003960] CHAR(1)
,[bsp_group_cid_1003956] CHAR(1)
,[bsp_group_cid_10066833] CHAR(1)
,[bsp_group_cid_1003962] CHAR(1)
,[full_benefit_flag] CHAR(1)
,CONSTRAINT [PK_ref_mcaid_rac_code] PRIMARY KEY CLUSTERED ([rac_code])
);
GO