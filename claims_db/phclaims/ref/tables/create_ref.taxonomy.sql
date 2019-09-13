
USE [PHClaims];
GO

/*
-- Version 1
IF OBJECT_ID('[ref].[mcaid_prvdr_txnmy]', 'U') IS NOT NULL
DROP TABLE [ref].[mcaid_prvdr_txnmy];
CREATE TABLE [ref].[mcaid_prvdr_txnmy]
([taxonomy_code] VARCHAR(200) NOT NULL
,[desc] VARCHAR(1000) NULL
,[cms_taxonomy_group] VARCHAR(200) NULL
,[cms_specialty_code] VARCHAR(200) NULL
,CONSTRAINT [PK_ref_mcaid_prvdr_txnmy] PRIMARY KEY CLUSTERED([taxonomy_code]));
GO
*/

IF OBJECT_ID('[ref].[taxonomy]', 'U') IS NOT NULL
DROP TABLE [ref].[taxonomy];
CREATE TABLE [ref].[taxonomy]
([taxonomy_code] VARCHAR(255) NOT NULL
,[specialty_desc] VARCHAR(255) NULL
,[taxonomy_general_type] VARCHAR(255) NULL
,[taxonomy_provider_type] VARCHAR(255) NULL
,[taxonomy_specialization_area] VARCHAR(255) NULL
,CONSTRAINT [PK_taxonomy] PRIMARY KEY CLUSTERED([taxonomy_code]));
GO