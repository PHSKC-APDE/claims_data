
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
