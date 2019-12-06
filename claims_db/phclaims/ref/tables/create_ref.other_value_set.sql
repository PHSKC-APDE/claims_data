
USE [PHClaims];
GO

IF OBJECT_ID('[ref].[other_value_set]', 'U') IS NOT NULL
DROP TABLE [ref].[other_value_set];
CREATE TABLE [ref].[other_value_set]
([value_set_group] VARCHAR(200) NOT NULL
,[value_set_name] VARCHAR(200) NOT NULL
,[code_set] VARCHAR(50) NOT NULL
,[code] VARCHAR(20) NOT NULL
,[desc_1] VARCHAR(200) NULL
,CONSTRAINT [pk_other_value_set] PRIMARY KEY CLUSTERED([value_set_name], [code_set], [code]));
GO