
USE [PHClaims];
GO

IF OBJECT_ID('[ref].[ahrq_value_set]', 'U') IS NOT NULL
DROP TABLE [ref].[ahrq_value_set];
CREATE TABLE [ref].[ahrq_value_set]
([value_set_group] VARCHAR(20) NOT NULL
,[value_set_name] VARCHAR(100) NOT NULL
,[data_source_type] VARCHAR(50) NOT NULL
,[code_set] VARCHAR(50) NOT NULL
,[code] VARCHAR(50) NOT NULL
,[desc_1] VARCHAR(200) NULL
,CONSTRAINT [pk_ahrq_value_set] PRIMARY KEY CLUSTERED([value_set_name], [data_source_type], [code_set], [code]));
GO