
USE [PHClaims];
GO

IF OBJECT_ID('[ref].[bree_value_set]') IS NOT NULL
DROP TABLE [ref].[bree_value_set];
CREATE TABLE [ref].[bree_value_set]
([value_set_group] VARCHAR(20) NOT NULL
,[value_set_name] VARCHAR(100) NOT NULL
,[data_source_type] VARCHAR(50) NOT NULL
,[code_set] VARCHAR(50) NOT NULL
,[code] VARCHAR(20) NOT NULL
,[drug_name] VARCHAR(255) NULL
,[drug_label_name] VARCHAR(255) NULL
,[drug_generic_short_name] VARCHAR(255) NULL
,[drug_route_desc] VARCHAR(255) NULL
,[cough_and_cold_flag] TINYINT NULL
,CONSTRAINT [PK_bree_value_set] PRIMARY KEY CLUSTERED([value_set_name], [data_source_type], [code_set], [code]))
ON [PRIMARY];
GO