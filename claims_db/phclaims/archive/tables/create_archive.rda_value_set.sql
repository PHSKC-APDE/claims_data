
USE [PHClaims];
GO

IF OBJECT_ID('[archive].[rda_value_set]', 'U') IS NOT NULL
DROP TABLE [archive].[rda_value_set];
CREATE TABLE [archive].[rda_value_set]
([value_set_group] VARCHAR(20) NOT NULL
,[value_set_name] VARCHAR(100) NOT NULL
,[data_source_type] VARCHAR(50) NOT NULL
,[sub_group] VARCHAR(50) NOT NULL
,[code_set] VARCHAR(50) NOT NULL
,[code] VARCHAR(20) NOT NULL
,[desc_1] VARCHAR(200) NULL
,[desc_2] VARCHAR(200) NULL
,[active] CHAR(1) NULL
,[from_date] DATE NULL
,[to_date] DATE NULL
,CONSTRAINT [pk_rda_value_set] PRIMARY KEY CLUSTERED([value_set_name], [data_source_type], [sub_group], [code_set], [code]));
GO

INSERT INTO [archive].[rda_value_set]
SELECT * FROM [ref].[rda_value_set] 
ORDER BY [value_set_name], [data_source_type], [sub_group], [code_set], [code];