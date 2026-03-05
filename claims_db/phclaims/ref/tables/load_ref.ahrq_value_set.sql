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

USE [PHClaims];
GO

TRUNCATE TABLE [ref].[ahrq_value_set];

INSERT INTO [ref].[ahrq_value_set]
([value_set_group]
,[value_set_name]
,[data_source_type]
,[code_set]
,[code]
,[desc_1])
SELECT
 [value_set_group]
,[value_set_name]
,[data_source_type]
,[code_set]
,SUBSTRING([code], 13, LEN([code]) - 12) AS [code]
,[desc_1]
FROM [tmp].[ref.ahrq_value_set.xlsx]
ORDER BY 
 [value_set_name]
,[data_source_type]
,[code_set]
,[code];

IF OBJECT_ID('[tmp].[ref.ahrq_value_set.xlsx]') IS NOT NULL
DROP TABLE [tmp].[ref.ahrq_value_set.xlsx];
