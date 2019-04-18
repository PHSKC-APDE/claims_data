
USE [PHClaims]
GO

IF OBJECT_ID('[final].[perf_measure]', 'U') IS NOT NULL
DROP TABLE [final].[perf_measure];
CREATE TABLE [final].[perf_measure]
([beg_year_month] INT NOT NULL
,[end_year_month] INT NOT NULL
,[id] VARCHAR(200) NOT NULL
,[end_month_age] INT NOT NULL
,[age_grp] VARCHAR(200) NOT NULL
,[measure_id] INT NOT NULL
,[denominator] INT NOT NULL
,[numerator] INT NOT NULL
,[load_date] DATE NOT NULL
,CONSTRAINT [PK_final_perf_measure] PRIMARY KEY CLUSTERED([measure_id], [id], [end_year_month]));
GO