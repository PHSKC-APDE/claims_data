
USE [PHClaims]
GO

IF OBJECT_ID('[final].[perf_measures]', 'U') IS NOT NULL
DROP TABLE [final].[perf_measures];
CREATE TABLE [final].[perf_measures]
([beg_year_month] INT NOT NULL
,[end_year_month] INT NOT NULL
,[id] VARCHAR(200) NOT NULL
,[end_month_age] INT NOT NULL
,[age_grp] VARCHAR(200) NOT NULL
,[measure_id] INT NOT NULL
,[denominator] INT NOT NULL
,[numerator] INT NOT NULL
,[load_date] DATE NOT NULL
,CONSTRAINT [PK_final_perf_measures] PRIMARY KEY CLUSTERED([measure_id], [id], [end_year_month]));
GO

/*
INSERT INTO [final].[perf_measures]
SELECT
 [beg_year_month]
,[end_year_month]
,[id]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date]
FROM [dbo].[p4p_measures];

SELECT
 [end_year_month]
,[measure_name]
,COUNT(*)
FROM [PHClaims].[final].[perf_measures] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
GROUP BY [end_year_month], [measure_name]
ORDER BY [measure_name], [end_year_month];
*/