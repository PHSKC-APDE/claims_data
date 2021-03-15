
--USE [PHClaims];
--USE [DCHS_Analytics];
GO

IF OBJECT_ID('[ref].[perf_year_month]', 'U') IS NOT NULL
DROP TABLE [ref].[perf_year_month];

SELECT [n] * 100 + [month] AS [year_month]
      ,[n] AS [year]
      ,[month]
	  ,[month_name]
	  ,CAST([n] AS VARCHAR(4)) + '-' + [month_name] AS [year_month_name]
	  ,DATEFROMPARTS([n], [month], 1) AS [beg_month]
	  ,EOMONTH(DATEFROMPARTS([n], [month], 1)) AS [end_month]
	  ,DATEADD(MONTH, -11, DATEFROMPARTS([n], [month], 1)) AS [12_month_prior]
	  ,DATEADD(MONTH, -23, DATEFROMPARTS([n], [month], 1)) AS [24_month_prior]
	  ,CAST(YEAR(DATEADD(MONTH, -11, DATEFROMPARTS([n], [month], 1))) AS INT) * 100 + CAST(MONTH(DATEADD(MONTH, -11, DATEFROMPARTS([n], [month], 1))) AS INT) AS [beg_measure_year_month]
	  ,CAST(YEAR(DATEADD(MONTH, -1, DATEFROMPARTS([n], [month], 1))) AS INT) * 100 + CAST(MONTH(DATEADD(MONTH, -1, DATEFROMPARTS([n], [month], 1))) AS INT) AS [lag_year_month]
	  ,CAST(YEAR(DATEADD(MONTH, 1, DATEFROMPARTS([n], [month], 1))) AS INT) * 100 + CAST(MONTH(DATEADD(MONTH, 1, DATEFROMPARTS([n], [month], 1))) AS INT) AS [lead_year_month]
INTO [ref].[perf_year_month]
FROM 
(
SELECT [n] FROM [ref].[num] WHERE n BETWEEN 2010 AND 2030
) AS a
CROSS JOIN 
(
VALUES(1, 'JAN'), (2, 'FEB'), (3, 'MAR'), (4, 'APR'), (5, 'MAY'), (6, 'JUN'), (7, 'JUL'), (8, 'AUG'), (9, 'SEP'), (10, 'OCT'), (11, 'NOV'), (12, 'DEC')
) AS b([month], [month_name])
ORDER BY [end_month];

ALTER TABLE [ref].[perf_year_month] ALTER COLUMN [year_month] INT NOT NULL;
GO
ALTER TABLE [ref].[perf_year_month] ADD CONSTRAINT PK_ref_perf_year_month PRIMARY KEY([year_month]);
GO
CREATE UNIQUE NONCLUSTERED INDEX [idx_nc_ref_perf_year_month_beg_month_end_month] 
ON [ref].[perf_year_month]([beg_month], [end_month]);
GO

SELECT * FROM [ref].[perf_year_month];