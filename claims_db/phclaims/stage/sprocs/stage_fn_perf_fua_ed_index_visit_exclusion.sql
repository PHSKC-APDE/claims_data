
USE PHClaims;
GO

IF OBJECT_ID('[stage].[fn_perf_fua_ed_index_visit_exclusion]', 'TF') IS NOT NULL
DROP FUNCTION [stage].[fn_perf_fua_ed_index_visit_exclusion];
GO
CREATE FUNCTION [stage].[fn_perf_fua_ed_index_visit_exclusion]
(@measurement_start_date DATE
,@measurement_end_date DATE
,@age INT
,@dx_value_set_name VARCHAR(100))
RETURNS @ed_index_visit_exclusions TABLE
([id] VARCHAR(200) NULL 
,[age] INT NULL
,[tcn] VARCHAR(200) NULL
,[from_date] DATE NULL
,[to_date] DATE NULL
,[flag] INT NOT NULL
,[ed_within_30_day] INT NOT NULL
,[inpatient_within_30_day] INT NOT NULL)
 AS
BEGIN

DECLARE @pre_sorted TABLE
([id] VARCHAR(200) NULL 
,[age] INT NULL
,[tcn] VARCHAR(200) NULL
,[from_date] DATE NULL
,[to_date] DATE NULL
,[flag] INT NOT NULL
,[increment] INT NOT NULL
,[rank] INT NOT NULL
,[drop] INT NOT NULL);

DECLARE @post_sorted TABLE
([id] VARCHAR(200) NULL 
,[age] INT NULL
,[tcn] VARCHAR(200) NULL
,[from_date] DATE NULL
,[to_date] DATE NULL
,[flag] INT NOT NULL
,[increment] INT NOT NULL
,[rank] INT NOT NULL
,[drop] INT NOT NULL);

DECLARE @inpatient_within_30_day TABLE
([id] VARCHAR(200) NULL 
,[tcn] VARCHAR(200) NULL
,[from_date] DATE NULL
,[to_date] DATE NULL
,[inpatient_within_30_day] INT NOT NULL
,INDEX idx_cl_id_from_date CLUSTERED([id], [from_date]));

INSERT INTO @inpatient_within_30_day
([id]
,[tcn]
,[from_date]
,[to_date]
,[inpatient_within_30_day])

SELECT 
 hd.[id]
,hd.[tcn]
,hd.[from_date]
,hd.[to_date]
,1 AS [inpatient_within_30_day]

FROM [dbo].[mcaid_claim_header] AS hd
INNER JOIN [dbo].[mcaid_claim_line] AS ln
ON hd.[tcn] = ln.[tcn]
INNER JOIN [ref].[hedis_code_system] AS hed
ON [value_set_name] IN 
('Inpatient Stay')
AND hed.[code_system] = 'UBREV'
AND ln.[rcode] = hed.[code]
WHERE hd.[from_date] BETWEEN @measurement_start_date AND @measurement_end_date;

INSERT INTO @pre_sorted
([id]
,[age]
,[tcn]
,[from_date]
,[to_date]
,[flag]
,[increment]
,[rank]
,[drop])

SELECT 
 [id]
,[age]
,[tcn]
,[from_date]
,[to_date]
,[flag]
,0 AS [increment]
,0 AS [rank]
,0 AS [drop]
FROM [stage].[fn_perf_fua_ed_index_visit](@measurement_start_date, @measurement_end_date, @age, @dx_value_set_name);

-- While Loop  
WHILE 
(
SELECT COUNT(*)
FROM 
(
SELECT
 [increment]
,ROW_NUMBER() OVER(PARTITION BY [drop], [id], [increment] ORDER BY [from_date], [to_date], [tcn]) AS [rank]
,[drop]
FROM 
(
SELECT 
 [id]
,[tcn]
,[from_date]
,[to_date]
,CASE WHEN ROW_NUMBER() OVER(PARTITION BY [drop], [id] ORDER BY [from_date], [to_date], [tcn]) = 1 THEN 0
      WHEN DATEDIFF(DAY, LAG([from_date]) OVER(PARTITION BY [drop], [id] ORDER BY [from_date], [to_date], [tcn]), [from_date]) >= 31 THEN 0
	  WHEN DATEDIFF(DAY, LAG([from_date]) OVER(PARTITION BY [drop], [id] ORDER BY [from_date], [to_date], [tcn]), [from_date]) < 31 THEN 1
 END AS [increment]
,[rank]
,[drop]
FROM @pre_sorted
) AS [increment]
) AS [rank]
WHERE [increment] = 1
  AND [rank] = 1
  AND [drop] = 0
) > 0

BEGIN

DELETE FROM @post_sorted;

WITH [increment] AS
(
SELECT 
 [id]
,[age]
,[tcn]
,[from_date]
,[to_date]
,[flag]
/*
,ROW_NUMBER() OVER(PARTITION BY [id] ORDER BY [from_date], [to_date]) AS row_num
,DATEDIFF(DAY, LAG([from_date]) OVER(PARTITION BY [id] ORDER BY [from_date], [to_date]), [from_date]) AS date_diff
*/
,CASE WHEN ROW_NUMBER() OVER(PARTITION BY [drop], [id] ORDER BY [from_date], [to_date], [tcn]) = 1 THEN 0
      WHEN DATEDIFF(DAY, LAG([from_date]) OVER(PARTITION BY [drop], [id] ORDER BY [from_date], [to_date], [tcn]), [from_date]) >= 31 THEN 0
	  WHEN DATEDIFF(DAY, LAG([from_date]) OVER(PARTITION BY [drop], [id] ORDER BY [from_date], [to_date], [tcn]), [from_date]) < 31 THEN 1
 END AS [increment]
,[rank]
,[drop]

FROM @pre_sorted
)

INSERT INTO @post_sorted
([id]
,[age]
,[tcn]
,[from_date]
,[to_date]
,[flag]
,[increment]
,[rank]
,[drop])

SELECT 
 [id]
,[age]
,[tcn]
,[from_date]
,[to_date]
,[flag]
,[increment]
,ROW_NUMBER() OVER(PARTITION BY [drop], [id], [increment] ORDER BY [from_date], [to_date], [tcn]) AS [rank]
,CASE WHEN [increment] = 1 AND ROW_NUMBER() OVER(PARTITION BY [drop], [id], [increment] ORDER BY [from_date], [to_date], [tcn]) = 1 AND [drop] = 0 THEN 1 ELSE [drop] END AS [drop]
FROM [increment];

DELETE FROM @pre_sorted;

INSERT INTO @pre_sorted
([id]
,[age]
,[tcn]
,[from_date]
,[to_date]
,[flag]
,[increment]
,[rank]
,[drop])

SELECT 
 [id]
,[age]
,[tcn]
,[from_date]
,[to_date]
,[flag]
,[increment]
,[rank]
,[drop]
FROM @post_sorted;

END;

INSERT INTO @ed_index_visit_exclusions
([id]
,[age]
,[tcn]
,[from_date]
,[to_date]
,[flag]
,[ed_within_30_day]
,[inpatient_within_30_day])

SELECT DISTINCT
 a.[id]
,a.[age]
,a.[tcn]
,a.[from_date]
,a.[to_date]
,[flag]
,[drop] AS [ed_within_30_day]
,ISNULL([inpatient_within_30_day], 0) AS [inpatient_within_30_day]
--,0 AS [inpatient_within_30_day]

FROM @pre_sorted AS a
LEFT JOIN @inpatient_within_30_day AS b
ON a.[id] = b.[id]
AND b.[from_date] BETWEEN a.[to_date] AND DATEADD(DAY, 30, a.[to_date]) OPTION(RECOMPILE);

/*
LEFT JOIN 
(
SELECT 
 hd.[id]
,hd.[tcn]
,hd.[from_date]
,hd.[to_date]
,1 AS [inpatient_within_30_day]

FROM [dbo].[mcaid_claim_header] AS hd
INNER JOIN [dbo].[mcaid_claim_line] AS ln
ON hd.[tcn] = ln.[tcn]
INNER JOIN [ref].[hedis_code_system] AS hed
ON [value_set_name] IN 
('Inpatient Stay')
AND hed.[code_system] = 'UBREV'
AND ln.[rcode] = hed.[code]
) AS b
ON a.[id] = b.[id]
AND b.[from_date] BETWEEN a.[to_date] AND DATEADD(DAY, 30, a.[to_date])
--AND a.[drop] = 0
*/

RETURN  
END;
GO

/*
IF OBJECT_ID('tempdb..#temp', 'U') IS NOT NULL
DROP TABLE #temp;
SELECT * 
INTO #temp
FROM [stage].[fn_perf_fua_ed_index_visit_exclusion]('2017-01-01', '2017-12-31', 6, 'Mental Illness');

SELECT TOP 1000 *
FROM #temp;

SELECT
 [ed_within_30_day]
,[inpatient_within_30_day]
,COUNT(*)
FROM #temp
GROUP BY [ed_within_30_day], [inpatient_within_30_day];

IF OBJECT_ID('tempdb..#temp', 'U') IS NOT NULL
DROP TABLE #temp;
SELECT * 
INTO #temp
FROM [stage].[fn_perf_fua_ed_index_visit_exclusion]('2017-01-01', '2017-12-31', 13, 'AOD Abuse and Dependence');

SELECT TOP 1000 *
FROM #temp;

SELECT
 [ed_within_30_day]
,[inpatient_within_30_day]
,COUNT(*)
FROM #temp
GROUP BY [ed_within_30_day], [inpatient_within_30_day];
*/