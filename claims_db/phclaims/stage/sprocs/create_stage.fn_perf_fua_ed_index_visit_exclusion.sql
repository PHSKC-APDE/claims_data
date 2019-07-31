
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
([id_mcaid] VARCHAR(255) NULL 
,[age] INT NULL
,[claim_header_id] BIGINT NULL
,[first_service_date] DATE NULL
,[last_service_date] DATE NULL
,[flag] INT NOT NULL
,[ed_within_30_day] INT NOT NULL
,[inpatient_within_30_day] INT NOT NULL)
 AS
BEGIN

DECLARE @pre_sorted TABLE
([id_mcaid] VARCHAR(255) NULL 
,[age] INT NULL
,[claim_header_id] BIGINT NULL
,[first_service_date] DATE NULL
,[last_service_date] DATE NULL
,[flag] INT NOT NULL
,[increment] INT NOT NULL
,[rank] INT NOT NULL
,[drop] INT NOT NULL);

DECLARE @post_sorted TABLE
([id_mcaid] VARCHAR(255) NULL 
,[age] INT NULL
,[claim_header_id] BIGINT NULL
,[first_service_date] DATE NULL
,[last_service_date] DATE NULL
,[flag] INT NOT NULL
,[increment] INT NOT NULL
,[rank] INT NOT NULL
,[drop] INT NOT NULL);

DECLARE @inpatient_within_30_day TABLE
([id_mcaid] VARCHAR(255) NULL 
,[claim_header_id] BIGINT NULL
,[first_service_date] DATE NULL
,[last_service_date] DATE NULL
,[inpatient_within_30_day] INT NOT NULL
,INDEX idx_cl_id_mcaid_first_service_date CLUSTERED([id_mcaid], [first_service_date]));

INSERT INTO @inpatient_within_30_day
([id_mcaid]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[inpatient_within_30_day])

SELECT 
 ln.[id_mcaid]
,ln.[claim_header_id]
,ln.[first_service_date]
,ln.[last_service_date]
,1 AS [inpatient_within_30_day]

FROM [final].[mcaid_claim_line] AS ln
INNER JOIN [archive].[hedis_code_system] AS hed
ON [value_set_name] IN 
('Inpatient Stay')
AND hed.[code_system] = 'UBREV'
AND ln.[rev_code] = hed.[code]
WHERE ln.[first_service_date] BETWEEN @measurement_start_date AND @measurement_end_date;

INSERT INTO @pre_sorted
([id_mcaid]
,[age]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[flag]
,[increment]
,[rank]
,[drop])

SELECT 
 [id_mcaid]
,[age]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
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
,ROW_NUMBER() OVER(PARTITION BY [drop], [id_mcaid], [increment] ORDER BY [first_service_date], [last_service_date], [claim_header_id]) AS [rank]
,[drop]
FROM 
(
SELECT 
 [id_mcaid]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,CASE WHEN ROW_NUMBER() OVER(PARTITION BY [drop], [id_mcaid] ORDER BY [first_service_date], [last_service_date], [claim_header_id]) = 1 THEN 0
      WHEN DATEDIFF(DAY, LAG([first_service_date]) OVER(PARTITION BY [drop], [id_mcaid] ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) >= 31 THEN 0
	  WHEN DATEDIFF(DAY, LAG([first_service_date]) OVER(PARTITION BY [drop], [id_mcaid] ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) < 31 THEN 1
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
 [id_mcaid]
,[age]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[flag]
/*
,ROW_NUMBER() OVER(PARTITION BY [id_mcaid] ORDER BY [first_service_date], [last_service_date]) AS row_num
,DATEDIFF(DAY, LAG([first_service_date]) OVER(PARTITION BY [id_mcaid] ORDER BY [first_service_date], [last_service_date]), [first_service_date]) AS date_diff
*/
,CASE WHEN ROW_NUMBER() OVER(PARTITION BY [drop], [id_mcaid] ORDER BY [first_service_date], [last_service_date], [claim_header_id]) = 1 THEN 0
      WHEN DATEDIFF(DAY, LAG([first_service_date]) OVER(PARTITION BY [drop], [id_mcaid] ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) >= 31 THEN 0
	  WHEN DATEDIFF(DAY, LAG([first_service_date]) OVER(PARTITION BY [drop], [id_mcaid] ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) < 31 THEN 1
 END AS [increment]
,[rank]
,[drop]

FROM @pre_sorted
)

INSERT INTO @post_sorted
([id_mcaid]
,[age]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[flag]
,[increment]
,[rank]
,[drop])

SELECT 
 [id_mcaid]
,[age]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[flag]
,[increment]
,ROW_NUMBER() OVER(PARTITION BY [drop], [id_mcaid], [increment] ORDER BY [first_service_date], [last_service_date], [claim_header_id]) AS [rank]
,CASE WHEN [increment] = 1 AND ROW_NUMBER() OVER(PARTITION BY [drop], [id_mcaid], [increment] ORDER BY [first_service_date], [last_service_date], [claim_header_id]) = 1 AND [drop] = 0 THEN 1 ELSE [drop] END AS [drop]
FROM [increment];

DELETE FROM @pre_sorted;

INSERT INTO @pre_sorted
([id_mcaid]
,[age]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[flag]
,[increment]
,[rank]
,[drop])

SELECT 
 [id_mcaid]
,[age]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[flag]
,[increment]
,[rank]
,[drop]
FROM @post_sorted;

END;

INSERT INTO @ed_index_visit_exclusions
([id_mcaid]
,[age]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[flag]
,[ed_within_30_day]
,[inpatient_within_30_day])

SELECT DISTINCT
 a.[id_mcaid]
,a.[age]
,a.[claim_header_id]
,a.[first_service_date]
,a.[last_service_date]
,[flag]
,[drop] AS [ed_within_30_day]
,ISNULL([inpatient_within_30_day], 0) AS [inpatient_within_30_day]
--,0 AS [inpatient_within_30_day]

FROM @pre_sorted AS a
LEFT JOIN @inpatient_within_30_day AS b
ON a.[id_mcaid] = b.[id_mcaid]
AND b.[first_service_date] BETWEEN a.[last_service_date] AND DATEADD(DAY, 30, a.[last_service_date]) OPTION(RECOMPILE);

/*
LEFT JOIN 
(
SELECT 
 ln.[id_mcaid]
,ln.[claim_header_id]
,ln.[first_service_date]
,ln.[last_service_date]
,1 AS [inpatient_within_30_day]

FROM [final].[mcaid_claim_line] AS ln
INNER JOIN [archive].[hedis_code_system] AS hed
ON [value_set_name] IN 
('Inpatient Stay')
AND hed.[code_system] = 'UBREV'
AND ln.[rev_code] = hed.[code]
) AS b
ON a.[id_mcaid] = b.[id_mcaid]
AND b.[first_service_date] BETWEEN a.[last_service_date] AND DATEADD(DAY, 30, a.[last_service_date])
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
FROM [stage].[fn_perf_fua_ed_index_visit_exclusion]('2018-01-01', '2018-12-31', 6, 'Mental Illness');

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
FROM [stage].[fn_perf_fua_ed_index_visit_exclusion]('2018-01-01', '2018-12-31', 13, 'AOD Abuse and Dependence');

SELECT TOP 1000 *
FROM #temp;

SELECT
 [ed_within_30_day]
,[inpatient_within_30_day]
,COUNT(*)
FROM #temp
GROUP BY [ed_within_30_day], [inpatient_within_30_day];
*/