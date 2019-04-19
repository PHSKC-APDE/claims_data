
USE PHClaims;
GO

IF OBJECT_ID('[stage].[sp_perf_measures]','P') IS NOT NULL
DROP PROCEDURE [stage].[sp_perf_measures];
GO
CREATE PROCEDURE [stage].[sp_perf_measures]
 @end_month_int INT = 201712
,@measure_name VARCHAR(200) = NULL
AS
--SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN
IF @measure_name = 'All-Cause ED Visits'
BEGIN

DELETE FROM [stage].[mcaid_perf_measure]
FROM [stage].[mcaid_perf_measure] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [end_year_month] = @end_month_int;

SET @SQL = @SQL + N'
WITH CTE AS
(
SELECT
 ym.[beg_measure_year_month] AS [beg_year_month]
,ym.[year_month] AS [end_year_month]
,den.[end_quarter]
,mem.[id]
,den.[end_month_age]
,CASE WHEN ref.[age_group] = ''age_grp_1'' THEN age.[age_grp_1]
      WHEN ref.[age_group] = ''age_grp_2'' THEN age.[age_grp_2]
      WHEN ref.[age_group] = ''age_grp_3'' THEN age.[age_grp_3]
      WHEN ref.[age_group] = ''age_grp_4'' THEN age.[age_grp_4]
      WHEN ref.[age_group] = ''age_grp_5'' THEN age.[age_grp_5]
      WHEN ref.[age_group] = ''age_grp_6'' THEN age.[age_grp_6]
      WHEN ref.[age_group] = ''age_grp_7'' THEN age.[age_grp_7]
      WHEN ref.[age_group] = ''age_grp_8'' THEN age.[age_grp_8]
 END AS [age_grp]
,ref.[measure_id]
,den.[full_criteria_t_12_m]
,den.[hospice_t_12_m]
,den.[full_criteria_t_12_m] AS [denominator]
,SUM(ISNULL(stg.[measure_value], 0)) OVER(PARTITION BY mem.[id] ORDER BY ym.[year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [numerator]

FROM [ref].[perf_year_month] AS ym

CROSS JOIN [stage].[perf_distinct_member] AS mem

LEFT JOIN [stage].[perf_enroll_denom] AS den
ON mem.[id] = den.[id]
AND ym.[year_month] = den.[year_month]
AND den.[year_month] = ' + CAST(@end_month_int AS CHAR(6)) + '

LEFT JOIN [ref].[perf_measure] AS ref
ON ref.[measure_name] = ''' + @measure_name + '''

LEFT JOIN [ref].[age_grp] AS age
ON den.[end_month_age] = age.[age]

LEFT JOIN [stage].[perf_staging] AS stg
ON mem.[id] = stg.[id]
AND ym.[year_month] = stg.[year_month]
AND ref.[measure_id] = stg.[measure_id]
AND stg.[num_denom] = ''N''

WHERE ym.[year_month] >= (SELECT [beg_measure_year_month] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
AND ym.[year_month] <= ' + CAST(@end_month_int AS CHAR(6)) + '
)

INSERT INTO [stage].[mcaid_perf_measure]
([beg_year_month]
,[end_year_month]
,[id]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 [beg_year_month]
,[end_year_month]
,[id]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM CTE

WHERE 1 = 1
AND [end_month_age] >= 0
AND [full_criteria_t_12_m] >= 7
AND [hospice_t_12_m] = 0;'
END

IF @measure_name = 'Acute Hospital Utilization'
BEGIN

DELETE FROM [stage].[mcaid_perf_measure]
FROM [stage].[mcaid_perf_measure] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [end_year_month] = @end_month_int;

SET @SQL = @SQL + N'
WITH CTE AS
(
SELECT
 ym.[beg_measure_year_month] AS [beg_year_month]
,ym.[year_month] AS [end_year_month]
,den.[end_quarter]
,mem.[id]
,den.[end_month_age]
,CASE WHEN ref.[age_group] = ''age_grp_1'' THEN age.[age_grp_1]
      WHEN ref.[age_group] = ''age_grp_2'' THEN age.[age_grp_2]
      WHEN ref.[age_group] = ''age_grp_3'' THEN age.[age_grp_3]
      WHEN ref.[age_group] = ''age_grp_4'' THEN age.[age_grp_4]
      WHEN ref.[age_group] = ''age_grp_5'' THEN age.[age_grp_5]
      WHEN ref.[age_group] = ''age_grp_6'' THEN age.[age_grp_6]
      WHEN ref.[age_group] = ''age_grp_7'' THEN age.[age_grp_7]
      WHEN ref.[age_group] = ''age_grp_8'' THEN age.[age_grp_8]
 END AS [age_grp]
,ref.[measure_id]
,den.[full_criteria_t_12_m]
,den.[hospice_t_12_m]
,1 AS [denominator]
,SUM(ISNULL(stg.[measure_value], 0)) OVER(PARTITION BY mem.[id] ORDER BY ym.[year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [numerator]
,CASE WHEN SUM(ISNULL(stg.[measure_value], 0)) OVER(PARTITION BY mem.[id] ORDER BY ym.[year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) >= 3 THEN 1 ELSE 0 END AS [outlier]

FROM [ref].[perf_year_month] AS ym

CROSS JOIN [stage].[perf_distinct_member] AS mem

LEFT JOIN [stage].[perf_enroll_denom] AS den
ON mem.[id] = den.[id]
AND ym.[year_month] = den.[year_month]
AND den.[year_month] = ' + CAST(@end_month_int AS CHAR(6)) + '

LEFT JOIN [ref].[perf_measure] AS ref
ON ref.[measure_name] = ''' + @measure_name + '''

LEFT JOIN [ref].[age_grp] AS age
ON den.[end_month_age] = age.[age]

LEFT JOIN [stage].[perf_staging] AS stg
ON mem.[id] = stg.[id]
AND ym.[year_month] = stg.[year_month]
AND ref.[measure_id] = stg.[measure_id]
AND stg.[num_denom] = ''N''

WHERE ym.[year_month] >= (SELECT [beg_measure_year_month] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
AND ym.[year_month] <= ' + CAST(@end_month_int AS CHAR(6)) + '
)

INSERT INTO [stage].[mcaid_perf_measure]
([beg_year_month]
,[end_year_month]
,[id]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 [beg_year_month]
,[end_year_month]
,[id]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM CTE

WHERE 1 = 1
AND [end_month_age] >= 18
AND [full_criteria_t_12_m] >= 11
AND [hospice_t_12_m] = 0
AND [outlier] = 0;'
END

IF @measure_name = 'Follow-up ED visit for Alcohol/Drug Abuse'
BEGIN

DELETE FROM [stage].[mcaid_perf_measure]
FROM [stage].[mcaid_perf_measure] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] LIKE @measure_name + '%'
AND [end_year_month] = @end_month_int;

SET @SQL = @SQL + N'

WITH CTE AS
(
SELECT
 ym.[beg_measure_year_month] AS [beg_year_month]
,stg.[year_month] AS [end_year_month]
,den.[end_quarter]
,stg.[id]
,den.[end_month_age]
,CASE WHEN ref.[age_group] = ''age_grp_1'' THEN age.[age_grp_1]
      WHEN ref.[age_group] = ''age_grp_2'' THEN age.[age_grp_2]
      WHEN ref.[age_group] = ''age_grp_3'' THEN age.[age_grp_3]
      WHEN ref.[age_group] = ''age_grp_4'' THEN age.[age_grp_4]
      WHEN ref.[age_group] = ''age_grp_5'' THEN age.[age_grp_5]
      WHEN ref.[age_group] = ''age_grp_6'' THEN age.[age_grp_6]
      WHEN ref.[age_group] = ''age_grp_7'' THEN age.[age_grp_7]
      WHEN ref.[age_group] = ''age_grp_8'' THEN age.[age_grp_8]
      WHEN ref.[age_group] = ''age_grp_9_months'' THEN age.[age_grp_9_months]
 END AS [age_grp]
,ref.[measure_id]
,den.[full_criteria]
,den.[hospice]
/*
Members need coverage in month following index event.
*/
,[full_criteria_p_2_m]
,[hospice_p_2_m]

,stg.[event_date]
/*
If index visit occurs on 1st of month, then 31-day follow-up period contained
within calendar month.
Then, [full_criteria_p_2_m], [hospice_p_2_m] are not used
*/
,CASE WHEN DAY(stg.[event_date]) = 1 AND MONTH([event_date]) IN (1, 3, 5, 7, 8, 10, 12)
THEN 1 ELSE 0 END AS [need_1_month_coverage]

,stg.[denominator]
,stg.[numerator]

FROM [stage].[perf_staging_event_date] AS stg

INNER JOIN [ref].[perf_measure] AS ref
ON stg.[measure_id] = ref.[measure_id]
AND ref.[measure_name] LIKE ''' + CAST(@measure_name AS VARCHAR(200)) + '%' + '''

INNER JOIN [ref].[perf_year_month] AS ym
ON stg.[year_month] = ym.[year_month]

LEFT JOIN [stage].[perf_enroll_denom] AS den
ON stg.[id] = den.[id]
AND stg.[year_month] = den.[year_month]

/*
Join age_grp columns here, use CASE above to select age_grp_x from ref.perf_measure
*/
LEFT JOIN [ref].[age_grp] AS age
ON den.[end_month_age] = age.[age]

WHERE stg.[event_date] >= (SELECT [12_month_prior] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
/*
Cut off index visits during last 31-day period
because of insufficient follow-up period
*/
AND stg.[event_date] <= (SELECT DATEADD(DAY, -30, [end_month]) FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
)

INSERT INTO [stage].[mcaid_perf_measure]
([beg_year_month]
,[end_year_month]
,[id]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 (SELECT [beg_measure_year_month] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ') AS [beg_year_month]
,' + CAST(@end_month_int AS CHAR(6)) + ' AS [end_year_month]
,[id]
,[end_month_age]
,[age_grp]
,[measure_id]
,SUM([denominator]) AS [denominator]
,SUM([numerator]) AS [numerator]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM [CTE]

WHERE 1 = 1
AND [end_month_age] >= 13
AND [full_criteria] = 1
AND [hospice] = 0
AND (([need_1_month_coverage] = 1) OR ([full_criteria_p_2_m] = 2 AND [hospice_p_2_m] = 0))

GROUP BY 
 [id]
,[end_month_age]
,[age_grp]
,[measure_id];'
END

IF @measure_name = 'Follow-up ED visit for Mental Illness'
BEGIN

DELETE FROM [stage].[mcaid_perf_measure]
FROM [stage].[mcaid_perf_measure] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] LIKE @measure_name + '%'
AND [end_year_month] = @end_month_int;

SET @SQL = @SQL + N'

WITH CTE AS
(
SELECT
 ym.[beg_measure_year_month] AS [beg_year_month]
,stg.[year_month] AS [end_year_month]
,den.[end_quarter]
,stg.[id]
,den.[end_month_age]
,CASE WHEN ref.[age_group] = ''age_grp_1'' THEN age.[age_grp_1]
      WHEN ref.[age_group] = ''age_grp_2'' THEN age.[age_grp_2]
      WHEN ref.[age_group] = ''age_grp_3'' THEN age.[age_grp_3]
      WHEN ref.[age_group] = ''age_grp_4'' THEN age.[age_grp_4]
      WHEN ref.[age_group] = ''age_grp_5'' THEN age.[age_grp_5]
      WHEN ref.[age_group] = ''age_grp_6'' THEN age.[age_grp_6]
      WHEN ref.[age_group] = ''age_grp_7'' THEN age.[age_grp_7]
      WHEN ref.[age_group] = ''age_grp_8'' THEN age.[age_grp_8]
      WHEN ref.[age_group] = ''age_grp_9_months'' THEN age.[age_grp_9_months]
 END AS [age_grp]
,ref.[measure_id]
,den.[full_criteria]
,den.[hospice]
/*
Members need coverage in month following index event.
*/
,[full_criteria_p_2_m]
,[hospice_p_2_m]

,stg.[event_date]
/*
If index visit occurs on 1st of month, then 31-day follow-up period contained
within calendar month.
Then, [full_criteria_p_2_m], [hospice_p_2_m] are not used
*/
,CASE WHEN DAY(stg.[event_date]) = 1 AND MONTH([event_date]) IN (1, 3, 5, 7, 8, 10, 12)
THEN 1 ELSE 0 END AS [need_1_month_coverage]

,stg.[denominator]
,stg.[numerator]

FROM [stage].[perf_staging_event_date] AS stg

INNER JOIN [ref].[perf_measure] AS ref
ON stg.[measure_id] = ref.[measure_id]
AND ref.[measure_name] LIKE ''' + CAST(@measure_name AS VARCHAR(200)) + '%' + '''

INNER JOIN [ref].[perf_year_month] AS ym
ON stg.[year_month] = ym.[year_month]

LEFT JOIN [stage].[perf_enroll_denom] AS den
ON stg.[id] = den.[id]
AND stg.[year_month] = den.[year_month]

/*
Join age_grp columns here, use CASE above to select age_grp_x from ref.perf_measure
*/
LEFT JOIN [ref].[age_grp] AS age
ON den.[end_month_age] = age.[age]

WHERE stg.[event_date] >= (SELECT [12_month_prior] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
/*
Cut off index visits during last 31-day period
because of insufficient follow-up period
*/
AND stg.[event_date] <= (SELECT DATEADD(DAY, -30, [end_month]) FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
)

INSERT INTO [stage].[mcaid_perf_measure]
([beg_year_month]
,[end_year_month]
,[id]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 (SELECT [beg_measure_year_month] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ') AS [beg_year_month]
,' + CAST(@end_month_int AS CHAR(6)) + ' AS [end_year_month]
,[id]
,[end_month_age]
,[age_grp]
,[measure_id]
,SUM([denominator]) AS [denominator]
,SUM([numerator]) AS [numerator]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM [CTE]

WHERE 1 = 1
AND [end_month_age] >= 6
AND [full_criteria] = 1
AND [hospice] = 0
AND (([need_1_month_coverage] = 1) OR ([full_criteria_p_2_m] = 2 AND [hospice_p_2_m] = 0))

GROUP BY 
 [id]
,[end_month_age]
,[age_grp]
,[measure_id];'
END

IF @measure_name = 'Child and Adolescent Access to Primary Care'
BEGIN

DELETE FROM [stage].[mcaid_perf_measure]
FROM [stage].[mcaid_perf_measure] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [end_year_month] = @end_month_int;

SET @SQL = @SQL + N'

WITH CTE AS
(
SELECT
 ym.[beg_measure_year_month] AS [beg_year_month]
,ym.[year_month] AS [end_year_month]
,den.[end_quarter]
,mem.[id]
,den.[end_month_age]
,den.[age_in_months]
,CASE WHEN ref.[age_group] = ''age_grp_1'' THEN age.[age_grp_1]
      WHEN ref.[age_group] = ''age_grp_2'' THEN age.[age_grp_2]
      WHEN ref.[age_group] = ''age_grp_3'' THEN age.[age_grp_3]
      WHEN ref.[age_group] = ''age_grp_4'' THEN age.[age_grp_4]
      WHEN ref.[age_group] = ''age_grp_5'' THEN age.[age_grp_5]
      WHEN ref.[age_group] = ''age_grp_6'' THEN age.[age_grp_6]
      WHEN ref.[age_group] = ''age_grp_7'' THEN age.[age_grp_7]
      WHEN ref.[age_group] = ''age_grp_8'' THEN age.[age_grp_8]
      WHEN ref.[age_group] = ''age_grp_9_months'' THEN age.[age_grp_9_months]
 END AS [age_grp]
,ref.[measure_id]
,den.[full_criteria_t_12_m]
,den.[full_criteria_prior_t_12_m]
,den.[hospice_t_12_m]
,den.[hospice_prior_t_12_m]
,1 AS [denominator]
,CASE WHEN MAX(ISNULL(stg.[measure_value], 0)) OVER(PARTITION BY mem.[id] ORDER BY ym.[year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) > 0 THEN 1 ELSE 0 END AS [numerator_t_12_m]
,CASE WHEN MAX(ISNULL(stg.[measure_value], 0)) OVER(PARTITION BY mem.[id] ORDER BY ym.[year_month] ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) > 0 THEN 1 ELSE 0 END AS [numerator_t_24_m]

FROM [ref].[perf_year_month] AS ym

CROSS JOIN [stage].[perf_distinct_member] AS mem

LEFT JOIN [stage].[perf_enroll_denom] AS den
ON mem.[id] = den.[id]
AND ym.[year_month] = den.[year_month]
AND den.[year_month] = ' + CAST(@end_month_int AS CHAR(6)) + '

LEFT JOIN [ref].[perf_measure] AS ref
ON ref.[measure_name] = ''' + @measure_name + '''

LEFT JOIN [ref].[age_grp] AS age
ON den.[age_in_months] = age.[age]

LEFT JOIN [stage].[perf_staging] AS stg
ON mem.[id] = stg.[id]
AND ym.[year_month] = stg.[year_month]
AND ref.[measure_id] = stg.[measure_id]
AND stg.[num_denom] = ''N''

WHERE ym.[year_month] >= (SELECT [beg_measure_year_month] - 100 FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
AND ym.[year_month] <= ' + CAST(@end_month_int AS CHAR(6)) + '
)

INSERT INTO [stage].[mcaid_perf_measure]
([beg_year_month]
,[end_year_month]
,[id]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 [beg_year_month]
,[end_year_month]
,[id]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,CASE WHEN [age_grp] IN (''Age 12-24 Months'', ''Age 25 Months-6'') THEN [numerator_t_12_m] WHEN [age_grp] IN (''Age 7-11'', ''Age 12-19'') THEN [numerator_t_24_m] END AS [numerator]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM CTE

WHERE 1 = 1
AND [age_in_months] >= 12
AND [end_month_age] <= 19
AND [full_criteria_t_12_m] >= 11
AND [hospice_t_12_m] = 0
AND CASE WHEN [age_grp] IN (''Age 7-11'', ''Age 12-19'') THEN [full_criteria_prior_t_12_m] ELSE 11 END >= 11
AND CASE WHEN [age_grp] IN (''Age 7-11'', ''Age 12-19'') THEN [hospice_prior_t_12_m] ELSE 0 END = 0;'
END

PRINT @SQL;
END
EXEC sp_executeSQL @statement=@SQL, 
                   @params=N'@end_month_int INT, @measure_name VARCHAR(200)',
				   @end_month_int=@end_month_int, @measure_name=@measure_name;
GO