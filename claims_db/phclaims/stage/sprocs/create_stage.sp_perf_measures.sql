
/*
This procedure calculates each performance measure for a @measure_name and for 
a measurement year ending @end_month_int.

The structure is 
IF @measure_name = 'All-Cause ED Visits', run a SQL batch
IF @measure_name = 'Acute Hospital Utilization', run a SQL batch

Calls on:
[ref].[perf_measure], table of measure characteristics
[ref].[perf_year_month], month-by-month table of dates
[stage].[perf_distinct_member], one row per distinct person
[stage].[mcaid_perf_enroll_denom], eligibility criteria per distinct person per month
[stage].[perf_staging], utilization activity pre-aggregated to person-month level
[stage].[perf_staging_event_date], utilization activity pre-aggregated to person-date level

Loads to:
[stage].[mcaid_perf_measure]

Run for one measure at a time, one measurement period at a time
(e.g., this creates Mental Health Treatment Penetration measure 
for 201610-201709 (2016-10-01-2017-09-30) period.

EXEC [stage].[sp_perf_measures]
 @end_month_int = 201709
--,@measure_name = 'All-Cause ED Visits';
--,@measure_name = 'Acute Hospital Utilization';
--,@measure_name = 'Follow-up ED visit for Alcohol/Drug Abuse';
--,@measure_name = 'Follow-up ED visit for Mental Illness';
,@measure_name = 'Mental Health Treatment Penetration';
--,@measure_name = 'Child and Adolescent Access to Primary Care';

Author: Philip Sylling
Modified: 2019-07-19: Modified to utilize new analytic tables
*/

USE [PHClaims];
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
DECLARE @end_month_date DATE;

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
,mem.[id_mcaid]
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
,SUM(ISNULL(stg.[measure_value], 0)) OVER(PARTITION BY mem.[id_mcaid] ORDER BY ym.[year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [numerator]

FROM [ref].[perf_year_month] AS ym

CROSS JOIN [stage].[perf_distinct_member] AS mem

LEFT JOIN [stage].[mcaid_perf_enroll_denom] AS den
ON mem.[id_mcaid] = den.[id_mcaid]
AND ym.[year_month] = den.[year_month]
AND den.[year_month] = ' + CAST(@end_month_int AS CHAR(6)) + '

LEFT JOIN [ref].[perf_measure] AS ref
ON ref.[measure_name] = ''' + @measure_name + '''

LEFT JOIN [ref].[age_grp] AS age
ON den.[end_month_age] = age.[age]

LEFT JOIN [stage].[perf_staging] AS stg
ON mem.[id_mcaid] = stg.[id_mcaid]
AND ym.[year_month] = stg.[year_month]
/*
This JOIN condition gets only utilization rows for the relevant measure
*/
AND ref.[measure_id] = stg.[measure_id]
AND stg.[num_denom] = ''N''

WHERE ym.[year_month] >= (SELECT [beg_measure_year_month] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
AND ym.[year_month] <= ' + CAST(@end_month_int AS CHAR(6)) + '
)

INSERT INTO [stage].[mcaid_perf_measure]
([beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 [beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM CTE

WHERE 1 = 1
AND [end_month_age] >= 0
-- [full_criteria_t_12_m] will be NULL in all rows except were [end_year_month] = @end_month_int
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
,mem.[id_mcaid]
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
-- Denominator is per 1K members
,1 AS [denominator]
,SUM(ISNULL(stg.[measure_value], 0)) OVER(PARTITION BY mem.[id_mcaid] ORDER BY ym.[year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [numerator]
,CASE WHEN SUM(ISNULL(stg.[measure_value], 0)) OVER(PARTITION BY mem.[id_mcaid] ORDER BY ym.[year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) >= 3 THEN 1 ELSE 0 END AS [outlier]

FROM [ref].[perf_year_month] AS ym

CROSS JOIN [stage].[perf_distinct_member] AS mem

LEFT JOIN [stage].[mcaid_perf_enroll_denom] AS den
ON mem.[id_mcaid] = den.[id_mcaid]
AND ym.[year_month] = den.[year_month]
AND den.[year_month] = ' + CAST(@end_month_int AS CHAR(6)) + '

LEFT JOIN [ref].[perf_measure] AS ref
ON ref.[measure_name] = ''' + @measure_name + '''

LEFT JOIN [ref].[age_grp] AS age
ON den.[end_month_age] = age.[age]

LEFT JOIN [stage].[perf_staging] AS stg
ON mem.[id_mcaid] = stg.[id_mcaid]
AND ym.[year_month] = stg.[year_month]
/*
This JOIN condition gets only utilization rows for the relevant measure
*/
AND ref.[measure_id] = stg.[measure_id]
AND stg.[num_denom] = ''N''

WHERE ym.[year_month] >= (SELECT [beg_measure_year_month] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
AND ym.[year_month] <= ' + CAST(@end_month_int AS CHAR(6)) + '
)

INSERT INTO [stage].[mcaid_perf_measure]
([beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 [beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM CTE

WHERE 1 = 1
AND [end_month_age] >= 18
-- [full_criteria_t_12_m] will be NULL in all rows except were [end_year_month] = @end_month_int
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
,stg.[id_mcaid]

/*
[stage].[mcaid_perf_measure] requires one row per person per measurement year. 
However, for event-based measures, a person may have two different ages at 
different index events during the same measurement year. Thus, insert age at 
last index event [end_month_age] into [stage].[mcaid_perf_measure] BUT filter 
for inclusion below by age at each index event [event_date_age].
*/
,MAX(DATEDIFF(YEAR, den.[dob], stg.[event_date]) - 
 CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, den.[dob], stg.[event_date]), den.[dob]) > 
 stg.[event_date] THEN 1 ELSE 0 END) OVER(PARTITION BY stg.[id_mcaid]) AS [end_month_age]

,DATEDIFF(YEAR, den.[dob], stg.[event_date]) - 
 CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, den.[dob], stg.[event_date]), den.[dob]) > 
 stg.[event_date] THEN 1 ELSE 0 END AS [event_date_age]

,ref.[measure_id]
,den.[full_criteria]
,den.[hospice]
/*
Members need coverage in month following index event.
*/
,den.[full_criteria_p_2_m]
,den.[hospice_p_2_m]
/*
Members need King County residency for 11+ months during measurement year
*/
,res.[enrolled_any_t_12_m]

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

/*
[stage].[mcaid_perf_enroll_denom] must be joined TWICE
(1) Member must have comprehensive, non-dual, non-tpl, no-hospice coverage from 
[event_date] through 30 days after [event_date]
(2) Member must have residence in the ACH region for 11 out of 12 months in the
measurement year. This is proxied by [enrolled_any_t_12_m]
*/

LEFT JOIN [stage].[mcaid_perf_enroll_denom] AS den
ON stg.[id_mcaid] = den.[id_mcaid]
AND stg.[year_month] = den.[year_month]

LEFT JOIN [stage].[mcaid_perf_enroll_denom] AS res
ON stg.[id_mcaid] = res.[id_mcaid]
AND res.[year_month] = ' + CAST(@end_month_int AS CHAR(6)) + '

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
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 (SELECT [beg_measure_year_month] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ') AS [beg_year_month]
,' + CAST(@end_month_int AS CHAR(6)) + ' AS [end_year_month]
,[id_mcaid]
,[end_month_age]
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
,a.[measure_id]
,SUM([denominator]) AS [denominator]
,SUM([numerator]) AS [numerator]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM [CTE] AS a

INNER JOIN [ref].[perf_measure] AS ref
ON a.[measure_id] = ref.[measure_id]

/*
Join age_grp columns here, use CASE above to select age_grp_x from ref.perf_measure
*/
LEFT JOIN [ref].[age_grp] AS age
ON a.[end_month_age] = age.[age]

WHERE 1 = 1
/*
Filter by age at time of index event
*/
AND [event_date_age] >= 13
-- For follow-up measures, enrollment is required at time of index event
AND [full_criteria] = 1
AND [hospice] = 0
AND (([need_1_month_coverage] = 1) OR ([full_criteria_p_2_m] = 2 AND [hospice_p_2_m] = 0))
-- For ACH regional attribution, ANY enrollment is used as a proxy for King County residence
AND [enrolled_any_t_12_m] >= 11

GROUP BY 
 [id_mcaid]
,[end_month_age]
,CASE WHEN ref.[age_group] = ''age_grp_1'' THEN age.[age_grp_1]
      WHEN ref.[age_group] = ''age_grp_2'' THEN age.[age_grp_2]
      WHEN ref.[age_group] = ''age_grp_3'' THEN age.[age_grp_3]
      WHEN ref.[age_group] = ''age_grp_4'' THEN age.[age_grp_4]
      WHEN ref.[age_group] = ''age_grp_5'' THEN age.[age_grp_5]
      WHEN ref.[age_group] = ''age_grp_6'' THEN age.[age_grp_6]
      WHEN ref.[age_group] = ''age_grp_7'' THEN age.[age_grp_7]
      WHEN ref.[age_group] = ''age_grp_8'' THEN age.[age_grp_8]
      WHEN ref.[age_group] = ''age_grp_9_months'' THEN age.[age_grp_9_months]
 END
,a.[measure_id];'
END

IF @measure_name = 'Follow-up Hospitalization for Mental Illness'
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
,stg.[id_mcaid]

/*
[stage].[mcaid_perf_measure] requires one row per person per measurement year. 
However, for event-based measures, a person may have two different ages at 
different index events during the same measurement year. Thus, insert age at 
last index event [end_month_age] into [stage].[mcaid_perf_measure] BUT filter 
for inclusion below by age at each index event [event_date_age].
*/
,MAX(DATEDIFF(YEAR, den.[dob], stg.[event_date]) - 
 CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, den.[dob], stg.[event_date]), den.[dob]) > 
 stg.[event_date] THEN 1 ELSE 0 END) OVER(PARTITION BY stg.[id_mcaid]) AS [end_month_age]

,DATEDIFF(YEAR, den.[dob], stg.[event_date]) - 
 CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, den.[dob], stg.[event_date]), den.[dob]) > 
 stg.[event_date] THEN 1 ELSE 0 END AS [event_date_age]

,ref.[measure_id]
,den.[full_criteria]
,den.[hospice]
/*
Members need coverage in month following index event.
*/
,den.[full_criteria_p_2_m]
,den.[hospice_p_2_m]

,res.[enrolled_any_t_12_m]

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

/*
[stage].[mcaid_perf_enroll_denom] must be joined TWICE
(1) Member must have comprehensive, non-dual, no-hospice coverage from 
[event_date] through 30 days after [event_date]
(2) Member must have residence in the ACH region for 11 out of 12 months in the
measurement year. This is proxied by [enrolled_any_t_12_m]
*/

LEFT JOIN [stage].[mcaid_perf_enroll_denom] AS den
ON stg.[id_mcaid] = den.[id_mcaid]
AND stg.[year_month] = den.[year_month]

LEFT JOIN [stage].[mcaid_perf_enroll_denom] AS res
ON stg.[id_mcaid] = res.[id_mcaid]
AND res.[year_month] = ' + CAST(@end_month_int AS CHAR(6)) + '

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
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 (SELECT [beg_measure_year_month] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ') AS [beg_year_month]
,' + CAST(@end_month_int AS CHAR(6)) + ' AS [end_year_month]
,[id_mcaid]
,[end_month_age]
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
,a.[measure_id]
,SUM([denominator]) AS [denominator]
,SUM([numerator]) AS [numerator]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM [CTE] AS a

INNER JOIN [ref].[perf_measure] AS ref
ON a.[measure_id] = ref.[measure_id]

/*
Join age_grp columns here, use CASE above to select age_grp_x from ref.perf_measure
*/
LEFT JOIN [ref].[age_grp] AS age
ON a.[end_month_age] = age.[age]

WHERE 1 = 1
/*
Filter by age at time of index event
*/
AND [event_date_age] >= 6
-- For follow-up measures, enrollment is required at time of index event
AND [full_criteria] = 1
AND [hospice] = 0
AND (([need_1_month_coverage] = 1) OR ([full_criteria_p_2_m] = 2 AND [hospice_p_2_m] = 0))
-- For ACH regional attribution, ANY enrollment is used as a proxy for King County residence
AND [enrolled_any_t_12_m] >= 11

GROUP BY 
 [id_mcaid]
,[end_month_age]
,CASE WHEN ref.[age_group] = ''age_grp_1'' THEN age.[age_grp_1]
      WHEN ref.[age_group] = ''age_grp_2'' THEN age.[age_grp_2]
      WHEN ref.[age_group] = ''age_grp_3'' THEN age.[age_grp_3]
      WHEN ref.[age_group] = ''age_grp_4'' THEN age.[age_grp_4]
      WHEN ref.[age_group] = ''age_grp_5'' THEN age.[age_grp_5]
      WHEN ref.[age_group] = ''age_grp_6'' THEN age.[age_grp_6]
      WHEN ref.[age_group] = ''age_grp_7'' THEN age.[age_grp_7]
      WHEN ref.[age_group] = ''age_grp_8'' THEN age.[age_grp_8]
      WHEN ref.[age_group] = ''age_grp_9_months'' THEN age.[age_grp_9_months]
 END
,a.[measure_id];'
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
,stg.[id_mcaid]

/*
[stage].[mcaid_perf_measure] requires one row per person per measurement year. 
However, for event-based measures, a person may have two different ages at 
different index events during the same measurement year. Thus, insert age at 
last index event [end_month_age] into [stage].[mcaid_perf_measure] BUT filter 
for inclusion below by age at each index event [event_date_age].
*/
,MAX(DATEDIFF(YEAR, den.[dob], stg.[event_date]) - 
 CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, den.[dob], stg.[event_date]), den.[dob]) > 
 stg.[event_date] THEN 1 ELSE 0 END) OVER(PARTITION BY stg.[id_mcaid]) AS [end_month_age]

,DATEDIFF(YEAR, den.[dob], stg.[event_date]) - 
 CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, den.[dob], stg.[event_date]), den.[dob]) > 
 stg.[event_date] THEN 1 ELSE 0 END AS [event_date_age]

,ref.[measure_id]
,den.[full_criteria]
,den.[hospice]
/*
Members need coverage in month following index event.
*/
,den.[full_criteria_p_2_m]
,den.[hospice_p_2_m]

,res.[enrolled_any_t_12_m]

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

/*
[stage].[mcaid_perf_enroll_denom] must be joined TWICE
(1) Member must have comprehensive, non-dual, no-hospice coverage from 
[event_date] through 30 days after [event_date]
(2) Member must have residence in the ACH region for 11 out of 12 months in the
measurement year. This is proxied by [enrolled_any_t_12_m]
*/

LEFT JOIN [stage].[mcaid_perf_enroll_denom] AS den
ON stg.[id_mcaid] = den.[id_mcaid]
AND stg.[year_month] = den.[year_month]

LEFT JOIN [stage].[mcaid_perf_enroll_denom] AS res
ON stg.[id_mcaid] = res.[id_mcaid]
AND res.[year_month] = ' + CAST(@end_month_int AS CHAR(6)) + '

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
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 (SELECT [beg_measure_year_month] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ') AS [beg_year_month]
,' + CAST(@end_month_int AS CHAR(6)) + ' AS [end_year_month]
,[id_mcaid]
,[end_month_age]
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
,a.[measure_id]
,SUM([denominator]) AS [denominator]
,SUM([numerator]) AS [numerator]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM [CTE] AS a

INNER JOIN [ref].[perf_measure] AS ref
ON a.[measure_id] = ref.[measure_id]

/*
Join age_grp columns here, use CASE above to select age_grp_x from ref.perf_measure
*/
LEFT JOIN [ref].[age_grp] AS age
ON a.[end_month_age] = age.[age]

WHERE 1 = 1
AND [event_date_age] >= 6
-- For follow-up measures, enrollment is required at time of index event
AND [full_criteria] = 1
AND [hospice] = 0
AND (([need_1_month_coverage] = 1) OR ([full_criteria_p_2_m] = 2 AND [hospice_p_2_m] = 0))
-- For ACH regional attribution, ANY enrollment is used as a proxy for King County residence
AND [enrolled_any_t_12_m] >= 11

GROUP BY 
 [id_mcaid]
,[end_month_age]
,CASE WHEN ref.[age_group] = ''age_grp_1'' THEN age.[age_grp_1]
      WHEN ref.[age_group] = ''age_grp_2'' THEN age.[age_grp_2]
      WHEN ref.[age_group] = ''age_grp_3'' THEN age.[age_grp_3]
      WHEN ref.[age_group] = ''age_grp_4'' THEN age.[age_grp_4]
      WHEN ref.[age_group] = ''age_grp_5'' THEN age.[age_grp_5]
      WHEN ref.[age_group] = ''age_grp_6'' THEN age.[age_grp_6]
      WHEN ref.[age_group] = ''age_grp_7'' THEN age.[age_grp_7]
      WHEN ref.[age_group] = ''age_grp_8'' THEN age.[age_grp_8]
      WHEN ref.[age_group] = ''age_grp_9_months'' THEN age.[age_grp_9_months]
 END
,a.[measure_id];'
END

IF @measure_name = 'Mental Health Treatment Penetration'
BEGIN

DELETE FROM [stage].[mcaid_perf_measure]
FROM [stage].[mcaid_perf_measure] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [end_year_month] = @end_month_int;

SET @SQL = @SQL + N'
IF OBJECT_ID(''tempdb..#temp'') IS NOT NULL
DROP TABLE #temp;
WITH CTE AS
(
SELECT
 ym.[beg_measure_year_month] AS [beg_year_month]
,ym.[year_month] AS [end_year_month]
,den.[end_quarter]
,mem.[id_mcaid]
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
,den.[full_criteria_t_12_m]

,stg_den.[measure_value] AS [denominator]
,stg_num.[measure_value] AS [numerator]

FROM [ref].[perf_year_month] AS ym

CROSS JOIN [stage].[perf_distinct_member] AS mem

LEFT JOIN [stage].[mcaid_perf_enroll_denom] AS den
ON mem.[id_mcaid] = den.[id_mcaid]
AND ym.[year_month] = den.[year_month]
AND den.[year_month] = ' + CAST(@end_month_int AS CHAR(6)) + '

LEFT JOIN [ref].[perf_measure] AS ref
ON ref.[measure_name] = ''' + @measure_name + '''

LEFT JOIN [ref].[age_grp] AS age
ON den.[end_month_age] = age.[age]

LEFT JOIN [stage].[perf_staging] AS stg_den
ON mem.[id_mcaid] = stg_den.[id_mcaid]
AND ym.[year_month] = stg_den.[year_month]
/*
This JOIN condition gets only utilization rows for the relevant measure
*/
AND ref.[measure_id] = stg_den.[measure_id]
AND stg_den.[num_denom] = ''D''

LEFT JOIN [stage].[perf_staging] AS stg_num
ON mem.[id_mcaid] = stg_num.[id_mcaid]
AND ym.[year_month] = stg_num.[year_month]
-- [beg_measure_year_month] denotes 12-month identification period for numerator
AND stg_num.[year_month] >= (SELECT [beg_measure_year_month] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
/*
This JOIN condition gets only utilization rows for the relevant measure
*/
AND ref.[measure_id] = stg_num.[measure_id]
AND stg_num.[num_denom] = ''N''

-- [beg_measure_year_month] - 100 denotes 24-month identification period for denominator
WHERE ym.[year_month] >= (SELECT [beg_measure_year_month] - 100 FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
AND ym.[year_month] <= ' + CAST(@end_month_int AS CHAR(6)) + '
)

SELECT *
INTO #temp
FROM CTE;
CREATE CLUSTERED INDEX idx_cl_#temp ON #temp([id_mcaid], [end_year_month]);

WITH CTE AS
(
SELECT
 [beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[full_criteria_t_12_m]
-- 24-month identification period for denominator
,MAX(ISNULL([denominator], 0)) OVER(PARTITION BY [id_mcaid] ORDER BY [end_year_month] ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS [denominator]
-- 12-month identification period for numerator
,MAX(ISNULL([numerator], 0)) OVER(PARTITION BY [id_mcaid] ORDER BY [end_year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [numerator]
FROM #temp
)

INSERT INTO [stage].[mcaid_perf_measure]
([beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 [beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM CTE

WHERE 1 = 1
AND [end_month_age] >= 6
AND [denominator] = 1
-- [full_criteria_t_12_m] will be NULL in all rows except were [end_year_month] = @end_month_int
AND [full_criteria_t_12_m] >= 11;'
END

IF @measure_name = 'SUD Treatment Penetration'
BEGIN

DELETE FROM [stage].[mcaid_perf_measure]
FROM [stage].[mcaid_perf_measure] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [end_year_month] = @end_month_int;

SET @SQL = @SQL + N'
IF OBJECT_ID(''tempdb..#temp'') IS NOT NULL
DROP TABLE #temp;
WITH CTE AS
(
SELECT
 ym.[beg_measure_year_month] AS [beg_year_month]
,ym.[year_month] AS [end_year_month]
,den.[end_quarter]
,mem.[id_mcaid]
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
,den.[full_criteria_t_12_m]

,stg_den.[measure_value] AS [denominator]
,stg_num.[measure_value] AS [numerator]

FROM [ref].[perf_year_month] AS ym

CROSS JOIN [stage].[perf_distinct_member] AS mem

LEFT JOIN [stage].[mcaid_perf_enroll_denom] AS den
ON mem.[id_mcaid] = den.[id_mcaid]
AND ym.[year_month] = den.[year_month]
AND den.[year_month] = ' + CAST(@end_month_int AS CHAR(6)) + '

LEFT JOIN [ref].[perf_measure] AS ref
ON ref.[measure_name] = ''' + @measure_name + '''

LEFT JOIN [ref].[age_grp] AS age
ON den.[end_month_age] = age.[age]

LEFT JOIN [stage].[perf_staging] AS stg_den
ON mem.[id_mcaid] = stg_den.[id_mcaid]
AND ym.[year_month] = stg_den.[year_month]
/*
This JOIN condition gets only utilization rows for the relevant measure
*/
AND ref.[measure_id] = stg_den.[measure_id]
AND stg_den.[num_denom] = ''D''

LEFT JOIN [stage].[perf_staging] AS stg_num
ON mem.[id_mcaid] = stg_num.[id_mcaid]
AND ym.[year_month] = stg_num.[year_month]
-- [beg_measure_year_month] denotes 12-month identification period for numerator
AND stg_num.[year_month] >= (SELECT [beg_measure_year_month] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
/*
This JOIN condition gets only utilization rows for the relevant measure
*/
AND ref.[measure_id] = stg_num.[measure_id]
AND stg_num.[num_denom] = ''N''

-- [beg_measure_year_month] - 100 denotes 24-month identification period for denominator
WHERE ym.[year_month] >= (SELECT [beg_measure_year_month] - 100 FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
AND ym.[year_month] <= ' + CAST(@end_month_int AS CHAR(6)) + '
)

SELECT *
INTO #temp
FROM CTE;
CREATE CLUSTERED INDEX idx_cl_#temp ON #temp([id_mcaid], [end_year_month]);

WITH CTE AS
(
SELECT
 [beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[full_criteria_t_12_m]
-- 24-month identification period for denominator
,MAX(ISNULL([denominator], 0)) OVER(PARTITION BY [id_mcaid] ORDER BY [end_year_month] ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS [denominator]
-- 12-month identification period for denominator
,MAX(ISNULL([numerator], 0)) OVER(PARTITION BY [id_mcaid] ORDER BY [end_year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [numerator]
FROM #temp
)

INSERT INTO [stage].[mcaid_perf_measure]
([beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 [beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM CTE

WHERE 1 = 1
AND [end_month_age] >= 12
AND [denominator] = 1
-- [full_criteria_t_12_m] will be NULL in all rows except were [end_year_month] = @end_month_int
AND [full_criteria_t_12_m] >= 11;'
END

IF @measure_name = 'SUD Treatment Penetration (Opioid)'
BEGIN

DELETE FROM [stage].[mcaid_perf_measure]
FROM [stage].[mcaid_perf_measure] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [end_year_month] = @end_month_int;

SET @SQL = @SQL + N'
IF OBJECT_ID(''tempdb..#temp'') IS NOT NULL
DROP TABLE #temp;
WITH CTE AS
(
SELECT
 ym.[beg_measure_year_month] AS [beg_year_month]
,ym.[year_month] AS [end_year_month]
,den.[end_quarter]
,mem.[id_mcaid]
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
,den.[full_criteria_t_12_m]

,stg_den.[measure_value] AS [denominator]
,stg_num.[measure_value] AS [numerator]

FROM [ref].[perf_year_month] AS ym

CROSS JOIN [stage].[perf_distinct_member] AS mem

LEFT JOIN [stage].[mcaid_perf_enroll_denom] AS den
ON mem.[id_mcaid] = den.[id_mcaid]
AND ym.[year_month] = den.[year_month]
AND den.[year_month] = ' + CAST(@end_month_int AS CHAR(6)) + '

LEFT JOIN [ref].[perf_measure] AS ref
ON ref.[measure_name] = ''' + @measure_name + '''

LEFT JOIN [ref].[age_grp] AS age
ON den.[end_month_age] = age.[age]

LEFT JOIN [stage].[perf_staging] AS stg_den
ON mem.[id_mcaid] = stg_den.[id_mcaid]
AND ym.[year_month] = stg_den.[year_month]
/*
This JOIN condition gets only utilization rows for the relevant measure
*/
AND ref.[measure_id] = stg_den.[measure_id]
AND stg_den.[num_denom] = ''D''

LEFT JOIN [stage].[perf_staging] AS stg_num
ON mem.[id_mcaid] = stg_num.[id_mcaid]
AND ym.[year_month] = stg_num.[year_month]
-- [beg_measure_year_month] denotes 12-month identification period for numerator
AND stg_num.[year_month] >= (SELECT [beg_measure_year_month] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
/*
This JOIN condition gets only utilization rows for the relevant measure
*/
AND ref.[measure_id] = stg_num.[measure_id]
AND stg_num.[num_denom] = ''N''

-- [beg_measure_year_month] - 100 denotes 24-month identification period for denominator
WHERE ym.[year_month] >= (SELECT [beg_measure_year_month] - 100 FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
AND ym.[year_month] <= ' + CAST(@end_month_int AS CHAR(6)) + '
)

SELECT *
INTO #temp
FROM CTE;
CREATE CLUSTERED INDEX idx_cl_#temp ON #temp([id_mcaid], [end_year_month]);

WITH CTE AS
(
SELECT
 [beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[full_criteria_t_12_m]
-- 24-month identification period for denominator
,MAX(ISNULL([denominator], 0)) OVER(PARTITION BY [id_mcaid] ORDER BY [end_year_month] ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS [denominator]
-- 12-month identification period for denominator
,MAX(ISNULL([numerator], 0)) OVER(PARTITION BY [id_mcaid] ORDER BY [end_year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [numerator]
FROM #temp
)

INSERT INTO [stage].[mcaid_perf_measure]
([beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 [beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM CTE

WHERE 1 = 1
AND [end_month_age] >= 18
AND [denominator] = 1
-- [full_criteria_t_12_m] will be NULL in all rows except were [end_year_month] = @end_month_int
AND [full_criteria_t_12_m] >= 11;'
END

IF @measure_name = 'Plan All-Cause Readmissions (30 days)'
BEGIN

DELETE FROM [stage].[mcaid_perf_measure]
FROM [stage].[mcaid_perf_measure] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_name] = @measure_name
AND [end_year_month] = @end_month_int;

SET @SQL = @SQL + N'
IF OBJECT_ID(''tempdb..#temp'', ''U'') IS NOT NULL
DROP TABLE #temp;
WITH CTE AS
(
SELECT
 ym.[beg_measure_year_month] AS [beg_year_month]
,stg.[year_month] AS [end_year_month]
,den.[end_quarter]
,stg.[id_mcaid]

/*
[stage].[mcaid_perf_measure] requires one row per person per measurement year. 
However, for event-based measures, a person may have two different ages at 
different index events during the same measurement year. Thus, insert age at 
last index event [end_month_age] into [stage].[mcaid_perf_measure] BUT filter 
for inclusion below by age at each index event [event_date_age].
*/
,MAX(DATEDIFF(YEAR, den.[dob], stg.[event_date]) - 
 CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, den.[dob], stg.[event_date]), den.[dob]) > 
 stg.[event_date] THEN 1 ELSE 0 END) OVER(PARTITION BY stg.[id_mcaid]) AS [end_month_age]

,DATEDIFF(YEAR, den.[dob], stg.[event_date]) - 
 CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, den.[dob], stg.[event_date]), den.[dob]) > 
 stg.[event_date] THEN 1 ELSE 0 END AS [event_date_age]

,ref.[measure_id]
/*
Members need coverage in 11/12 months prior to index event
*/
,den.[full_criteria_t_12_m]
,den.[hospice_t_12_m]

,den.[full_criteria]
,den.[hospice]
/*
Members need coverage in month following index event.
*/
,den.[full_criteria_p_2_m]
,den.[hospice_p_2_m]

,stg.[event_date]
/*
If index event occurs on 1st of month, then 31-day follow-up period contained
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
AND ref.[measure_name] = ''' + @measure_name + '''

INNER JOIN [ref].[perf_year_month] AS ym
ON stg.[year_month] = ym.[year_month]

/*
Backward-looking and forward-looking enrollment criteria at time of index event
are joined to year_month of index event
*/
LEFT JOIN [stage].[mcaid_perf_enroll_denom] AS den
ON stg.[id_mcaid] = den.[id_mcaid]
AND stg.[year_month] = den.[year_month]

WHERE stg.[event_date] >= (SELECT [12_month_prior] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
/*
Cut off index events during last 31-day period
because of insufficient follow-up period
*/
AND stg.[event_date] <= (SELECT DATEADD(DAY, -30, [end_month]) FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
)
SELECT *
INTO #temp
FROM CTE;

INSERT INTO [stage].[mcaid_perf_measure]
([beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 (SELECT [beg_measure_year_month] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ') AS [beg_year_month]
,' + CAST(@end_month_int AS CHAR(6)) + ' AS [end_year_month]
,[id_mcaid]
,[end_month_age]
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
,a.[measure_id]
,SUM([denominator]) AS [denominator]
,SUM([numerator]) AS [numerator]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM #temp AS a

INNER JOIN [ref].[perf_measure] AS ref
ON a.[measure_id] = ref.[measure_id]

/*
Join age_grp columns here, use CASE above to select age_grp_x from ref.perf_measure
*/
LEFT JOIN [ref].[age_grp] AS age
ON a.[end_month_age] = age.[age]

WHERE 1 = 1
/*
Filter by age at time of index event
*/
AND [event_date_age] BETWEEN 18 AND 64
AND [end_month_age] BETWEEN 18 AND 64

/*
For readmission measure, members need coverage for 11/12 months prior to Index Discharge Date and for 30 days following Index Discharge Date
*/
AND [full_criteria_t_12_m] >= 11
AND [hospice_t_12_m] = 0
AND [full_criteria] = 1
AND [hospice] = 0
AND (([need_1_month_coverage] = 1) OR ([full_criteria_p_2_m] = 2 AND [hospice_p_2_m] = 0))

GROUP BY 
 [id_mcaid]
,[end_month_age]
,CASE WHEN ref.[age_group] = ''age_grp_1'' THEN age.[age_grp_1]
      WHEN ref.[age_group] = ''age_grp_2'' THEN age.[age_grp_2]
      WHEN ref.[age_group] = ''age_grp_3'' THEN age.[age_grp_3]
      WHEN ref.[age_group] = ''age_grp_4'' THEN age.[age_grp_4]
      WHEN ref.[age_group] = ''age_grp_5'' THEN age.[age_grp_5]
      WHEN ref.[age_group] = ''age_grp_6'' THEN age.[age_grp_6]
      WHEN ref.[age_group] = ''age_grp_7'' THEN age.[age_grp_7]
      WHEN ref.[age_group] = ''age_grp_8'' THEN age.[age_grp_8]
      WHEN ref.[age_group] = ''age_grp_9_months'' THEN age.[age_grp_9_months]
 END
,a.[measure_id];'
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
,mem.[id_mcaid]
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
,CASE WHEN MAX(ISNULL(stg.[measure_value], 0)) OVER(PARTITION BY mem.[id_mcaid] ORDER BY ym.[year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) > 0 THEN 1 ELSE 0 END AS [numerator_t_12_m]
,CASE WHEN MAX(ISNULL(stg.[measure_value], 0)) OVER(PARTITION BY mem.[id_mcaid] ORDER BY ym.[year_month] ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) > 0 THEN 1 ELSE 0 END AS [numerator_t_24_m]

FROM [ref].[perf_year_month] AS ym

CROSS JOIN [stage].[perf_distinct_member] AS mem

LEFT JOIN [stage].[mcaid_perf_enroll_denom] AS den
ON mem.[id_mcaid] = den.[id_mcaid]
AND ym.[year_month] = den.[year_month]
AND den.[year_month] = ' + CAST(@end_month_int AS CHAR(6)) + '

LEFT JOIN [ref].[perf_measure] AS ref
ON ref.[measure_name] = ''' + @measure_name + '''

LEFT JOIN [ref].[age_grp] AS age
ON den.[age_in_months] = age.[age]

LEFT JOIN [stage].[perf_staging] AS stg
ON mem.[id_mcaid] = stg.[id_mcaid]
AND ym.[year_month] = stg.[year_month]
/*
This JOIN condition gets only utilization rows for the relevant measure
*/
AND ref.[measure_id] = stg.[measure_id]
AND stg.[num_denom] = ''N''

-- [beg_measure_year_month] - 100 denotes 24-month identification period for some age groups
WHERE ym.[year_month] >= (SELECT [beg_measure_year_month] - 100 FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
AND ym.[year_month] <= ' + CAST(@end_month_int AS CHAR(6)) + '
)

INSERT INTO [stage].[mcaid_perf_measure]
([beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 [beg_year_month]
,[end_year_month]
,[id_mcaid]
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
-- [full_criteria_t_12_m] will be NULL in all rows except were [end_year_month] = @end_month_int
AND [full_criteria_t_12_m] >= 11
AND [hospice_t_12_m] = 0
AND CASE WHEN [age_grp] IN (''Age 7-11'', ''Age 12-19'') THEN [full_criteria_prior_t_12_m] ELSE 11 END >= 11
AND CASE WHEN [age_grp] IN (''Age 7-11'', ''Age 12-19'') THEN [hospice_prior_t_12_m] ELSE 0 END = 0;'
END

IF @measure_name = 'MH Treatment Penetration by Diagnosis'
BEGIN

DELETE FROM [stage].[mcaid_perf_measure]
FROM [stage].[mcaid_perf_measure] AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
WHERE b.[measure_etl_name] = @measure_name
AND [end_year_month] = @end_month_int;

SET @SQL = @SQL + N'
IF OBJECT_ID(''tempdb..#temp'') IS NOT NULL
DROP TABLE #temp;
WITH CTE AS
(
SELECT
 ym.[beg_measure_year_month] AS [beg_year_month]
,ym.[year_month] AS [end_year_month]
,den.[end_quarter]
,mem.[id_mcaid]
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
,den.[enrolled_any_t_12_m]

,stg_den.[measure_value] AS [denominator]
,stg_num.[measure_value] AS [numerator]

FROM [ref].[perf_year_month] AS ym

CROSS JOIN [stage].[perf_distinct_member] AS mem

LEFT JOIN [stage].[mcaid_perf_enroll_denom] AS den
ON mem.[id_mcaid] = den.[id_mcaid]
AND ym.[year_month] = den.[year_month]
AND den.[year_month] = ' + CAST(@end_month_int AS CHAR(6)) + '

LEFT JOIN [ref].[perf_measure] AS ref
ON ref.[measure_etl_name] = ''' + @measure_name + '''

LEFT JOIN [ref].[age_grp] AS age
ON den.[end_month_age] = age.[age]

LEFT JOIN [stage].[perf_staging] AS stg_den
ON mem.[id_mcaid] = stg_den.[id_mcaid]
AND ym.[year_month] = stg_den.[year_month]
/*
This JOIN condition gets only utilization rows for the relevant measure
*/
AND ref.[measure_id] = stg_den.[measure_id]
AND stg_den.[num_denom] = ''D''

LEFT JOIN [stage].[perf_staging] AS stg_num
ON mem.[id_mcaid] = stg_num.[id_mcaid]
AND ym.[year_month] = stg_num.[year_month]
-- [beg_measure_year_month] denotes 12-month identification period for numerator
AND stg_num.[year_month] >= (SELECT [beg_measure_year_month] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
/*
This JOIN condition gets only utilization rows for the relevant measure
*/
AND ref.[measure_id] = stg_num.[measure_id]
AND stg_num.[num_denom] = ''N''

-- [beg_measure_year_month] - 100 denotes 24-month identification period for denominator
WHERE ym.[year_month] >= (SELECT [beg_measure_year_month] - 100 FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
AND ym.[year_month] <= ' + CAST(@end_month_int AS CHAR(6)) + '
)

SELECT *
INTO #temp
FROM CTE;
CREATE CLUSTERED INDEX idx_cl_#temp ON #temp([id_mcaid], [measure_id], [end_year_month]);

WITH CTE AS
(
SELECT
 [beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[enrolled_any_t_12_m]
-- 24-month identification period for denominator
,MAX(ISNULL([denominator], 0)) OVER(PARTITION BY [id_mcaid], [measure_id] ORDER BY [end_year_month] ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS [denominator]
-- 12-month identification period for numerator
,MAX(ISNULL([numerator], 0)) OVER(PARTITION BY [id_mcaid], [measure_id] ORDER BY [end_year_month] ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS [numerator]
FROM #temp
)

INSERT INTO [stage].[mcaid_perf_measure]
([beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,[load_date])

SELECT
 [beg_year_month]
,[end_year_month]
,[id_mcaid]
,[end_month_age]
,[age_grp]
,[measure_id]
,[denominator]
,[numerator]
,CAST(GETDATE() AS DATE) AS [load_date]

FROM CTE

WHERE 1 = 1
AND [denominator] = 1
-- [enrolled_any_t_12_m] will be NULL in all rows except were [end_year_month] = @end_month_int
AND [enrolled_any_t_12_m] >= 1;'
END

PRINT @SQL;
END

EXEC sp_executeSQL @statement=@SQL, 
                   @params=N'@end_month_int INT, @measure_name VARCHAR(200)',
				   @end_month_int=@end_month_int, @measure_name=@measure_name;

GO

/*
EXEC [stage].[sp_perf_measures]
 @end_month_int = 201912
--,@measure_name = 'MH Treatment Penetration by Diagnosis';
--,@measure_name = 'Follow-up ED visit for Alcohol/Drug Abuse';
,@measure_name = 'Follow-up ED visit for Mental Illness';
*/

/*
SELECT
 [end_year_month]
,[measure_name]
,SUM([denominator])
,SUM([numerator])
,[load_date]
FROM [stage].[mcaid_perf_measure] AS a
LEFT JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
GROUP BY
 [end_year_month]
,[measure_name]
,[load_date]
ORDER BY
 [measure_name]
,[end_year_month]
,[load_date];
*/