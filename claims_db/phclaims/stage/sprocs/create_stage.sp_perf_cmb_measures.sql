
/*
This procedure calculates each performance measure from combined Medicaid and 
BHO data for a @measure_name and for a measurement year ending @end_month_int.

The structure is 
IF @measure_name = 'Follow-up Hospitalization for Mental Illness', run a SQL batch...

Calls on:
[PHClaims_RO].[PHClaims].[ref].[perf_measure], table of measure characteristics
[PHClaims_RO].[PHClaims].[ref].[perf_year_month], month-by-month table of dates
[PHClaims_RO].[PHClaims].[stage].[perf_distinct_member], one row per distinct person
##perf_cmb_staging], utilization activity pre-aggregated to person-month level
##perf_cmb_staging_event_date], utilization activity pre-aggregated to person-date level

Loads to:
##perf_cmb_measure]

Run for one measure at a time, one measurement period at a time

EXEC [stage].[sp_perf_measures]
 @end_month_int = 201709
,@measure_name = 'Mental Health Treatment Penetration';

Author: Philip Sylling
Modified: 2019-10-07: Modified to accomodate combined Medicaid/BHO measures
*/

USE [DCHS_Analytics];
GO

IF OBJECT_ID('[stage].[sp_perf_cmb_measures]','P') IS NOT NULL
DROP PROCEDURE [stage].[sp_perf_cmb_measures];
GO
CREATE PROCEDURE [stage].[sp_perf_cmb_measures]
 @end_month_int INT = 201712
,@measure_name VARCHAR(200) = NULL
AS
--SET NOCOUNT ON;
DECLARE @SQL NVARCHAR(MAX) = '';
DECLARE @end_month_date DATE;

BEGIN

IF @measure_name = 'Follow-up Hospitalization for Mental Illness'
BEGIN

SET @SQL = @SQL + N'
WITH CTE AS
(
SELECT
 ym.[beg_measure_year_month] AS [beg_year_month]
,stg.[year_month] AS [end_year_month]
,den.[end_quarter]
,stg.[id_mcaid]

/*
##perf_cmb_measure requires one row per person per measurement year. 
However, for event-based measures, a person may have two different ages at 
different index events during the same measurement year. Thus, insert age at 
last index event [end_month_age] into ##perf_cmb_measure BUT filter 
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

FROM ##perf_cmb_staging_event_date AS stg

INNER JOIN [ref].[perf_measure] AS ref
ON stg.[measure_id] = ref.[measure_id]
AND ref.[measure_name] LIKE ''' + CAST(@measure_name AS VARCHAR(200)) + '%' + '''

INNER JOIN [ref].[perf_year_month] AS ym
ON stg.[year_month] = ym.[year_month]

/*
[PHClaims_RO].[PHClaims].[stage].[perf_enroll_denom] must be joined TWICE
(1) Member must have comprehensive, non-dual, no-hospice coverage from 
[event_date] through 30 days after [event_date]
(2) Member must have residence in the ACH region for 11 out of 12 months in the
measurement year. This is proxied by [enrolled_any_t_12_m]
*/

LEFT JOIN [PHClaims_RO].[PHClaims].[stage].[perf_enroll_denom] AS den
ON stg.[id_mcaid] = den.[id_mcaid]
AND stg.[year_month] = den.[year_month]

LEFT JOIN [PHClaims_RO].[PHClaims].[stage].[perf_enroll_denom] AS res
ON stg.[id_mcaid] = res.[id_mcaid]
AND res.[year_month] = ' + CAST(@end_month_int AS CHAR(6)) + '

WHERE stg.[event_date] >= (SELECT [12_month_prior] FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
/*
Cut off index visits during last 31-day period
because of insufficient follow-up period
*/
AND stg.[event_date] <= (SELECT DATEADD(DAY, -30, [end_month]) FROM [ref].[perf_year_month] WHERE [year_month] = ' + CAST(@end_month_int AS CHAR(6)) + ')
)

INSERT INTO ##perf_cmb_measure
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

PRINT @SQL;
END

EXEC sp_executeSQL @statement=@SQL, 
                   @params=N'@end_month_int INT, @measure_name VARCHAR(200)',
				   @end_month_int=@end_month_int, @measure_name=@measure_name;

GO

/*
IF OBJECT_ID('tempdb..##perf_cmb_measure') IS NOT NULL
DROP TABLE ##perf_cmb_measure;
CREATE TABLE ##perf_cmb_measure
([beg_year_month] INT NULL
,[end_year_month] INT NULL
,[id_mcaid] VARCHAR(255) NULL
,[end_month_age] INT NULL
,[age_grp] VARCHAR(20) NULL
,[measure_id] INT NOT NULL
,[denominator] INT NOT NULL
,[numerator] INT NOT NULL
,[load_date] VARCHAR(10) NOT NULL
) ON [PRIMARY];
GO

EXEC [stage].[sp_perf_cmb_measures]
 @end_month_int = 201812
,@measure_name = 'Follow-up Hospitalization for Mental Illness';

SELECT
 [end_year_month]
,[measure_name]
,[age_grp]
,SUM([denominator]) AS [denominator]
,SUM([numerator]) AS [numerator]
FROM ##perf_cmb_measure AS a
INNER JOIN [ref].[perf_measure] AS b
ON a.[measure_id] = b.[measure_id]
GROUP BY
 [end_year_month]
,[measure_name]
,[age_grp];
*/