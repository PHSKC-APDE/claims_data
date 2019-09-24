
library("chron")
library("DBI")
library("dbplyr")
library("dplyr")
library("glue")
library("janitor")
library("lubridate")
library("odbc")
library("openxlsx")
library("stringr")
library("tidyr")
library("xlsx")
library("bit64")

# Connect to PHClaims
PHClaims <- dbConnect(odbc(), "PHClaims")
# Connect to DCHS_Analytics
DCHS_Analytics <- dbConnect(odbc(), "Analytics")

# Integrate BHO + Medicaid inpatient stays
# Then, de-duplicate inpatient stays (about 6% of stays appear to have nearly the 
# same admit and discharge dates in BHO and Medicaid) 
index_stay_mental_illness <- dbGetQuery(DCHS_Analytics, "
WITH [inpatient_stays] AS
(
SELECT
 'BHO' AS [data_source]
,'Mental Illness' AS [value_set_name]
,[p1_id] AS [id_mcaid]
,[age]
,[admit_date]
,[discharge_date]
,[flag]
FROM [stage].[fn_perf_fuh_inpatient_index_stay]('2017-01-01', '2018-12-31', 6)
WHERE [p1_id] IS NOT NULL

UNION ALL

SELECT
 'Medicaid' AS [data_source]
,[value_set_name]
,[id_mcaid]
,[age]
,[admit_date]
,[discharge_date]
,[flag]
FROM [PHClaims_RO].[PHClaims].[stage].[v_perf_fuh_inpatient_index_stay]
WHERE 1 = 1
AND [discharge_date] >= '2017-01-01'
AND [discharge_date] <= '2018-12-31'
AND [age] >= 6
AND [value_set_name] = 'Mental Illness'
),

[increment_stay_by_person] AS
(
SELECT
 [data_source]
,[value_set_name]
,[id_mcaid]
,[age]
-- If prior_discharge_date IS NULL, then it is the first chronological discharge for the person
,LAG([discharge_date]) OVER(PARTITION BY [id_mcaid] ORDER BY [admit_date], [discharge_date], [data_source]) AS [prior_discharge_date]
,[admit_date]
,[discharge_date]
-- Number of days between consecutive discharges
,DATEDIFF(DAY, LAG([discharge_date]) OVER(PARTITION BY [id_mcaid] 
 ORDER BY [admit_date], [discharge_date], [data_source]), [admit_date]) AS [date_diff]
/*
Create a chronological (0, 1) indicator column.
If 0, it is the first stay for the person OR the stay is contiguous with the prior stay.
If 1, the prior stay is NOT contiguous with the next stay.
This indicator column will be summed to create an episode_id.
*/
,CASE WHEN ROW_NUMBER() OVER(PARTITION BY [id_mcaid] 
      ORDER BY [admit_date], [discharge_date], [data_source]) = 1 THEN 0
      WHEN DATEDIFF(DAY, LAG([discharge_date]) OVER(PARTITION BY [id_mcaid]
	  ORDER BY [admit_date], [discharge_date], [data_source]), [admit_date]) <= 0 THEN 0
	  WHEN DATEDIFF(DAY, LAG([discharge_date]) OVER(PARTITION BY [id_mcaid]
	  ORDER BY [admit_date], [discharge_date], [data_source]), [admit_date]) > 0 THEN 1
 END AS [increment]
FROM [inpatient_stays]
--ORDER BY [id_mcaid], [admit_date], [discharge_date], [data_source]
),

/*
Sum [increment] column (Cumulative Sum) within person to create an episode_id that
combines contiguous stays.
*/
[create_episode_id] AS
(
SELECT
 [data_source]
,[value_set_name]
,[id_mcaid]
,[age]
,[prior_discharge_date]
,[admit_date]
,[discharge_date]
,[date_diff]
,[increment]
,SUM([increment]) OVER(PARTITION BY [id_mcaid] ORDER BY [admit_date], [discharge_date], [data_source] ROWS UNBOUNDED PRECEDING) + 1 AS [episode_id]
FROM [increment_stay_by_person]
--ORDER BY [id_mcaid], [admit_date], [discharge_date], [data_source]
),

/*
Calculate episode start/end dates using FIRST_VALUE([admit_date]), 
LAST_VALUE([discharge_date]) grouping by [id_mcaid] and the [episode_id] created in the
previous step.
*/
[episode_admit_discharge_date] AS
(
SELECT
 [data_source]
,[value_set_name]
,[id_mcaid]
,[age]
,[prior_discharge_date]
,[admit_date]
,[discharge_date]
,[date_diff]
,[increment]
,[episode_id]
,LAST_VALUE([age]) OVER(PARTITION BY [id_mcaid], [episode_id] ORDER BY [admit_date], [discharge_date], [data_source] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_age]
,FIRST_VALUE([admit_date]) OVER(PARTITION BY [id_mcaid], [episode_id] ORDER BY [admit_date], [discharge_date], [data_source] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_admit_date]
,LAST_VALUE([discharge_date]) OVER(PARTITION BY [id_mcaid], [episode_id] ORDER BY [admit_date], [discharge_date], [data_source] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_discharge_date]
,COUNT(*) OVER(PARTITION BY [id_mcaid], [episode_id]) AS [count_stays]
,ROW_NUMBER() OVER(PARTITION BY [id_mcaid], [episode_id] ORDER BY [admit_date], [discharge_date], [data_source]) AS [episode_service_id]
FROM [create_episode_id]
--ORDER BY [id_mcaid], [admit_date], [discharge_date], [data_source]
)

SELECT
 'BHO+Medicaid' AS [data_source]
,[value_set_name]
,[id_mcaid]
--,[age]
--,[prior_discharge_date]
--,[admit_date]
--,[discharge_date]
--,[date_diff]
--,[increment]
--,[episode_id]
,[episode_age] AS [age]
,[episode_admit_date] AS [admit_date]
,[episode_discharge_date] AS [discharge_date]
--,[count_stays]
--,[episode_service_id]
,1 AS [flag]
FROM [episode_admit_discharge_date]
WHERE [episode_service_id] = 1
--ORDER BY [id_mcaid], [admit_date], [discharge_date], [data_source];
")

# Now, integrate BHO + Medicaid inpatient stays for Mental Health Diagnosis value set
# Then, de-duplicate based on same method as above
index_stay_mental_health_diagnosis <- dbGetQuery(DCHS_Analytics, "
WITH [inpatient_stays] AS
(
SELECT
 'BHO' AS [data_source]
,'Mental Health Diagnosis' AS [value_set_name]
,[p1_id] AS [id_mcaid]
,[age]
,[admit_date]
,[discharge_date]
,[flag]
FROM [stage].[fn_perf_fuh_inpatient_index_stay]('2017-01-01', '2018-12-31', 6)
WHERE [p1_id] IS NOT NULL

UNION ALL

SELECT
 'Medicaid' AS [data_source]
,[value_set_name]
,[id_mcaid]
,[age]
,[admit_date]
,[discharge_date]
,[flag]
FROM [PHClaims_RO].[PHClaims].[stage].[v_perf_fuh_inpatient_index_stay]
WHERE 1 = 1
AND [discharge_date] >= '2017-01-01'
AND [discharge_date] <= '2018-12-31'
AND [age] >= 6
AND [value_set_name] = 'Mental Health Diagnosis'
),

[increment_stay_by_person] AS
(
SELECT
 [data_source]
,[value_set_name]
,[id_mcaid]
,[age]
-- If prior_discharge_date IS NULL, then it is the first chronological discharge for the person
,LAG([discharge_date]) OVER(PARTITION BY [id_mcaid] ORDER BY [admit_date], [discharge_date], [data_source]) AS [prior_discharge_date]
,[admit_date]
,[discharge_date]
-- Number of days between consecutive discharges
,DATEDIFF(DAY, LAG([discharge_date]) OVER(PARTITION BY [id_mcaid] 
 ORDER BY [admit_date], [discharge_date], [data_source]), [admit_date]) AS [date_diff]
/*
Create a chronological (0, 1) indicator column.
If 0, it is the first stay for the person OR the stay is contiguous with the prior stay.
If 1, the prior stay is NOT contiguous with the next stay.
This indicator column will be summed to create an episode_id.
*/
,CASE WHEN ROW_NUMBER() OVER(PARTITION BY [id_mcaid] 
      ORDER BY [admit_date], [discharge_date], [data_source]) = 1 THEN 0
      WHEN DATEDIFF(DAY, LAG([discharge_date]) OVER(PARTITION BY [id_mcaid]
	  ORDER BY [admit_date], [discharge_date], [data_source]), [admit_date]) <= 0 THEN 0
	  WHEN DATEDIFF(DAY, LAG([discharge_date]) OVER(PARTITION BY [id_mcaid]
	  ORDER BY [admit_date], [discharge_date], [data_source]), [admit_date]) > 0 THEN 1
 END AS [increment]
FROM [inpatient_stays]
--ORDER BY [id_mcaid], [admit_date], [discharge_date], [data_source]
),

/*
Sum [increment] column (Cumulative Sum) within person to create an episode_id that
combines contiguous stays.
*/
[create_episode_id] AS
(
SELECT
 [data_source]
,[value_set_name]
,[id_mcaid]
,[age]
,[prior_discharge_date]
,[admit_date]
,[discharge_date]
,[date_diff]
,[increment]
,SUM([increment]) OVER(PARTITION BY [id_mcaid] ORDER BY [admit_date], [discharge_date], [data_source] ROWS UNBOUNDED PRECEDING) + 1 AS [episode_id]
FROM [increment_stay_by_person]
--ORDER BY [id_mcaid], [admit_date], [discharge_date], [data_source]
),

/*
Calculate episode start/end dates using FIRST_VALUE([admit_date]), 
LAST_VALUE([discharge_date]) grouping by [id_mcaid] and the [episode_id] created in the
previous step.
*/
[episode_admit_discharge_date] AS
(
SELECT
 [data_source]
,[value_set_name]
,[id_mcaid]
,[age]
,[prior_discharge_date]
,[admit_date]
,[discharge_date]
,[date_diff]
,[increment]
,[episode_id]
,LAST_VALUE([age]) OVER(PARTITION BY [id_mcaid], [episode_id] ORDER BY [admit_date], [discharge_date], [data_source] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_age]
,FIRST_VALUE([admit_date]) OVER(PARTITION BY [id_mcaid], [episode_id] ORDER BY [admit_date], [discharge_date], [data_source] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_admit_date]
,LAST_VALUE([discharge_date]) OVER(PARTITION BY [id_mcaid], [episode_id] ORDER BY [admit_date], [discharge_date], [data_source] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_discharge_date]
,COUNT(*) OVER(PARTITION BY [id_mcaid], [episode_id]) AS [count_stays]
,ROW_NUMBER() OVER(PARTITION BY [id_mcaid], [episode_id] ORDER BY [admit_date], [discharge_date], [data_source]) AS [episode_service_id]
FROM [create_episode_id]
--ORDER BY [id_mcaid], [admit_date], [discharge_date], [data_source]
)

SELECT
 'BHO+Medicaid' AS [data_source]
,[value_set_name]
,[id_mcaid]
--,[age]
--,[prior_discharge_date]
--,[admit_date]
--,[discharge_date]
--,[date_diff]
--,[increment]
--,[episode_id]
,[episode_age] AS [age]
,[episode_admit_date] AS [admit_date]
,[episode_discharge_date] AS [discharge_date]
--,[count_stays]
--,[episode_service_id]
,1 AS [flag]
FROM [episode_admit_discharge_date]
WHERE [episode_service_id] = 1
--ORDER BY [id_mcaid], [admit_date], [discharge_date], [data_source];
")

readmit <- dbGetQuery(PHClaims, "
SELECT
 'BHO+Medicaid' AS [data_source]
,[id_mcaid]
,[admit_date]
,[discharge_date]
,[acuity]
,[flag]
FROM [stage].[fn_perf_fuh_inpatient_index_stay_readmit]('2017-01-01', '2018-12-31');
")

direct_transfer <- inner_join(index_stay_mental_illness, index_stay_mental_health_diagnosis, c("flag")) %>%
  mutate(date_diff=difftime(admit_date.y, discharge_date.x, units="days"))

  filter(id_mcaid.x==id_mcaid.y & admit_date.y>discharge_date.x )
difftime(time1, time2, tz,
         units = c("auto", "secs", "mins", "hours",
                   "days", "weeks"))

# Output results #
# today <- Sys.Date()
# filename <- paste0("L:/DCHSPHClaimsData/Analyses/Philip/06_Medicaid_BHO_Encounters/R_Output_", today, ".xlsx")
# write.xlsx(as.data.frame(wide), file = filename, sheetName = "Sheet1")
