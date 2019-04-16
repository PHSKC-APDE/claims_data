
USE PHClaims;
GO

IF OBJECT_ID('[stage].[v_perf_ah_inpatient_direct_transfer]', 'V') IS NOT NULL
DROP VIEW [stage].[v_perf_ah_inpatient_direct_transfer];
GO
CREATE VIEW [stage].[v_perf_ah_inpatient_direct_transfer]
AS
/* 
In the future, adapt this code to use admit and discharge dates
*/
WITH [get_acute_inpatient] AS
(
-- Acute Inpatient Discharges
SELECT 
 [id]
,[tcn]
,[from_date]
,[to_date]
,[patient_status]
FROM [dbo].[mcaid_claim_header]
WHERE [clm_type_code] IN (31, 33)
),

[increment_stays_by_person] AS
(
SELECT 
 [id]
,[tcn]
-- If prior_to_date IS NULL, then it is the first chronological stay for the person
,LAG([to_date]) OVER(PARTITION BY [id] ORDER BY [from_date], [to_date], [tcn]) AS prior_to_date
,[from_date]
,[to_date]
,[patient_status]
-- Number of days between consecutive stays
,DATEDIFF(DAY, LAG([to_date]) OVER(PARTITION BY [id] ORDER BY [from_date], [to_date], [tcn]), [from_date]) AS date_diff
/*
Create a chronological (0, 1) indicator column.
If 0, it is the first stay for the person OR the stay is within 1 day of the prior stay (direct transfer).
If 1, the prior stay is NOT within 1 day of the prior stay (new stay).
This indicator column will be summed to create a stay_id.
*/
,CASE WHEN ROW_NUMBER() OVER(PARTITION BY [id] ORDER BY [from_date], [to_date], [tcn]) = 1 THEN 0
      WHEN DATEDIFF(DAY, LAG([to_date]) OVER(PARTITION BY [id] ORDER BY [from_date], [to_date], [tcn]), [from_date]) <= 1 THEN 0
	  WHEN DATEDIFF(DAY, LAG([to_date]) OVER(PARTITION BY [id] ORDER BY [from_date], [to_date], [tcn]), [from_date]) > 1 THEN 1
 END AS [increment]
FROM [get_acute_inpatient]
),

/*
Sum [increment] column (Cumulative Sum) within person to create an episode_id which
combines stays within 1 day of each other
*/
[create_episode_id] AS
(
SELECT 
 [id]
,[tcn]
,[prior_to_date]
,[from_date]
,[to_date]
,[patient_status]
,[date_diff]
,[increment]
,SUM([increment]) OVER(PARTITION BY [id] ORDER BY [from_date], [to_date], [tcn] ROWS UNBOUNDED PRECEDING) + 1 AS [episode_id]
FROM [increment_stays_by_person]
)

SELECT 
 [id]
,[tcn]
,[prior_to_date] AS [prior_claim_to_date]
,[from_date] AS [claim_from_date]
,[to_date] AS [claim_to_date]
,[patient_status]
,[date_diff]
,[increment]
,[episode_id]
,FIRST_VALUE([from_date]) OVER(PARTITION BY [id], [episode_id] ORDER BY [from_date], [to_date], [tcn] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_from_date]
,LAST_VALUE([to_date]) OVER(PARTITION BY [id], [episode_id] ORDER BY [from_date], [to_date], [tcn] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_to_date]
,COUNT(*) OVER(PARTITION BY [id], [episode_id] ORDER BY [id], [episode_id], [tcn]) AS [count_stays]
--,ROW_NUMBER() OVER(PARTITION BY [id], [episode_id] ORDER BY [from_date] DESC, [to_date] DESC, [tcn] DESC) AS [stay_id]
--,CASE WHEN [patient_status] = 'Expired' THEN 1 ELSE 0 END AS [death_during_stay]
,ROW_NUMBER() OVER(PARTITION BY [id], [episode_id] ORDER BY [from_date], [to_date], [tcn]) AS [stay_id]
,MAX(CASE WHEN [patient_status] = 'Expired' THEN 1 ELSE 0 END) OVER(PARTITION BY [id], [episode_id] ORDER BY [id], [episode_id]) AS [death_during_stay]
FROM [create_episode_id];
GO

/*
SELECT * 
FROM [stage].[v_perf_ah_inpatient_direct_transfer]
ORDER BY [id], [episode_id], [stay_id];
*/