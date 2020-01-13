
/*
This view gets inpatient stays by
RDA DEFINITION [clm_type_code] IN (31, 33).
Then the stays within 1 day of each other are connected into an episode.

Author: Philip Sylling
Created: 2019-04-24
Modified: 2019-08-06 | Point to new [final] analytic tables

Returns:
 [id_mcaid]
,[claim_header_id]
,[prior_claim_last_service_date], previous claim [last_service_date] within person, if any
,[claim_first_service_date], [first_service_date] from individual claim before creating episode
,[claim_last_service_date], [last_service_date] from individual claim before creating episode
,[patient_status], from individual claim before creating episode
,[date_diff], number of days between previous claim [last_service_date] and current claim [last_service_date] within person
,[increment], indicator to mark new episode
,[episode_id], created counter to delineate episodes within person
,[episode_first_service_date], first [first_service_date] of first claim within episode
,[episode_last_service_date], last [last_service_date] of last claim within episode
,[count_stays], number of claims within episode
,[stay_id], created counter to delineate claims within episode
,[death_during_stay], death from [patient_status], anytime during episode

Note Hierarchy:
[stay_id] within [episode_id] within person [id_mcaid]
*/

USE [PHClaims];
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
 [id_mcaid]
,[claim_header_id]
,[first_service_date]
,[last_service_date]
,[patient_status]
FROM [final].[mcaid_claim_header]
WHERE [clm_type_mcaid_id] IN (31, 33)
),

[increment_stays_by_person] AS
(
SELECT 
 [id_mcaid]
,[claim_header_id]
-- If prior_last_service_date IS NULL, then it is the first chronological stay for the person
,LAG([last_service_date]) OVER(PARTITION BY [id_mcaid] 
 ORDER BY [first_service_date], [last_service_date], [claim_header_id]) AS prior_last_service_date
,[first_service_date]
,[last_service_date]
,[patient_status]
-- Number of days between consecutive stays
,DATEDIFF(DAY, LAG([last_service_date]) OVER(PARTITION BY [id_mcaid]
 ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) AS [date_diff]
/*
Create a chronological (0, 1) indicator column.
If 0, it is the first stay for the person OR the stay is within 1 day of the prior stay (direct transfer).
If 1, the prior stay is NOT within 1 day of the prior stay (new stay).
This indicator column will be summed to create a stay_id.
*/
,CASE WHEN ROW_NUMBER() OVER(PARTITION BY [id_mcaid] 
      ORDER BY [first_service_date], [last_service_date], [claim_header_id]) = 1 THEN 0
      WHEN DATEDIFF(DAY, LAG([last_service_date]) OVER(PARTITION BY [id_mcaid]
	  ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) <= 1 THEN 0
	  WHEN DATEDIFF(DAY, LAG([last_service_date]) OVER(PARTITION BY [id_mcaid]
	  ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) > 1 THEN 1
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
 [id_mcaid]
,[claim_header_id]
,[prior_last_service_date]
,[first_service_date]
,[last_service_date]
,[patient_status]
,[date_diff]
,[increment]
,SUM([increment]) OVER(PARTITION BY [id_mcaid] 
 ORDER BY [first_service_date], [last_service_date], [claim_header_id] ROWS UNBOUNDED PRECEDING) + 1 AS [episode_id]
FROM [increment_stays_by_person]
)

SELECT 
 [id_mcaid]
,[claim_header_id]
,[prior_last_service_date] AS [prior_claim_last_service_date]
,[first_service_date] AS [claim_first_service_date]
,[last_service_date] AS [claim_last_service_date]
,[patient_status]
,[date_diff]
,[increment]
,[episode_id]
,FIRST_VALUE([first_service_date]) OVER(PARTITION BY [id_mcaid], [episode_id] 
 ORDER BY [first_service_date], [last_service_date], [claim_header_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_first_service_date]
,LAST_VALUE([last_service_date]) OVER(PARTITION BY [id_mcaid], [episode_id] 
 ORDER BY [first_service_date], [last_service_date], [claim_header_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_last_service_date]
,COUNT(*) OVER(PARTITION BY [id_mcaid], [episode_id]) AS [count_stays]
,ROW_NUMBER() OVER(PARTITION BY [id_mcaid], [episode_id] ORDER BY [first_service_date], [last_service_date], [claim_header_id]) AS [stay_id]
,MAX(CASE WHEN [patient_status] = '20' THEN 1 ELSE 0 END) OVER(PARTITION BY [id_mcaid], [episode_id] ORDER BY [id_mcaid], [episode_id]) AS [death_during_stay]
,DENSE_RANK() OVER(ORDER BY [id_mcaid], [episode_id]) AS [acute_inpatient_id]

FROM [create_episode_id];
GO

/*
SELECT * 
FROM [stage].[v_perf_ah_inpatient_direct_transfer]
ORDER BY [id_mcaid], [episode_id], [stay_id];

SELECT
 [acute_inpatient_id]
,[claim_header_id]
,[episode_first_service_date]
,[episode_last_service_date]
,[count_stays]

FROM [stage].[v_perf_ah_inpatient_direct_transfer]
ORDER BY [id_mcaid], [episode_id], [stay_id];


*/
