/*
This view gets acute inpatient stays by
HEDIS DEFINITION: Inpatient Stay VALUE SET EXCLUDING Nonacute Inpatient Stay VALUE SET.
Then the stays within 1 day of each other are connected into an episode.

Author: Philip Sylling
Created: 2019-04-24
Last Modified: 2019-04-24

Returns:
 [id]
,[tcn]
,[prior_claim_to_date], previous claim [to_date] within person, if any
,[claim_from_date], [from_date] from individual claim before creating episode
,[claim_to_date], [to_date] from individual claim before creating episode
,[patient_status], from individual claim before creating episode
,[date_diff], number of days between previous claim [to_date] and current claim [to_date] within person
,[increment], indicator to mark new episode
,[episode_id], created counter to delineate episodes within person
,[episode_from_date], first [from_date] of first claim within episode
,[episode_to_date], last [to_date] of last claim within episode
,[count_stays], number of claims within episode
,[stay_id], created counter to delineate claims within episode
,[death_during_stay], death from [patient_status], anytime during episode

Note Hierarchy:
[stay_id] within [episode_id] within person [id]
*/

USE PHClaims;
GO

IF OBJECT_ID('[stage].[v_perf_pcr_inpatient_direct_transfer]', 'V') IS NOT NULL
DROP VIEW [stage].[v_perf_pcr_inpatient_direct_transfer];
GO
CREATE VIEW [stage].[v_perf_pcr_inpatient_direct_transfer]
AS
/* 
In the future, adapt this code to use admit and discharge dates

SELECT [value_set_name]
      ,[code_system]
      ,COUNT([code])
FROM [ref].[hedis_code_system]
WHERE [value_set_name] IN
('Inpatient Stay'
,'Nonacute Inpatient Stay')
GROUP BY [value_set_name], [code_system]
ORDER BY [value_set_name], [code_system];
*/
WITH [get_acute_inpatient] AS
(
-- Acute Inpatient Discharges
SELECT 
 hd.[id]
,hd.[tcn]
,hd.[from_date]
,hd.[to_date]
,hd.[patient_status]

FROM [dbo].[mcaid_claim_header] AS hd
INNER JOIN [dbo].[mcaid_claim_line] AS ln
ON hd.[tcn] = ln.[tcn]
INNER JOIN [ref].[hedis_code_system] AS hed
ON hed.[value_set_name] IN 
('Inpatient Stay')
AND hed.[code_system] = 'UBREV'
AND ln.[rcode] = hed.[code]

EXCEPT

(
SELECT 
 hd.[id]
,hd.[tcn]
,hd.[from_date]
,hd.[to_date]
,hd.[patient_status]

FROM [dbo].[mcaid_claim_header] AS hd
INNER JOIN [dbo].[mcaid_claim_line] AS ln
ON hd.[tcn] = ln.[tcn]
INNER JOIN [ref].[hedis_code_system] AS hed
ON [value_set_name] IN 
('Nonacute Inpatient Stay')
AND hed.[code_system] = 'UBREV'
AND ln.[rcode] = hed.[code]

UNION

SELECT 
 hd.[id]
,hd.[tcn]
,hd.[from_date]
,hd.[to_date]
,hd.[patient_status]

FROM [dbo].[mcaid_claim_header] AS hd
INNER JOIN [ref].[hedis_code_system] AS hed
ON [value_set_name] IN 
('Nonacute Inpatient Stay')
AND hed.[code_system] = 'UBTOB' 
AND hd.[bill_type_code] = hed.[code]
)),

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
If 1, the prior stay is NOT within 1 day of the next stay (new stay).
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
,ROW_NUMBER() OVER(PARTITION BY [id], [episode_id] ORDER BY [from_date], [to_date], [tcn]) AS [stay_id]
,MAX(CASE WHEN [patient_status] = 'Expired' THEN 1 ELSE 0 END) OVER(PARTITION BY [id], [episode_id] ORDER BY [id], [episode_id]) AS [death_during_stay]
FROM [create_episode_id];
GO

/*
SELECT * 
FROM [stage].[v_perf_pcr_inpatient_direct_transfer]
ORDER BY [id], [episode_id], [stay_id];
*/
