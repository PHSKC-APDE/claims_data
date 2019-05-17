
USE PHClaims;
GO

IF OBJECT_ID('[ref].[age_grp]', 'U') IS NOT NULL
DROP TABLE [ref].[age_grp];
CREATE TABLE [ref].[age_grp]
([age] INT NOT NULL
,[age_grp_0] VARCHAR(20) NULL
,[age_grp_1] VARCHAR(20) NULL
,[age_grp_2] VARCHAR(20) NULL
,[age_grp_3] VARCHAR(20) NULL
,[age_grp_4] VARCHAR(20) NULL
,[age_grp_5] VARCHAR(20) NULL
,[age_grp_6] VARCHAR(20) NULL
,[age_grp_7] VARCHAR(20) NULL
,[age_grp_8] VARCHAR(20) NULL
,[age_grp_9_months] VARCHAR(20) NULL
,[age_grp_10] VARCHAR(20) NULL
,CONSTRAINT [PK_ref_age_grp] PRIMARY KEY CLUSTERED ([age]));
GO

WITH CTE AS
(
SELECT -1 AS age UNION ALL
SELECT 0 AS age UNION ALL
SELECT n AS age FROM [ref].[num] WHERE n BETWEEN 1 AND 250
)

INSERT INTO [ref].[age_grp]
([age]
,[age_grp_0]
,[age_grp_1]
,[age_grp_2]
,[age_grp_3]
,[age_grp_4]
,[age_grp_5]
,[age_grp_6]
,[age_grp_7]
,[age_grp_8]
,[age_grp_9_months]
,[age_grp_10])

SELECT
 [age]
,CASE WHEN [age] BETWEEN 0 AND 4 THEN 'Age 0-4' WHEN [age] BETWEEN 5 AND 11 THEN 'Age 5-11' WHEN [age] BETWEEN 12 AND 17 THEN 'Age 12-17' WHEN [age] BETWEEN 18 AND 24 THEN 'Age 18-24' WHEN [age] BETWEEN 25 AND 44 THEN 'Age 25-44' WHEN [age] BETWEEN 45 AND 64 THEN 'Age 45-64' WHEN [age] >= 65 THEN 'Age 65+'
 END AS [age_grp_0]
,CASE WHEN [age] >= 18 THEN 'Age 18+'
 END AS [age_grp_1]
,CASE WHEN [age] BETWEEN 0 AND 17 THEN 'Age 0-17' WHEN [age] BETWEEN 18 AND 64 THEN 'Age 18-64' WHEN [age] >= 65 THEN 'Age 65+'
 END AS [age_grp_2]
,CASE WHEN [age] >= 13 THEN 'Age 13+'
 END AS [age_grp_3]
,CASE WHEN [age] >= 6 THEN 'Age 6+'
 END AS [age_grp_4]
,CASE WHEN [age] BETWEEN 6 AND 17 THEN 'Age 6-17' WHEN [age] BETWEEN 18 AND 64 THEN 'Age 18-64' WHEN [age] >= 65 THEN 'Age 65+'
 END AS [age_grp_5]
,CASE WHEN [age] BETWEEN 12 AND 17 THEN 'Age 12-17' WHEN [age] BETWEEN 18 AND 64 THEN 'Age 18-64' WHEN [age] >= 65 THEN 'Age 65+'
 END AS [age_grp_6]
,CASE WHEN [age] BETWEEN 18 AND 64 THEN 'Age 18-64' WHEN [age] >= 65 THEN 'Age 65+'
 END AS [age_grp_7]
,CASE WHEN [age] BETWEEN 18 AND 64 THEN 'Age 18-64'
 END AS [age_grp_8]
,CASE WHEN [age] BETWEEN 0 AND 11 THEN 'Age 0-11 Months' WHEN [age] BETWEEN 12 AND 24 THEN 'Age 12-24 Months' WHEN [age] BETWEEN 25 AND 83 THEN 'Age 25 Months-6' WHEN [age] BETWEEN 84 AND 143 THEN 'Age 7-11' WHEN [age] BETWEEN 144 AND 239 THEN 'Age 12-19'
 END AS [age_grp_9_months]
,CASE WHEN [age] BETWEEN 5 AND 11 THEN 'Age 5-11' WHEN [age] BETWEEN 12 AND 18 THEN 'Age 12-18' WHEN [age] BETWEEN 19 AND 50 THEN 'Age 19-50' WHEN [age] BETWEEN 51 AND 64 THEN 'Age 51-64'
 END AS [age_grp_10]
FROM CTE;

SELECT * FROM [ref].[age_grp];