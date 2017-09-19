-- Code to process the Medicaid eligibility table from HCA
-- Public Health — Seattle & King County
-- Alastair Matheson
-- 2017-05

/*
This code aims to reorganize and consolidate the Medicaid eligibility data from the WA Healthcare Authority

Table 01 - Overall eligibility regardless of RAC/address/etc., single row per person per coverage period
Table 02 - Eligibility accounting for RAC and address (note, need to reference cleaned address table)

*/

-- Overall eligibility
-- Only looks at continuous coverage for an ID/SSN combo

--EXEC sp_addlinkedserver @server = 'KCITSQLPRPDBM50'
--:CONNECT KCITSQLUTPDBH51
--:CONNECT KCITSQLPRPDBM50

--IF OBJECT_ID('dbo.elig_overall', 'U') IS NOT NULL 
--DROP TABLE dbo.elig_overall;

--Alastair's original code (with table drop/create statements commented out)

SELECT
	dt_2.MEDICAID_RECIPIENT_ID, dt_2.SOCIAL_SECURITY_NMBR,
	dt_2.startdate, dt_2.enddate,
	DATEDIFF(mm, startdate, enddate) + 1 AS cov_time_mth
	--INTO PHClaims.dbo.elig_overall
	FROM (
	SELECT
		dt_1.MEDICAID_RECIPIENT_ID, dt_1.SOCIAL_SECURITY_NMBR,
		-- Want to move the person's end date to the end of the month so use DATEADD below
		MIN(calmonth) AS startdate, DATEADD(day, -1, DATEADD(month, 1, MAX(calmonth))) AS enddate,
		dt_1.group_num
		  FROM (
		  SELECT
			DISTINCT CONVERT(datetime, CLNDR_YEAR_MNTH + '01', 112) AS calmonth,
			x.MEDICAID_RECIPIENT_ID, x.SOCIAL_SECURITY_NMBR, 
			DATEDIFF(MONTH, 0, CONVERT(datetime, CLNDR_YEAR_MNTH + '01', 112)) - ROW_NUMBER() 
				OVER(PARTITION BY x.MEDICAID_RECIPIENT_ID, x.SOCIAL_SECURITY_NMBR
				ORDER BY CONVERT(datetime, CLNDR_YEAR_MNTH + '01', 112)) AS 'group_num'
					FROM (
						SELECT DISTINCT y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, y.CLNDR_YEAR_MNTH
						FROM [PHClaims].[dbo].[NewEligibility] y
					) AS x
			) AS dt_1
		GROUP BY MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, group_num
	) AS dt_2
	ORDER BY  MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, startdate, enddate

----------------BREAKING DOWN THE CODE-------------------------------------

--Block one
--Purpose: select distinct rows of Medicaid ID, SSN, calendar year month and set this as data set "y"

SELECT DISTINCT y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, y.CLNDR_YEAR_MNTH
FROM (
	SELECT TOP 10000 z.MEDICAID_RECIPIENT_ID, z.SOCIAL_SECURITY_NMBR, z.CLNDR_YEAR_MNTH
	FROM [PHClaims].[dbo].[NewEligibility] as z
	) as y

--Block two
--Purpose: Assign a unique group # to each calendar month

SELECT DISTINCT CONVERT(datetime, CLNDR_YEAR_MNTH + '01', 112) AS calmonth, /*create new var for elig month: add day (1), and format as yyyymmdd */
x.MEDICAID_RECIPIENT_ID, x.SOCIAL_SECURITY_NMBR, /*select distinct ID and SSN from x data table */
DATEDIFF(MONTH, 0, CONVERT(datetime, CLNDR_YEAR_MNTH + '01', 112)) /* number of months between 0 and elig month */ - ROW_NUMBER()
	OVER(PARTITION BY x.MEDICAID_RECIPIENT_ID, x.SOCIAL_SECURITY_NMBR /*groups data set by combos of ID and SSN, these are the groups that row # is assigned to */
	ORDER BY CONVERT(datetime, CLNDR_YEAR_MNTH + '01', 112)) AS 'group_num' /* tells SQL in which order to apply the row number result, and then creates a new column to hold this */
FROM (
	SELECT DISTINCT y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, y.CLNDR_YEAR_MNTH
	FROM (
		SELECT TOP 10000 z.MEDICAID_RECIPIENT_ID, z.SOCIAL_SECURITY_NMBR, z.CLNDR_YEAR_MNTH
		FROM [PHClaims].[dbo].[NewEligibility] as z
		) as y
	) as x

--Block three
--Purpose: Group rows by ID, SSN and group number (i.e. calendar month) and then create columns for start and end date of eligibility period

SELECT dt_1.MEDICAID_RECIPIENT_ID, dt_1.SOCIAL_SECURITY_NMBR,
-- Want to move the person's end date to the end of the month so use DATEADD below
MIN(calmonth) AS startdate, DATEADD(day, -1, DATEADD(month, 1, MAX(calmonth))) AS enddate,
dt_1.group_num
FROM (
	SELECT DISTINCT CONVERT(datetime, CLNDR_YEAR_MNTH + '01', 112) AS calmonth, /*create new var for elig month: add day (1), and format as yyyymmdd */
	x.MEDICAID_RECIPIENT_ID, x.SOCIAL_SECURITY_NMBR, /*select distinct ID and SSN from x data table */
	DATEDIFF(MONTH, 0, CONVERT(datetime, CLNDR_YEAR_MNTH + '01', 112)) /* number of months between 0 and elig month */ - ROW_NUMBER()
		OVER(PARTITION BY x.MEDICAID_RECIPIENT_ID, x.SOCIAL_SECURITY_NMBR /*groups data set by combos of ID and SSN, these are the groups that row # is assigned to */
		ORDER BY CONVERT(datetime, CLNDR_YEAR_MNTH + '01', 112)) AS 'group_num' /* tells SQL in which order to apply the row number result, and then creates a new column to hold this */
	FROM (
		SELECT DISTINCT y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, y.CLNDR_YEAR_MNTH
		FROM (
			SELECT TOP 10000 z.MEDICAID_RECIPIENT_ID, z.SOCIAL_SECURITY_NMBR, z.CLNDR_YEAR_MNTH
			FROM [PHClaims].[dbo].[NewEligibility] as z
		) as y
	) as x
) AS dt_1
GROUP BY MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, group_num

--Block four (all code)
--Purpose: Only addition is to add a column for the number of coverage months for each person
SELECT dt_2.MEDICAID_RECIPIENT_ID, dt_2.SOCIAL_SECURITY_NMBR, dt_2.startdate, dt_2.enddate, DATEDIFF(mm, startdate, enddate) + 1 AS cov_time_mth
	--INTO PHClaims.dbo.elig_overall
FROM (
SELECT dt_1.MEDICAID_RECIPIENT_ID, dt_1.SOCIAL_SECURITY_NMBR,
-- Want to move the person's end date to the end of the month so use DATEADD below
MIN(calmonth) AS startdate, DATEADD(day, -1, DATEADD(month, 1, MAX(calmonth))) AS enddate,
dt_1.group_num
FROM (
	SELECT DISTINCT CONVERT(datetime, CLNDR_YEAR_MNTH + '01', 112) AS calmonth, /*create new var for elig month: add day (1), and format as yyyymmdd */
		x.MEDICAID_RECIPIENT_ID, x.SOCIAL_SECURITY_NMBR, /*select distinct ID and SSN from x data table */
		DATEDIFF(MONTH, 0, CONVERT(datetime, CLNDR_YEAR_MNTH + '01', 112)) /* number of months between 0 and elig month */ - ROW_NUMBER()
			OVER(PARTITION BY x.MEDICAID_RECIPIENT_ID, x.SOCIAL_SECURITY_NMBR /*groups data set by combos of ID and SSN, these are the groups that row # is assigned to */
			ORDER BY CONVERT(datetime, CLNDR_YEAR_MNTH + '01', 112)) AS 'group_num' /* tells SQL in which order to apply the row number result, and then creates a new column to hold this */
		FROM (
			SELECT DISTINCT y.MEDICAID_RECIPIENT_ID, y.SOCIAL_SECURITY_NMBR, y.CLNDR_YEAR_MNTH
			FROM (
				SELECT TOP 10000 z.MEDICAID_RECIPIENT_ID, z.SOCIAL_SECURITY_NMBR, z.CLNDR_YEAR_MNTH
				FROM [PHClaims].[dbo].[NewEligibility] as z
			) as y
		) as x
	) AS dt_1
	GROUP BY MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, group_num
) AS dt_2
ORDER BY  MEDICAID_RECIPIENT_ID, SOCIAL_SECURITY_NMBR, startdate, enddate