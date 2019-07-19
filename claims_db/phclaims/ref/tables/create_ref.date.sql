/*
This script creates a [ref].[date] reference table for time aggregation date 
lookups (Weekends, Holidays).

Adapted from Aaron Bertrand's script
https://www.mssqltips.com/sqlservertip/4054/creating-a-date-dimension-or-calendar-table-in-sql-server/

Author: Philip Sylling
Created: 2019-07-19

Returns:
SELECT 
 [date]
,[year_month_day] -- integer format
,[day]
,[day_suffix]
,[week_day_name]
,[is_weekend]
,[is_holiday]
,[holiday_name]
,[dow_in_month] -- e.g., first Thursday, third Monday (for Holiday calculations)
,[day_of_year]
,[week_of_month]
,[week_of_year]
,[month]
,[month_name]
,[year_month] -- integer format
,[first_day_month]
,[last_day_month]
,[quarter]
,[quarter_name]
,[year_quarter] -- integer format
,[first_day_quarter]
,[last_day_quarter]
,[year]
,[first_day_year]
,[last_day_year]
*/

USE PHClaims;
GO

DECLARE @StartDate DATE = '19900101', @NumberOfYears INT = 40;

-- prevent set or regional settings from interfering with 
-- interpretation of dates / literals

SET DATEFIRST 7;
SET DATEFORMAT mdy;
--SET LANGUAGE US_ENGLISH;

DECLARE @CutoffDate DATE = DATEADD(YEAR, @NumberOfYears, @StartDate);

-- this is just a holding table for intermediate calculations:
IF OBJECT_ID('tempdb..#dim') IS NOT NULL
DROP TABLE #dim;
CREATE TABLE #dim
([date]			DATE PRIMARY KEY
,[day]			AS DATEPART(DAY, [date])
,[month]		AS DATEPART(MONTH, [date])
,[FirstOfMonth] AS CONVERT(DATE, DATEADD(MONTH, DATEDIFF(MONTH, 0, [date]), 0))
,[MonthName]	AS DATENAME(MONTH, [date])
,[week]			AS DATEPART(WEEK, [date])
,[ISOweek]		AS DATEPART(ISO_WEEK, [date])
,[DayOfWeek]	AS DATEPART(WEEKDAY, [date])
,[quarter]		AS DATEPART(QUARTER, [date])
,[year]			AS DATEPART(YEAR, [date])
,[FirstOfYear]  AS CONVERT(DATE, DATEADD(YEAR, DATEDIFF(YEAR, 0, [date]), 0))
,[Style112]     AS CONVERT(CHAR(8), [date], 112)
,[Style101]     AS CONVERT(CHAR(10), [date], 101)
);

-- use the catalog views to generate as many rows as we need

INSERT #dim([date]) 
SELECT d
FROM
(
  SELECT d = DATEADD(DAY, rn - 1, @StartDate)
  FROM 
  (
    SELECT TOP (DATEDIFF(DAY, @StartDate, @CutoffDate)) 
      rn = ROW_NUMBER() OVER (ORDER BY s1.[object_id])
    FROM sys.all_objects AS s1
    CROSS JOIN sys.all_objects AS s2
    -- on my system this would support > 5 million days
    ORDER BY s1.[object_id]
  ) AS x
) AS y;

IF OBJECT_ID('[ref].[date]') IS NOT NULL
DROP TABLE [ref].[date];
CREATE TABLE [ref].[date]
([date]					DATE        NOT NULL
,[year_month_day]       INT         NOT NULL
,[day]					TINYINT     NOT NULL
,[day_suffix]           CHAR(2)     NOT NULL
,[week_day_name]        VARCHAR(10) NOT NULL
,[is_weekend]           VARCHAR(10) NOT NULL
,[is_holiday]           VARCHAR(20) NOT NULL
,[holiday_name]         VARCHAR(100) NULL
,[dow_in_month]         TINYINT     NOT NULL
,[day_of_year]			SMALLINT    NOT NULL
,[week_of_month]        TINYINT     NOT NULL
,[week_of_year]         TINYINT     NOT NULL
,[month]				TINYINT     NOT NULL
,[month_name]			VARCHAR(10) NOT NULL
,[year_month]			INT			NOT NULL
,[first_day_month]		DATE        NOT NULL
,[last_day_month]		DATE        NOT NULL
,[quarter]				TINYINT     NOT NULL
,[quarter_name]         VARCHAR(6)  NOT NULL
,[year_quarter]			INT         NOT NULL
,[first_day_quarter]	DATE        NOT NULL
,[last_day_quarter]		DATE        NOT NULL
,[year]					INT         NOT NULL
,[first_day_year]		DATE        NOT NULL
,[last_day_year]		DATE        NOT NULL
,CONSTRAINT [PK_ref_date] PRIMARY KEY CLUSTERED ([date]))
ON [PRIMARY];
GO

INSERT [ref].[date] WITH (TABLOCKX)
SELECT
 [date]					= [date]
,[year_month_day]		= CONVERT(INT, [Style112])
,[day]					= CONVERT(TINYINT, [day])
,[day_suffix]			= CONVERT(CHAR(2), CASE WHEN [day] / 10 = 1 THEN 'th' 
												ELSE CASE RIGHT([day], 1) WHEN '1' THEN 'st'
																	      WHEN '2' THEN 'nd'
																	      WHEN '3' THEN 'rd' 
																	      ELSE 'th' END END)
,[week_day_name]		= CONVERT(VARCHAR(10), DATENAME(WEEKDAY, [date]))
,[is_weekend]			= CONVERT(VARCHAR(10), CASE WHEN [DayOfWeek] IN (1,7) THEN 'Weekend' ELSE 'Weekday' END)
,[is_holiday]			= CONVERT(VARCHAR(20), 'Non-Holiday')
,[holiday_name]			= CONVERT(VARCHAR(64), NULL)
,[dow_in_month]			= CONVERT(TINYINT, ROW_NUMBER() OVER(PARTITION BY [FirstOfMonth], [DayOfWeek] ORDER BY [date]))
,[day_of_year]			= CONVERT(SMALLINT, DATEPART(DAYOFYEAR, [date]))
,[week_of_month]		= CONVERT(TINYINT, DENSE_RANK() OVER(PARTITION BY [year], [month] ORDER BY [week]))
,[week_of_year]			= CONVERT(TINYINT, [week])
,[month]				= CONVERT(TINYINT, [month])
,[month_name]			= CONVERT(VARCHAR(10), [MonthName])
,[year_month]			= CONVERT(INT, [year] * 100 + [month])
,[first_day_month]		= [FirstOfMonth]
,[last_day_month]		= MAX([date]) OVER (PARTITION BY [year], [month])
,[quarter]				= CONVERT(TINYINT, [quarter])
,[quarter_name]			= CONVERT(VARCHAR(6), CASE [quarter] WHEN 1 THEN 'First' WHEN 2 THEN 'Second' WHEN 3 THEN 'Third' WHEN 4 THEN 'Fourth' END)
,[year_quarter]			= CONVERT(INT, [year] * 100 + [quarter])
,[first_day_quarter]	= MIN([date]) OVER (PARTITION BY [year], [quarter])
,[last_day_quarter]		= MAX([date]) OVER (PARTITION BY [year], [quarter])
,[year]					= [year]
,[first_day_year]		= [FirstOfYear]
,[last_day_year]		= MAX([date]) OVER (PARTITION BY [year])
FROM #dim;

;WITH x AS 
(
SELECT 
 [year_month_day]
,[date]
,[is_holiday]
,[holiday_name]
,[first_day_year]
,[dow_in_month]
,[month_name]
,[week_day_name]
,[day]
,[last_dow_in_month] = ROW_NUMBER() OVER(PARTITION BY [first_day_month], [week_day_name] ORDER BY [date] DESC)
FROM [ref].[date]
)

UPDATE x 
SET 
 [is_holiday] = 'Holiday'
,[holiday_name] =

CASE
WHEN ([date] = [first_day_year]) THEN 'New Year''s Day'
-- (3rd Monday in January)
WHEN ([dow_in_month] = 3 AND [month_name] = 'January' AND [week_day_name] = 'Monday') THEN 'Martin Luther King Day'
-- (3rd Monday in February)
WHEN ([dow_in_month] = 3 AND [month_name] = 'February' AND [week_day_name] = 'Monday') THEN 'President''s Day'
-- (last Monday in May)
WHEN ([last_dow_in_month] = 1 AND [month_name] = 'May' AND [week_day_name] = 'Monday') THEN 'Memorial Day'
-- (July 4th)
WHEN ([month_name] = 'July' AND [day] = 4) THEN 'Independence Day'
-- (first Monday in September)
WHEN ([dow_in_month] = 1 AND [month_name] = 'September' AND [week_day_name] = 'Monday') THEN 'Labor Day'
-- Columbus Day (second Monday in October)
WHEN ([dow_in_month] = 2 AND [month_name] = 'October' AND [week_day_name] = 'Monday') THEN 'Columbus Day'
-- Veterans' Day (November 11th)
WHEN ([month_name] = 'November' AND [day] = 11) THEN 'Veterans'' Day'
-- Thanksgiving Day (fourth Thursday in November)
WHEN ([dow_in_month] = 4 AND [month_name] = 'November' AND [week_day_name] = 'Thursday') THEN 'Thanksgiving Day'
WHEN ([month_name] = 'December' AND [day] = 25) THEN 'Christmas Day'
END

WHERE 
([date] = first_day_year) OR
([dow_in_month] = 3 AND [month_name] = 'January' AND [week_day_name] = 'Monday') OR
([dow_in_month] = 3 AND [month_name] = 'February' AND [week_day_name] = 'Monday') OR
([last_dow_in_month] = 1 AND [month_name] = 'May' AND [week_day_name] = 'Monday') OR
([month_name] = 'July' AND [day] = 4) OR
([dow_in_month] = 1 AND [month_name] = 'September' AND [week_day_name] = 'Monday') OR
([dow_in_month] = 2 AND [month_name] = 'October' AND [week_day_name] = 'Monday') OR
([month_name] = 'November' AND [day] = 11) OR
([dow_in_month] = 4 AND [month_name] = 'November' AND [week_day_name] = 'Thursday') OR
([month_name] = 'December' AND [day] = 25);

SELECT * FROM [ref].[date] ORDER BY [date];
