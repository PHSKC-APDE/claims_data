-- Code to create a SQL table dbo.mcaid_elig_timevar, which contains the from_ and to_dates for Medicaid enrollment over contiguous
--		dual eligibility, address, and RAC code for each ID (i.e., time-varying factors of interest)
-- Alastair Matheson and Eli Kern
-- 2018-09

-- Takes ~10m to run

-- Remove existing table if present
IF object_id('[PHClaims].[dbo].[mcaid_elig_timevar_load]') IS NOT NULL
	DROP TABLE [PHClaims].[dbo].[mcaid_elig_timevar_load]

-- Select variables for loading

-- Collapse rows one last time and select variables for loading
SELECT 
	CAST(j.id as varchar(200)) AS 'id',
	CAST(MIN(j.from_date) as date) AS 'from_date',
	CAST(MAX(j.to_date) as date) AS 'to_date',
	CAST(j.dual AS VARCHAR(200)) AS 'dual',
	CAST(j.rac_code AS VARCHAR(200)) AS 'rac_code',
	CAST(j.add1_new AS VARCHAR(2000)) AS 'add1_new',
	CAST(j.city_new AS VARCHAR(2000)) AS 'city_new',
	CAST(j.state_new AS VARCHAR(2000)) AS 'state_new',
	CAST(j.zip_new AS VARCHAR(2000)) AS 'zip_new',
	DATEDIFF(dd, MIN(j.from_date), MAX(j.to_date)) + 1 as cov_time_day

	INTO [PHClaims].[dbo].[mcaid_elig_timevar_load]

-- Identify rows that can be further collapsed
		FROM (
			SELECT i.id, i.dual, i.add1_new, i.city_new, i.state_new, i.zip_new, i.rac_code, i.from_date, i.to_date, i.group_num2,
				SUM(CASE WHEN i.group_num2 IS NULL THEN 0 ELSE 1 END) OVER
					(PARTITION BY i.id, i.dual, i.add1_new, i.city_new, i.state_new, i.zip_new, i.rac_code
						ORDER BY i.temp_row ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS group_num3

-- Collapse contiguous periods of coverage to a single row
			FROM (
				SELECT h.id, h.dual, h.add1_new, h.city_new, h.state_new, h.zip_new, h.rac_code, h.from_date, h.to_date,
					CASE
						WHEN h.from_date - lag(h.to_date) OVER
							(PARTITION BY h.id, h.dual, h.add1_new, h.city_new, h.state_new, h.zip_new, h.rac_code
								ORDER BY h.id, h.from_date) <= 1 THEN NULL
						ELSE row_number() OVER 
							(PARTITION BY h.id, h.dual, h.add1_new, h.city_new, h.state_new, h.zip_new, h.rac_code 
								ORDER BY h.from_date)
						END AS group_num2,
					row_number() OVER 
						(PARTITION BY h.id, h.dual, h.add1_new, h.city_new, h.state_new, h.zip_new, h.rac_code 
							ORDER BY h.id, h.from_date, h.to_date) AS temp_row

-- Find the min and max dates for a block of rows where all variables are the same
				FROM (
					SELECT g.id, g.dual, g.add1_new, g.city_new, g.state_new, g.zip_new, g.rac_code, g.from_date, g.to_date,
						MIN(g.from_date) OVER 
							(PARTITION BY g.id, g.dual, g.add1_new, g.city_new, g.state_new, g.zip_new, g.rac_code 
								ORDER BY g.id, g.from_date, g.to_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS 'min_from',
						MAX(g.to_date) OVER 
							(PARTITION BY g.id, g.dual,g.add1_new, g.city_new, g.state_new, g.zip_new, g.rac_code 
								ORDER BY g.id, g.from_date, g.to_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS 'max_to'
                                 
-- Incorporate sub-month coverage
					FROM (
						SELECT f.id, f.dual, f.add1_new, f.city_new, f.state_new, f.zip_new, f.rac_code, f.group_num,
							CASE 
								WHEN f.startdate >= f.fromdate THEN f.startdate
								WHEN f.startdate < f.fromdate THEN f.fromdate
								ELSE null
								END AS from_date,	
							CASE 
								WHEN f.enddate <= f.todate THEN f.enddate
								WHEN f.enddate > f.todate THEN f.todate
								ELSE null
								END AS to_date
                                   
-- Make a start and end date for that month
						FROM (
							SELECT e.id, e.dual, e.add1_new, e.city_new, e.state_new, e.zip_new, e.rac_code,
								MIN(calmonth) AS startdate, dateadd(day, - 1, dateadd(month, 1, MAX(calmonth))) AS enddate,
								e.group_num, e.fromdate, e.todate
                                     
-- Remove any duplicate rows
							FROM (
								SELECT DISTINCT d.id, d.dual, d.add1_new, d.city_new, d.state_new, d.zip_new, d.rac_code,
									d.calmonth, d.group_num, d.fromdate, d.todate
                                       
-- Assign a number to months to identify contiguous periods
								FROM (
									SELECT DISTINCT c.id, c.dual, c.add1_new, c.city_new, c.state_new, c.zip_new, c.rac_code,
										c.calmonth, c.fromdate, c.todate,
										DATEDIFF(month, 0, calmonth) - row_number() OVER 
											(PARTITION BY c.id, c.dual, c.add1_new, c.city_new, c.state_new, c.zip_new, c.rac_code 
												ORDER BY calmonth) AS group_num   
										
-- Keep useful time-varying data		                                      
									FROM (
										SELECT DISTINCT a.id, a.dual, CONVERT(DATETIME, CAST(a.CLNDR_YEAR_MNTH as varchar(200)) + '01', 112) AS calmonth,
											a.fromdate, a.todate, b.add1_new, b.city_new, b.state_new, IIF(b.zip_new IS NULL, a.zip, b.zip_new) AS zip_new,
											a.rac_code     
										  
-- Pull columns from raw data
										FROM (
											SELECT MEDICAID_RECIPIENT_ID AS 'id', DUAL_ELIG AS 'dual', CLNDR_YEAR_MNTH,
												FROM_DATE AS 'fromdate', TO_DATE AS 'todate',
												RSDNTL_ADRS_LINE_1 AS 'add1', RSDNTL_ADRS_LINE_2 AS 'add2',
												RSDNTL_CITY_NAME as 'city',
												RSDNTL_STATE_CODE AS 'state', RSDNTL_POSTAL_CODE AS 'zip',
												RAC_CODE AS 'rac_code'
                                             FROM [PHClaims].[dbo].[mcaid_elig_raw]
											 ) a
-- Join to cleaned up addresses 
                                           LEFT JOIN (
                                             SELECT add1, add2, city, state, zip, add1_new, city_new, state_new, zip_new
                                             FROM [PHClaims].[dbo].[mcaid_elig_address_clean]
											 ) b
											ON
											(a.add1 = b.add1 OR (a.add1 IS NULL AND b.add1 IS NULL)) AND
											(a.add2 = b.add2 OR (a.add2 IS NULL AND b.add2 IS NULL)) AND 
											(a.city = b.city OR (a.city IS NULL AND b.city IS NULL)) AND 
											(a.state = b.state OR (a.state IS NULL AND b.state IS NULL)) AND 
											(a.zip = b.zip OR (a.zip IS NULL AND b.zip IS NULL))
											) c
									 ) d
                             ) e
                             GROUP BY e.id, e.dual, e.add1_new, e.city_new, e.state_new, e.zip_new, e.rac_code, e.group_num, e.fromdate, e.todate
                         ) f
                     ) g
                 ) h
                 WHERE h.from_date >= h.min_from AND h.to_date = h.max_to
             ) i
       ) j
       GROUP BY j.id, j.dual, j.add1_new, j.city_new, j.state_new, j.zip_new, j.rac_code, j.group_num3
       ORDER BY j.id, from_date