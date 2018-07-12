-- Code to create a SQL table dbo.mcaid_elig_address which holds clean and geocoded address information for each ID and time period combination
-- Alastair Matheson and Eli Kern
-- 2018-07

-- Takes ~17m to run

-- Remove existing table if present
IF object_id('[PHClaims].[dbo].[mcaid_elig_address]') IS NOT NULL
	DROP TABLE [PHClaims].[dbo].[mcaid_elig_address]

-- Select variables for loading
SELECT k.*,
	l.add_geocoded,
	l.zip_geocoded,
	l.add_type,
	l.check_esri,
	l.check_opencage,
	l.geocode_source,
	l.zip_centroid,
	l.x,
	l.y,
	l.lon,
	l.lat,
	l.statefp10,
	l.countyfp10,
	l.tractce10,
	l.blockce10,
	l.block_geoid10,
	l.pumace10,
	l.puma_geoid10,
	l.puma_name,
	l.zcta5ce10,
	l.zcta_geoid10,
	l.hra_id,
	l.hra,
	l.region_id,
	l.region,
	l.school_geoid10,
	l.school,
	l.kcc_dist,
	l.wa_legdist,
	l.scc_dist
INTO [PHClaims].[dbo].[mcaid_elig_address]
FROM (
	-- Collapse to single row again (2nd and final time given we have now removed nested periods)
	SELECT cast(j.id AS VARCHAR(200)) AS 'id',
		cast(min(j.from_date) AS DATE) AS 'from_date',
		cast(max(j.to_date) AS DATE) AS 'to_date',
		cast(j.add1_new AS VARCHAR(2000)) AS 'add1_new',
		cast(j.add2_new AS VARCHAR(2000)) AS 'add2_new',
		cast(j.city_new AS VARCHAR(2000)) AS 'city_new',
		cast(j.state_new AS VARCHAR(2000)) AS 'state_new',
		cast(j.zip_new AS VARCHAR(2000)) AS 'zip_new',
		cast(j.cntyfips_new AS VARCHAR(2000)) AS 'cntyfips_new',
		cast(j.cntyname_new AS VARCHAR(2000)) AS 'cntyname_new',
		cast(j.confidential AS VARCHAR(2000)) AS 'confidential',
		cast(j.homeless AS VARCHAR(2000)) AS 'homeless',
		cast(j.mailbox AS VARCHAR(2000)) AS 'mailbox',
		cast(j.care_of AS VARCHAR(2000)) AS 'care_of',
		cast(j.overridden AS VARCHAR(2000)) AS 'overridden',
		datediff(dd, min(j.from_date), max(j.to_date)) + 1 AS 'cov_time_day'
	FROM (
		-- Set up groups where there is contiguous coverage (2nd time around given we have now removed nested periods)
		SELECT i.id,
			i.add1_new,
			i.add2_new,
			i.city_new,
			i.state_new,
			i.zip_new,
			i.cntyfips_new,
			i.cntyname_new,
			i.confidential,
			i.homeless,
			i.mailbox,
			i.care_of,
			i.overridden,
			i.from_date,
			i.to_date,
			i.group_num2,
			sum(CASE WHEN i.group_num2 IS NULL THEN 0 ELSE 1 END) OVER (
				PARTITION BY i.id,
				i.add1_new,
				i.add2_new,
				i.city_new,
				i.state_new,
				i.zip_new,
				i.cntyfips_new,
				i.cntyname_new,
				i.confidential,
				i.homeless,
				i.mailbox,
				i.care_of,
				i.overridden ORDER BY i.temp_row rows BETWEEN unbounded preceding
						AND CURRENT row
				) AS group_num3
		FROM (
			-- Set up flag for when there is a break in coverage, and drop nested time periods
			SELECT h.id,
				h.add1_new,
				h.add2_new,
				h.city_new,
				h.state_new,
				h.zip_new,
				h.cntyfips_new,
				h.cntyname_new,
				h.confidential,
				h.homeless,
				h.mailbox,
				h.care_of,
				h.overridden,
				h.from_date,
				h.to_date,
				CASE WHEN h.from_date - lag(h.to_date) OVER (
							PARTITION BY h.id,
							h.add1_new,
							h.add2_new,
							h.city_new,
							h.state_new,
							h.zip_new,
							h.cntyfips_new,
							h.cntyname_new,
							h.confidential,
							h.homeless,
							h.mailbox,
							h.care_of,
							h.overridden ORDER BY h.id,
								h.from_date
							) <= 1 THEN NULL ELSE row_number() OVER (
							PARTITION BY h.id,
							h.add1_new,
							h.add2_new,
							h.city_new,
							h.state_new,
							h.zip_new,
							h.cntyfips_new,
							h.cntyname_new,
							h.confidential,
							h.homeless,
							h.mailbox,
							h.care_of,
							h.overridden ORDER BY h.from_date
							) END AS group_num2,
				row_number() OVER (
					PARTITION BY h.id,
					h.add1_new,
					h.add2_new,
					h.city_new,
					h.state_new,
					h.zip_new,
					h.cntyfips_new,
					h.cntyname_new,
					h.confidential,
					h.homeless,
					h.mailbox,
					h.care_of,
					h.overridden ORDER BY h.id,
						h.from_date,
						h.to_date
					) AS temp_row
			FROM (
				--Flag nested time periods (occurs due to multiple RACs with overlapping time)
				SELECT g.id,
					g.add1_new,
					g.add2_new,
					g.city_new,
					g.state_new,
					g.zip_new,
					g.cntyfips_new,
					g.cntyname_new,
					g.confidential,
					g.homeless,
					g.mailbox,
					g.care_of,
					g.overridden,
					g.from_date,
					g.to_date,
					--Sorting by ID, from_date and to_date (descending so tied from_dates have most recent to_date listed first), 
					--go down rows and find minimum from date thus far
					min(g.from_date) OVER (
						PARTITION BY g.id,
						g.add1_new,
						g.add2_new,
						g.city_new,
						g.state_new,
						g.zip_new,
						g.cntyfips_new,
						g.cntyname_new,
						g.confidential,
						g.homeless,
						g.mailbox,
						g.care_of,
						g.overridden ORDER BY g.id,
							g.from_date,
							g.to_date DESC rows BETWEEN unbounded preceding
								AND CURRENT row
						) AS 'min_from',
					--Sorting by ID, from_date and to_date (descending so tied from_dates have most recent to_date listed first), 
					--go down rows and find maximum to date thus far
					max(g.to_date) OVER (
						PARTITION BY g.id,
						g.add1_new,
						g.add2_new,
						g.city_new,
						g.state_new,
						g.zip_new,
						g.cntyfips_new,
						g.cntyname_new,
						g.confidential,
						g.homeless,
						g.mailbox,
						g.care_of,
						g.overridden ORDER BY g.id,
							g.from_date,
							g.to_date DESC rows BETWEEN unbounded preceding
								AND CURRENT row
						) AS 'max_to'
				FROM (
					-- Use the from and to date info to find sub-month coverage
					SELECT f.id,
						f.add1_new,
						f.add2_new,
						f.city_new,
						f.state_new,
						f.zip_new,
						f.cntyfips_new,
						f.cntyname_new,
						f.confidential,
						f.homeless,
						f.mailbox,
						f.care_of,
						f.overridden,
						f.group_num,
						--recreate from_date
						CASE WHEN f.startdate >= f.fromdate THEN f.startdate WHEN f.startdate < f.fromdate THEN f.fromdate ELSE NULL 
							END AS from_date,
						--recreate to_date
						CASE WHEN f.enddate <= f.todate THEN f.enddate WHEN f.enddate > f.todate THEN f.todate ELSE NULL END AS to_date
					FROM (
						-- Now take the max and min of each ID/contiguous date combo to collapse to one row
						SELECT e.id,
							e.add1_new,
							e.add2_new,
							e.city_new,
							e.state_new,
							e.zip_new,
							e.cntyfips_new,
							e.cntyname_new,
							e.confidential,
							e.homeless,
							e.mailbox,
							e.care_of,
							e.overridden,
							min(calmonth) AS startdate,
							dateadd(day, - 1, dateadd(month, 1, max(calmonth))) AS enddate,
							e.group_num,
							e.fromdate,
							e.todate
						FROM (
							-- Keep just the variables formed in the select statement below
							SELECT DISTINCT d.id,
								d.add1_new,
								d.add2_new,
								d.city_new,
								d.state_new,
								d.zip_new,
								d.cntyfips_new,
								d.cntyname_new,
								d.confidential,
								d.homeless,
								d.mailbox,
								d.care_of,
								d.overridden,
								d.calmonth,
								d.group_num,
								d.fromdate,
								d.todate
							FROM (
								-- This sets assigns a contiguous set of months to the same group number per id
								SELECT DISTINCT c.id,
									c.add1_new,
									c.add2_new,
									c.city_new,
									c.state_new,
									c.zip_new,
									c.cntyfips_new,
									c.cntyname_new,
									c.confidential,
									c.homeless,
									c.mailbox,
									c.care_of,
									c.overridden,
									c.calmonth,
									c.fromdate,
									c.todate,
									datediff(month, 0, calmonth) - row_number() OVER (
										PARTITION BY c.id,
										c.add1_new,
										c.add2_new,
										c.city_new,
										c.state_new,
										c.zip_new,
										c.cntyfips_new,
										c.cntyname_new,
										c.confidential,
										c.homeless,
										c.mailbox,
										c.care_of,
										c.overridden ORDER BY calmonth
										) AS group_num
								FROM (
									-- Start here by pulling out the row per month data and converting the row per month field into a date
									-- also join to cleaned address table
									SELECT DISTINCT a.MEDICAID_RECIPIENT_ID AS id,
										a.RSDNTL_ADRS_LINE_1 AS add1,
										a.RSDNTL_ADRS_LINE_2 AS add2,
										a.RSDNTL_CITY_NAME AS city,
										a.RSDNTL_STATE_CODE AS STATE,
										a.RSDNTL_POSTAL_CODE AS zip,
										a.RSDNTL_COUNTY_CODE AS cntyfips,
										a.RSDNTL_COUNTY_NAME AS cntyname,
										b.add1_new,
										b.add2_new,
										b.city_new,
										b.state_new,
										b.zip_new,
										b.cntyfips_new,
										b.cntyname_new,
										b.confidential,
										b.homeless,
										b.mailbox,
										b.care_of,
										b.overridden,
										CONVERT(DATETIME, a.CLNDR_YEAR_MNTH + '01', 112) AS calmonth,
										a.FROM_DATE AS fromdate,
										a.TO_DATE AS todate
									FROM (
										SELECT *
										FROM [PHClaims].[dbo].[NewEligibility]
										) a
									LEFT JOIN (
										SELECT *
										FROM [PHClaims].[dbo].[mcaid_elig_address_clean]
										) b ON (
											a.RSDNTL_ADRS_LINE_1 = b.add1
											OR (
												a.RSDNTL_ADRS_LINE_1 IS NULL
												AND b.add1 IS NULL
												)
											)
										AND (
											a.RSDNTL_ADRS_LINE_2 = b.add2
											OR (
												a.RSDNTL_ADRS_LINE_2 IS NULL
												AND b.add2 IS NULL
												)
											)
										AND (
											a.RSDNTL_CITY_NAME = b.city
											OR (
												a.RSDNTL_CITY_NAME IS NULL
												AND b.city IS NULL
												)
											)
										AND (
											a.RSDNTL_STATE_CODE = b.STATE
											OR (
												a.RSDNTL_STATE_CODE IS NULL
												AND b.STATE IS NULL
												)
											)
										AND (
											a.RSDNTL_POSTAL_CODE = b.zip
											OR (
												a.RSDNTL_POSTAL_CODE IS NULL
												AND b.zip IS NULL
												)
											)
										AND (
											a.RSDNTL_COUNTY_CODE = b.cntyfips
											OR (
												a.RSDNTL_COUNTY_CODE IS NULL
												AND b.cntyfips IS NULL
												)
											)
										AND (
											a.RSDNTL_COUNTY_NAME = b.cntyname
											OR (
												a.RSDNTL_COUNTY_NAME IS NULL
												AND b.cntyname IS NULL
												)
											)
									) c
								) d
							) e
						GROUP BY e.id,
							e.add1_new,
							e.add2_new,
							e.city_new,
							e.state_new,
							e.zip_new,
							e.cntyfips_new,
							e.cntyname_new,
							e.confidential,
							e.homeless,
							e.mailbox,
							e.care_of,
							e.overridden,
							e.group_num,
							e.fromdate,
							e.todate
						) f
					) g
				) h
			WHERE h.from_date >= h.min_from
				AND h.to_date = h.max_to
			) i
		) j
	GROUP BY j.id,
		j.add1_new,
		j.add2_new,
		j.city_new,
		j.state_new,
		j.zip_new,
		j.cntyfips_new,
		j.cntyname_new,
		j.confidential,
		j.homeless,
		j.mailbox,
		j.care_of,
		j.overridden,
		j.group_num3
	) k
-- Now join to geocoded reference table
LEFT JOIN (
	SELECT *
	FROM [PHClaims].[dbo].[ref_address_geocoded]
	) l ON (
		k.add1_new = l.add1_new
		OR (
			k.add1_new IS NULL
			AND l.add1_new IS NULL
			)
		)
	AND (
		k.city_new = l.city_new
		OR (
			k.city_new IS NULL
			AND l.city_new IS NULL
			)
		)
	AND (
		k.state_new = l.state_new
		OR (
			k.state_new IS NULL
			AND l.state_new IS NULL
			)
		)
	AND (
		k.zip_new = l.zip_new
		OR (
			k.zip_new IS NULL
			AND l.zip_new IS NULL
			)
		)
GROUP BY k.id,
	k.from_date,
	k.to_date,
	k.add1_new,
	k.add2_new,
	k.city_new,
	k.state_new,
	k.zip_new,
	k.cntyfips_new,
	k.cntyname_new,
	k.confidential,
	k.homeless,
	k.mailbox,
	k.care_of,
	k.overridden,
	k.cov_time_day,
	l.add_geocoded,
	l.zip_geocoded,
	l.add_type,
	l.x,
	l.y,
	l.check_esri,
	l.check_opencage,
	l.geocode_source,
	l.zip_centroid,
	l.lon,
	l.lat,
	l.statefp10,
	l.countyfp10,
	l.tractce10,
	l.blockce10,
	l.block_geoid10,
	l.pumace10,
	l.puma_geoid10,
	l.puma_name,
	l.zcta5ce10,
	l.zcta_geoid10,
	l.hra_id,
	l.hra,
	l.region_id,
	l.region,
	l.school_geoid10,
	l.school,
	l.kcc_dist,
	l.wa_legdist,
	l.scc_dist
ORDER BY k.id,
	k.from_date