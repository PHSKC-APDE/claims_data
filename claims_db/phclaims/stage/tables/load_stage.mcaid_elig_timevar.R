###############################################################################
# Alastair Matheson
# 2019-06-07

# Code to create a SQL table that holds start and end dates of time-varying 
# elements of the Medicaid eligibility table.

# Adapted from SQL code written by Eli Kern, Alastair Matheson, and Danny Colombara

###############################################################################


print("Creating stage.mcaid_elig_timevar. This will take ~25 minutes to run.")

#### STEP 1: PULL COLUMNS FROM RAW DATA AND GET CLEAN ADDRESSES ####
# Note, some people have 2 secondary RAC codes on separate rows.
# Need to create third RAC code field and collapse to a single row.
# First pull in relevant columns and set an index to speed later sorting
try(odbc::dbRemoveTable(db_claims, "##timevar_01a", temporary = T), silent = T)

step1a_sql <- glue::glue_sql(
  "SELECT DISTINCT a.id_mcaid, 
    CONVERT(DATE, CAST(a.CLNDR_YEAR_MNTH as varchar(200)) + '01', 112) AS calmonth, 
    a.fromdate, a.todate, a.dual, a.tpl, a.bsp_group_cid, 
    b.full_benefit_1, c.full_benefit_2, a.cov_type, a.mco_id,
    d.geo_add1, d.geo_add2, d.geo_city, d.geo_state, d.geo_zip
    INTO ##timevar_01a 
    FROM
    (SELECT MEDICAID_RECIPIENT_ID AS 'id_mcaid', 
      CLNDR_YEAR_MNTH, FROM_DATE AS 'fromdate', TO_DATE AS 'todate',
      DUAL_ELIG AS 'dual', TPL_FULL_FLAG AS 'tpl', 
      RPRTBL_RAC_CODE AS 'rac_code_1', SECONDARY_RAC_CODE AS 'rac_code_2', 
      RPRTBL_BSP_GROUP_CID AS 'bsp_group_cid', 
      COVERAGE_TYPE_IND AS 'cov_type', 
      MC_PRVDR_ID AS 'mco_id', 
      RSDNTL_ADRS_LINE_1 AS 'geo_add1_raw', RSDNTL_ADRS_LINE_2 AS 'geo_add2_raw',
      RSDNTL_CITY_NAME as 'geo_city_raw', RSDNTL_STATE_CODE AS 'geo_state_raw', 
      RSDNTL_POSTAL_CODE AS 'geo_zip_raw'
      FROM PHClaims.stage.mcaid_elig) a
      LEFT JOIN
      (SELECT rac_code, 
        CASE 
          WHEN full_benefit = 'Y' THEN 1
          WHEN full_benefit = 'N' THEN 0
          ELSE NULL END AS full_benefit_1
        FROM ref.mcaid_rac_code) b
      ON a.rac_code_1 = b.rac_code
      LEFT JOIN
      (SELECT rac_code, 
        CASE 
          WHEN full_benefit = 'Y' THEN 1
          WHEN full_benefit = 'N' THEN 0
          ELSE NULL END AS full_benefit_2
        FROM ref.mcaid_rac_code) c
      ON a.rac_code_2 = c.rac_code
      LEFT JOIN
      (SELECT geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw,
        geo_add1_clean AS geo_add1, geo_add2_clean AS geo_add2, 
        geo_city_clean AS geo_city, geo_state_clean AS geo_state, 
        geo_zip_clean AS geo_zip
        FROM ref.address_clean
        WHERE geo_source_mcaid = 1) d
      ON 
      (a.geo_add1_raw = d.geo_add1_raw OR (a.geo_add1_raw IS NULL AND d.geo_add1_raw IS NULL)) AND
      (a.geo_add2_raw = d.geo_add2_raw OR (a.geo_add2_raw IS NULL AND d.geo_add2_raw IS NULL)) AND 
      (a.geo_city_raw = d.geo_city_raw OR (a.geo_city_raw IS NULL AND d.geo_city_raw IS NULL)) AND 
      (a.geo_state_raw = d.geo_state_raw OR (a.geo_state_raw IS NULL AND d.geo_state_raw IS NULL)) AND 
      (a.geo_zip_raw = d.geo_zip_raw OR (a.geo_zip_raw IS NULL AND d.geo_zip_raw IS NULL))",
  .con = db_claims)

print("Running step 1a: pull columns from raw, join to clean addresses, and add index")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step1a_sql)


# Add an index to the temp table to make ordering much faster
odbc::dbGetQuery(conn = db_claims,
           "CREATE CLUSTERED INDEX [timevar_01_idx] ON ##timevar_01a 
           (id_mcaid, calmonth, fromdate)")

time_end <- Sys.time()
print(paste0("Step 1a took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))


# Setup full_benefit flag and drop secondary RAC rows
try(odbc::dbRemoveTable(db_claims, "##timevar_01b", temporary = T), silent = T)

step1b_sql <- glue::glue_sql(
  "SELECT a.id_mcaid, a.calmonth, a.fromdate, a.todate, a.dual, 
  a.tpl, a.bsp_group_cid, a.full_benefit, a.cov_type, a.mco_id, 
  a.geo_add1, a.geo_add2, a.geo_city, a.geo_state, a.geo_zip
  INTO ##timevar_01b
  FROM
  (SELECT id_mcaid, calmonth, fromdate, todate, dual, 
  tpl, bsp_group_cid, cov_type, mco_id,
  CASE 
    WHEN COALESCE(MAX(full_benefit_1), 0) + COALESCE(MAX(full_benefit_2), 0) >= 1 THEN 1
    WHEN COALESCE(MAX(full_benefit_1), 0) + COALESCE(MAX(full_benefit_2), 0) = 0 THEN 0
    END AS full_benefit,
  geo_add1, geo_add2, geo_city, geo_state, geo_zip,
  ROW_NUMBER() OVER(PARTITION BY id_mcaid, calmonth, fromdate  
                    ORDER BY id_mcaid, calmonth, fromdate) AS group_row 
  FROM ##timevar_01a
  GROUP BY id_mcaid, calmonth, fromdate, todate, dual, 
  tpl, bsp_group_cid, cov_type, mco_id,
  geo_add1, geo_add2, geo_city, geo_state, geo_zip) a
  WHERE a.group_row = 1",
  .con = db_claims)


print("Running step 1b: set up full_benefit flag and drop secondary RAC rows")
time_start <- Sys.time()

odbc::dbGetQuery(conn = db_claims, step1b_sql)
time_end <- Sys.time()
print(paste0("Step 1b took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))



#### STEP 2: MAKE START AND END DATES ####
# Make a start and end date for each month
try(odbc::dbRemoveTable(db_claims, "##timevar_02a", temporary = T), silent = T)

step2a_sql <- glue::glue_sql(
  "SELECT id_mcaid, dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip, 
    calmonth AS startdate, dateadd(day, - 1, dateadd(month, 1, calmonth)) AS enddate,
    fromdate, todate
    INTO ##timevar_02a
    FROM ##timevar_01b
    GROUP BY id_mcaid, calmonth, dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip, fromdate, todate",
  .con = db_claims)

print("Running step 2a: Make a start and end date for each month")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step2a_sql)
time_end <- Sys.time()
print(paste0("Step 2a took ", round(difftime(time_end, time_start, units = "secs"), 2),
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))


# Incorporate sub-month coverage (identify the smallest possible time interval)
# Note that the step above leads to duplicate rows so use distinct here to clear
#  them out once submonth coverage is accounted for
try(odbc::dbRemoveTable(db_claims, "##timevar_02b", temporary = T), silent = T)

step2b_sql <- glue::glue_sql(
  "SELECT DISTINCT a.id_mcaid, a.from_date, a.to_date, a.dual, a.tpl, 
  a.bsp_group_cid, a.full_benefit, a.cov_type, a.mco_id, 
  a.geo_add1, a.geo_add2, a.geo_city, a.geo_state, a.geo_zip
  INTO ##timevar_02b
  FROM
  (SELECT id_mcaid, dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip, 
    CASE 
      WHEN fromdate IS NULL THEN startdate 
      WHEN startdate >= fromdate THEN startdate
      WHEN startdate < fromdate THEN fromdate
      ELSE null END AS from_date,	
    CASE 
      WHEN todate IS NULL THEN enddate 
      WHEN enddate <= todate THEN enddate
      WHEN enddate > todate THEN todate
      ELSE null END AS to_date
    FROM ##timevar_02a) a",
  .con = db_claims)


print("Running step 2b: Incorporate submonth coverage")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step2b_sql)

# Add an index to the temp table to make the next step much faster
odbc::dbGetQuery(conn = db_claims,
                 "CREATE CLUSTERED INDEX [timevar_02b_idx] ON ##timevar_02b 
                 (id_mcaid, from_date, to_date)")

time_end <- Sys.time()
print(paste0("Step 2b took ", round(difftime(time_end, time_start, units = "secs"), 2),
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))



#### STEP 3: IDENTIFY CONTIGUOUS PERIODS ####
# Calculate the number of days between each from_date and the previous to_date
try(odbc::dbRemoveTable(db_claims, "##timevar_03a", temporary = T), silent = T)

step3a_sql <- glue::glue_sql(
  "SELECT DISTINCT id_mcaid, from_date, to_date, 
  dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
  geo_add1, geo_add2, geo_city, geo_state, geo_zip, 
  DATEDIFF(day, lag(to_date) OVER (
    PARTITION BY id_mcaid, 
      dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
      geo_add1, geo_add2, geo_city, geo_state, geo_zip
      ORDER BY id_mcaid, from_date), from_date) AS group_num
  INTO ##timevar_03a
  FROM ##timevar_02b"
)

print("Running step 3a: calculate # days between each from_date and previous to_date")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step3a_sql)
time_end <- Sys.time()
print(paste0("Step 3a took ", round(difftime(time_end, time_start, units = "secs"), 2),
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))


# Give a unique identifier (row number) to the first date in a continguous series of dates 
# (meaning 0 or 1 days between each from_date and the previous to_date)
try(odbc::dbRemoveTable(db_claims, "##timevar_03b", temporary = T), silent = T)

step3b_sql <- glue::glue_sql(
  "SELECT DISTINCT id_mcaid, from_date, to_date,
    dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip,
    CASE 
      WHEN group_num > 1  OR group_num IS NULL THEN ROW_NUMBER() OVER (PARTITION BY id_mcaid ORDER BY from_date) + 1
      WHEN group_num <= 1 THEN NULL
      END AS group_num
    INTO ##timevar_03b
    FROM ##timevar_03a",
  .con = db_claims)

print("Running step 3b: Generate unique ID for first date in contiguous series of dates")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step3b_sql)

# Add an index to the temp table to make the next step faster (not sure it's actually helping)
odbc::dbGetQuery(conn = db_claims,
                 "CREATE CLUSTERED INDEX [timevar_03b_idx] ON ##timevar_03b 
                 (id_mcaid, from_date, to_date)")

time_end <- Sys.time()
print(paste0("Step 3b took ", round(difftime(time_end, time_start, units = "secs"), 2),
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))


# Use the row number for the first in the series of contiguous dates as an 
# identifier for that set of contiguous dates
try(odbc::dbRemoveTable(db_claims, "##timevar_03c", temporary = T), silent = T)

step3c_sql <- glue::glue_sql(
  "SELECT DISTINCT id_mcaid, from_date, to_date,
    dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip,
    group_num = max(group_num) OVER 
      (PARTITION BY id_mcaid, dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
        geo_add1, geo_add2, geo_city, geo_state, geo_zip 
        ORDER BY from_date)
    INTO ##timevar_03c
    FROM ##timevar_03b",
  .con = db_claims)

print("Running step 3c: Replicate unique ID across contiguous dates")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step3c_sql)
time_end <- Sys.time()
print(paste0("Step 3c took ", round(difftime(time_end, time_start, units = "secs"), 2),
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))



#### STEP 4: FIND MIN/MAX DATES AND CONTIGUOUS PERIODS ####
# Find the min/max dates
try(odbc::dbRemoveTable(db_claims, "##timevar_04a", temporary = T), silent = T)

step4a_sql <- glue::glue_sql(
  "SELECT id_mcaid, dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip, 
    MIN(from_date) AS from_date,
    MAX(to_date) AS to_date
    INTO ##timevar_04a
    FROM ##timevar_03c
    GROUP BY id_mcaid, dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip,
    group_num",
  .con = db_claims)

print("Running step 4a: Find the min/max dates for contiguous periods")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step4a_sql)
time_end <- Sys.time()
print(paste0("Step 4a took ", round(difftime(time_end, time_start, units = "secs"), 2),
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))


# Calculate coverage time
try(odbc::dbRemoveTable(db_claims, "##timevar_04b", temporary = T), silent = T)

step4b_sql <- glue::glue_sql(
  "SELECT id_mcaid, from_date, to_date, dual, tpl, 
    bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip, 
    DATEDIFF(dd, from_date, to_date) + 1 as cov_time_day
    INTO ##timevar_04b
    FROM ##timevar_04a",
  .con = db_claims)

print("Running step 4b: Find the min/max dates for contiguous periods")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step4b_sql)
time_end <- Sys.time()
print(paste0("Step 4b took ", round(difftime(time_end, time_start, units = "secs"), 2),
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
             " mins)"))



#### STEP 5: ADD CONTIGUOUS FLAG, RECODE DUAL, JOIN TO GEOCODES, AND LOAD TO STAGE TABLE ####
# Truncate stage.mcaid_elig_timevar (just in case)
print("Running step 5a: Truncate stage table")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, "TRUNCATE TABLE stage.mcaid_elig_timevar")
time_end <- Sys.time()
print(paste0("Step 5a took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2), 
             " mins)"))

# Now do the final transformation plus loading
step5b_sql <- glue::glue_sql(
  "INSERT INTO stage.mcaid_elig_timevar WITH (TABLOCK)
    SELECT
    a.id_mcaid, a.from_date, a.to_date, 
    CASE WHEN DATEDIFF(day, lag(a.to_date, 1) OVER 
      (PARTITION BY a.id_mcaid order by a.id_mcaid, a.from_date), a.from_date) = 1
      THEN 1 ELSE 0 END AS contiguous, 
    CASE WHEN a.dual = 'Y' THEN 1 ELSE 0 END AS dual,
    CASE WHEN a.tpl = 'Y' THEN 1 ELSE 0 END AS tpl,
    a.bsp_group_cid, a.full_benefit, a.cov_type, a.mco_id,
    a.geo_add1, a.geo_add2, a.geo_city, a.geo_state, a.geo_zip,
    b.geo_zip_centroid, b.geo_street_centroid, b.geo_county_code, b.geo_tract_code, 
    b.geo_hra_code, b.geo_school_code, a.cov_time_day,
    {Sys.time()} AS last_run
    FROM
    (SELECT id_mcaid, from_date, to_date, 
      dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
      geo_add1, geo_add2, geo_city, geo_state, geo_zip,
      cov_time_day
      FROM ##timevar_04b) a
      LEFT JOIN
      (SELECT geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
        geo_zip_centroid, geo_street_centroid, geo_countyfp10 AS geo_county_code, 
        geo_tractce10 AS geo_tract_code, geo_hra_id AS geo_hra_code, 
        geo_school_geoid10 AS geo_school_code
        FROM PHClaims.ref.address_geocode) b
      ON 
      (a.geo_add1 = b.geo_add1_clean OR (a.geo_add1 IS NULL AND b.geo_add1_clean IS NULL)) AND 
      (a.geo_city = b.geo_city_clean OR (a.geo_city IS NULL AND b.geo_city_clean IS NULL)) AND 
      (a.geo_state = b.geo_state_clean OR (a.geo_state IS NULL AND b.geo_state_clean IS NULL)) AND 
      (a.geo_zip = b.geo_zip_clean OR (a.geo_zip IS NULL AND b.geo_zip_clean IS NULL))",
  .con = db_claims)

print("Running step 5b: Join to geocodes and load to stage table")
time_start <- Sys.time()
odbc::dbGetQuery(conn = db_claims, step5b_sql)
time_end <- Sys.time()
print(paste0("Step 5b took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2), 
             " mins)"))


#### STEP 6: REMOVE TEMPORARY TABLES ####
print("Running step 6: Remove temporary tables")
time_start <- Sys.time()
try(odbc::dbRemoveTable(db_claims, "##timevar_01a", temporary = T), silent = T)
try(odbc::dbRemoveTable(db_claims, "##timevar_01b", temporary = T), silent = T)
try(odbc::dbRemoveTable(db_claims, "##timevar_02a", temporary = T), silent = T)
try(odbc::dbRemoveTable(db_claims, "##timevar_02b", temporary = T), silent = T)
try(odbc::dbRemoveTable(db_claims, "##timevar_03a", temporary = T), silent = T)
try(odbc::dbRemoveTable(db_claims, "##timevar_03b", temporary = T), silent = T)
try(odbc::dbRemoveTable(db_claims, "##timevar_03c", temporary = T), silent = T)
try(odbc::dbRemoveTable(db_claims, "##timevar_04a", temporary = T), silent = T)
try(odbc::dbRemoveTable(db_claims, "##timevar_04b", temporary = T), silent = T)
rm(list = ls(pattern = "step(.){1,2}_sql"))
time_end <- Sys.time()
print(paste0("Step 6 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
             " secs (", round(difftime(time_end, time_start, units = "mins"), 2), 
             " mins)"))
