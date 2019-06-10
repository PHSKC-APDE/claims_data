###############################################################################
# Alastair Matheson
# 2019-06-07

# Code to create a SQL table that holds start and end dates of time-varying 
# elements of the Medicaid eligibility table.

# Adapted from SQL code written by Eli Kern, Alastair Matheson, and Danny Colombara

###############################################################################


load_stage.mcaid_elig_timevar_f <- function(conn = db_claims) {
  
  #### STEP 1: PULL COLUMNS FROM RAW DATA AND GET CLEAN ADDRESSES ####
  try(odbc::dbRemoveTable(db_claims, "##timevar_01", temporary = T))
  
  step1_sql <- glue::glue_sql(
    "SELECT DISTINCT a.id_mcaid, 
    CONVERT(DATETIME, CAST(a.CLNDR_YEAR_MNTH as varchar(200)) + '01', 112) AS calmonth, 
    a.fromdate, a.todate, a.dual, a.tpl,
    a.rac_code_1, a.rac_code_2, a.mco_id,
    b.geo_add1_clean, b.geo_add2_clean, b.geo_city_clean,
    b.geo_state_clean, b.geo_zip_clean
    INTO ##timevar_01
    FROM
    (SELECT MEDICAID_RECIPIENT_ID AS 'id_mcaid', 
      CLNDR_YEAR_MNTH, FROM_DATE AS 'fromdate', TO_DATE AS 'todate',
      DUAL_ELIG AS 'dual', TPL_FULL_FLAG AS 'tpl', 
      RPRTBL_RAC_CODE AS 'rac_code_1', SECONDARY_RAC_CODE AS 'rac_code_2',
      MC_PRVDR_ID AS 'mco_id', 
      RSDNTL_ADRS_LINE_1 AS 'geo_add1_raw', RSDNTL_ADRS_LINE_2 AS 'geo_add2_raw',
      RSDNTL_CITY_NAME as 'geo_city_raw', RSDNTL_STATE_CODE AS 'geo_state_raw', 
      RSDNTL_POSTAL_CODE AS 'geo_zip_raw'
      FROM PHClaims.stage.mcaid_elig) a
      LEFT JOIN
      (SELECT geo_add1_raw, geo_add2_raw, geo_city_raw, geo_state_raw, geo_zip_raw,
        geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean
        FROM ref.address_clean) b
      ON 
      (a.geo_add1_raw = b.geo_add1_raw OR (a.geo_add1_raw IS NULL AND b.geo_add1_raw IS NULL)) AND
      (a.geo_add2_raw = b.geo_add2_raw OR (a.geo_add2_raw IS NULL AND b.geo_add2_raw IS NULL)) AND 
      (a.geo_city_raw = b.geo_city_raw OR (a.geo_city_raw IS NULL AND b.geo_city_raw IS NULL)) AND 
      (a.geo_state_raw = b.geo_state_raw OR (a.geo_state_raw IS NULL AND b.geo_state_raw IS NULL)) AND 
      (a.geo_zip_raw = b.geo_zip_raw OR (a.geo_zip_raw IS NULL AND b.geo_zip_raw IS NULL))",
    .con = conn)
  
  
  print("Running step 1: pull columns from raw and join to clean addresses")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = db_claims, step1_sql)
  time_end <- Sys.time()
  print(paste0("Step 1 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
               " mins)"))
  
 
  #### STEP 2: IDENTIFY CONTIGUOUS PERIODS ####
  # Calculate the number of months between each calmonth and the previous calmonth
  try(odbc::dbRemoveTable(db_claims, "##timevar_02a", temporary = T))
  
  step2a_sql <- glue::glue_sql(
    "SELECT DISTINCT id_mcaid, calmonth, fromdate, todate,
    dual, tpl, rac_code_1, rac_code_2, mco_id, 
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
    DATEDIFF(month, lag(calmonth) OVER (
      PARTITION BY id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id, 
      geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean
      ORDER BY calmonth), calmonth) AS group_num
    INTO ##timevar_02a
    FROM ##timevar_01
    ORDER BY calmonth",
    .con = conn)
  
  print("Running step 2a: calculate # months between each calmonth and previous calmonth")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = db_claims, step2a_sql)
  time_end <- Sys.time()
  print(paste0("Step 2a took ", round(difftime(time_end, time_start, units = "secs"), 2),
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
               " mins)"))
  
  # Give a unique identifier (row number) to the first date in a continguous series of dates 
  # (meaning 0 or 1 months between each month and the previous month)
  try(odbc::dbRemoveTable(db_claims, "##timevar_02b", temporary = T))
  
  step2b_sql <- glue::glue_sql(
    "SELECT DISTINCT id_mcaid, calmonth, fromdate, todate,
    dual, tpl, rac_code_1, rac_code_2, mco_id, 
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
    CASE 
    WHEN group_num >1  OR group_num IS NULL THEN ROW_NUMBER() OVER (PARTITION BY id_mcaid ORDER BY calmonth) + 1
    WHEN group_num = 1 OR group_num = 0 THEN NULL
    END AS group_num
    INTO ##timevar_02b
    FROM ##timevar_02a",
    .con = conn)
  
  print("Running step 2b: Generate unique ID for first date in contiguous series of dates")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = db_claims, step2b_sql)
  time_end <- Sys.time()
  print(paste0("Step 2b took ", round(difftime(time_end, time_start, units = "secs"), 2),
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
               " mins)"))

  # Use the row number for the first in the series of contigous dates as an 
  # identifier for that set of contiguous dates
  try(odbc::dbRemoveTable(db_claims, "##timevar_02c", temporary = T))
  
  step2c_sql <- glue::glue_sql(
    "SELECT DISTINCT id_mcaid, calmonth, fromdate, todate,
    dual, tpl, rac_code_1, rac_code_2, mco_id, 
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
    group_num = max(group_num) OVER 
      (PARTITION BY id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id, 
        geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean 
        ORDER BY calmonth)
    INTO ##timevar_02c
    FROM ##timevar_02b",
    .con = conn)
  
  print("Running step 2c: Replicate unique ID across contiguous dates")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = db_claims, step2c_sql)
  time_end <- Sys.time()
  print(paste0("Step 2c took ", round(difftime(time_end, time_start, units = "secs"), 2),
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
               " mins)"))

  
  #### STEP 3: REMOVE DUPLICATE ROWS ####
  try(odbc::dbRemoveTable(db_claims, "##timevar_03", temporary = T))
  
  step3_sql <- glue::glue_sql(
    "SELECT DISTINCT id_mcaid, calmonth, group_num, fromdate, todate,
    dual, tpl, rac_code_1, rac_code_2, mco_id, 
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean
    INTO ##timevar_03
    FROM ##timevar_02c",
    .con = conn)
  
  print("Running step 3: Remove duplicate rows")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = db_claims, step3_sql)
  time_end <- Sys.time()
  print(paste0("Step 3 took ", round(difftime(time_end, time_start, units = "secs"), 2),
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
               " mins)"))
  
  
  #### STEP 4: MAKE START AND END DATES ####
  # Make a start and end date for each month
  try(odbc::dbRemoveTable(db_claims, "##timevar_04a", temporary = T))
  
  step4a_sql <- glue::glue_sql(
    "SELECT id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id,
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean, 
    MIN(calmonth) AS startdate, dateadd(day, - 1, dateadd(month, 1, MAX(calmonth))) AS enddate,
    group_num, fromdate, todate
    INTO ##timevar_04a
    FROM ##timevar_03
    GROUP BY id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id,
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean, 
    group_num, fromdate, todate",
    .con = conn)
  
  print("Running step 4a: Make a start and end date for each month")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = db_claims, step4a_sql)
  time_end <- Sys.time()
  print(paste0("Step 4a took ", round(difftime(time_end, time_start, units = "secs"), 2),
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
               " mins)"))
  
 
  # Incorporate sub-month coverage (identify the smallest possible time interval)
  try(odbc::dbRemoveTable(db_claims, "##timevar_04b", temporary = T))
  
  step4b_sql <- glue::glue_sql(
    "SELECT id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id,
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean, 
    group_num,
    CASE 
      WHEN startdate >= fromdate THEN startdate
      WHEN startdate < fromdate THEN fromdate
      ELSE null END AS from_date,	
    CASE 
      WHEN enddate <= todate THEN enddate
      WHEN enddate > todate THEN todate
      ELSE null END AS to_date
    INTO ##timevar_04b
    FROM ##timevar_04a",
    .con = conn)
  
  print("Running step 4b: Incorporate submonth coverage")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = db_claims, step4b_sql)
  time_end <- Sys.time()
  print(paste0("Step 5a took ", round(difftime(time_end, time_start, units = "secs"), 2),
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
               " mins)"))
  
  
  #### STEP 5: FIND MIN/MAX DATES AND CONTIGUOUS PERIODS ####
  # Find the min/max dates
  try(odbc::dbRemoveTable(db_claims, "##timevar_05a", temporary = T))
  
  step5a_sql <- glue::glue_sql(
    "SELECT id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id,
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean, 
    from_date, to_date,
    MIN(from_date) OVER 
    (PARTITION BY id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id,
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean 
      ORDER BY id_mcaid, from_date, to_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS 'min_from',
    MAX(to_date) OVER 
    (PARTITION BY id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id,
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean 
      ORDER BY id_mcaid, from_date, to_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS 'max_to'
    INTO ##timevar_05a
    FROM ##timevar_04b",
    .con = conn)
  
  print("Running step 5a: Find the min/max dates for contiguous periods")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = db_claims, step5a_sql)
  time_end <- Sys.time()
  print(paste0("Step 5a took ", round(difftime(time_end, time_start, units = "secs"), 2),
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
               " mins)"))
  
  # Identify breaks in coverage
  try(odbc::dbRemoveTable(db_claims, "##timevar_05b", temporary = T))
  
  step5b_sql <- glue::glue_sql(
    "SELECT id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id,
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean, 
    from_date, to_date,
    CASE
      WHEN from_date - lag(to_date) OVER 
      (PARTITION BY id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id,
        geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean
        ORDER BY id_mcaid, from_date, to_date DESC) <= 1 THEN NULL
    ELSE row_number() OVER 
      (PARTITION BY id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id,
        geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean 
        ORDER BY from_date, to_date DESC)
    END AS group_num2,
    row_number() OVER 
      (PARTITION BY id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id,
        geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean 
        ORDER BY id_mcaid, from_date, to_date DESC) AS temp_row
    INTO ##timevar_05b
    FROM ##timevar_05a
    WHERE NOT(from_date > min_from AND to_date < max_to)
    order by id_mcaid, from_date, to_date DESC",
    .con = conn)
  
  print("Running step 5b: Identify breaks in coverage")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = db_claims, step5b_sql)
  time_end <- Sys.time()
  print(paste0("Step 5b took ", round(difftime(time_end, time_start, units = "secs"), 2),
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
               " mins)"))
  
  # Create lagged variables to be used to identify additional nesting
  try(odbc::dbRemoveTable(db_claims, "##timevar_05c", temporary = T))
  
  step5c_sql <- glue::glue_sql(
    "SELECT *, 
    lagged_to = LAG(to_date) OVER 
      (PARTITION BY id_mcaid ORDER BY id_mcaid, from_date, to_date DESC),
    lagged_from = LAG(from_date) OVER 
      (PARTITION BY id_mcaid ORDER BY id_mcaid, from_date, to_date DESC)
    INTO ##timevar_05c
    FROM ##timevar_05b",
    .con = conn)
  
  print("Running step 5c: Create lagged variables to be used to identify additional nesting")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = db_claims, step5c_sql)
  time_end <- Sys.time()
  print(paste0("Step 5c took ", round(difftime(time_end, time_start, units = "secs"), 2),
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
               " mins)"))
  
  
  #### STEP 6: COLLAPSE ROWS ####
  # Drop dates nested within other dates when they have the same start date
  try(odbc::dbRemoveTable(db_claims, "##timevar_06a", temporary = T))
  
  step6a_sql <- glue::glue_sql(
    "SELECT id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id,
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean, 
    from_date, to_date, group_num2, temp_row
    INTO ##timevar_06a
    FROM ##timevar_05c
    WHERE NOT(from_date = lagged_from AND to_date < lagged_to AND 
              lagged_from IS NOT NULL AND lagged_to IS NOT NULL)
    ORDER BY id_mcaid, from_date, to_date DESC",
    .con = conn)
  
  print("Running step 6a: Drop dates nested within other dates when they have the same start date")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = db_claims, step6a_sql)
  time_end <- Sys.time()
  print(paste0("Step 6a took ", round(difftime(time_end, time_start, units = "secs"), 2),
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
               " mins)"))
  
  # Identify rows that can be further collapsed
  try(odbc::dbRemoveTable(db_claims, "##timevar_06b", temporary = T))
  
  step6b_sql <- glue::glue_sql(
    "SELECT id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id,
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean, 
    from_date, to_date, group_num2,
    SUM(CASE WHEN group_num2 IS NULL THEN 0 ELSE 1 END) OVER
    (PARTITION BY id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id,
      geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean
      ORDER BY temp_row ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS group_num3
    INTO ##timevar_06b
    FROM ##timevar_06a",
    .con = conn)
  
  print("Running step 6b: Identify rows that can be further collapsed")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = db_claims, step6b_sql)
  time_end <- Sys.time()
  print(paste0("Step 6b took ", round(difftime(time_end, time_start, units = "secs"), 2),
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
               " mins)"))
  
  # Collapse rows one last time and select variables for loading
  try(odbc::dbRemoveTable(db_claims, "##timevar_06c", temporary = T))
  
  step6c_sql <- glue::glue_sql(
    "SELECT id_mcaid, 
    CAST(MIN(from_date) as date) AS 'from_date',
		CAST(MAX(to_date) as date) AS 'to_date',
    dual, tpl, rac_code_1, rac_code_2, mco_id,
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,  
    DATEDIFF(dd, MIN(from_date), MAX(to_date)) + 1 as cov_time_day
    INTO ##timevar_06c
    FROM ##timevar_06b
    GROUP BY id_mcaid, dual, tpl, rac_code_1, rac_code_2, mco_id,
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean, 
    group_num3
    ORDER BY id_mcaid, from_date",
    .con = conn)
  
  print("Running step 6c: Collapse rows one last time and select variables for loading")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = db_claims, step6c_sql)
  time_end <- Sys.time()
  print(paste0("Step 6c took ", round(difftime(time_end, time_start, units = "secs"), 2),
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
               " mins)"))
  
  
  #### STEP 7: ADD CONTIGUOUS FLAG, RECODE DUAL, JOIN TO GEOCODES, AND LOAD TO STAGE TABLE ####
  # Truncate stage.mcaid_elig_timevar (just in case)
  print("Running step 7a: Truncate stage table")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = db_claims, "TRUNCATE TABLE stage.mcaid_elig_timevar")
  time_end <- Sys.time()
  print(paste0("Step 7a took ", round(difftime(time_end, time_start, units = "secs"), 2), 
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2), 
               " mins)"))
  
  # Now do the final transformation plus loading
  step7b_sql <- glue::glue_sql(
    "INSERT INTO stage.mcaid_elig_timevar WITH (TABLOCK)
    SELECT
    a.id_mcaid, a.from_date, a.to_date, 
    CASE WHEN DATEDIFF(day, lag(a.to_date, 1) OVER 
      (PARTITION BY a.id_mcaid order by a.id_mcaid, a.from_date), a.from_date) = 1
      THEN 1 ELSE 0 END AS 'contiguous', 
    CASE WHEN a.dual = 'Y' THEN 1 ELSE 0 END AS dual,
    CASE WHEN a.tpl = 'Y' THEN 1 ELSE 0 END AS tpl,
    a.rac_code_1, a.rac_code_2, a.mco_id,
    a.geo_add1_clean, a.geo_add2_clean, a.geo_city_clean, a.geo_state_clean, a.geo_zip_clean,
    b.geo_zip_centroid, b.geo_street_centroid, b.geo_countyfp10, b.geo_tractce10, 
    b.geo_hra_id, b.geo_school_geoid10, a.cov_time_day,
    {Sys.time()} AS last_run
    FROM
    (SELECT id_mcaid, from_date, to_date, 
      dual, tpl, rac_code_1, rac_code_2, mco_id,
      geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
      cov_time_day
      FROM ##timevar_06c) a
      LEFT JOIN
      (SELECT geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
        geo_zip_centroid, geo_street_centroid, geo_countyfp10, geo_tractce10, 
        geo_hra_id, geo_school_geoid10
        FROM PHClaims.ref.address_geocode) b
      ON 
      (a.geo_add1_clean = b.geo_add1_clean OR (a.geo_add1_clean IS NULL AND b.geo_add1_clean IS NULL)) AND 
      (a.geo_city_clean = b.geo_city_clean OR (a.geo_city_clean IS NULL AND b.geo_city_clean IS NULL)) AND 
      (a.geo_state_clean = b.geo_state_clean OR (a.geo_state_clean IS NULL AND b.geo_state_clean IS NULL)) AND 
      (a.geo_zip_clean = b.geo_zip_clean OR (a.geo_zip_clean IS NULL AND b.geo_zip_clean IS NULL))",
    .con = conn)
  
  print("Running step 7b: Join to geocodes and load to stage table")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = db_claims, step7b_sql)
  time_end <- Sys.time()
  print(paste0("Step 7b took ", round(difftime(time_end, time_start, units = "secs"), 2), 
               " secs (", round(difftime(time_end, time_start, units = "mins"), 2), 
               " mins)"))
}
