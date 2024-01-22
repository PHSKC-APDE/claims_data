# Alastair Matheson
# 2019-06-07

# Code to create a SQL table that holds start and end dates of time-varying 
# elements of the Medicaid eligibility table.

# Adapted from SQL code written by Eli Kern, Alastair Matheson, and Danny Colombara




### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

load_stage_mcaid_elig_timevar_f <- function(conn = NULL,
                                        server = c("hhsaw", "phclaims"),
                                        config = NULL,
                                        get_config = F) {
  
  
  # Set up variables specific to the server
  server <- match.arg(server)
  
  if (get_config == T){
    if (stringr::str_detect(config, "^http")) {
      config <- yaml::yaml.load(getURL(config))
    } else{
      stop("A URL must be specified in config if using get_config = T")
    }
  }
  
  from_schema <- config[[server]][["from_schema"]]
  from_table <- config[[server]][["from_table"]]
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  ref_schema <- config[[server]][["ref_schema"]]
  ref_table <- ifelse(is.null(config[[server]][["ref_table"]]), '',
                      config[[server]][["ref_table"]])
  address_schema <- config[["hhsaw"]][["address_schema"]]
  address_table <- config[["hhsaw"]][["address_table"]]
  geocode_table <- config[["hhsaw"]][["geocode_table"]]
  geokc_table <- config[["hhsaw"]][["geokc_table"]]
  
  message("Creating ", to_schema, ".", to_table, ". This will take ~80 minutes to run.")
  

  #### STEP 1: PULL COLUMNS FROM RAW DATA AND GET CLEAN ADDRESSES (~15 MINS) ####
  # Note, some people have 2 secondary RAC codes on separate rows.
  # Need to create third RAC code field and collapse to a single row.
  # First pull in relevant columns and set an index to speed later sorting
  # Step 1a = ~10 mins (latest was 35 mins)
  try(odbc::dbRemoveTable(conn, "##timevar_01a", temporary = T), silent = T)
  if (server == "hhsaw") {
    address_clean_table <- glue::glue_sql("{`address_schema`}.{`address_table`}", 
                                          .con = conn)
  } else {
    conn_hhsaw <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)
    df <- odbc::dbGetQuery(conn_hhsaw, 
                           glue::glue_sql("SELECT geo_hash_raw, 
                                          geo_add1_clean, 
                                          geo_add2_clean,         
                                          geo_city_clean, 
                                          geo_state_clean,         
                                          geo_zip_clean,
                                          geo_hash_clean,
                                          geo_hash_geocode
                                        FROM {`address_schema`}.{`address_table`}",                                         
                                          .con = conn_hhsaw))
    conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)
    df2 <- odbc::dbGetQuery(conn, 
                            glue::glue_sql("SELECT DISTINCT geo_hash_raw                                         
                                         FROM {`from_schema`}.{`from_table`}",                                         
                                           .con = conn))
    df_address_clean <- inner_join(df, df2)
    try(odbc::dbRemoveTable(conn, "##address_clean_01a", temporary = T), silent = T)
    DBI::dbWriteTable(conn, 
                      name = "##address_clean_01a", 
                      value = df_address_clean)
    address_clean_table <- "##address_clean_01a"
  }
  
  step1a_sql <- glue::glue_sql(paste0(
    "SELECT DISTINCT a.id_mcaid, 
    CONVERT(DATE, CAST(a.CLNDR_YEAR_MNTH as varchar(200)) + '01', 112) AS calmonth, 
    a.fromdate, a.todate, a.dual, a.tpl, a.bsp_group_cid, 
    b.full_benefit_1, c.full_benefit_2, a.cov_type, a.mco_id, 
    d.geo_add1, d.geo_add2, d.geo_city, d.geo_state, d.geo_zip, 
    d.geo_hash_clean, d.geo_hash_geocode
    INTO ##timevar_01a 
    FROM
    (SELECT MEDICAID_RECIPIENT_ID AS 'id_mcaid', 
      CLNDR_YEAR_MNTH, FROM_DATE AS 'fromdate', TO_DATE AS 'todate',
      DUAL_ELIG AS 'dual', 
      ISNULL([TPL_FULL_FLAG], '') AS 'tpl',
      RPRTBL_RAC_CODE AS 'rac_code_1', SECONDARY_RAC_CODE AS 'rac_code_2', 
      RPRTBL_BSP_GROUP_CID AS 'bsp_group_cid', 
      COVERAGE_TYPE_IND AS 'cov_type', 
      MC_PRVDR_ID AS 'mco_id',
      geo_hash_raw
      FROM {`from_schema`}.{`from_table`}) a
      LEFT JOIN
      (SELECT rac_code, 
        CASE 
          WHEN full_benefit = 'Y' THEN 1
          WHEN full_benefit = 'N' THEN 0
          ELSE NULL END AS full_benefit_1
        FROM {`ref_schema`}.{DBI::SQL(ref_table)}mcaid_rac_code) b
      ON a.rac_code_1 = b.rac_code
      LEFT JOIN
      (SELECT rac_code, 
        CASE 
          WHEN full_benefit = 'Y' THEN 1
          WHEN full_benefit = 'N' THEN 0
          ELSE NULL END AS full_benefit_2
        FROM {`ref_schema`}.{DBI::SQL(ref_table)}mcaid_rac_code) c
      ON a.rac_code_2 = c.rac_code
      LEFT HASH JOIN
      (SELECT geo_hash_raw,
        geo_add1_clean AS geo_add1, geo_add2_clean AS geo_add2, 
        geo_city_clean AS geo_city, geo_state_clean AS geo_state, 
        geo_zip_clean AS geo_zip, geo_hash_clean, geo_hash_geocode
        FROM ", address_clean_table, ") d
      ON 
      a.geo_hash_raw = d.geo_hash_raw"),
    .con = conn)
  
  message("Running step 1a: pull columns from raw, join to clean addresses, and add index")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = conn, step1a_sql)
  
  
  # Add an index to the temp table to make ordering much faster
  odbc::dbGetQuery(conn = conn,
                   "CREATE CLUSTERED INDEX [timevar_01_idx] ON ##timevar_01a 
           (id_mcaid, calmonth, fromdate)")
  
  time_end <- Sys.time()
  message(paste0("Step 1a took ", round(difftime(time_end, time_start, units = "secs"), 2), 
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
                 " mins)"))
  
  
  # Setup full_benefit flag and drop secondary RAC rows
  # Step 1b = ~5 mins
  try(odbc::dbRemoveTable(conn, "##timevar_01b", temporary = T), silent = T)
  
  step1b_sql <- glue::glue_sql(
    "SELECT a.id_mcaid, a.calmonth, a.fromdate, a.todate, a.dual, 
  a.tpl, a.bsp_group_cid, a.full_benefit, a.cov_type, a.mco_id, 
  a.geo_add1, a.geo_add2, a.geo_city, a.geo_state, a.geo_zip,
  a.geo_hash_clean, a.geo_hash_geocode
  INTO ##timevar_01b
  FROM
  (SELECT id_mcaid, calmonth, fromdate, todate, dual, 
  tpl, bsp_group_cid, cov_type, mco_id,
  CASE 
    WHEN COALESCE(MAX(full_benefit_1), 0) + COALESCE(MAX(full_benefit_2), 0) >= 1 THEN 1
    WHEN COALESCE(MAX(full_benefit_1), 0) + COALESCE(MAX(full_benefit_2), 0) = 0 THEN 0
    END AS full_benefit,
  geo_add1, geo_add2, geo_city, geo_state, geo_zip, geo_hash_clean, geo_hash_geocode, 
  ROW_NUMBER() OVER(PARTITION BY id_mcaid, calmonth, fromdate  
                    ORDER BY id_mcaid, calmonth, fromdate) AS group_row 
  FROM ##timevar_01a
  GROUP BY id_mcaid, calmonth, fromdate, todate, dual, 
  tpl, bsp_group_cid, cov_type, mco_id,
  geo_add1, geo_add2, geo_city, geo_state, geo_zip, geo_hash_clean, geo_hash_geocode) a
  WHERE a.group_row = 1",
    .con = conn)
  
  
  message("Running step 1b: set up full_benefit flag and drop secondary RAC rows")
  time_start <- Sys.time()
  
  odbc::dbGetQuery(conn = conn, step1b_sql)
  time_end <- Sys.time()
  message(paste0("Step 1b took ", round(difftime(time_end, time_start, units = "secs"), 2), 
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
                 " mins)"))
  
  
  #### STEP 2: MAKE START AND END DATES (~13 MINS) ####
  # Make a start and end date for each month
  # Step 2a = ~4.5 mins
  try(odbc::dbRemoveTable(conn, "##timevar_02a", temporary = T), silent = T)
  
  step2a_sql <- glue::glue_sql(
    "SELECT id_mcaid, dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip, geo_hash_clean, geo_hash_geocode, 
    calmonth AS startdate, dateadd(day, - 1, dateadd(month, 1, calmonth)) AS enddate,
    fromdate, todate
    INTO ##timevar_02a
    FROM ##timevar_01b
    GROUP BY id_mcaid, calmonth, dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip, geo_hash_clean, geo_hash_geocode, fromdate, todate",
    .con = conn)
  
  message("Running step 2a: Make a start and end date for each month")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = conn, step2a_sql)
  time_end <- Sys.time()
  message(paste0("Step 2a took ", round(difftime(time_end, time_start, units = "secs"), 2),
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
                 " mins)"))
  
  
  # Incorporate sub-month coverage (identify the smallest possible time interval)
  # Note that the step above leads to duplicate rows so use distinct here to clear
  #  them out once submonth coverage is accounted for
  # Step 2b = ~8.5 mins
  try(odbc::dbRemoveTable(conn, "##timevar_02b", temporary = T), silent = T)
  
  step2b_sql <- glue::glue_sql(
    "SELECT DISTINCT a.id_mcaid, a.from_date, a.to_date, a.dual, a.tpl, 
  a.bsp_group_cid, a.full_benefit, a.cov_type, a.mco_id, 
  a.geo_add1, a.geo_add2, a.geo_city, a.geo_state, a.geo_zip, 
  a.geo_hash_clean, a.geo_hash_geocode
  INTO ##timevar_02b
  FROM
  (SELECT id_mcaid, dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip, geo_hash_clean, geo_hash_geocode, 
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
    .con = conn)
  
  
  message("Running step 2b: Incorporate submonth coverage")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = conn, step2b_sql)
  
  # Add an index to the temp table to make the next step much faster
  odbc::dbGetQuery(conn = conn,
                   "CREATE CLUSTERED INDEX [timevar_02b_idx] ON ##timevar_02b 
                 (id_mcaid, from_date, to_date)")
  
  time_end <- Sys.time()
  message(paste0("Step 2b took ", round(difftime(time_end, time_start, units = "secs"), 2),
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
                 " mins)"))
  
  
  
  #### STEP 3: IDENTIFY CONTIGUOUS PERIODS (~45 MINS↑) ####
  # Calculate the number of days between each from_date and the previous to_date
  # Step 3a = ~8 mins
  try(odbc::dbRemoveTable(conn, "##timevar_03a", temporary = T), silent = T)
  
  step3a_sql <- glue::glue_sql(
    "SELECT DISTINCT id_mcaid, from_date, to_date, 
  dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
  geo_add1, geo_add2, geo_city, geo_state, geo_zip, geo_hash_clean, geo_hash_geocode, 
  DATEDIFF(day, lag(to_date) OVER (
    PARTITION BY id_mcaid, 
      dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
      geo_add1, geo_add2, geo_city, geo_state, geo_zip, geo_hash_clean, geo_hash_geocode
      ORDER BY id_mcaid, from_date), from_date) AS group_num
  INTO ##timevar_03a
  FROM ##timevar_02b"
  )
  
  message("Running step 3a: calculate # days between each from_date and previous to_date")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = conn, step3a_sql)
  time_end <- Sys.time()
  message(paste0("Step 3a took ", round(difftime(time_end, time_start, units = "secs"), 2),
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
                 " mins)"))
  
  
  # Give a unique identifier (row number) to the first date in a continguous series of dates 
  # (meaning 0 or 1 days between each from_date and the previous to_date)
  # Step 3b = ~25 mins
  try(odbc::dbRemoveTable(conn, "##timevar_03b", temporary = T), silent = T)
  
  step3b_sql <- glue::glue_sql(
    "SELECT DISTINCT id_mcaid, from_date, to_date,
    dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip, geo_hash_clean, geo_hash_geocode, 
    CASE 
      WHEN group_num > 1  OR group_num IS NULL THEN ROW_NUMBER() OVER (PARTITION BY id_mcaid ORDER BY from_date) + 1
      WHEN group_num <= 1 THEN NULL
      END AS group_num
    INTO ##timevar_03b
    FROM ##timevar_03a",
    .con = conn)
  
  message("Running step 3b: Generate unique ID for first date in contiguous series of dates")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = conn, step3b_sql)
  
  # Add an index to the temp table to make the next step faster (not sure it's actually helping)
  odbc::dbGetQuery(conn = conn,
                   "CREATE CLUSTERED INDEX [timevar_03b_idx] ON ##timevar_03b 
                 (id_mcaid, from_date, to_date)")
  
  time_end <- Sys.time()
  message(paste0("Step 3b took ", round(difftime(time_end, time_start, units = "secs"), 2),
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
                 " mins)"))
  
  
  # Use the row number for the first in the series of contiguous dates as an 
  # identifier for that set of contiguous dates
  # Step 3c = ~11 mins
  try(odbc::dbRemoveTable(conn, "##timevar_03c", temporary = T), silent = T)
  
  step3c_sql <- glue::glue_sql(
    "SELECT DISTINCT id_mcaid, from_date, to_date,
    dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip,
    geo_hash_clean, geo_hash_geocode, 
    group_num = max(group_num) OVER 
      (PARTITION BY id_mcaid, dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
        geo_add1, geo_add2, geo_city, geo_state, geo_zip, geo_hash_clean, geo_hash_geocode 
        ORDER BY from_date)
    INTO ##timevar_03c
    FROM ##timevar_03b",
    .con = conn)
  
  message("Running step 3c: Replicate unique ID across contiguous dates")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = conn, step3c_sql)
  time_end <- Sys.time()
  message(paste0("Step 3c took ", round(difftime(time_end, time_start, units = "secs"), 2),
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
                 " mins)"))
  
  
  
  #### STEP 4: FIND MIN/MAX DATES AND CONTIGUOUS PERIODS (~6 MINS) ####
  # Find the min/max dates
  # Step 4a = ~6 mins
  try(odbc::dbRemoveTable(conn, "##timevar_04a", temporary = T), silent = T)
  
  step4a_sql <- glue::glue_sql(
    "SELECT id_mcaid, dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip, geo_hash_clean, geo_hash_geocode,
    MIN(from_date) AS from_date,
    MAX(to_date) AS to_date
    INTO ##timevar_04a
    FROM ##timevar_03c
    GROUP BY id_mcaid, dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip, geo_hash_clean, geo_hash_geocode,
    group_num",
    .con = conn)
  
  message("Running step 4a: Find the min/max dates for contiguous periods")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = conn, step4a_sql)
  time_end <- Sys.time()
  message(paste0("Step 4a took ", round(difftime(time_end, time_start, units = "secs"), 2),
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
                 " mins)"))
  
  
  # Calculate coverage time
  # Step 4b = ~<1 min
  try(odbc::dbRemoveTable(conn, "##timevar_04b", temporary = T), silent = T)
  
  step4b_sql <- glue::glue_sql(
    "SELECT id_mcaid, from_date, to_date, dual, tpl, 
    bsp_group_cid, full_benefit, cov_type, mco_id,
    geo_add1, geo_add2, geo_city, geo_state, geo_zip, 
    geo_hash_clean, geo_hash_geocode,
    DATEDIFF(dd, from_date, to_date) + 1 as cov_time_day
    INTO ##timevar_04b
    FROM ##timevar_04a",
    .con = conn)
  
  message("Running step 4b: Find the min/max dates for contiguous periods")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = conn, step4b_sql)
  time_end <- Sys.time()
  message(paste0("Step 4b took ", round(difftime(time_end, time_start, units = "secs"), 2),
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
                 " mins)"))
  
  
  #### STEP 5: ADD CONTIGUOUS FLAG, RECODE DUAL, JOIN TO GEOCODES, AND LOAD TO STAGE TABLE (~ 1.5 MINS) ####
  # Drop stage mcaid_elig_timevar (just in case)
  # Step 5a = ~<1 min
  message("Running step 5a: Drop stage table")
  time_start <- Sys.time()
  if (dbExistsTable(conn = conn,
                   name = DBI::Id(schema = to_schema, table = to_table))) {
    odbc::dbGetQuery(conn = conn, 
                     glue::glue_sql("DROP TABLE {`to_schema`}.{`to_table`}", .con = conn))
  }
  time_end <- Sys.time()
  message(paste0("Step 5a took ", round(difftime(time_end, time_start, units = "secs"), 2), 
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2), 
                 " mins)"))
  
  # Now do the final transformation plus loading
  # Step 5b = ~1.5 mins
  if (server == "hhsaw") {
    address_geocode_table <- glue::glue_sql("{`address_schema`}.{`geocode_table`}", 
                                            .con = conn)
    address_geokc_table <- glue::glue_sql("{`to_schema`}.{`geokc_table`}", 
                                          .con = conn)
  } else {
    conn_hhsaw <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)
    df_address_geocode <- odbc::dbGetQuery(conn_hhsaw, 
                           glue::glue_sql("SELECT geo_hash_geocode,        
                                          geo_id10_county,         
                                          geo_id10_tract, 
                                          geo_id10_hra,         
                                          geo_id10_schooldistrict        
                                          FROM {`address_schema`}.{`geocode_table`}",                                         
                                          .con = conn_hhsaw))
    conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)
    try(odbc::dbRemoveTable(conn, "##address_geocode_05b", temporary = T), silent = T)
    DBI::dbWriteTable(conn, 
                      name = "##address_geocode_05b", 
                      value = df_address_geocode)
    address_geocode_table <- "##address_geocode_05b"
  }
  step5b_sql <- glue::glue_sql(paste0(
    "SELECT
    a.id_mcaid, a.from_date, a.to_date, 
    CASE WHEN DATEDIFF(day, lag(a.to_date, 1) OVER 
      (PARTITION BY a.id_mcaid order by a.id_mcaid, a.from_date), a.from_date) = 1
      THEN 1 ELSE 0 END AS contiguous, 
    CASE WHEN a.dual = 'Y' THEN 1 ELSE 0 END AS dual,
    CASE WHEN a.tpl = 'Y' THEN 1 ELSE 0 END AS tpl,
    a.bsp_group_cid, a.full_benefit, 
    CASE WHEN a.dual <> 'Y' AND a.tpl <> 'Y' AND a.full_benefit = 1 THEN 1 ELSE 0
      END AS full_criteria, 
    a.cov_type, a.mco_id,
    a.geo_add1, a.geo_add2, a.geo_city, a.geo_state, a.geo_zip, 
    a.geo_hash_clean, a.geo_hash_geocode, 
    b.geo_county_code, b.geo_tract_code, 
    b.geo_hra_code, b.geo_school_code, a.cov_time_day,
    c.geo_kc_new
    {Sys.time()} AS last_run
    INTO {`to_schema`}.{`to_table`}
    FROM
    (SELECT id_mcaid, from_date, to_date, 
      dual, tpl, bsp_group_cid, full_benefit, cov_type, mco_id,
      geo_add1, geo_add2, geo_city, geo_state, geo_zip, geo_hash_clean, geo_hash_geocode, 
      cov_time_day
      FROM ##timevar_04b) a
      LEFT JOIN
      (SELECT DISTINCT geo_hash_geocode,
        geo_id10_county AS geo_county_code, 
        geo_id10_tract AS geo_tract_code, geo_id10_hra AS geo_hra_code, 
        geo_id10_schooldistrict AS geo_school_code
        FROM ", address_geocode_table, ") b
      ON a.geo_hash_geocode = b.geo_hash_geocode
      LEFT JOIN
      (SELECT DISTINCT geo_zip, geo_kc AS geo_kc_new
       FROM ", address_geokc_table, ") c
      ON a.geo_zip = c.geo_zip
    "),
    .con = conn)
  
  message("Running step 5b: Join to geocodes and load to stage table")
  time_start <- Sys.time()
  odbc::dbGetQuery(conn = conn, step5b_sql)
  time_end <- Sys.time()
  message(paste0("Step 5b took ", round(difftime(time_end, time_start, units = "secs"), 2), 
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2), 
                 " mins)"))
  
  # Use APDE method for King County residence flag
  # Step 5c ~ XX minutes
  step5c_sql <- glue::glue_sql(paste0(
    "ALTER TABLE {`to_schema`}.{`to_table`}
       ADD geo_kc AS
         CASE
           WHEN (geo_county_code IS NOT NULL) AND (geo_county_code IN (033, 53033)) THEN 1
           WHEN (geo_county_code IS NULL) AND (geo_kc_new = 1) THEN 1
           ELSE 0
         END
       DROP COLUMN geo_kc_new
    "),
    .con = conn)
  
  
  #### STEP 6: REMOVE TEMPORARY TABLES ####
  message("Running step 6: Remove temporary tables")
  time_start <- Sys.time()
  try(odbc::dbRemoveTable(conn, "##timevar_01a", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##timevar_01b", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##timevar_02a", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##timevar_02b", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##timevar_03a", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##timevar_03b", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##timevar_03c", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##timevar_04a", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##timevar_04b", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##address_clean_01a", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##address_geocode_05b", temporary = T), silent = T)
  rm(list = ls(pattern = "step(.){1,2}_sql"))
  time_end <- Sys.time()
  message(paste0("Step 6 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2), 
                 " mins)"))
}

