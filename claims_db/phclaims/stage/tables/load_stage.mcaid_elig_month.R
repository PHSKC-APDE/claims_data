# Eli Kern
# 2025-06-10

# Code to create a SQL table that holds time-varying information at level of Medicaid member-month.


## Function elements
# conn = database connection
# server = HHSAW
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

load_stage_mcaid_elig_month_f <- function(conn = NULL,
                                        server = c("hhsaw"),
                                        config = NULL,
                                        get_config = F) {
  
  
  # Set up variables from config file
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
  date_table <- config[["hhsaw"]][["date_table"]]
  
  message("Creating ", to_schema, ".", to_table, ". This will take ~10 minutes to run.")
  

  #### STEP 1: Address filtering ####
  
  step1_sql <- glue::glue_sql(paste0(
    "
    IF OBJECT_ID('tempdb..#address_clean_01a') IS NOT NULL DROP TABLE #address_clean_01a;
    
    CREATE TABLE #address_clean_01a
    WITH (
        DISTRIBUTION = HASH(geo_hash_raw),
        HEAP
    )
    AS
    SELECT a.*
    FROM {`address_schema`}.{`address_table`} a
    INNER JOIN (
        SELECT DISTINCT geo_hash_raw FROM {`from_schema`}.{`from_table`}
    ) b ON a.geo_hash_raw = b.geo_hash_raw;"),
    .con = conn)
  
  message("Running step 1: Filter addresses to those found in Medicaid elig data")
  time_start <- Sys.time()
  odbc::dbSendQuery(conn = conn, step1_sql)
  time_end <- Sys.time()
  message(paste0("Step 1 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
                 " mins)"))
  
  
  #### STEP 2: Member data join ####
  
  step2_sql <- glue::glue_sql(
    "
    IF OBJECT_ID('tempdb..#month_01a') IS NOT NULL DROP TABLE #month_01a;

    CREATE TABLE #month_01a
    WITH (
        DISTRIBUTION = HASH(id_mcaid),
        HEAP
    )
    AS
    SELECT DISTINCT
        a.id_mcaid,
    	CAST(a.CLNDR_YEAR_MNTH as int) as year_month,
        CONVERT(DATE, CAST(a.CLNDR_YEAR_MNTH as varchar(6)) + '01', 112) AS calmonth,
        a.fromdate, a.todate, a.dual, a.health_home_flag, a.bsp_group_cid,
        b.full_benefit, a.cov_type, a.mco_id,
        d.geo_add1_clean as geo_add1, d.geo_add2_clean as geo_add2, d.geo_city_clean as geo_city,
    	d.geo_state_clean as geo_state, d.geo_zip_clean as geo_zip, d.geo_hash_clean, d.geo_hash_geocode
    FROM (
        SELECT
            MBR_H_SID AS id_mcaid,
            CLNDR_YEAR_MNTH,
            RAC_FROM_DATE AS fromdate,
            RAC_TO_DATE AS todate,
            CASE WHEN MIN(DUALELIGIBLE_INDICATOR) = 'N/A'
                THEN MAX(DUALELIGIBLE_INDICATOR)
                ELSE MIN(DUALELIGIBLE_INDICATOR) END AS dual,
            HEALTH_HOME_CLINICAL_INDICATOR AS health_home_flag,
            RAC_CODE AS rac_code,
            RPRTBL_BSP_GROUP_CID AS bsp_group_cid,
            COVERAGE_TYPE_IND AS cov_type,
            MC_PRVDR_ID AS mco_id,
            geo_hash_raw
        FROM {`from_schema`}.{`from_table`}
        GROUP BY MBR_H_SID, CLNDR_YEAR_MNTH, RAC_FROM_DATE, RAC_TO_DATE,
                 HEALTH_HOME_CLINICAL_INDICATOR, RAC_CODE,
                 RPRTBL_BSP_GROUP_CID, COVERAGE_TYPE_IND, MC_PRVDR_ID, geo_hash_raw
    ) a
    LEFT JOIN (
        SELECT rac_code,
            CASE WHEN full_benefit = 'Y' THEN 1
                 WHEN full_benefit = 'N' THEN 0
                 ELSE NULL END AS full_benefit
        FROM {`ref_schema`}.{DBI::SQL(ref_table)}mcaid_rac_code
    ) b ON a.rac_code = b.rac_code
    LEFT JOIN #address_clean_01a d ON a.geo_hash_raw = d.geo_hash_raw;",
    .con = conn)
  
  
  message("Running step 2: Join member elig data to clean addresses and create full_benefit flag")
  time_start <- Sys.time()
  odbc::dbSendQuery(conn = conn, step2_sql)
  time_end <- Sys.time()
  message(paste0("Step 2 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
                 " mins)"))
  
  
  #### STEP 3: Dedup by calmonth/fromdate/todate ####
  
  step3_sql <- glue::glue_sql(
    "
    IF OBJECT_ID('tempdb..#month_01b') IS NOT NULL DROP TABLE #month_01b;

    CREATE TABLE #month_01b
    WITH (
        DISTRIBUTION = HASH(id_mcaid),
        HEAP
    )
    AS
    SELECT a.id_mcaid, a.year_month, a.calmonth, a.fromdate, a.todate, a.dual, a.health_home_flag, 
      a.bsp_group_cid, a.full_benefit, a.cov_type, a.mco_id, 
      a.geo_add1, a.geo_add2, a.geo_city, a.geo_state, a.geo_zip,
      a.geo_hash_clean, a.geo_hash_geocode
      INTO #month_01b
      FROM (
    	  SELECT id_mcaid, year_month, calmonth, fromdate, todate, dual, health_home_flag, 
    	  bsp_group_cid, cov_type, mco_id,
    	  MAX(full_benefit) OVER(PARTITION BY id_mcaid, calmonth) as full_benefit,
    	  geo_add1, geo_add2, geo_city, geo_state, geo_zip, geo_hash_clean, geo_hash_geocode, 
    	  ROW_NUMBER() OVER(PARTITION BY id_mcaid, calmonth, fromdate, todate ORDER BY id_mcaid, calmonth, fromdate, todate) AS group_row 
    	  FROM #month_01a
      ) a
      WHERE a.group_row = 1;",
    .con = conn)
  
  message("Running step 3: Deduplicate by calmonth/fromdate/todate")
  time_start <- Sys.time()
  odbc::dbSendQuery(conn = conn, step3_sql)
  time_end <- Sys.time()
  message(paste0("Step 3 took ", round(difftime(time_end, time_start, units = "secs"), 2),
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
                 " mins)"))
  
  
  #### STEP 4: Add calendar boundaries ####
  
  step4_sql <- glue::glue_sql(
    "
    IF OBJECT_ID('tempdb..#month_02a') IS NOT NULL DROP TABLE #month_02a;

    CREATE TABLE #month_02a
    WITH (
        DISTRIBUTION = HASH(id_mcaid),
        HEAP
    )
    AS
    SELECT
        id_mcaid, year_month, dual, health_home_flag, bsp_group_cid, full_benefit,
        cov_type, mco_id,
        geo_add1, geo_add2, geo_city, geo_state, geo_zip,
        geo_hash_clean, geo_hash_geocode,
        calmonth AS startdate,
        DATEADD(DAY, -1, DATEADD(MONTH, 1, calmonth)) AS enddate,
        fromdate,
        MAX(todate) AS todate
    FROM #month_01b
    GROUP BY id_mcaid, year_month, calmonth, dual, health_home_flag, bsp_group_cid, full_benefit,
             cov_type, mco_id, geo_add1, geo_add2, geo_city, geo_state, geo_zip,
             geo_hash_clean, geo_hash_geocode, fromdate;",
    .con = conn)
  
  message("Running step 4: Add calendar boundaries")
  time_start <- Sys.time()
  odbc::dbSendQuery(conn = conn, step4_sql)
  time_end <- Sys.time()
  message(paste0("Step 4 took ", round(difftime(time_end, time_start, units = "secs"), 2),
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
                 " mins)"))
  
  
  #### STEP 5: Limit submonth intervals ####
  
  step5_sql <- glue::glue_sql(paste0(
    "
    IF OBJECT_ID('tempdb..#month_02b') IS NOT NULL DROP TABLE #month_02b;

    CREATE TABLE #month_02b
    WITH (
        DISTRIBUTION = HASH(id_mcaid),
        HEAP
    )
    AS
    SELECT DISTINCT
        id_mcaid,
    	year_month,
        CASE
            WHEN fromdate IS NULL THEN startdate
            WHEN startdate >= fromdate THEN startdate
    		WHEN startdate < fromdate AND MIN(fromdate) OVER(PARTITION BY id_mcaid, startdate) <= startdate THEN startdate
            WHEN startdate < fromdate THEN MIN(fromdate) OVER (PARTITION BY id_mcaid, startdate)
            ELSE NULL
        END AS from_date,
        CASE
            WHEN todate IS NULL THEN enddate
            WHEN enddate <= todate THEN enddate
    		WHEN enddate > todate AND MAX(todate) OVER (partition BY id_mcaid, startdate) >= enddate THEN enddate
            WHEN enddate > todate THEN MAX(todate) OVER (PARTITION BY id_mcaid, startdate)
            ELSE NULL
        END AS to_date,
        dual, health_home_flag, bsp_group_cid, full_benefit, cov_type, mco_id,
        geo_add1, geo_add2, geo_city, geo_state, geo_zip,
        geo_hash_clean, geo_hash_geocode
    FROM #month_02a;"),
    .con = conn)
  
  message("Running step 5: Limit submonth intervals")
  time_start <- Sys.time()
  odbc::dbSendQuery(conn = conn, step5_sql)
  time_end <- Sys.time()
  message(paste0("Step 5 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2), 
                 " mins)"))
  
  
  #### STEP 6: Collapse full_benefit ####
  
  step6_sql <- glue::glue_sql(paste0(
    "
    IF OBJECT_ID('tempdb..#month_02c') IS NOT NULL DROP TABLE #month_02c;

    CREATE TABLE #month_02c
    WITH (
        DISTRIBUTION = HASH(id_mcaid),
        HEAP
    )
    AS
    SELECT
        id_mcaid, year_month, from_date, to_date, dual, health_home_flag, bsp_group_cid,
        cov_type, mco_id, geo_add1, geo_add2, geo_city, geo_state, geo_zip,
        geo_hash_clean, geo_hash_geocode,
        MAX(full_benefit) AS full_benefit
    FROM #month_02b
    GROUP BY
        id_mcaid, year_month, from_date, to_date, dual, health_home_flag, bsp_group_cid,
        cov_type, mco_id, geo_add1, geo_add2, geo_city, geo_state, geo_zip,
        geo_hash_clean, geo_hash_geocode;"),
    .con = conn)
  
  message("Running step 6: Collapse full_benefit")
  time_start <- Sys.time()
  odbc::dbSendQuery(conn = conn, step6_sql)
  time_end <- Sys.time()
  message(paste0("Step 6 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2), 
                 " mins)"))
  
  
  #### STEP 7: Insert data into table shell ####
  
  #First, create table shell
  create_table_f(conn = conn, 
                 server = server,
                 config = config,
                 overwrite = T)
  
  #Prep and run SQL code
  step7_sql <- glue::glue_sql(paste0(
    "
    INSERT INTO {`to_schema`}.{`to_table`}
    SELECT
        a.id_mcaid, a.from_date, a.to_date,
        d.year_month, d.year_quarter, d.[year],
        CASE WHEN a.dual IN ('DualEligible', 'PartialDual', 'Y') THEN 1 ELSE 0 END AS dual,
        CASE WHEN a.health_home_flag IN ('Y', 'YES') THEN 1 ELSE 0 END AS health_home_flag,
        a.bsp_group_cid, a.full_benefit,
        CASE WHEN a.dual NOT IN ('DualEligible', 'PartialDual', 'Y') AND a.full_benefit = 1 THEN 1 ELSE 0 END AS full_criteria,
        a.cov_type, a.mco_id,
        a.geo_add1, a.geo_add2, a.geo_city, a.geo_state, a.geo_zip,
        a.geo_hash_clean, a.geo_hash_geocode,
        b.geo_county_code, b.geo_tract_code, b.geo_hra_code, b.geo_school_code,
        CASE
            WHEN b.geo_county_code IN (033, 53033) THEN 1
            WHEN b.geo_county_code IS NULL AND c.geo_kc_new = 1 THEN 1
            ELSE 0
        END AS geo_kc,
    	DATEDIFF(DAY, a.from_date, a.to_date) + 1 AS cov_time_day,
        GETDATE() AS last_run
    FROM (
        SELECT *,
            DATEDIFF(DAY, from_date, to_date) + 1 AS cov_time_day
        FROM #month_02c
    ) a
    LEFT JOIN (
        SELECT DISTINCT geo_hash_geocode,
            geo_id20_county AS geo_county_code,
            geo_id20_tract AS geo_tract_code,
            geo_id20_hra AS geo_hra_code,
            geo_id20_schooldistrict AS geo_school_code
        FROM {`address_schema`}.{`geocode_table`}
    ) b ON a.geo_hash_geocode = b.geo_hash_geocode
    LEFT JOIN (
        SELECT DISTINCT geo_zip, geo_kc AS geo_kc_new
        FROM {`ref_schema`}.{`geokc_table`}
    ) c ON a.geo_zip = c.geo_zip
    LEFT JOIN (
        SELECT DISTINCT year_month, year_quarter, [year]
        FROM {`ref_schema`}.{`date_table`}
    ) d ON a.year_month = d.year_month;"),
    .con = conn)
  
  message("Running step 7: Insert data into table shell")
  time_start <- Sys.time()
  odbc::dbSendQuery(conn = conn, step7_sql)
  time_end <- Sys.time()
  message(paste0("Step 7 took ", round(difftime(time_end, time_start, units = "secs"), 2), 
                 " secs (", round(difftime(time_end, time_start, units = "mins"), 2), 
                 " mins)"))
  
}