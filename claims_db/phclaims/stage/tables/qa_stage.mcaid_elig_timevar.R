###############################################################################
# Alastair Matheson
# 2019-05

# Code to QA stage.mcaid_elig_timevar

###############################################################################


# No overlaps

qa_mcaid_elig_timevar_f <- function(conn = db_claims,
                                    load_only = F) {
  
  # If this is the first time ever loading data, only load values.
  #   Otherwise, check against existing QA values
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  # Rows in current table
  row_count <- as.numeric(odbc::dbGetQuery(conn, 
                                           "SELECT COUNT (*) FROM stage.mcaid_elig_timevar"))
  
  ### Pull out run date of stage.mcaid_elig_timevar
  last_run <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_elig_timevar")[[1]])
  
  
  if (load_only == F) {
    #### COUNT NUMBER OF ROWS ####
    # Pull in the reference value
    previous_rows <- as.numeric(
      odbc::dbGetQuery(conn, 
                       "SELECT a.* FROM
                       (SELECT * FROM metadata.qa_mcaid_values
                         WHERE table_name = 'stage.mcaid_elig_timevar' AND
                          qa_item = 'row_count') a
                       INNER JOIN
                       (SELECT MAX(qa_date) AS max_date 
                         FROM metadata.qa_mcaid_values
                         WHERE table_name = 'stage.mcaid_elig_timevar' AND
                          qa_item = 'row_count') b
                       ON a.qa_date = b.max_date"))
    
    row_diff <- row_count < previous_rows
    
    if (row_diff < 0) {
      odbc::dbGetQuery(
        conn = conn,
        glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_elig_timevar',
                   'Number new rows compared to most recent run', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {row_diff} fewer rows in the most recent table 
                       ({row_count} vs. {previous_rows})')",
                       .con = conn))
      
      stop(glue::glue("Fewer rows than found last time.  
                  Check metadata.qa_mcaid for details (last_run = {last_run}"))
    } else {
      odbc::dbGetQuery(
        conn = conn,
        glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_elig_timevar',
                   'Number new rows compared to most recent run', 
                   'PASS', 
                   {Sys.time()}, 
                   'There were {row_diff} more rows in the most recent table 
                       ({row_count} vs. {previous_rows})')",
                       .con = conn))
      
    }
  }
  
  
  #### CHECK DISTINCT IDS = DISTINCT IN STAGE.MCAID_ELIG ####
  id_count_timevar <- as.numeric(odbc::dbGetQuery(
    conn, "SELECT COUNT (DISTINCT id_mcaid) AS count FROM stage.mcaid_elig_timevar"))
  
  id_count_elig <- as.numeric(odbc::dbGetQuery(
    conn, "SELECT COUNT (DISTINCT MEDICAID_RECIPIENT_ID) as count FROM stage.mcaid_elig"))
  
  if (id_count_timevar != id_count_elig) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcaid_elig_timevar',
                       'Number distinct IDs', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {id_count_timevar} distinct IDs but {id_count_elig} in the raw data (should be the same)')",
                     .con = conn))
    
    stop(glue::glue("Number of distinct IDs doesn't match the number of rows. 
                      Check metadata.qa_mcaid for details (last_run = {last_run}"))
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcaid_elig_timevar',
                       'Number distinct IDs', 
                       'PASS', 
                       {Sys.time()}, 
                       'The number of distinct IDs matched number in raw data ({id_count_timevar})')",
                     .con = conn))
  }
  
  
  
  #### CHECK FOR DUPLICATE ROWS  ####
  dup_row_count <- as.numeric(odbc::dbGetQuery(
    conn, 
    "SELECT COUNT (*) AS count FROM 
    (SELECT DISTINCT id_mcaid, from_date, to_date, 
    dual, tpl, bsp_group_name, full_benefit, cov_type, mco_id,
    geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
    cov_time_day 
    FROM stage.mcaid_elig_timevar) a"))
  
  
  if (dup_row_count != row_count) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcaid_elig_timevar',
                       'Duplicate rows', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {dup_row_count} distinct rows (excl. ref_geo vars) but {row_count} rows overall (should be the same)')",
                     .con = conn))
    
    stop(glue::glue("There appear to be duplicate rows. 
                      Check metadata.qa_mcaid for details (last_run = {last_run}"))
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcaid_elig_timevar',
                       'Duplicate rows', 
                       'PASS', 
                       {Sys.time()}, 
                       'The number of distinct rows (excl. ref_geo vars) matched number total rows ({row_count})')",
                     .con = conn))
  }
  
  
  
  #### MIN AND MAX DATES IN DATA ####
  date_range_timevar <- dbGetQuery(db_claims, 
                           "SELECT MIN(from_date) AS from_date, max(to_date) as to_date 
                           FROM stage.mcaid_elig_timevar")
  date_range_elig <- dbGetQuery(db_claims, 
                         "SELECT MIN(CLNDR_YEAR_MNTH) AS from_date, max(CLNDR_YEAR_MNTH) as to_date 
                           FROM stage.mcaid_elig")
  date_range_elig <- date_range_elig %>%
    mutate(
      from_date = as.Date(paste0(from_date, "01"), format = "%Y%m%d"),
      to_date = 
        as.Date(paste0(to_date, "01"), format = "%Y%m%d") + months(1) - ddays(1)
    )
  
  
  if (date_range_timevar$from_date < date_range_elig$from_date | 
      date_range_timevar$to_date > date_range_elig$to_date) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             'stage.mcaid_elig_timevar',
                             'Date range',
                             'FAIL',
                             {Sys.time()}, 
                             'Some from/to dates fell outside the CLNDR_YEAR_MNTH range 
                             (min: {`from`}, max: {`to`})')",
                     .con = conn,
                     from = dbQuoteIdentifier(conn, as.character(date_range_timevar$from_date)),
                     to = dbQuoteIdentifier(conn, as.character(date_range_timevar$to_date))
                     ))
    
    stop(glue::glue("Some from/to dates fell outside the CLNDR_YEAR_MNTH range. 
                    Check metadata.qa_mcaid for details (last_run = {last_run}"))
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             'stage.mcaid_elig_timevar',
                             'Date range',
                             'PASS',
                             {Sys.time()}, 
                             'All from/to dates fell within the CLNDR_YEAR_MNTH range 
                             (min: {`from`}, max: {`to`})')",
                     .con = conn,
                     from = dbQuoteIdentifier(conn, as.character(date_range_elig$from_date)),
                     to = dbQuoteIdentifier(conn, as.character(date_range_elig$to_date))
                     ))
  }
  
  
  
  #### CHECK SPECIFIC INDIVIDUALS TO ENSURE THEIR DATES WORK ####
  timevar_ind <- read.csv("//dchs-shares01/dchsdata/DCHSPHClaimsData/Data/QA_specific/stage.mcaid_elig_timevar_qa_ind.csv",
                          stringsAsFactors = F)
  
  timevar_ind_sql <- glue::glue_sql("SELECT id_mcaid, from_date, to_date FROM stage.mcaid_elig_timevar 
                                    WHERE id_mcaid IN ({ind_ids*})
                                    ORDER BY id_mcaid, from_date",
                                    ind_ids = unlist(distinct(timevar_ind, id_mcaid)),
                                    .con = conn)
  
  timevar_ind_stage <- dbGetQuery(conn, timevar_ind_sql)
  
  if (all_equal(timevar_ind_stage, select(timevar_ind, -notes)) == FALSE) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             'stage.mcaid_elig_timevar',
                             'Specific IDs',
                             'FAIL',
                             {Sys.time()}, 
                             'Some from/to dates did not match expected results for specific IDs')",
                     .con = conn))
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                             'stage.mcaid_elig_timevar',
                             'Specific IDs',
                             'PASS',
                             {Sys.time()}, 
                             'All from/to dates matched expected results for specific IDs')",
                     .con = conn))
  }
  
  
  
  #### LOAD VALUES TO QA_VALUES TABLE ####
  load_sql <- glue::glue_sql("INSERT INTO metadata.qa_mcaid_values
                             (table_name, qa_item, qa_value, qa_date, note) 
                             VALUES ('stage.mcaid_elig_timevar',
                                     'row_count', 
                                     {row_count}, 
                                     {Sys.time()}, 
                                     '')",
                             .con = conn)
  
  odbc::dbGetQuery(conn = conn, load_sql)
  
  
}


