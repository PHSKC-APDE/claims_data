## Header ####
# Author: Danny Colombara, based on code by Alatair Matheson
# 
# R version: 3.5.3
#
# Purpose: Code to QA Medicare time varying data (stage.mcare_elig_timevar)
# 
# Notes: Type the <Alt> + <o> at the same time to collapse the code and view the structure
#

# This is all one function ----
qa_mcare_elig_timevar_f <- function(conn = db_claims,
                                    load_only = F) {
  
  # If this is the first time ever loading data, only load values.
  #   Otherwise, check against existing QA values
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  # Rows in current table
  row_count <- as.numeric(odbc::dbGetQuery(conn, 
                                           "SELECT COUNT (*) FROM stage.mcare_elig_timevar"))
  
  ### Pull out run date of stage.mcare_elig_timevar
  last_run <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcare_elig_timevar")[[1]])
  
  
  if (load_only == F) {
    #### COUNT NUMBER OF ROWS ####
    # Pull in the reference value
    previous_rows <- as.numeric(
      odbc::dbGetQuery(conn, 
                       "SELECT c.qa_value from
                       (SELECT a.* FROM
                       (SELECT * FROM metadata.qa_mcare_values
                       WHERE table_name = 'stage.mcare_elig_timevar' AND
                       qa_item = 'row_count') a
                       INNER JOIN
                       (SELECT MAX(qa_date) AS max_date 
                       FROM metadata.qa_mcare_values
                       WHERE table_name = 'stage.mcare_elig_timevar' AND
                       qa_item = 'row_count') b
                       ON a.qa_date = b.max_date)c"))

    if(is.na(previous_rows)){previous_rows = 0}
    
    row_diff <- row_count - previous_rows
    
    if (row_diff < 0) {
      odbc::dbGetQuery(
        conn = conn,
        glue::glue_sql("INSERT INTO metadata.qa_mcare
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcare_elig_timevar',
                       'Number new rows compared to most recent run', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {row_diff} fewer rows in the most recent table 
                       ({row_count} vs. {previous_rows})')",
                       .con = conn))
      
      problem.row_diff <- glue::glue("Fewer rows than found last time.  
                      Check metadata.qa_mcare for details (last_run = {last_run})
                      \n")
    } else {
      odbc::dbGetQuery(
        conn = conn,
        glue::glue_sql("INSERT INTO metadata.qa_mcare
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcare_elig_timevar',
                       'Number new rows compared to most recent run', 
                       'PASS', 
                       {Sys.time()}, 
                       'There were {row_diff} more rows in the most recent table 
                       ({row_count} vs. {previous_rows})')",
                       .con = conn))
      
      problem.row_diff <- glue::glue(" ") # no problem, so empty error message
      
    }
  }
  
  
  #### CHECK DISTINCT IDS = DISTINCT IN STAGE.MCARE_ELIG (WA State) ####
  id_count_timevar <- as.numeric(odbc::dbGetQuery(
    conn, "SELECT COUNT (DISTINCT id_mcare) AS count FROM stage.mcare_elig_timevar"))

  id_count_mbsf <- as.numeric(odbc::dbGetQuery(
    conn, "SELECT COUNT(*)
    FROM
    (
    SELECT bene_id
    FROM stage.mcare_mbsf_ab
    WHERE bene_id IS NOT NULL
    UNION
    SELECT bene_id
    FROM stage.mcare_mbsf_abcd
    WHERE bene_id IS NOT NULL
    ) A;"))

  if (id_count_timevar != id_count_mbsf) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcare
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES (
                      {last_run}, 
                     'stage.mcare_elig_timevar',
                     'Number distinct IDs', 
                     'FAIL', 
                     {Sys.time()}, 
                     'There were {id_count_timevar} distinct IDs but {id_count_mbsf} in the WA MBSF data (should be the same)'
                     )
                     ",
                     .con = conn))
    
    problem.ids  <- glue::glue("Number of distinct IDs doesn't match the number in WA MBSF data. 
                    Check metadata.qa_mcare for details (last_run = {last_run})
                   \n")
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcare
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.mcare_elig_timevar',
                     'Number distinct IDs', 
                     'PASS', 
                     {Sys.time()}, 
                     'The number of distinct IDs matched number in the WA MBSF data  
                     ({id_count_timevar})')",
                     .con = conn))
      
    problem.ids  <- glue::glue(" ") # no problem
  }
  
  #### CHECK FOR DUPLICATE ROWS  ####
  dup_row_count <- as.numeric(odbc::dbGetQuery(
    conn, 
    "SELECT COUNT (*) AS count FROM 
    (SELECT DISTINCT id_mcare, from_date, to_date, contiguous, dual, buyin, part_a, part_b, part_c, zip, kc, cov_time_day
    FROM stage.mcare_elig_timevar) a"))
  
  
  if (dup_row_count != row_count) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcare
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.mcare_elig_timevar',
                     'Duplicate rows', 
                     'FAIL', 
                     {Sys.time()}, 
                     'There were {dup_row_count} distinct rows but {row_count} rows overall (should be the same)')",
                     .con = conn))
    
    problem.dups <- glue::glue("There appear to be duplicate rows. 
                    Check metadata.qa_mcare for details (last_run = {last_run})
                   \n")
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcare
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.mcare_elig_timevar',
                     'Duplicate rows', 
                     'PASS', 
                     {Sys.time()}, 
                     'The number of distinct rows (excl. ref_geo vars) matched number total rows ({row_count})')",
                     .con = conn))
    
    problem.dups <- glue::glue(" ")
  }
  
  
  
  #### MIN AND MAX DATES IN DATA ####
  date_range_timevar <- dbGetQuery(db_claims, 
                                   "SELECT MIN(from_date) AS from_date, max(to_date) as to_date 
                                   FROM stage.mcare_elig_timevar")
  date_range_elig <- data.table(from_date = as.Date("2011-01-01"), # hard coded from and to dates
                                to_date = as.Date("2016-12-31"))

  if (date_range_timevar$from_date < date_range_elig$from_date | 
      date_range_timevar$to_date > date_range_elig$to_date) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcare 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.mcare_elig_timevar',
                     'Date range',
                     'FAIL',
                     {Sys.time()}, 
                     'Some from/to dates fell outside the CLNDR_YEAR_MNTH range 
                     (min: {`from`}, max: {`to`})')",
                     .con = conn,
                     from = dbQuoteIdentifier(conn, as.character(date_range_timevar$from_date)),
                     to = dbQuoteIdentifier(conn, as.character(date_range_timevar$to_date))
                     ))
    
    problem.dates <- glue::glue("Some from/to dates fell outside the CLNDR_YEAR_MNTH range. 
                    Check metadata.qa_mcare for details (last_run = {last_run})
                    \n")
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcare 
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.mcare_elig_timevar',
                     'Date range',
                     'PASS',
                     {Sys.time()}, 
                     'All from/to dates fell within the CLNDR_YEAR_MNTH range 
                     (min: {`from`}, max: {`to`})')",
                     .con = conn,
                     from = dbQuoteIdentifier(conn, as.character(date_range_elig$from_date)),
                     to = dbQuoteIdentifier(conn, as.character(date_range_elig$to_date))
                     ))
    
    problem.dates <- glue::glue("")
  }
  
  #### LOAD VALUES TO QA_VALUES TABLE ####
  load_sql <- glue::glue_sql("INSERT INTO metadata.qa_mcare_values
                             (table_name, qa_item, qa_value, qa_date, note) 
                             VALUES ('stage.mcare_elig_timevar',
                             'row_count', 
                             {row_count}, 
                             {Sys.time()}, 
                             '')",
                             .con = conn)
  
  odbc::dbGetQuery(conn = conn, load_sql)
  
  
  #### Identify problems / fails ####
  if(problem.row_diff >1 | problem.ids>1 | problem.dups>1 | problem.dates>1){
                    problems <- glue::glue("****STOP!!!!!!!!****
                         Please address the following issues that have been logged in [PHClaims].[metadata].[qa_mcare] ... \n", 
                         problem.row_diff, "\n", 
                         problem.ids, "\n", 
                         problem.dups, "\n", 
                         problem.dates)}else{
                    problems <- glue::glue("All QA checks passed and recorded to [PHClaims].[metadata].[qa_mcare]")       
                         }
  message(problems)
  
}

# The end! ----
