###############################################################################
# Alastair Matheson
# 2019-05

# Code to QA stage.mcaid_elig_demo

###############################################################################


qa_mcaid_elig_demo_f <- function(conn = db_claims,
                                 load_only = F) {
  
  print("Running QA on stage.mcaid_elig_demo")
  # If this is the first time ever loading data, skip some checks.
  #   Otherwise, check against existing QA values
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  # Rows in current table
  row_count <- as.numeric(odbc::dbGetQuery(conn, 
                                           "SELECT COUNT (*) FROM stage.mcaid_elig_demo"))
  
  
  ### Pull out run date of stage.mcaid_elig_demo
  last_run <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_elig_demo")[[1]])
  
  if (load_only == F) {
    #### COUNT NUMBER OF ROWS ####
    # Pull in the reference value
    previous_rows <- as.numeric(
      odbc::dbGetQuery(conn, 
                       "SELECT a.qa_value FROM
                       (SELECT * FROM metadata.qa_mcaid_values
                         WHERE table_name = 'stage.mcaid_elig_demo' AND
                          qa_item = 'row_count') a
                       INNER JOIN
                       (SELECT MAX(qa_date) AS max_date 
                         FROM metadata.qa_mcaid_values
                         WHERE table_name = 'stage.mcaid_elig_demo' AND
                          qa_item = 'row_count') b
                       ON a.qa_date = b.max_date"))
    
    row_diff <- row_count - previous_rows
    
    if (row_diff < 0) {
      row_qa_fail <- 1
      odbc::dbGetQuery(
        conn = conn,
        glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_elig_demo',
                   'Number new rows compared to most recent run', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {row_diff} fewer rows in the most recent table 
                       ({row_count} vs. {previous_rows})')",
                       .con = conn))
      
      message(glue::glue("Fewer rows than found last time.  
                  Check metadata.qa_mcaid for details (last_run = {last_run}"))
    } else {
      row_qa_fail <- 0
      odbc::dbGetQuery(
        conn = conn,
        glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_elig_demo',
                   'Number new rows compared to most recent run', 
                   'PASS', 
                   {Sys.time()}, 
                   'There were {row_diff} more rows in the most recent table 
                       ({row_count} vs. {previous_rows})')",
                       .con = conn))
      
    }
    
  }
  
  #### CHECK DISTINCT IDS = NUMBER OF ROWS ####
  id_count <- as.numeric(odbc::dbGetQuery(conn, 
                                          "SELECT COUNT (DISTINCT id_mcaid) 
                                            FROM stage.mcaid_elig_demo"))
  
  if (id_count != row_count) {
    id_distinct_qa_fail <- 1
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcaid_elig_demo',
                       'Number distinct IDs', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {id_count} distinct IDs but {row_count} rows (should be the same)')",
                     .con = conn))
    
    message(glue::glue("Number of distinct IDs doesn't match the number of rows. 
                      Check metadata.qa_mcaid for details (last_run = {last_run}"))
  } else {
    id_distinct_qa_fail <- 0
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcaid_elig_demo',
                       'Number distinct IDs', 
                       'PASS', 
                       {Sys.time()}, 
                       'The number of distinct IDs matched the number of rows ({id_count})')",
                     .con = conn))
    
  }
  
  
  #### CHECK DISTINCT IDS = DISTINCT IDS IN STAGE.MCAID_ELIG ####
  id_count_raw <- as.numeric(odbc::dbGetQuery(conn, 
                                          "SELECT COUNT (DISTINCT MEDICAID_RECIPIENT_ID) 
                                            FROM stage.mcaid_elig"))
  
  if (id_count != id_count_raw) {
    id_stage_qa_fail <- 1
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcaid_elig_demo',
                       'Number distinct IDs matches raw data', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {id_count} distinct IDs but {id_count_raw} IDs in the raw data (should be the same)')",
                     .con = conn))
    
    message(glue::glue("Number of distinct IDs doesn't match the number of rows. 
                      Check metadata.qa_mcaid for details (last_run = {last_run}"))
  } else {
    id_stage_qa_fail <- 0
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_mcaid
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcaid_elig_demo',
                       'Number distinct IDs matches raw data', 
                       'PASS', 
                       {Sys.time()}, 
                       'The number of distinct IDs matched the number in the raw data ({id_count})')",
                     .con = conn))
    
  }
  
  
  #### LOAD VALUES TO QA_VALUES TABLE ####
  print("Loading values to metadata.qa_mcaid_values")
  
  load_sql <- glue::glue_sql("INSERT INTO metadata.qa_mcaid_values
                             (table_name, qa_item, qa_value, qa_date, note) 
                             VALUES ('stage.mcaid_elig_demo',
                                     'row_count', 
                                     {row_count}, 
                                     {Sys.time()}, 
                                     'Count after refresh')",
                             .con = conn)
  
  odbc::dbGetQuery(conn = conn, load_sql)
  
  print("QA complete, see above for any error messages")
  
  qa_total <- row_qa_fail + id_distinct_qa_fail + id_stage_qa_fail
  return(qa_total)
  
}


