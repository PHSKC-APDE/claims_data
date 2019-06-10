###############################################################################
# Alastair Matheson
# 2019-05

# Code to QA stage.mcaid_elig_timevar

###############################################################################


qa_mcaid_elig_timevar_f <- function(conn = db_claims,
                                    load_only = F) {
  
  # If this is the first time ever loading data, only load values.
  #   Otherwise, check against existing QA values
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  # Rows in current table
  row_count <- as.numeric(odbc::dbGetQuery(conn, 
                                           "SELECT COUNT (*) FROM stage.mcaid_elig_timevar"))
  
  
  if (load_only == F) {
    ### Pull out run date of stage.mcaid_elig_timevar
    last_run <- odbc::dbGetQuery(conn, "SELECT MAX (last_run) FROM stage.mcaid_elig_timevar")
    
    
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
  
  
  #### CHECK DISTINCT IDS = DISTINCT IN STAGE ####
  id_count_timevar <- as.numeric(odbc::dbGetQuery(conn, 
                                                  "SELECT COUNT (DISTINCT id_mcaid) 
                                                    FROM stage.mcaid_elig_timevar"))
  
  id_count_elig <- as.numeric(odbc::dbGetQuery(conn, 
                                               "SELECT COUNT (DISTINCT id_mcaid) 
                                                 FROM stage.mcaid_elig"))
  
  if (id_count != row_count) {
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
  
  
  #### LOAD VALUES TO QA_VALUES TABLE ####
  load_sql <- glue::glue_sql("INSERT INTO metadata.qa_mcaid_values
                             (table_name, qa_item, qa_value, qa_date, note) 
                             VALUES ('load_raw.mcaid_elig',
                                     'row_count', 
                                     {row_count}, 
                                     {Sys.time()}, 
                                     '')",
                             .con = conn)
  
  odbc::dbGetQuery(conn = conn, load_sql)
  
  
}


