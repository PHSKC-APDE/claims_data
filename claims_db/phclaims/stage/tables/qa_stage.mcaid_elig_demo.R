###############################################################################
# Alastair Matheson
# 2019-05

# Code to QA stage.mcaid_elig_demo

###############################################################################


qa_mcaid_elig_demo_f <- function(conn = db_claims,
                                 load_only = F) {
  
  # If this is the first time ever loading data, only load values.
  #   Otherwise, check against existing QA values
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  # Rows in current table
  row_count <- as.numeric(odbc::dbGetQuery(conn, 
                                           "SELECT COUNT (*) FROM stage.mcaid_elig_demo"))
  
  
  if (load_only == F) {
    ### Pull out run date of stage.mcaid_elig_demo
    last_run <- odbc::dbGetQuery(conn, "SELECT MAX (last_run) FROM stage.mcaid_elig_demo")
    
    
    #### COUNT NUMBER OF ROWS ####
    # Pull in the reference value
    previous_rows <- as.numeric(
      odbc::dbGetQuery(conn, 
                       "SELECT a.* FROM
                       (SELECT * FROM metadata.qa_mcaid_values
                         WHERE table_name = 'stage_mcaid_elig_demo' AND
                          qa_item = 'row_count') a
                       INNER JOIN
                       (SELECT MAX(qa_date) AS max_date 
                         FROM metadata.qa_mcaid_values
                         WHERE table_name = 'stage_mcaid_elig_demo' AND
                          qa_item = 'row_count') b
                       ON a.qa_date = b.max_date"))
    
    row_diff <- row_count < previous_rows
    
    if (row_diff < 0) {
      odbc::dbGetQuery(
        conn = db_claims,
        glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_elig_demo',
                   'Number new rows compared to most recent run', 
                   'FAIL', 
                   {Sys.time()}, 
                   'There were {row_diff} fewer rows in the most recent table 
                       ({row_count} vs. {previous_rows}')",
                       .con = conn))
      
      stop(glue::glue("Fewer rows than found last time.  
                  Check metadata.qa_mcaid for details (last_run = {last_run}"))
    } else {
      odbc::dbGetQuery(
        conn = db_claims,
        glue::glue_sql("INSERT INTO metadata.qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({last_run}, 
                   'stage.mcaid_elig_demo',
                   'Number new rows compared to most recent run', 
                   'PASS', 
                   {Sys.time()}, 
                   'There were {row_diff} more rows in the most recent table 
                       ({row_count} vs. {previous_rows}')",
                       .con = conn))
      
    }
    
    
    #### CHECK DISTINCT IDS = NUMBER OF ROWS ####
    id_count <- as.numeric(odbc::dbGetQuery(conn, 
                                             "SELECT COUNT (DISTINCT id_mcaid) 
                                            FROM stage.mcaid_elig_demo"))
    
    if (id_count != row_count) {
      odbc::dbGetQuery(
        conn = db_claims,
        glue::glue_sql("INSERT INTO metadata.qa_mcaid
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcaid_elig_demo',
                       'Number distinct IDs', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {id_count} distinct IDs but {row_count} rows (should be the same')",
                       .con = conn))
      
      stop(glue::glue("Fewer rows than found last time.  
                      Check metadata.qa_mcaid for details (last_run = {last_run}"))
    } else {
      odbc::dbGetQuery(
        conn = db_claims,
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


