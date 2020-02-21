# Header ####
  # Author: Danny Colombara
  # Date: February 20, 2020
  # Purpose: QA stage.mcaid_mcare_elig_timevar for SQL
  #
  # This code is designed to be run as part of the master Medicaid/Medicare script:
  # https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_mcare_analytic.R
  #
  #

## Open the function ----
qa_mcaid_mcare_elig_timevar_f <- function(conn = db_claims, load_only = F) {
  
## (1) set up ----
  stage.count <- as.numeric(odbc::dbGetQuery(db_claims, "SELECT COUNT (*) FROM stage.mcaid_mcare_elig_timevar"))
  last_run <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_mcare_elig_timevar")[[1]])
  
## (2) Simple QA ----
  if (load_only == F) {
    # check that rows in stage are not less than the last time that it was created ----
      # count number of rows
      previous_rows <- as.numeric(
        odbc::dbGetQuery(db_claims, 
                         "SELECT c.qa_value from
                         (SELECT a.* FROM
                         (SELECT * FROM metadata.qa_xwalk_values
                         WHERE table_name = 'stage.mcaid_mcare_elig_timevar' AND
                         qa_item = 'row_count') a
                         INNER JOIN
                         (SELECT MAX(qa_date) AS max_date 
                         FROM metadata.qa_xwalk_values
                         WHERE table_name = 'stage.mcaid_mcare_elig_timevar' AND
                         qa_item = 'row_count') b
                         ON a.qa_date = b.max_date)c"))
      
      if(is.na(previous_rows)){previous_rows = 0}
      
      row_diff <- stage.count - previous_rows
      
      if (row_diff < 0) {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_xwalk
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES ({last_run}, 
                         'stage.mcaid_mcare_elig_timevar',
                         'Number new rows compared to most recent run', 
                         'FAIL', 
                         {Sys.time()}, 
                         'There were {row_diff} fewer rows in the most recent table 
                         ({stage.count} vs. {previous_rows})')",
                         .con = db_claims))
        
        problem.row_diff <- glue::glue("Fewer rows than found last time.  
                                       Check metadata.qa_xwalk for details (last_run = {last_run})
                                       \n")
      } else {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_xwalk
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES ({last_run}, 
                         'stage.mcaid_mcare_elig_timevar',
                         'Number new rows compared to most recent run', 
                         'PASS', 
                         {Sys.time()}, 
                         'There were {row_diff} more rows in the most recent table 
                         ({stage.count} vs. {previous_rows})')",
                         .con = db_claims))
        
        problem.row_diff <- glue::glue(" ") # no problem, so empty error message
        
      }
    
    # check that the number of distinct IDs not less than the last time that it was created ----
      # get count of unique id (each id should only appear once)
      current.unique.id <- as.numeric(odbc::dbGetQuery(
        db_claims, "SELECT COUNT (DISTINCT id_apde) 
        FROM stage.mcaid_mcare_elig_timevar"))
      
      previous.unique.id <- as.numeric(
        odbc::dbGetQuery(db_claims, 
                         "SELECT c.qa_value from
                         (SELECT a.* FROM
                         (SELECT * FROM metadata.qa_xwalk_values
                         WHERE table_name = 'stage.mcaid_mcare_elig_timevar' AND
                         qa_item = 'id_count') a
                         INNER JOIN
                         (SELECT MAX(qa_date) AS max_date 
                         FROM metadata.qa_xwalk_values
                         WHERE table_name = 'stage.mcaid_mcare_elig_timevar' AND
                         qa_item = 'id_count') b
                         ON a.qa_date = b.max_date)c"))
      
      if(is.na(previous.unique.id)){previous.unique.id = 0}
      
      id_diff <- current.unique.id - previous.unique.id
      
      if (id_diff < 0) {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_xwalk
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES ({last_run}, 
                         'stage.mcaid_mcare_elig_timevar',
                         'Number distinct IDs compared to most recent run', 
                         'FAIL', 
                         {Sys.time()}, 
                         'There were {id_diff} fewer IDs in the most recent table 
                         ({current.unique.id} vs. {previous.unique.id})')",
                         .con = db_claims))
        
        problem.id_diff <- glue::glue("Fewer unique IDs than found last time.  
                                       Check metadata.qa_xwalk for details (last_run = {last_run})
                                       \n")
      } else {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_xwalk
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES ({last_run}, 
                         'stage.mcaid_mcare_elig_timevar',
                         'Number distinct IDs compared to most recent run', 
                         'PASS', 
                         {Sys.time()}, 
                         'There were {id_diff} more IDs in the most recent table 
                         ({current.unique.id} vs. {previous.unique.id})')",
                         .con = db_claims))
        
        problem.id_diff <- glue::glue(" ") # no problem, so empty error message
      }
    
    # create summary of errors ---- 
      problems <- glue::glue(
        problem.row_diff, "\n",
        problem.id_diff)
  } # close the load_only == F condition
  
## (3) Fill qa_xwalk_values table ----
    qa.values <- glue::glue_sql("INSERT INTO metadata.qa_xwalk_values
                                (table_name, qa_item, qa_value, qa_date, note) 
                                VALUES ('stage.mcaid_mcare_elig_timevar',
                                'row_count', 
                                {stage.count}, 
                                {Sys.time()}, 
                                '')",
                                .con = db_claims)
    
    odbc::dbGetQuery(conn = db_claims, qa.values)
    
    qa.values2 <- glue::glue_sql("INSERT INTO metadata.qa_xwalk_values
                                (table_name, qa_item, qa_value, qa_date, note) 
                                VALUES ('stage.mcaid_mcare_elig_timevar',
                                'id_count', 
                                {current.unique.id}, 
                                {Sys.time()}, 
                                '')",
                                .con = db_claims)
    
    odbc::dbGetQuery(conn = db_claims, qa.values2)
    

## (4) Print error messages ----
    if(load_only == F){
      if(problems >1){
        message(glue::glue("WARNING ... MCAID_MCARE_ELIG_TIMEVAR FAILED AT LEAST ONE QA TEST", "\n",
                           "Summary of problems in MCAID_MCARE_ELIG_TIMEVAR: ", "\n", 
                           problems))
      }else{message("Staged MCAID_MCARE_ELIG_TIMEVAR passed all QA tests")}
    }

} # close the function

## The end! ----
