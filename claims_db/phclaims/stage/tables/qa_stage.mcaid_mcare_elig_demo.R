# Header ####
  # Author: Danny Colombara
  # Date: August 28, 2019
  # Purpose: QA stage.mcaid_mcare_elig_demo in SQL
  #
  # This code is designed to be run as part of the master Medicaid/Medicare script:
  # https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_mcare_analytic.R
  #

## Open the function ----
qa_mcaid_mcare_elig_demo_f <- function(conn = db_claims, load_only = F) {
  
## (1) Set up ----    
  stage.count <- as.numeric(odbc::dbGetQuery(db_claims, "SELECT COUNT (*) FROM stage.mcaid_mcare_elig_demo"))
  last_run <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_mcare_elig_demo")[[1]])
  
## (2) Simple QA ----
  if (load_only == F) {
    
    # check that rows in stage are not less than the last time that it was created ----
      # count number of rows
      previous_rows <- as.numeric(
        odbc::dbGetQuery(db_claims, 
                         "SELECT c.qa_value from
                         (SELECT a.* FROM
                         (SELECT * FROM metadata.qa_xwalk_values
                         WHERE table_name = 'stage.mcaid_mcare_elig_demo' AND
                         qa_item = 'row_count') a
                         INNER JOIN
                         (SELECT MAX(qa_date) AS max_date 
                         FROM metadata.qa_xwalk_values
                         WHERE table_name = 'stage.mcaid_mcare_elig_demo' AND
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
                         'stage.mcaid_mcare_elig_demo',
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
                         'stage.mcaid_mcare_elig_demo',
                         'Number new rows compared to most recent run', 
                         'PASS', 
                         {Sys.time()}, 
                         'There were {row_diff} more rows in the most recent table 
                         ({stage.count} vs. {previous_rows})')",
                         .con = db_claims))
        
        problem.row_diff <- glue::glue(" ") # no problem, so empty error message
        
      }
    
    # check that there are no duplicates ----
      # get count of unique id (each id should only appear once)
      stage.count.unique <- as.numeric(odbc::dbGetQuery(
        db_claims, "SELECT COUNT (DISTINCT id_apde) 
        FROM stage.mcaid_mcare_elig_demo"))
      
      if (stage.count.unique != stage.count) {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_xwalk
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES (
                         {last_run}, 
                         'stage.mcaid_mcare_elig_demo',
                         'Number distinct IDs', 
                         'FAIL', 
                         {Sys.time()}, 
                         'There were {stage.count.unique} distinct IDs but {stage.count} rows overall (should be the same)'
                         )
                         ",
                         .con = db_claims))
        
        problem.ids  <- glue::glue("Number of distinct IDs doesn't match the number of rows. 
                                   Check metadata.qa_xwalk for details (last_run = {last_run})
                                   \n")
      } else {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_xwalk
                         (last_run, table_name, qa_item, qa_result, qa_date, note) 
                         VALUES ({last_run}, 
                         'stage.mcaid_mcare_elig_demo',
                         'Number distinct IDs', 
                         'PASS', 
                         {Sys.time()}, 
                         'The number of distinct IDs matched number of overall rows ({stage.count.unique})')",
                         .con = db_claims))
        
        problem.ids  <- glue::glue(" ") # no problem
      }
    
## (3) create summary of errors ---- 
      problems <- glue::glue(
        problem.ids, "\n",
        problem.row_diff)
  } # close condition "if (load_only == F)" 
  
## (4) Fill qa_xwalk_values table ----
    qa.values <- glue::glue_sql("INSERT INTO metadata.qa_xwalk_values
                                (table_name, qa_item, qa_value, qa_date, note) 
                                VALUES ('stage.mcaid_mcare_elig_demo',
                                'row_count', 
                                {stage.count}, 
                                {Sys.time()}, 
                                '')",
                                .con = db_claims)
    
    odbc::dbGetQuery(conn = db_claims, qa.values)

## (5) Print error messages ----
    if (load_only == F) {
      if(problems >1){
        message(glue::glue("WARNING ... MCARE_ELIG_DEMO FAILED AT LEAST ONE QA TEST", "\n",
                           "Summary of problems in MCARE_ELIG_DEMO: ", "\n", 
                           problems))
      }else{message("Staged MCAID_MCARE_ELIG_DEMO passed all QA tests")}
    }
    
} # close the function

## The end! ----
