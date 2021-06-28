## Header ####
# Author: Danny Colombara, based on code by Alatair Matheson
# 
# R version: 3.6.2
#
# Purpose: Simple QA for PHClaims.stage.xwalk_apde_mcaid_mcare_pha
# 
# Notes: Type the <Alt> + <o> at the same time to collapse the code and view the structure
# 
# This code is designed to be run as part of the master Medicaid/Medicare script:
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_mcare_analytic.R
#

# This is all one function ----
qa_xwalk_apde_mcaid_mcare_pha_f <- function(conn = db_claims,
                                    load_only = F) {
  
  # If this is the first time ever loading data, only load values.
  #   Otherwise, check against existing QA values
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  # Rows in current table
  row_count <- as.numeric(odbc::dbGetQuery(conn, 
                                           "SELECT COUNT (*) FROM stage.xwalk_apde_mcaid_mcare_pha"))
  
  ### Pull out run date of stage.xwalk_apde_mcaid_mcare_pha
  last_run <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.xwalk_apde_mcaid_mcare_pha")[[1]])
  
  
  if (load_only == F) {
  #### COUNT NUMBER OF ROWS ####
    # Pull in the reference value
    previous_rows <- as.numeric(
      odbc::dbGetQuery(conn, 
                       "SELECT c.qa_value from
                       (SELECT a.* FROM
                       (SELECT * FROM metadata.qa_xwalk_values
                       WHERE table_name = 'stage.xwalk_apde_mcaid_mcare_pha' AND
                       qa_item = 'row_count') a
                       INNER JOIN
                       (SELECT MAX(qa_date) AS max_date 
                       FROM metadata.qa_xwalk_values
                       WHERE table_name = 'stage.xwalk_apde_mcaid_mcare_pha' AND
                       qa_item = 'row_count') b
                       ON a.qa_date = b.max_date)c"))

    if(is.na(previous_rows)){previous_rows = 0}
    
    row_diff <- row_count - previous_rows
    
    if (row_diff < 0) {
      odbc::dbGetQuery(
        conn = conn,
        glue::glue_sql("INSERT INTO metadata.qa_xwalk
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.xwalk_apde_mcaid_mcare_pha',
                       'Number new rows compared to most recent run', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {row_diff} fewer rows in the most recent table 
                       ({row_count} vs. {previous_rows})')",
                       .con = conn))
      
      problem.row_diff <- glue::glue("Fewer rows than found last time.  
                      Check metadata.qa_xwalk for details (last_run = {last_run})
                      \n")
    } else {
      odbc::dbGetQuery(
        conn = conn,
        glue::glue_sql("INSERT INTO metadata.qa_xwalk
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.xwalk_apde_mcaid_mcare_pha',
                       'Number new rows compared to most recent run', 
                       'PASS', 
                       {Sys.time()}, 
                       'There were {row_diff} more rows in the most recent table 
                       ({row_count} vs. {previous_rows})')",
                       .con = conn))
      
      problem.row_diff <- glue::glue(" ") # no problem, so empty error message
      
    }

  
  #### CHECK DISTINCT MCARE IDS >= DISTINCT IN MCARE ELIG DEMO ####
  id_count_mcare <- as.numeric(odbc::dbGetQuery(
    conn, "SELECT COUNT (DISTINCT id_mcare) AS count FROM stage.xwalk_apde_mcaid_mcare_pha"))

  id_count_mcare_elig_demo <- as.numeric(odbc::dbGetQuery(
    conn, "SELECT c.qa_value from
    (SELECT a.* FROM
    (SELECT * FROM metadata.qa_mcare_values
    WHERE table_name = 'stage.mcare_elig_demo' AND
    qa_item = 'row_count') a
    INNER JOIN
    (SELECT MAX(qa_date) AS max_date 
    FROM metadata.qa_mcare_values
    WHERE table_name = 'stage.mcare_elig_demo' AND
    qa_item = 'row_count') b
    ON a.qa_date = b.max_date)c"
  ))
  
    # NOTE ... it is possible / probable that the linkage has more IDS than the ELIG DEMO because not everyone is in ELIG DEMO
  if (id_count_mcare < id_count_mcare_elig_demo) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_xwalk
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES (
                      {last_run}, 
                     'stage.xwalk_apde_mcaid_mcare_pha',
                     'Number distinct IDs', 
                     'FAIL', 
                     {Sys.time()}, 
                     'There were {id_count_mcare} distinct MCARE IDs but {id_count_mcare_elig_demo} in the most recent MCARE ELIG DEMO (xwalk should have >= # in elig demo)'
                     )
                     ",
                     .con = conn))
    
    problem.mcare_id  <- glue::glue("Number of distinct MCARE IDs is less than the number in MCARE ELIG DEMO data. 
                    Check metadata.qa_xwalk for details (last_run = {last_run})
                   \n")
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_xwalk
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.xwalk_apde_mcaid_mcare_pha',
                     'Number distinct IDs', 
                     'PASS', 
                     {Sys.time()}, 
                     'The number of distinct MCARE IDs ({id_count_mcare}) is >= the number in MCARE ELIG DEMO  
                     ({id_count_mcare_elig_demo})')",
                     .con = conn))
      
    problem.mcare_id  <- glue::glue(" ") # no problem
  }
  
  
  #### CHECK DISTINCT MCAID IDS == DISTINCT IN MCAID ELIG DEMO ####
  id_count_mcaid <- as.numeric(odbc::dbGetQuery(
    conn, "SELECT COUNT (DISTINCT id_mcaid) AS count FROM stage.xwalk_apde_mcaid_mcare_pha"))
  
  id_count_mcaid_elig_demo <- as.numeric(odbc::dbGetQuery(
    conn, "SELECT c.qa_value from
    (SELECT a.* FROM
    (SELECT * FROM metadata.qa_mcaid_values
    WHERE table_name = 'stage.mcaid_elig_demo' AND
    qa_item = 'row_count') a
    INNER JOIN
    (SELECT MAX(qa_date) AS max_date 
    FROM metadata.qa_mcaid_values
    WHERE table_name = 'stage.mcaid_elig_demo' AND
    qa_item = 'row_count') b
    ON a.qa_date = b.max_date)c"
  ))
  
  
  if (id_count_mcaid != id_count_mcaid_elig_demo) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_xwalk
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES (
                      {last_run}, 
                     'stage.xwalk_apde_mcaid_mcaid_pha',
                     'Number distinct IDs', 
                     'FAIL', 
                     {Sys.time()}, 
                     'There were {id_count_mcaid} distinct MCAID IDs but {id_count_mcaid_elig_demo} in the most recent MCAID ELIG DEMO (they should be equal)'
                     )
                     ",
                     .con = conn))
    
    problem.mcaid_id  <- glue::glue("Number of distinct MCAID IDs is different from the number in MCAID ELIG DEMO data. 
                    Check metadata.qa_xwalk for details (last_run = {last_run})
                   \n")
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_xwalk
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.xwalk_apde_mcaid_mcare_pha',
                     'Number distinct IDs', 
                     'PASS', 
                     {Sys.time()}, 
                     'The number of distinct MCAID IDs ({id_count_mcaid}) is equal to the number in MCAID ELIG DEMO  
                     ({id_count_mcaid_elig_demo})')",
                     .con = conn))
    
    problem.mcaid_id  <- glue::glue(" ") # no problem
  }
  
  #### CHECK DISTINCT ID_KC_PHA == DISTINCT ID_KC_PHA IN [PH_APDEStore].[stage].[pha]####
  id_count_pid <- as.numeric(odbc::dbGetQuery(
    conn, "SELECT COUNT (DISTINCT id_kc_pha) AS count FROM stage.xwalk_apde_mcaid_mcare_pha"))
  
  id_count_pid_orig <- as.numeric(odbc::dbGetQuery(
    conn, "SELECT COUNT (DISTINCT id_kc_pha) AS count FROM PH_APDEStore.stage.pha"))
  
  if (id_count_pid != id_count_pid_orig) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_xwalk
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES (
                      {last_run}, 
                     'stage.xwalk_apde_mcaid_mcaid_pha',
                     'Number distinct IDs', 
                     'FAIL', 
                     {Sys.time()}, 
                     'There were {id_count_pid} distinct PHA IDs but {id_count_pid_orig} in the most recent [PH_APDEStore].[stage].[pha] (they should be equal)'
                     )
                     ",
                     .con = conn))
    
    problem.id_pha  <- glue::glue("Number of distinct PHA IDs is different from the number in [PH_APDEStore].[stage].[pha] data. 
                    Check metadata.qa_xwalk for details (last_run = {last_run})
                   \n")
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_xwalk
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.xwalk_apde_mcaid_mcare_pha',
                     'Number distinct IDs', 
                     'PASS', 
                     {Sys.time()}, 
                     'The number of distinct PHA IDs ({id_count_pid}) is equal to the number in [PH_APDEStore].[stage].[pha]  
                     ({id_count_pid_orig})')",
                     .con = conn))
    
    problem.id_pha  <- glue::glue(" ") # no problem
  }
  
  #### CHECK THAT id_mcare ARE DISTINCT (ONLY IN ONE ROW) ----
  distinct_id_mcare <- setDT(odbc::dbGetQuery(
    conn, "SELECT id_mcare, COUNT(id_mcare) 
           FROM [PHClaims].[stage].[xwalk_apde_mcaid_mcare_pha] 
           GROUP BY id_mcare 
           HAVING COUNT(id_mcare) > 1"))
  
  
  if (nrow(distinct_id_mcare) !=0) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_xwalk
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.xwalk_apde_mcaid_mcare_pha',
                     'Duplicate id_mcare', 
                     'FAIL', 
                     {Sys.time()}, 
                     'There were duplicate id_mcare (i.e., a given ID appeared in more than one row)')",
                     .con = conn))
    
    problem.dup.id_mcare <- glue::glue("There appear to be duplicate id_mcare 
                    Check metadata.qa_xwalk for details (last_run = {last_run})
                   \n")
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_xwalk
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.xwalk_apde_mcaid_mcare_pha',
                     'Duplicate id_mcare', 
                     'PASS', 
                     {Sys.time()}, 
                     'There were NO duplicate id_mcare (good job!)')",
                     .con = conn))
    
    problem.dup.id_mcare <- glue::glue(" ")
  }
  
  
  #### CHECK THAT id_mcaid ARE DISTINCT (ONLY IN ONE ROW) ----
  distinct_id_mcaid <- setDT(odbc::dbGetQuery(
    conn, "SELECT id_mcaid, COUNT(id_mcaid) 
           FROM [PHClaims].[stage].[xwalk_apde_mcaid_mcare_pha]
           GROUP BY id_mcaid 
           HAVING COUNT(id_mcaid) > 1"))
  
  
  if (nrow(distinct_id_mcaid) !=0) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_xwalk
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.xwalk_apde_mcaid_mcaid_pha',
                     'Duplicate id_mcaid', 
                     'FAIL', 
                     {Sys.time()}, 
                     'There were duplicate id_mcaid (i.e., a given ID appeared in more than one row)')",
                     .con = conn))
    
    problem.dup.id_mcaid <- glue::glue("There appear to be duplicate id_mcaid 
                    Check metadata.qa_xwalk for details (last_run = {last_run})
                   \n")
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_xwalk
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.xwalk_apde_mcaid_mcaid_pha',
                     'Duplicate id_mcaid', 
                     'PASS', 
                     {Sys.time()}, 
                     'There were NO duplicate id_mcaid (good job!)')",
                     .con = conn))
    
    problem.dup.id_mcaid <- glue::glue(" ")
  }
  
  
  #### CHECK THAT id_kc_pha ARE DISTINCT (ONLY IN ONE ROW) ----
  distinct_pid <- setDT(odbc::dbGetQuery(
    conn, "SELECT id_kc_pha, COUNT(id_kc_pha) 
           FROM [PHClaims].[stage].[xwalk_apde_mcaid_mcare_pha] 
           GROUP BY id_kc_pha 
           HAVING COUNT(id_kc_pha) > 1"))
  
  
  if (nrow(distinct_pid) !=0) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_xwalk
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.xwalk_apde_mcaid_mcaid_pha',
                     'Duplicate id_kc_pha', 
                     'FAIL', 
                     {Sys.time()}, 
                     'There were duplicate id_kc_pha (i.e., a given ID appeared in more than one row)')",
                     .con = conn))
    
    problem.dup.id_pha <- glue::glue("There appear to be duplicate id_kc_pha 
                    Check metadata.qa_xwalk for details (last_run = {last_run})
                   \n")
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_xwalk
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.xwalk_apde_mcaid_mcaid_pha',
                     'Duplicate id_kc_pha', 
                     'PASS', 
                     {Sys.time()}, 
                     'There were NO duplicate id_kc_pha (good job!)')",
                     .con = conn))
    
    problem.dup.id_pha <- glue::glue(" ")
  }
  
  
  #### CHECK THAT id_apde ARE DISTINCT (ONLY IN ONE ROW) ----
  distinct_id_apde <- setDT(odbc::dbGetQuery(
    conn, "SELECT id_apde, COUNT(id_apde) 
           FROM [PHClaims].[stage].[xwalk_apde_mcaid_mcare_pha]  
           GROUP BY id_apde 
           HAVING COUNT(id_apde) > 1"))
  
  
  if (nrow(distinct_id_apde) !=0) {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_xwalk
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.xwalk_apde_mcaid_apde_pha',
                     'Duplicate id_apde', 
                     'FAIL', 
                     {Sys.time()}, 
                     'There were duplicate id_apde (i.e., a given ID appeared in more than one row)')",
                     .con = conn))
    
    problem.dup.id_apde <- glue::glue("There appear to be duplicate id_apde 
                    Check metadata.qa_xwalk for details (last_run = {last_run})
                   \n")
  } else {
    odbc::dbGetQuery(
      conn = conn,
      glue::glue_sql("INSERT INTO metadata.qa_xwalk
                     (last_run, table_name, qa_item, qa_result, qa_date, note) 
                     VALUES ({last_run}, 
                     'stage.xwalk_apde_mcaid_apde_pha',
                     'Duplicate id_apde', 
                     'PASS', 
                     {Sys.time()}, 
                     'There were NO duplicate id_apde (good job!)')",
                     .con = conn))
    
    problem.dup.id_apde <- glue::glue(" ")
  }
  
  
  } # close load_only condition above
  
  #### LOAD VALUES TO QA_VALUES TABLE ####
      # ROW COUNT ----
      load_sql <- glue::glue_sql("INSERT INTO metadata.qa_xwalk_values
                                 (table_name, qa_item, qa_value, qa_date, note) 
                                 VALUES ('stage.xwalk_apde_mcaid_mcare_pha',
                                 'row_count', 
                                 {row_count}, 
                                 {Sys.time()}, 
                                 '')",
                                 .con = conn)
      
      odbc::dbGetQuery(conn = conn, load_sql)
      

  #### Identify problems / fails ####
  if(problem.mcare_id >1 | problem.mcaid_id>1 | problem.id_pha>1 | problem.dup.id_mcare>1 | problem.dup.id_mcaid>1 | problem.dup.id_pha>1 | problem.dup.id_apde>1){
                    problems <- glue::glue("****STOP!!!!!!!!****
                         Please address the following issues that have been logged in [PHClaims].[metadata].[qa_xwalk] ... \n", 
                                           problem.mcare_id, "\n", 
                                           problem.mcaid_id, "\n", 
                                           problem.id_pha, "\n", 
                                           problem.dup.id_mcare, "\n", 
                                           problem.dup.id_mcaid, "\n", 
                                           problem.dup.id_pha, "\n", 
                                           problem.dup.id_apde)}else{
                    problems <- glue::glue("All QA checks passed and recorded to [PHClaims].[metadata].[qa_xwalk]")       
                         }
  message(problems)
  
}

# The end! ----
