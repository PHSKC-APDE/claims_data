# Header ####
  # Author: Danny Colombara
  # Date: August 28, 2019
  # Purpose: Create stage.mcaid_mcare_elig_demo for SQL

## Set up R Environment ----
  rm(list=ls())  # clear memory
  pacman::p_load(data.table, odbc, DBI, lubridate) # load packages
  options("scipen"=999) # turn off scientific notation  
  options(warning.length = 8170) # get lengthy warnings, needed for SQL
  
  start.time <- Sys.time()
  
  yaml.url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcaid_mcare_elig_demo.yaml"
  
## (1) Connect to SQL Server ----    
  db_claims <- dbConnect(odbc(), "PHClaims51")   
  
## (2) Load data from SQL ----  
  apde <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_apde, id_mcare, id_mcaid 
                                 FROM PHClaims.final.xwalk_apde_mcaid_mcare_pha"))
  
  mcare <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_mcare, dob, death_dt, geo_kc_ever, gender_female, gender_male, gender_me, gender_recent, race_eth_recent, race_recent,
                                  race_white, race_black, race_other, race_asian, race_asian_pi, race_aian, race_nhpi, race_latino, race_unk, race_eth_me, race_me 
                                  FROM PHClaims.final.mcare_elig_demo"))

  mcaid <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_mcaid, dob, gender_female, gender_male, gender_me, gender_recent, race_eth_recent, race_recent,
                                  race_me, race_eth_me, race_aian, race_asian, race_black, race_nhpi, race_white, race_latino 
                                  FROM PHClaims.final.mcaid_elig_demo"))

## (3) Merge on apde id ----
  mcare <- merge(apde[, .(id_apde, id_mcare)], mcare, by = "id_mcare", all.x = FALSE, all.y = TRUE)
  mcare[, id_mcare := NULL] # no longer needed now that have id_apde
  
  mcaid <- merge(apde[, .(id_apde, id_mcaid)], mcaid, by = "id_mcaid", all.x = FALSE, all.y = TRUE)
  mcaid[, id_mcaid := NULL] # no longer needed now that have id_apde
  
## (4) Identify the duals and split from non-duals ----
  dual.id <- intersect(mcaid$id_apde, mcare$id_apde)
  
  mcare.solo <- mcare[!id_apde %in% dual.id]
  mcaid.solo <- mcaid[!id_apde %in% dual.id]  
  
  mcare.dual <- mcare[id_apde %in% dual.id]
  mcaid.dual <- mcaid[id_apde %in% dual.id]

## (5) Combine the data for duals ----
  # some data is assumed to be more reliable in one dataset compared to the other
  dual <- merge(x = mcaid.dual, y = mcare.dual, by = "id_apde")
  setnames(dual, names(dual), gsub(".x$", ".mcaid", names(dual))) # clean up suffixes to eliminate confusion
  setnames(dual, names(dual), gsub(".y$", ".mcare", names(dual))) # clean up suffixes to eliminate confusion
  
  # ascribe MCARE data to duals
  dual[, dob := dob.mcaid] # default date of birth from Mcaid
  dual[!is.na(dob.mcare), dob := dob.mcare][, c("dob.mcaid", "dob.mcare") := NULL] # replace with Mcare when possible
    # race_asian_pi, death_dt, kc are only in Mcare 
  
  # loop to ascribe MCAID data to duals
  for(i in c("gender_me", "gender_female", "gender_male", "gender_recent", "race_eth_recent", "race_recent",
             "race_me", "race_eth_me", "race_aian", "race_asian", "race_black", "race_nhpi", "race_white", "race_latino")){
    dual[, paste0(i) := get(paste0(i, ".mcaid"))] # fill with Mcaid data
    dual[is.na(get(paste0(i))), paste0(i) := get(paste0(i, ".mcare"))] # If NA b/c missing Mcaid data, then fill with Mcare data
    dual[, paste0(i, ".mcaid") := NULL][, paste0(i, ".mcare") := NULL]
  }

  # add dual flag
    dual[, apde_dual := 1]
  
## (6) Append the duals to the non-duals ----
    elig <- rbindlist(list(dual, mcaid.solo, mcare.solo), use.names = TRUE, fill = TRUE)
    elig[is.na(apde_dual), apde_dual := 0] # fill in duals flag
    
## (7) Prep for pushing to SQL ----
    # set dates
      elig[, dob := as.Date(dob)]
      elig[, death_dt := as.Date(death_dt)]
    
    # recreate race unknown indicator
      elig[, race_unk := 0]
      elig[race_aian==0 & race_asian==0 & race_asian_pi==0 & race_black==0 & race_latino==0 & race_nhpi==0 & race_white==0, race_unk := 1] 
  
    # create time stamp
      elig[, last_run := Sys.time()] 
      
## (8) Write to SQL ----              
  # Pull YAML from GitHub
    table_config <- yaml::yaml.load(RCurl::getURL(yaml.url))
  
  # Create table ID
    tbl_id <- DBI::Id(schema = table_config$schema, 
                      table = table_config$table)  
  
  # Ensure columns are in same order in R & SQL
    setcolorder(elig, names(table_config$vars))
  
  # Write table to SQL
    dbWriteTable(db_claims, 
                 tbl_id, 
                 value = as.data.frame(elig),
                 overwrite = T, append = F, 
                 field.types = unlist(table_config$vars))

## (9) Simple QA ----
    # Confirm that all rows were loaded to SQL ----
      stage.count <- as.numeric(odbc::dbGetQuery(db_claims, "SELECT COUNT (*) FROM stage.mcaid_mcare_elig_demo"))
      if(stage.count != nrow(elig))
        stop("Mismatching row count, error reading in data")    
    
    # check that rows in stage are not less than the last time that it was created ----
      last_run <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcaid_mcare_elig_demo")[[1]])
    
      # count number of rows
      previous_rows <- as.numeric(
        odbc::dbGetQuery(db_claims, 
                         "SELECT c.qa_value from
                         (SELECT a.* FROM
                         (SELECT * FROM metadata.qa_mcare_values
                         WHERE table_name = 'stage.mcaid_mcare_elig_demo' AND
                         qa_item = 'row_count') a
                         INNER JOIN
                         (SELECT MAX(qa_date) AS max_date 
                         FROM metadata.qa_mcare_values
                         WHERE table_name = 'stage.mcaid_mcare_elig_demo' AND
                         qa_item = 'row_count') b
                         ON a.qa_date = b.max_date)c"))
      
      if(is.na(previous_rows)){previous_rows = 0}
      
      row_diff <- stage.count - previous_rows
      
      if (row_diff < 0) {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_mcare
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
                                       Check metadata.qa_mcare for details (last_run = {last_run})
                                       \n")
      } else {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_mcare
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
        db_claims, "SELECT COUNT (*) 
        FROM stage.mcaid_mcare_elig_demo"))
      
      if (stage.count.unique != stage.count) {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_mcare
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
                                   Check metadata.qa_mcare for details (last_run = {last_run})
                                   \n")
      } else {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_mcare
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
    
    # create summary of errors ---- 
      problems <- glue::glue(
        problem.ids, "\n",
        problem.row_diff)

## (10) Fill qa_mcare_values table ----
    qa.values <- glue::glue_sql("INSERT INTO metadata.qa_mcare_values
                                (table_name, qa_item, qa_value, qa_date, note) 
                                VALUES ('stage.mcaid_mcare_elig_demo',
                                'row_count', 
                                {stage.count}, 
                                {Sys.time()}, 
                                '')",
                                .con = db_claims)
    
    odbc::dbGetQuery(conn = db_claims, qa.values)

## (11) Print error messages ----
    if(problems >1){
      message(glue::glue("WARNING ... MCARE_ELIG_DEMO FAILED AT LEAST ONE QA TEST", "\n",
                         "Summary of problems in MCARE_ELIG_DEMO: ", "\n", 
                         problems))
    }else{message("Staged MCAID_MCARE_ELIG_DEMO passed all QA tests")}

## The end! ----
    run.time <- Sys.time() - start.time
    print(run.time)
    