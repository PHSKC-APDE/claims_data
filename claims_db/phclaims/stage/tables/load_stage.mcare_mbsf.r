## Header ####
    # Author: Danny Colombara
    # 
    # R version: 3.5.3
    #
    # Purpose: Processing of load_raw MBSF AB and ABCD to a single combined staged MBSF
    # 
    # WARNING: This is a memory intensive script ... please run on a machine with at least 32 GB RAM
    #
    # Notes: Medicare ids are case sensitive. 
    #        Will select distinct rows from load_raw in SQL (which hopefully will deduplicate most of the problems)
    #        Will perform another check for duplicates in R
    #
    #        This code takes approximately 45 minutes to run under normal SQL server traffic conditions

## Set up environment ----
    rm(list=ls())
    .libPaths("C:/Users/dcolombara/R.packages") # needed for 32 GB SAS computer.
    pacman::p_load(data.table, dplyr, odbc, lubridate, glue, httr)
    start.time <- Sys.time()
    options("scipen"=999) # turn off scientific notation  
    options(warning.length = 8170) # get lengthy warnings, needed for SQL
    setwd("C:/temp/")
    yaml.url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_mbsf.yaml"

## (1) Connect to SQL Server ----    
    db_claims <- dbConnect(odbc(), "PHClaims51") 

## (2) PULL & PREP MBSF AB ----
    # count rows in load_raw 
    ab.raw.count <- as.numeric(odbc::dbGetQuery(db_claims, 
                                             "SELECT COUNT (*) FROM load_raw.mcare_mbsf_ab"))
    # pull load_raw into R
    ab <- (
      odbc::dbGetQuery(db_claims, 
                       "SELECT DISTINCT * FROM load_raw.mcare_mbsf_ab"))
    setDT(ab)
    
    # ensure there are no errors when pulling data from SQL
    if(ab.raw.count != nrow(ab))
      stop("Mismatching row count, error reading in data")
    
    # read in YAML data for new stage file from GitHub
    table_config <- yaml::yaml.load(RCurl::getURL(yaml.url))
    
    # rename vars to match those in MBSF ABCD
    setnames(ab, names(table_config$rename_ab_abcd), unlist(table_config$rename_ab_abcd))
    
    # fix zip codes by adding preceding zeros and setting to character 
    ab[zip_cd=="999999999", zip_cd := NA]
    ab[, zip_cd := substr(zip_cd, 1, 5)]    
    
    # identify the column types needed for stage file in SQL
    ab.stage.cols <- data.table(col.name = names(table_config$vars), col.type = table_config$vars)[col.name %in% names(ab)]
    ab.characters <- ab.stage.cols[tolower(col.type) %like% "char", ]$col.name
    ab.integers <- ab.stage.cols[tolower(col.type) %like% "int", ]$col.name
    ab.dates <- ab.stage.cols[tolower(col.type) %like% "date", ]$col.name
    rm(ab.stage.cols)

    # change the class of the R columns if necessary
    ab[, (ab.characters):= lapply(.SD, as.character), .SDcols = ab.characters]
    ab[, (ab.integers):= lapply(.SD, as.integer), .SDcols = ab.integers]
    ab[, (ab.dates):= lapply(.SD, as.Date), .SDcols = ab.dates]
    
## (3) PULL & prep MBSF ABCD ---- 
    # count rows in load_raw 
    abcd.raw.count <- as.numeric(odbc::dbGetQuery(db_claims, 
                                             "SELECT COUNT (*) FROM load_raw.mcare_mbsf_abcd"))
    # pull load_raw into R
    abcd <- (
      odbc::dbGetQuery(db_claims, 
                       "SELECT DISTINCT * FROM load_raw.mcare_mbsf_abcd"))
    setDT(abcd)
    
    # drop 'data_year' because redundant with bene_enrollmt_ref_yr
    abcd[, data_year := NULL]

    # ensure there are no errors when pulling data from SQL
    if(abcd.raw.count != nrow(abcd))
      stop("Mismatching row count, error reading in data")
    
    # fix zip codes by adding preceding zeros and setting to character 
    abcd[zip_cd=="99999", zip_cd := NA]    
    abcd[, zip_cd := formatC(as.numeric(zip_cd), width = 5, format = "d", flag="0")]
    
    # identify the column types needed for stage file in SQL
    abcd.stage.cols <- data.table(col.name = names(table_config$vars), col.type = table_config$vars)[col.name %in% names(abcd)]
    abcd.characters <- abcd.stage.cols[tolower(col.type) %like% "char", ]$col.name
    abcd.integers <- abcd.stage.cols[tolower(col.type) %like% "int", ]$col.name
    abcd.dates <- abcd.stage.cols[tolower(col.type) %like% "date", ]$col.name
    rm(abcd.stage.cols)

    # change the class of the R columns if necessary
    abcd[, (abcd.characters):= lapply(.SD, as.character), .SDcols = abcd.characters]
    abcd[, (abcd.integers):= lapply(.SD, as.integer), .SDcols = abcd.integers]
    abcd[, (abcd.dates):= lapply(.SD, as.Date), .SDcols = abcd.dates]

## (4) Append MBSF AB & ABCD ----    
    abcd <- rbindlist(list(abcd, ab), fill = TRUE, use.names = TRUE)
    rm(ab)
    gc()
    
## (5) Identify potential duplicates ----
    begin.time <- Sys.time()
    abcd[, id_lowercase := tolower(bene_id)]
    by.cols = setdiff(names(abcd), 'bene_id') # all columns, except original case sensitive id column
    abcd[, dup := .N>1, by = by.cols] # create True false indicator for all duplicate rows (including the first copy of the duplicate)
    mbsf.duplicates <- abcd[dup == TRUE] # save the potential duplicate data as a different data.table
    setcolorder(mbsf.duplicates, c("bene_id", "id_lowercase"))
    abcd[, c("id_lowercase", "dup") := NULL]
    Sys.time() - begin.time
    rm(begin.time)
    
## (6) PUSH MBSF combined AB/ABCD ----
    # create last_run timestamp
    abcd[, last_run := Sys.time()]
    
    # create table ID for SQL
    tbl_id <- DBI::Id(schema = table_config$schema, 
                      table = table_config$table)  
    
    # ensure column order in R is the same as that in SQL
    setcolorder(abcd, names(table_config$vars))
    
    # calc max characters for each colum to troubleshoot why not loading to SQL
    #  chars <- lapply(abcd[, ..abcd.characters], function(x) max(nchar(x), na.rm = T))
    #  chars <- data.table(name = names(chars), value = chars)
    
    # Write table to SQL
    dbWriteTable(db_claims, 
                 tbl_id, 
                 value = as.data.frame(abcd),
                 overwrite = T, append = F, 
                 field.types = unlist(table_config$vars))
    
    # Confirm that all rows were loaded to sql
    stage.count <- as.numeric(odbc::dbGetQuery(db_claims, 
                                                "SELECT COUNT (*) FROM stage.mcare_mbsf"))
    if(stage.count != nrow(abcd))
      stop("Mismatching row count, error reading in data")

## (7) QA combined MBSF ----
    # Extract staged data from SQL ----
      last_run <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcare_mbsf")[[1]])
      
      raw.count <- ab.raw.count + abcd.raw.count
      
      row_diff <- raw.count - stage.count
    
    # check that rows in stage are not more than rows in load_raw ----
    if(row_diff<0){
      row_diff <- abs(row_diff) # get absolute value
      odbc::dbGetQuery(
        conn = db_claims,
        glue::glue_sql("INSERT INTO metadata.qa_mcare
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcare_mbsf',
                       'Number rows compared to load_raw', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {row_diff} excess rows in the staged tables 
                       ({stage.count} vs. {raw.count})')",
                       .con = db_claims))

      problem.raw.row.count <- glue::glue("Error: more rows in stage compared with load_raw.
                                          \n")
    } else {
      odbc::dbGetQuery(
        conn = db_claims,
        glue::glue_sql("INSERT INTO metadata.qa_mcare
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcare_mbsf',
                       'Number rows compared to load_raw', 
                       'PASS', 
                       {Sys.time()}, 
                       'There were {row_diff} fewer rows in the staged vs. raw tables 
                       ({stage.count} vs. {raw.count})')",
                       .con = db_claims))
      
      problem.raw.row.count <- glue::glue(" ") # no problem, so empty error message
      
    }# close check that load raw doesn't have less rows than stage
    
    # check that rows in stage are not less than the last time that it was created ----
      # count number of rows
      previous_rows <- as.numeric(
        odbc::dbGetQuery(db_claims, 
                         "SELECT c.qa_value from
                         (SELECT a.* FROM
                         (SELECT * FROM metadata.qa_mcare_values
                         WHERE table_name = 'stage.mcare_mbsf' AND
                         qa_item = 'row_count') a
                         INNER JOIN
                         (SELECT MAX(qa_date) AS max_date 
                         FROM metadata.qa_mcare_values
                         WHERE table_name = 'stage.mcare_mbsf' AND
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
                       'stage.mcare_mbsf',
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
                       'stage.mcare_mbsf',
                       'Number new rows compared to most recent run', 
                       'PASS', 
                       {Sys.time()}, 
                       'There were {row_diff} more rows in the most recent table 
                       ({stage.count} vs. {previous_rows})')",
                         .con = db_claims))
        
        problem.row_diff <- glue::glue(" ") # no problem, so empty error message
        
      }
    
    # check that there are no duplicates ----
      # get count of unique id/year combos (each id shoudl only appear once per year)
        stage.count.unique <- as.numeric(odbc::dbGetQuery(
          db_claims, "SELECT COUNT (*) 
                      FROM (Select bene_id, bene_enrollmt_ref_yr 
                        FROM PHClaims.stage.mcare_mbsf
                        GROUP BY bene_id, bene_enrollmt_ref_yr
                      )t;"
          ))

        if (stage.count.unique != stage.count) {
          odbc::dbGetQuery(
            conn = db_claims,
            glue::glue_sql("INSERT INTO metadata.qa_mcare
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES (
                        {last_run}, 
                       'stage.mcare_mbsf',
                       'Number distinct IDs', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {stage.count.unique} distinct ID/YEAR combinations but {stage.count} rows overall (should be the same)'
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
                       'stage.mcare_mbsf',
                       'Number distinct IDs', 
                       'PASS', 
                       {Sys.time()}, 
                       'The number of distinct ID/YEAR combinations matched number of overall rows ({stage.count.unique})')",
                           .con = db_claims))
          
          problem.ids  <- glue::glue(" ") # no problem
        }
      
    # create summary of errors ---- 
      problems <- glue::glue(
                                problem.ids, "\n",
                                problem.raw.row.count, "\n",
                                problem.row_diff)
      
    # clean-up MBSF AB objects ----
      rm(abcd, last_run, previous_rows, problem.ids, problem.raw.row.count, problem.row_diff, row_diff, tbl_id, table_config)
      gc()        
      
## (8) Fill qa_mcare_values table ----
      qa.values <- glue::glue_sql("INSERT INTO metadata.qa_mcare_values
                             (table_name, qa_item, qa_value, qa_date, note) 
                                     VALUES ('stage.mcare_mbsf',
                                     'row_count', 
                                     {stage.count}, 
                                     {Sys.time()}, 
                                     '')",
                                     .con = db_claims)
      
      odbc::dbGetQuery(conn = db_claims, qa.values)
      
      
## (9) Print error messages ----
      if(problems >1){
        message(glue::glue("WARNING ... MBSF ABCD FAILED AT LEAST ONE QA TEST", "\n",
                           "Summary of problems in MBSF ABCD: ", "\n", 
                           problems))
      }else{message("Staged MBSF passed all QA tests")}
      
## The end! ----
      run.time <- Sys.time() - start.time
      print(run.time)