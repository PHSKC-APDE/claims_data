## Header ####
    # Author: Danny Colombara
    # 
    # R version: 3.5.3
    #
    # Purpose: Simple processing of load_raw MBSF to staged MBSF
    # 
    # Notes: Medicare ids are case sensitive. Will have to deduplicate randomly (without regard to case sensitivity) becuase there
    #        is no way to know which one we should keep for linkages with other datasets. Checked on 8/15/2019 and found that there 
    #        were 378 IDs that would be dropped through this process
    #
    #        This code takes approximately 32 minutes to run under normal SQL server traffic conditions

## Set up environment ----
    rm(list=ls())
    pacman::p_load(data.table, dplyr, odbc, lubridate, glue, httr)
    start.time <- Sys.time()
    options("scipen"=999) # turn off scientific notation  
    options(warning.length = 8170) # get lengthy warnings, needed for SQL
    setwd("C:/temp/")
    yaml.ab.url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage_mcare_mbsf_ab.yaml"
    yaml.abcd.url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage_mcare_mbsf_abcd.yaml"
    
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
    
    # Add date stamp to data
    ab[, last_run := Sys.time()]
    
    # read in YAML data for new stage file from GitHub
    x <- GET(yaml.ab.url, authenticate(Sys.getenv("GITHUB_TOKEN"), ""))
    stop_for_status(x)
    x <- content(x, type="text", encoding = "ISO-8859-1")
    writeLines(x, con="temp.yaml")
    table_config <- yaml::read_yaml("temp.yaml")
    file.remove("temp.yaml")
    
    # identify the column types needed for stage file in SQL
    ab.stage.cols <- data.table(col.name = names(table_config$vars), col.type = table_config$vars)
    ab.characters <- ab.stage.cols[tolower(col.type) %like% "varchar", ]$col.name
    ab.integers <- ab.stage.cols[tolower(col.type) %like% "int", ]$col.name
    ab.dates <- ab.stage.cols[tolower(col.type) %like% "date", ]$col.name
    rm(ab.stage.cols)
    
    # fix zip codes by adding preceding zeros and setting to character 
    ab[, bene_zip_cd := formatC(as.numeric(bene_zip_cd), width = 5, format = "d", flag="0")]
    ab[bene_zip_cd=="999999999", bene_zip_cd := NA]
    
    # change the class of the R columns if necessary
    ab[, (ab.characters):= lapply(.SD, as.character), .SDcols = ab.characters]
    ab[, (ab.integers):= lapply(.SD, as.integer), .SDcols = ab.integers]
    ab[, (ab.dates):= lapply(.SD, as.Date), .SDcols = ab.dates]
    
## (3) PUSH MBSF AB ----    
    # create table ID for SQL
    tbl_id <- DBI::Id(schema = table_config$schema, 
                      table = table_config$table)  
    
    # ensure column order in R is the same as that in SQL
    setcolorder(ab, names(table_config$vars))
    
    # Write table to SQL
    dbWriteTable(db_claims, 
                 tbl_id, 
                 value = as.data.frame(ab),
                 overwrite = T, append = F, 
                 field.types = unlist(table_config$vars))

## (4) QA MBSF AB ----
    # Extract staged data from SQL ----
      last_run <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcare_mbsf_ab")[[1]])
      
      ab.stage.count <- as.numeric(odbc::dbGetQuery(db_claims, 
                                                  "SELECT COUNT (*) FROM stage.mcare_mbsf_ab"))
      
      row_diff <- ab.raw.count - ab.stage.count
    
    # check that rows in stage are not more than rows in load_raw ----
    if(row_diff<0){
      row_diff <- abs(row_diff) # get absolute value
      odbc::dbGetQuery(
        conn = db_claims,
        glue::glue_sql("INSERT INTO metadata.qa_mcare
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcare_mbsf_ab',
                       'Number rows compared to load_raw', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {row_diff} excess rows in the staged tables 
                       ({ab.stage.count} vs. {ab.raw.count})')",
                       .con = db_claims))

      problem.raw.row.count <- glue::glue("Error: more rows in stage compared with load_raw.
                                          \n")
    } else {
      odbc::dbGetQuery(
        conn = db_claims,
        glue::glue_sql("INSERT INTO metadata.qa_mcare
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcare_mbsf_ab',
                       'Number rows compared to load_raw', 
                       'PASS', 
                       {Sys.time()}, 
                       'There were {row_diff} fewer rows in the staged vs. raw tables 
                       ({ab.stage.count} vs. {ab.raw.count})')",
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
                         WHERE table_name = 'stage.mcare_mbsf_ab' AND
                         qa_item = 'row_count') a
                         INNER JOIN
                         (SELECT MAX(qa_date) AS max_date 
                         FROM metadata.qa_mcare_values
                         WHERE table_name = 'stage.mcare_mbsf_ab' AND
                         qa_item = 'row_count') b
                         ON a.qa_date = b.max_date)c"))
      
      if(is.na(previous_rows)){previous_rows = 0}
      
      row_diff <- ab.stage.count - previous_rows
      
      if (row_diff < 0) {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_mcare
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcare_mbsf_ab',
                       'Number new rows compared to most recent run', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {row_diff} fewer rows in the most recent table 
                       ({ab.stage.count} vs. {previous_rows})')",
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
                       'stage.mcare_mbsf_ab',
                       'Number new rows compared to most recent run', 
                       'PASS', 
                       {Sys.time()}, 
                       'There were {row_diff} more rows in the most recent table 
                       ({ab.stage.count} vs. {previous_rows})')",
                         .con = db_claims))
        
        problem.row_diff <- glue::glue(" ") # no problem, so empty error message
        
      }
    
    # check that there are no duplicates ----
      # get count of unique id/year combos (each id shoudl only appear once per year)
        ab.stage.count.unique <- as.numeric(odbc::dbGetQuery(
          db_claims, "SELECT COUNT (*) 
                      FROM (Select bene_id, bene_enrollmt_ref_yr 
                        FROM PHClaims.stage.mcare_mbsf_ab
                        GROUP BY bene_id, bene_enrollmt_ref_yr
                      )t;"
          ))

        if (ab.stage.count.unique != ab.stage.count) {
          odbc::dbGetQuery(
            conn = db_claims,
            glue::glue_sql("INSERT INTO metadata.qa_mcare
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES (
                        {last_run}, 
                       'stage.mcare_mbsf_ab',
                       'Number distinct IDs', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {ab.stage.count.unique} distinct ID/YEAR combinations but {ab.stage.count} rows overall (should be the same)'
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
                       'stage.mcare_mbsf_ab',
                       'Number distinct IDs', 
                       'PASS', 
                       {Sys.time()}, 
                       'The number of distinct ID/YEAR combinations matched number of overall rows ({ab.stage.count.unique})')",
                           .con = db_claims))
          
          problem.ids  <- glue::glue(" ") # no problem
        }
      
    # create summary of errors ---- 
      problems.ab <- glue::glue(
                                problem.ids, "\n",
                                problem.raw.row.count, "\n",
                                problem.row_diff)
    # clean-up MBSF AB ojbects ----
      rm(ab, last_run, previous_rows, problem.ids, problem.raw.row.count, problem.row_diff, row_diff, tbl_id, table_config)
      gc()

## (5) PULL & prep MBSF ABCD ---- 
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
    
    # Add date stamp to data
    abcd[, last_run := Sys.time()]
    
    # read in YAML data for new stage file from GitHub
    x <- GET(yaml.abcd.url, authenticate(Sys.getenv("GITHUB_TOKEN"), ""))
    stop_for_status(x)
    x <- content(x, type="text", encoding = "ISO-8859-1")
    writeLines(x, con="temp.yaml")
    table_config <- yaml::read_yaml("temp.yaml")
    file.remove("temp.yaml")
    
    # identify the column types needed for stage file in SQL
    abcd.stage.cols <- data.table(col.name = names(table_config$vars), col.type = table_config$vars)
    abcd.characters <- abcd.stage.cols[tolower(col.type) %like% "varchar", ]$col.name
    abcd.integers <- abcd.stage.cols[tolower(col.type) %like% "int", ]$col.name
    abcd.dates <- abcd.stage.cols[tolower(col.type) %like% "date", ]$col.name
    rm(abcd.stage.cols)
    
    # fix zip codes by adding preceding zeros and setting to character 
    abcd[, zip_cd := formatC(as.numeric(zip_cd), width = 5, format = "d", flag="0")]
    abcd[zip_cd=="99999", zip_cd := NA]

    # change the class of the R columns if necessary
    abcd[, (abcd.characters):= lapply(.SD, as.character), .SDcols = abcd.characters]
    abcd[, (abcd.integers):= lapply(.SD, as.integer), .SDcols = abcd.integers]
    abcd[, (abcd.dates):= lapply(.SD, as.Date), .SDcols = abcd.dates]

## (6) PUSH MBSF ABCD ----
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

## (7) QA MBSF ABCD ----
    # Extract staged data from SQL ----
      last_run <- as.POSIXct(odbc::dbGetQuery(db_claims, "SELECT MAX (last_run) FROM stage.mcare_mbsf_abcd")[[1]])
      
      abcd.stage.count <- as.numeric(odbc::dbGetQuery(db_claims, 
                                                  "SELECT COUNT (*) FROM stage.mcare_mbsf_abcd"))
      
      row_diff <- abcd.raw.count - abcd.stage.count
    
    # check that rows in stage are not more than rows in load_raw ----
    if(row_diff<0){
      row_diff <- abs(row_diff) # get absolute value
      odbc::dbGetQuery(
        conn = db_claims,
        glue::glue_sql("INSERT INTO metadata.qa_mcare
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcare_mbsf_abcd',
                       'Number rows compared to load_raw', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {row_diff} excess rows in the staged tables 
                       ({abcd.stage.count} vs. {abcd.raw.count})')",
                       .con = db_claims))

      problem.raw.row.count <- glue::glue("Error: more rows in stage compared with load_raw.
                                          \n")
    } else {
      odbc::dbGetQuery(
        conn = db_claims,
        glue::glue_sql("INSERT INTO metadata.qa_mcare
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcare_mbsf_abcd',
                       'Number rows compared to load_raw', 
                       'PASS', 
                       {Sys.time()}, 
                       'There were {row_diff} fewer rows in the staged vs. raw tables 
                       ({abcd.stage.count} vs. {abcd.raw.count})')",
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
                         WHERE table_name = 'stage.mcare_mbsf_abcd' AND
                         qa_item = 'row_count') a
                         INNER JOIN
                         (SELECT MAX(qa_date) AS max_date 
                         FROM metadata.qa_mcare_values
                         WHERE table_name = 'stage.mcare_mbsf_abcd' AND
                         qa_item = 'row_count') b
                         ON a.qa_date = b.max_date)c"))
      
      if(is.na(previous_rows)){previous_rows = 0}
      
      row_diff <- abcd.stage.count - previous_rows
      
      if (row_diff < 0) {
        odbc::dbGetQuery(
          conn = db_claims,
          glue::glue_sql("INSERT INTO metadata.qa_mcare
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES ({last_run}, 
                       'stage.mcare_mbsf_abcd',
                       'Number new rows compared to most recent run', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {row_diff} fewer rows in the most recent table 
                       ({abcd.stage.count} vs. {previous_rows})')",
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
                       'stage.mcare_mbsf_abcd',
                       'Number new rows compared to most recent run', 
                       'PASS', 
                       {Sys.time()}, 
                       'There were {row_diff} more rows in the most recent table 
                       ({abcd.stage.count} vs. {previous_rows})')",
                         .con = db_claims))
        
        problem.row_diff <- glue::glue(" ") # no problem, so empty error message
        
      }
    
    # check that there are no duplicates ----
      # get count of unique id/year combos (each id shoudl only appear once per year)
        abcd.stage.count.unique <- as.numeric(odbc::dbGetQuery(
          db_claims, "SELECT COUNT (*) 
                      FROM (Select bene_id, bene_enrollmt_ref_yr 
                        FROM PHClaims.stage.mcare_mbsf_abcd
                        GROUP BY bene_id, bene_enrollmt_ref_yr
                      )t;"
          ))

        if (abcd.stage.count.unique != abcd.stage.count) {
          odbc::dbGetQuery(
            conn = db_claims,
            glue::glue_sql("INSERT INTO metadata.qa_mcare
                       (last_run, table_name, qa_item, qa_result, qa_date, note) 
                       VALUES (
                        {last_run}, 
                       'stage.mcare_mbsf_abcd',
                       'Number distinct IDs', 
                       'FAIL', 
                       {Sys.time()}, 
                       'There were {abcd.stage.count.unique} distinct ID/YEAR combinations but {abcd.stage.count} rows overall (should be the same)'
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
                       'stage.mcare_mbsf_abcd',
                       'Number distinct IDs', 
                       'PASS', 
                       {Sys.time()}, 
                       'The number of distinct ID/YEAR combinations matched number of overall rows ({abcd.stage.count.unique})')",
                           .con = db_claims))
          
          problem.ids  <- glue::glue(" ") # no problem
        }
      
    # create summary of errors ---- 
      problems.abcd <- glue::glue(
                                problem.ids, "\n",
                                problem.raw.row.count, "\n",
                                problem.row_diff)
      
    # clean-up MBSF AB ojbects ----
      rm(abcd, last_run, previous_rows, problem.ids, problem.raw.row.count, problem.row_diff, row_diff, tbl_id, table_config)
      gc()        
      
## (8) Fill qa_mcare_values table ----
      # MBSF AB ----
      qa.values.ab <- glue::glue_sql("INSERT INTO metadata.qa_mcare_values
                             (table_name, qa_item, qa_value, qa_date, note) 
                                 VALUES ('stage.mcare_mbsf_ab',
                                 'row_count', 
                                 {ab.stage.count}, 
                                 {Sys.time()}, 
                                 '')",
                                 .con = db_claims)
      
      odbc::dbGetQuery(conn = db_claims, qa.values.ab)
      
      # MBSF ABCD ----
      qa.values.abcd <- glue::glue_sql("INSERT INTO metadata.qa_mcare_values
                             (table_name, qa_item, qa_value, qa_date, note) 
                                     VALUES ('stage.mcare_mbsf_abcd',
                                     'row_count', 
                                     {abcd.stage.count}, 
                                     {Sys.time()}, 
                                     '')",
                                     .con = db_claims)
      
      odbc::dbGetQuery(conn = db_claims, qa.values.abcd)
      
      
## (9) Print error messages ----
      if(problems.ab >1){
      message(glue::glue("WARNING ... MBSF AB FAILED AT LEAST ONE QA TEST", "\n",
                        "Summary of problems in MBSF AB: ", "\n", 
                        problems.ab))
      }else{message("MBSF AB passed all QA tests")}
      
      
      if(problems.abcd >1){
        message(glue::glue("WARNING ... MBSF ABCD FAILED AT LEAST ONE QA TEST", "\n",
                           "Summary of problems in MBSF ABCD: ", "\n", 
                           problems.abcd))
      }else{message("MBSF ABCD passed all QA tests")}
      
## The end! ----
      run.time <- Sys.time() - start.time
      print(run.time)