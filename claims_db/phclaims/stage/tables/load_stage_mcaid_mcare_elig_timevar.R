# Header ####
  # Author: Danny Colombara
  # Date: September 16, 2019
  # Purpose: Create stage.mcaid_mcare_elig_timevar for SQL
  #
  # Notes: BEFORE RUNNING THIS CODE, PLEASE BE SURE THE FOLLOWING ARE UP TO DATE ... 
  #       - [PHClaims].[stage].[mcaid_elig_timevar]
  #       - [PHClaims].[stage].[mcare_elig_timevar]
  #       - [PHClaims].[stage].[xwalk_apde_mcaid_mcare_pha]

## Set up R Environment ----
  rm(list=ls())  # clear memory
  pacman::p_load(data.table, odbc, DBI, lubridate) # load packages
  options("scipen"=999) # turn off scientific notation  
  options(warning.length = 8170) # get lengthy warnings, needed for SQL
  
  start.time <- Sys.time()
  
  kc.zips.url <- "https://raw.githubusercontent.com/PHSKC-APDE/reference-data/master/spatial_data/zip_admin.csv"
  
  yaml.url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage_mcaid_mcare_elig_timevar.yaml"
  
## (1) Connect to SQL Server ----    
  db_claims <- dbConnect(odbc(), "PHClaims51")   
  
## (2) Load data from SQL ----  
  apde <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_apde, id_mcare, id_mcaid
                                 FROM PHClaims.stage.xwalk_apde_mcaid_mcare_pha"))
  
  mcare <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_mcare, from_date, to_date, part_a, part_b, part_c, partial, buy_in, geo_zip 
                                  FROM PHClaims.stage.mcare_elig_timevar"))
           setnames(mcare, "geo_zip", "geo_zip_clean")
           mcare[, from_date := as.integer(as.Date(from_date))] # convert date string to a real date
           mcare[, to_date := as.integer(as.Date(to_date))] # convert date to an integer (temporarily for finding intersections)
           
  mcaid <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_mcaid, from_date, to_date, tpl, bsp_group_name, full_benefit, cov_type, mco_id, 
                                  geo_add1_clean, geo_add2_clean, geo_city_clean, geo_state_clean, geo_zip_clean, geo_zip_centroid, 
                                  geo_street_centroid, geo_countyfp10, geo_tractce10, geo_hra_id, geo_school_geoid10
                                  FROM PHClaims.final.mcaid_elig_timevar"))
          mcaid[, from_date := as.integer(as.Date(from_date))] # convert date string to a real date
          mcaid[, to_date := as.integer(as.Date(to_date))] # convert date to an integer (temporarily for finding intersections)
  
## (3) Merge on dual status ----
  mcare <- merge(apde[, .(id_apde, id_mcare)], mcare, by = "id_mcare", all.x = FALSE, all.y = TRUE)
  mcare[, id_mcare := NULL] # no longer needed now that have id_apde
  
  mcaid <- merge(apde[, .(id_apde, id_mcaid)], mcaid, by = "id_mcaid", all.x = FALSE, all.y = TRUE)
  mcaid[, id_mcaid := NULL] # no longer needed now that have id_apde
  
## (4) Identify the duals and split from non-duals for additional processing ----
  dual.id <- intersect(mcaid$id_apde, mcare$id_apde)
  
  mcaid.solo <- mcaid[!id_apde %in% dual.id]  
  mcare.solo <- mcare[!id_apde %in% dual.id]
  
  mcaid.dual <- mcaid[id_apde %in% dual.id]
  mcare.dual <- mcare[id_apde %in% dual.id]
  
  # for the duals, change the "_clean" suffix for address info to mcare so we can more easily pick and choose data to keep with Mcaid data
    setnames(mcare.dual, 
             grep("_clean$", names(mcare.dual), value = TRUE), # identify columns with "_clean" suffix
             gsub("_clean$", "_mcare", grep("_clean$", names(mcare.dual), value = TRUE))) # replace "_clean" with "_mcare"
  
  # drop main original datasets that are no longer needed
  rm(mcaid, mcare)
  gc()

## (5) Duals: Create master list of time intervals ----
    # melt data (wide to long) so start and end dates are in same column, regardless of original source
      duals <- rbind(
          melt(mcaid.dual[, c("id_apde", "from_date", "to_date")], id.vars = "id_apde"), 
          melt(mcare.dual[, c("id_apde", "from_date", "to_date")], id.vars = "id_apde")
        )
  
    # call the combined from_date/to_date column the "from_date" column
      setnames(duals, "value", "from_date")
      duals[, variable := NULL]
      duals <- unique(duals)
      
    # create the to_date by shifting the from_date up one row
      setkey(duals, id_apde, from_date) # sort from earliest to latest
      duals[, to_date := shift(from_date, fill = NA, type = "lead"), by = "id_apde"]
      duals <- duals[!is.na(to_date)] # the last observation per id will be dropped because it is the final to_date
      duals[, counter := 1:.N, by = id_apde] # create indicator so we can know which interval is the first interval for each id_apde
      duals[counter != 1, from_date := as.integer(from_date + 1)] # add one to from_date so that it does not overlap with the previous interval
      duals[, counter := NULL]
      
    # drop duplicate rows if they exist (they shouldn't, but just in case)
      duals <- unique(duals)

## (6) Duals: join mcare/mcaid data based on ID & overlapping time periods ----      
      duals[, c("from_date", "to_date") := lapply(.SD, as.integer), .SDcols = c("from_date", "to_date")] # ensure type==integer for foverlaps()
      setkey(duals, id_apde, from_date, to_date)    
      
      mcare.dual[, c("from_date", "to_date") := lapply(.SD, as.integer), .SDcols = c("from_date", "to_date")] # ensure type==integer for foverlaps()
      setkey(mcare.dual, id_apde, from_date, to_date)

      mcaid.dual[, c("from_date", "to_date") := lapply(.SD, as.integer), .SDcols = c("from_date", "to_date")] # ensure type==integer for foverlaps()
      setkey(mcaid.dual, id_apde, from_date, to_date)
      
      # join on the Medicaid duals data
      duals <- foverlaps(duals, mcaid.dual, type = "any", mult = "all")
      duals[, from_date := i.from_date] # when mcaid.dual didn't match, the from_date is NA. Need to replace it with the data saved in the i.from_date
      duals[, to_date := i.to_date] # when mcaid.dual didn't match, the from_date is NA. Need to replace it with the data saved in the i.from_date
      duals[, c("i.from_date", "i.to_date") := NULL] # no longer needed
      setkey(duals, id_apde, from_date, to_date)
      
      # join on the Medicare duals data
      duals <- foverlaps(duals, mcare.dual, type = "any", mult = "all")
      duals[, from_date := i.from_date] # when mcare.dual didn't match, the from_date is NA. Need to replace it with the data saved in the i.from_date
      duals[, to_date := i.to_date] # when mcare.dual didn't match, the from_date is NA. Need to replace it with the data saved in the i.from_date
      duals[, c("i.from_date", "i.to_date") := NULL] # no longer needed
      
## (7) Append duals and non-duals data ----
      timevar <- rbindlist(list(duals, mcare.solo, mcaid.solo), use.names = TRUE, fill = TRUE)
      setkey(timevar, id_apde, from_date)
      
## (8) Collapse data if dates are contiguous and all data is the same ----
    # Create unique ID for data chunks ----
      timevar.vars <- setdiff(names(timevar), c("from_date", "to_date")) # all vars except date vars
      timevar[, group := .GRP, by = timevar.vars] # create group id
      timevar[, group := cumsum( c(0, diff(group)!=0) )] # in situation like a:a:a:b:b:b:b:a:a:a, want to distinguish first set of "a" from second set of "a"
    
    # Create unique ID for contiguous times within a given data chunk ----
      setkey(timevar, id_apde, from_date)
      timevar[, prev_to_date := shift(to_date, 1L, type = "lag"), by = "group"] # create row with the previous 'to_date'
      timevar[, diff.prev := from_date - prev_to_date] # difference between from_date & prev_to_date will be 1 (day) if they are contiguous
      timevar[diff.prev != 1, diff.prev := NA] # set to NA if difference is not 1 day, i.e., it is not contiguous, i.e., it starts a new contiguous chunk
      timevar[is.na(diff.prev), contig.id := .I] # Give a unique number for each start of a new contiguous chunk (i.e., section starts with NA)
      setkey(timevar, group, from_date) # need to order the data so the following line will work.
      timevar[, contig.id  := contig.id[1], by=  .( group , cumsum(!is.na(contig.id))) ] # fill forward by group
      timevar[, c("prev_to_date", "diff.prev") := NULL] # drop columns that were just intermediates
      
    # Collapse rows where data chunks are constant and time is contiguous ----      
      timevar[, from_date := min(from_date), by = c("group", "contig.id")]
      timevar[, to_date := max(to_date), by = c("group", "contig.id")]
      timevar[, c("group", "contig.id") := NULL]
      timevar <- unique(timevar)
    
## (9) Prep for pushing to SQL ----
    # Create contiguous flag ----  
      # If contiguous with the PREVIOUS row, then it is marked as contiguous. This is the same as mcaid_elig_timevar
      timevar[, prev_to_date := shift(to_date, 1L, type = "lag"), by = "id_apde"]
      timevar[, contiguous := 0]
      timevar[from_date - prev_to_date == 1, contiguous := 1]
      timevar[, prev_to_date := NULL] # drop because no longer needed
      
    # Create cov_time_date ----
      timevar[, cov_time_day := as.integer(to_date - from_date)]
      
    # Set dates as.Date() ----
      timevar[, c("from_date", "to_date") := lapply(.SD, as.Date, origin = "1970-01-01"), .SDcols =  c("from_date", "to_date")] 

    # Select data from Medicare or Medicaid, as appropriate ----
      timevar[is.na(geo_zip_clean) & !is.na(geo_zip_mcare), geo_zip_clean := geo_zip_mcare]
      timevar[, geo_zip_mcare := NULL]
      
    # Add KC flag based on zip code ----  
      kc.zips <- fread(kc.zips.url)
      timevar[, geo_kc := 0]
      timevar[geo_zip_clean %in% unique(as.character(kc.zips$zip)), geo_kc := 1]
      rm(kc.zips)
      
    # create time stamp ----
      timevar[, last_run := Sys.time()] 
      
## (10) Write to SQL ----              
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
    