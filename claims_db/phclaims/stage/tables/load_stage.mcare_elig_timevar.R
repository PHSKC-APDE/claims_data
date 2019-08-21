## Header ####
    # Author: Danny Colombara
    # 
    # R version: 3.5.3
    #
    # Purpose: Create load_stage.mcare_elig_timevar on SQL server
    #          Will track time varying elements of Medicare enrollment data
    # 
    # Notes: Type the <Alt> + <o> at the same time to collapse the code and view the structure
    #
    #         Run time is approximately 22 minutes

## Set up environment ----
    rm(list=ls())
    .libPaths("C:/Users/dcolombara/R.packages") # needed for 32 GB SAS computer.
    pacman::p_load(data.table, dplyr, odbc, lubridate, glue, httr)
    options("scipen"=10) # turn off scientific notation  
    options(warning.length = 8170) # get lengthy warnings, needed for SQL
    setwd("C:/temp/")
    kc.zips.url <- "https://raw.githubusercontent.com/PHSKC-APDE/reference-data/master/spatial_data/zip_admin.csv"
    yaml.url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage_mcare_elig_timevar.yaml"
    qa.function.url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/qa_stage.mcare_elig_timevar.R"
    start.time <- Sys.time()
    
## (1) Load SQL data ----
    # Connect to SQL server
      db_claims <- dbConnect(odbc(), "PHClaims51") 
  
    # Identify variables of interest   
      mbsf.vars <- paste(
        c("bene_id", "bene_enrollmt_ref_yr", "zip_cd",
          paste0("dual_stus_cd_", formatC(1:12, width = 2, flag = "0")), 
          paste0("mdcr_entlmt_buyin_ind_", formatC(1:12, width = 2, flag = "0")), 
          paste0("hmo_ind_", formatC(1:12, width = 2, flag = "0"))),
        collapse = ", ")
      
    # Create query strings 
      query.string <- glue_sql ("SELECT ", mbsf.vars, " FROM stage.mcare_mbsf")
    
    # Pull data and save as data.table object
      dt <- setDT(DBI::dbGetQuery(db_claims, query.string))
      
    # Ensure that the complete number of rows were downloaded
      # count rows in load_raw 
      sql.row.count <- as.numeric(odbc::dbGetQuery(db_claims, 
                                                  "SELECT COUNT (*) FROM stage.mcare_mbsf"))
      if(sql.row.count != nrow(dt))
        stop("Mismatching row count, error reading in data")
      
## (2) Rename columns ---- 
     # create column names for renaming / reshaping ----
          dual.cols <- paste0("dual_", formatC(1:12, width = 2, flag = "0"))
          buyin.cols <- paste0("buyin_", formatC(1:12, width = 2, flag = "0"))
          hmo.cols <- paste0("hmo_", formatC(1:12, width = 2, flag = "0"))
      
      # rename columns ----
          setnames(dt, 
                   old = paste0("dual_stus_cd_", formatC(1:12, width = 2, flag = "0")), 
                   new = dual.cols)
          setnames(dt, 
                   old = paste0("mdcr_entlmt_buyin_ind_", formatC(1:12, width = 2, flag = "0")), 
                   new = buyin.cols)
          setnames(dt, 
                   old = paste0("hmo_ind_", formatC(1:12, width = 2, flag = "0")), 
                   new = hmo.cols)
          setnames(dt, old = c("zip_cd", "bene_id", "bene_enrollmt_ref_yr"), new = c("zip", "id_mcare", "data_year"))

## (3) Reshape wide to long ----
      # reshaping multiple unrelated columns simultaneously uses 'enhanced melt': https://cran.r-project.org/web/packages/data.table/vignettes/datatable-reshape.html
      dt <- melt(dt, 
                id.vars = c("id_mcare", "data_year", "zip"), 
                measure = list(dual.cols, buyin.cols, hmo.cols), 
                value.name = c("duals", "buyins", "hmos"), variable.name = c("month"))
      
## (4) Recode / create indicators ----
      # part a
      dt[buyins %in% c("1", "3", "A", "C"), part_a := 1]
      dt[buyins %in% c("0", "2", "B"), part_a := 0]
      
      # part b
      dt[buyins %in% c("2", "3", "B", "C"), part_b := 1]
      dt[buyins %in% c("0", "1", "A"), part_b := 0]
      
      # part c
      dt[hmos %in% c("1", "2", "A", "B", "C"), part_c := 1]
      dt[hmos %in% c("0", "4"), part_c := 0] # https://www.resdac.org/articles/identifying-medicare-managed-care-beneficiaries-master-beneficiary-summary-or-denominator
      
      # buyin
      dt[buyins %in% c("0", "1", "2", "3"), buyin := 0]
      dt[buyins %in% c("A", "B", "C"), buyin := 1]
      
      # partial dual (can't define for 2011-2014, i.e., MBSF AB)
      dt[data_year %in% c(2015:2016), partial := 0]
      dt[data_year %in% c(2015:2016) & duals %in% c(1, 3, 5, 6), partial := 1]

      # dual (defined differently for 2011-2014 & 2015+)
      dt[data_year %in% c(2011:2014), dual := buyin]
      
      dt[data_year %in% c(2015:2016), dual := 0]
      dt[data_year %in% c(2015:2016) & duals %in% c(1, 2, 3, 4, 5, 6, 8), dual := 1]
     
      # drop vars no longer needed
      dt[, c("buyins", "hmos", "duals") := NULL] 

## (5) Create start / end dates ----
      gc() # had memory problems, so added to see if it helps
      dt[, from_date := paste0(data_year, "-", month, "-01")] # from date is always first day of the month ... done step wise with hope helps with memory
      dt[, from_date := ymd(from_date)] # from date is always first day of the month
      dt[, to_date := days_in_month(from_date)] # identify last day of month (adapts for Leap years)
      dt[, to_date := paste0(data_year, "-", month, "-", to_date)] # again, piecewise with hope it helps with memory
      dt[, to_date := ymd(to_date)]
      dt[, c("data_year", "month") := NULL]
      gc()
      
## (6) Set the key to order the data ----      
      setkeyv(dt, c("id_mcare", "from_date"))
      
## (7) Create unique ID for data chunks ----
      timevar.vars <- setdiff(names(dt), c("from_date", "to_date")) # all vars except date vars
      dt[, group := .GRP, by = timevar.vars] # create identifier for each unique block of data per id (except dates)
      dt[, group := cumsum( c(0, diff(group)!=0) )] # in situation like a:a:a:b:b:b:b:a:a:a, want to distinguish first set of "a" from second set of "a"
      
## (8) Create unique ID for contiguous times within a given data chunk ----
      dt[, prev_to_date := shift(to_date, 1L, type = "lag"), by = "group"] # create row with the previous 'to_date'
      dt[, diff.prev := from_date - prev_to_date] # difference between from_date & prev_to_date will be 1 (day) if they are contiguous
      dt[diff.prev != 1, diff.prev := NA] # set to NA if difference is not 1 day, i.e., it is not contiguous, i.e., it starts a new contiguous chunk
      dt[is.na(diff.prev), contig.id := .I] # Give a unique number for each start of a new contiguous chunk (i.e., section starts with NA)
      setkeyv(dt, c("group", "from_date")) # need to order the data so the following line will work.
      dt[, contig.id := dt[!is.na(contig.id)][dt, contig.id, roll = T]] # Fill down values of contig.id to complete the identification of the contiguous segments
      dt[, c("prev_to_date", "diff.prev") := NULL] # drop columns that were just intermediates

## (9) Collapse rows where data chunks are constant and time is contiguous ----      
      # replace dates with their max and min within each group
        dt[, from_date := min(from_date), by = c("group", "contig.id")]
        dt[, to_date := max(to_date), by = c("group", "contig.id")]
        
      # drop group & contig.id variables b/c no longer needed
        # first confirmed that worked as planned.E.g., View(dt[id_mcare == "GGGGGGF6G6oFQuF"]) shows two separate contiguous periods within a given group id
        dt[, c("group", "contig.id") := NULL]
      
      # collapse by dropping duplicate rows that were made when replacing dates with min/max
        dt <- unique(dt)
        
      # drop the first row per id if all values are zero (i.e., they were not enrolled during that time because didn't begin on January 1 of that year)
        dt[, counter := 1:.N, by = "id_mcare"] # create row numbers for each person
        dt <- dt[!(counter == 1 & dual == 0 & buyin == 0 & part_a == 0 & part_b == 0 & part_c == 0)] # drop when first row for each person is all zeros
        dt[, counter := NULL] 
        
## (10) Identify contiguous periods ----
        # If contiguous with the NEXT row, then it is marked as contiguous. This is the same as mcaid_elig_timevar
        dt[, prev_to_date := shift(to_date, 1L, type = "lag"), by = "id_mcare"]
        dt[, contiguous := 0]
        dt[from_date - prev_to_date == 1, contiguous := 1]
        
        # simple error check
        if(nrow(dt[prev_to_date>=from_date]) != 0)
          stop("STOP!! 
                The previous 'to_date' is greater than or equal to the 'from_date'. 
                This indicates a logical error in the creation of the collapsed segments.
                Please review the code above to identify the error.")
        
        dt[, prev_to_date := NULL] # drop because no longer needed
        
## (11) Add King County indicator & cov_time_day ----
        kc.zips <- fread(kc.zips.url)
        dt[, kc := 0]
        dt[zip %in% unique(as.character(kc.zips$zip)), kc := 1]
        rm(kc.zips)
        
        dt[, cov_time_day := as.integer(to_date - from_date)]
        
## (12) Load to SQL ----
    # Add date stamp to data
        dt[, last_run := Sys.time()]
            
    # Pull YAML from GitHub
        table_config <- yaml::yaml.load(RCurl::getURL(yaml.url))
        
    # Create table ID
        tbl_id <- DBI::Id(schema = table_config$schema, 
                                     table = table_config$table)  
        
    # Ensure columns are in same order in R & SQL
        setcolorder(dt, names(table_config$vars))
        
    # Write table to SQL
        dbWriteTable(db_claims, 
                     tbl_id, 
                     value = as.data.frame(dt),
                     overwrite = T, append = F, 
                     field.types = unlist(table_config$vars))
        rm(table_config, tbl_id)
        
## (13) Run QA function ----
        source(qa.function.url)

        qa_mcare_elig_timevar_f(conn = db_claims, load_only = F)
        
## The end! ----
        run.time <- Sys.time() - start.time
        print(run.time)