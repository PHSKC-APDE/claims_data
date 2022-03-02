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
    #        Need at least 32 GB of RAM to run
    #
    #        Run time is approximately 22 minutes

## Set up environment ----
    rm(list=ls())
    pacman::p_load(data.table, dplyr, odbc, lubridate, glue, httr)
    options("scipen"=10) # turn off scientific notation  
    options(warning.length = 8170) # get lengthy warnings, needed for SQL
    kc.zips.url <- "https://raw.githubusercontent.com/PHSKC-APDE/reference-data/master/spatial_data/zip_admin.csv"
    yaml.url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_elig_timevar.yaml"
    qa.function.url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/qa_stage.mcare_elig_timevar.R"
    start.time <- Sys.time()
    
## (1) Load SQL data ----
    # Connect to SQL server
      db_claims <- dbConnect(odbc(), "PHClaims51") 
  
    ## Death date ----
        # Create query strings 
        query.string <- glue::glue_sql ("SELECT id_mcare, death_dt FROM final.mcare_elig_demo")
    
        # Pull data and save as data.table object
        death <- setDT(DBI::dbGetQuery(db_claims, query.string))

        # Ensure that the complete number of rows were downloaded
        sql.row.count <- as.numeric(odbc::dbGetQuery(db_claims, "SELECT COUNT (*) FROM final.mcare_elig_demo"))
        if(sql.row.count != nrow(death))
          stop("Mismatching row count, error reading in data")
        
        # only keep rows with death dates
        death <- death[!is.na(death_dt)]
        
        # convert death_dt to class == date
        death[, death_dt := ymd(death_dt)]
        
    ## MBSF data ----
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
          sql.row.count <- as.numeric(odbc::dbGetQuery(db_claims, 
                                                      "SELECT COUNT (*) FROM stage.mcare_mbsf"))
          if(sql.row.count != nrow(dt))
            stop("Mismatching row count, error reading in data")
      
## (2) Rename columns in MBSF ---- 
      setnames(dt, names(dt), gsub("dual_stus_cd_", "dual_", names(dt)))    
      setnames(dt, names(dt), gsub("mdcr_entlmt_buyin_ind_", "buyin_", names(dt)))    
      setnames(dt, names(dt), gsub("hmo_ind_", "hmo_", names(dt)))    
      setnames(dt, c("zip_cd", "bene_id", "bene_enrollmt_ref_yr"), c("geo_zip", "id_mcare", "data_year"))

## (3) Reshape wide to long ----
      # reshaping multiple unrelated columns simultaneously uses 'enhanced melt': https://cran.r-project.org/web/packages/data.table/vignettes/datatable-reshape.html
      dt <- melt(dt, 
                id.vars = c("id_mcare", "data_year", "geo_zip"), 
                measure = list(grep("dual", names(dt), value = T), 
                               grep("buyin", names(dt), value = T), 
                               grep("hmo", names(dt), value = T) ), 
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
      dt[buyins %in% c("0", "1", "2", "3"), buy_in := 0]
      dt[buyins %in% c("A", "B", "C"), buy_in := 1]
      
      # partial dual (can't define for 2011-2014, i.e., MBSF AB)
      dt[!data_year %in% c(2011:2014) & ( duals %in% c(0, 2, 4, 8, 9) | is.na(duals) ), partial := 0]
      dt[!data_year %in% c(2011:2014) & duals %in% c(1, 3, 5, 6), partial := 1]
      dt[data_year %in% c(2011:2014), partial := NA]

      # dual (defined differently for 2011-2014 & 2015+)
      dt[data_year %in% c(2011:2014), dual := buy_in]
      
      dt[!data_year %in% c(2011:2014) & ( duals %in% c(0) | is.na(duals) ), dual := 0]
      dt[!data_year %in% c(2011:2014)  & duals %in% c(1, 2, 3, 4, 5, 6, 8), dual := 1]
     
      # drop vars no longer needed
      dt[, c("buyins", "hmos", "duals") := NULL] 

## (5) Create start / end dates ----
      ## Clear memory because computer is wimpy ----
      gc() 
      
      ## Create from_date ----
      dt[, from_date := ymd(paste0(data_year, "-", month, "-01"))] # from date is always first day of the month 

      ## Create to_date ----
      dt[, to_date := days_in_month(from_date)] # identify last day of month (adapts for Leap years)
      dt[, to_date := ymd(paste0(data_year, "-", month, "-", to_date))] #

      ## Merge on death date ----
      dt <- merge(dt, death, by= "id_mcare", all.x = TRUE, all.y = FALSE)            
      gc()
      
      ## Truncate data based on death_dt ----
      dt <- dt[!(!is.na(death_dt) & from_date > death_dt)]  # drop rows when from_date is after death
      dt[to_date > death_dt, to_date := death_dt]

      ## Clean up ----
      dt[, c("data_year", "month", "death_dt") := NULL] # drop vars no longer needed
      dt <- dt[!(part_a == 0 & part_b == 0 & part_c == 0), ] # drop rows where not enrolled in Mcare
      gc()
      
## (6) Set the key to order the data ----      
      setkeyv(dt, c("id_mcare", "from_date"))
      
## (7) Collapse data if dates are contiguous and all data is the same ----
      dt[, gr := cumsum(from_date - shift(to_date, fill=1) != 1), by = c(setdiff(names(dt), c("from_date", "to_date")))] # unique group # (gr) for each set of contiguous dates & constant data 
      dt <- dt[, .(from_date=min(from_date), to_date=max(to_date)), by = c(setdiff(names(dt), c("from_date", "to_date")))] 
      dt[, gr := NULL]
      setkey(dt, id_mcare, from_date)      

## (8) Identify contiguous periods ----
        # If contiguous with the PREVIOUS row, then it is marked as contiguous. This is the same as mcaid_elig_timevar
        dt[, prev_to_date := c(NA, to_date[-.N]), by = "id_mcare"] # much faster than shift(..."lag") ... create row with the previous 'to_date'
        dt[, contiguous := 0]
        dt[from_date - prev_to_date == 1, contiguous := 1]
        
        # simple error check
        if(nrow(dt[prev_to_date>=from_date]) != 0)
          stop("STOP!! 
                The previous 'to_date' is greater than or equal to the 'from_date'. 
                This indicates a logical error in the creation of the collapsed segments.
                Please review the code above to identify the error.")
        
        dt[, prev_to_date := NULL] # drop because no longer needed
        
## (9) Add King County indicator & cov_time_day ----
        kc.zips <- fread(kc.zips.url)
        dt[, geo_kc := 0]
        dt[geo_zip %in% unique(as.character(kc.zips$zip)), geo_kc := 1]
        rm(kc.zips)
        
        dt[, cov_time_day := as.integer(to_date - from_date) + 1]
        
## (10) Load to SQL ----
    # Add date stamp to data
        dt[, last_run := Sys.time()]
            
    # Pull YAML from GitHub
        table_config <- yaml::yaml.load(httr::GET(yaml.url))
        
    # Create table ID
        tbl_id <- DBI::Id(schema = table_config$schema, 
                                     table = table_config$table)  
        
    # Ensure columns are in same order in R & SQL
        setcolorder(dt, names(table_config$vars))
        
    # Write table to SQL
        ### Sometimes get a network error if trying to do the whole thing so split into batches
        start <- 1L
        max_rows <- 100000L
        cycles <- ceiling(nrow(dt)/max_rows)
        
        lapply(seq(start, cycles), function(i) {
          start_row <- ifelse(i == 1, 1L, max_rows * (i-1) + 1)
          end_row <- min(nrow(dt), max_rows * i)
          
          message("Loading cycle ", i, " of ", cycles)
          if (i == 1) {
            dbWriteTable(db_claims, 
                         name = tbl_id, 
                         value = as.data.frame(dt[start_row:end_row]),
                         overwrite = T, append = F, 
                         field.types = unlist(table_config$vars))
          } else {
            dbWriteTable(db_claims, 
                         name = tbl_id, 
                         value = as.data.frame(dt[start_row:end_row]),
                         overwrite = F, append = T)
          }
        })
        
        rm(table_config, tbl_id)
        
## (11) Run QA function ----
        source(qa.function.url)

        qa_mcare_elig_timevar_f(conn = db_claims, load_only = F)
        
## The end! ----
        run.time <- Sys.time() - start.time
        print(run.time)