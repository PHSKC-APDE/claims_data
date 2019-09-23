## Header ----
  # Author: Danny Colombara
  # Date: September 4, 2019
  # R version: 3.5.3
  # Puprpose: Establish Medicare - PHA - Medicaid three-way linkage to identify duals
  #
  # Notes: Need to combined the previous linkages (mcare-mcaid, pha-mcare, pha-mcaid) to create a comprehensive
  #        linkage file. It is possible that individual linkages were misseed. 
  #        For example, person X could have been linked in mcare-mcaid and pha-mcaid, but not in pha-mcare
  #        for this reason we want to create a three column linkage / identify table.

## Set up R environment #####
    rm(list = ls())
    options(max.print = 350, tibble.print_max = 30, scipen = 999)
    
    library(odbc) # Used to connect to SQL server
    library(lubridate) # Used to manipulate dates
    library(tidyverse) # Used to manipulate data
    library(data.table) # used to manipulate data
    
    start.time <- Sys.time()
    
## Connect to the servers #####
    db_claims <- dbConnect(odbc(), "PHClaims51")
    db.apde51 <- dbConnect(odbc(), "PH_APDEStore51")

## Load in data ----
    mcare.mcaid <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_mcare, id_mcaid 
                                          FROM stage.xwalk_03_linkage_mcaid_mcare"))
    
    mcare.pha <- setDT(odbc::dbGetQuery(db_claims, "SELECT id_mcare, pid 
                                        FROM stage.xwalk_04_linkage_mcare_pha"))

    mcaid.pha <- setDT(odbc::dbGetQuery(db.apde51, "SELECT DISTINCT id_mcaid, pid 
                                FROM stage.mcaid_pha 
                                WHERE id_mcaid IS NOT NULL AND pid IS NOT NULL"))
    
    pha.only <- setDT(odbc::dbGetQuery(db.apde51, "SELECT DISTINCT pid 
                                FROM stage.mcaid_pha 
                                WHERE pid IS NOT NULL"))
    
    pid.dups <- setDT(odbc::dbGetQuery(db.apde51, "SELECT DISTINCT pid, alt_pid
                                FROM tmp.pid_dups
                                WHERE pid IS NOT NULL"))
    
## Clean the imported data (if needed) ----
    # check for duplicate IDs in mcare.mcaid
        if(length(unique(mcare.mcaid$id_mcare)) != nrow(mcare.mcaid)){
          stop("id_mcare in mcare.mcaid is not unique")
        }
        if(length(unique(mcare.mcaid$id_mcaid)) != nrow(mcare.mcaid)){
          stop("id_mcaid in mcare.mcaid is not unique")
        }
    
    # check for duplicate IDS in mcare.pha
        # fix known duplicates
        mcare.pha <- merge(mcare.pha, pid.dups, by.x = "pid", by.y = "alt_pid", all.x = TRUE, all.y = FALSE)
        mcare.pha[!is.na(pid.y), pid := pid.y][, pid.y := NULL] # ascribe the main pid when duplicated
        mcare.pha <- unique(mcare.pha)
    
        if(length(unique(mcare.pha$id_mcare)) != nrow(mcare.pha)){
          stop("id_mcare in mcare.pha is not unique")
        }
        if(length(unique(mcare.pha$pid)) != nrow(mcare.pha)){
          stop("pid in mcare.pha is not unique")
        }
    
    # check for duplicate IDs in mcaid.pha 
        # fix known duplicates
        mcaid.pha <- merge(mcaid.pha, pid.dups, by.x = "pid", by.y = "alt_pid", all.x = TRUE, all.y = FALSE)
        mcaid.pha[!is.na(pid.y), pid := pid.y][, pid.y := NULL] # ascribe the main pid when duplicated
        mcaid.pha <- unique(mcaid.pha)
        
        setorder(mcaid.pha, -id_mcaid) # sort with largest Mcaid ID at the top
        mcaid.pha[, dup.pid := 1:.N, by = pid] # Count duplicate pid (meaning has > 1 id_mcaid per pid)
        mcaid.pha <- mcaid.pha[dup.pid == 1] # only keep the first occure of pid, which is the one with the most recent Medicaid data
        mcaid.pha[, dup.pid := NULL] # drop duplicate indicator
      
        setorder(mcaid.pha, -pid) # sort with largest pid at the top
        mcaid.pha[, dup.mcaid := 1:.N, by = id_mcaid] # Count duplicate id_mcaid (meaning has > 1 pid per id_mcaid)
        mcaid.pha <- mcaid.pha[dup.mcaid == 1] # only keep the first occure of a given id_mcaid, which is the one with the most recent pid
        mcaid.pha[, dup.mcaid := NULL] # drop duplicate indicator
        
        if(length(unique(mcaid.pha$id_mcaid)) != nrow(mcaid.pha)){
          stop("id_mcaid in mcaid.pha is not unique")
        }
        if(length(unique(mcaid.pha$pid)) != nrow(mcaid.pha)){
          stop("pid in mcaid.pha is not unique")
        }
    
## Check that pid has the same general range in both mcare.pha and mcaid.pha ----
    # concerned because there was also a pid2 variable in stage.mcaid_pha & stage.mcaid_pha_demo
    summary(mcare.pha$pid)
    summary(mcaid.pha$pid)
    
## Merge Mcaid IDs onto MCARE-PHA ----
    mcare.pha.mcaid <- merge(mcare.pha, mcare.mcaid, by = "id_mcare", all.x = TRUE, all.y = FALSE) # add Mcaid IDs to pha-mcare
    
## Merge Mcare IDs onto MCAID-PHA ----
    mcaid.pha.mcare <- merge(mcaid.pha, mcare.mcaid, by = "id_mcaid", all.x = TRUE, all.y = FALSE) # add Mcare IDs to pha-mcaid
    
## Append MCARE-PHA-MCAID & MCAID-PHA-MCARE ----   
      xwalk <- rbind(mcare.pha.mcaid, mcaid.pha.mcare)
    
## Clean combined data ----
      setcolorder(xwalk, c('pid', "id_mcare", "id_mcaid"))
    
    # deduplicate when entire rows are repeated
      xwalk <- unique(xwalk) 
      
    # Want to collapse the data to one row when we have the following situation (example does not use real ids):
      # pid         id_mcare          id_mcaid
      # 12345       GGGGQQPHhPoFQPH	
      # 12345                         987654321WA
      xwalk[, dup.pid := .N, by = pid] # identify duplicate PID
      dup.pid.mcare.mcaid <- merge(xwalk[dup.pid == 2 & !is.na(id_mcare) & is.na(id_mcaid)][, id_mcaid := NULL], # potential mcare data to merge
                                   xwalk[dup.pid == 2 & !is.na(id_mcaid) & is.na(id_mcare)][, id_mcare := NULL], # potential mcaid data to merge
                                   by = "pid", all = FALSE)
      xwalk <- xwalk[!pid %in% dup.pid.mcare.mcaid$pid] # drop the pid that were merged above
      xwalk <- rbind(xwalk, dup.pid.mcare.mcaid, fill = TRUE) # append data that was merged above
      xwalk <- xwalk[, .(pid, id_mcare, id_mcaid)]
 
    # Fix when id_mcare & id_mciad are duplicated ----
      # know of at least one case where twins in pid matched with same id_mcare/id_mcaid pair, will drop one at random
      xwalk[, dup := 1:.N, by = c("id_mcare", "id_mcaid")] # this can identify duplicates or twins. Manually review and fix code
      xwalk <- xwalk[dup ==1,]
      if(nrow(xwalk[dup >1 & !(is.na(id_mcare) & is.na(id_mcaid)), ]) > 0){
        View(setorder(xwalk[dup >1 & !(is.na(id_mcare) & is.na(id_mcaid))], pid))
      }
      xwalk[, dup := NULL]
      
    # Fix when pid is duplicated ---- 
      xwalk[, dup := 1:.N, by = pid]
      xwalk <- xwalk[dup ==1,]
      if(nrow(xwalk[dup>1]) >0) {
        View(xwalk[dup > 1])
      }
      xwalk[, dup := NULL]
      
    # Fix when id_mcare is duplicated ---- 
      xwalk[, dup := .N, by = id_mcare]
      if(nrow(xwalk[dup>1 & !is.na(id_mcare)]) > 0) {
        View(xwalk[dup > 1])
      }
      xwalk[, dup := NULL]
      
    # Fix when id_mcaid is duplicate ----
      xwalk[, dup := .N, by = id_mcaid]
      if(nrow(xwalk[dup>1 & !is.na(id_mcaid)]) > 0) {
        View(xwalk[dup > 1])
      }
      xwalk[, dup := NULL]
      
    # keep only columns needed for export
      xwalk <- xwalk[, .(pid, id_mcare, id_mcaid)]
      
## Merge on complete list of PID (except duplicates) in order to have a denominator ----      
      xwalk <- merge(pha.only[!pid %in% pid.dups$alt_pid], xwalk, by = "pid", all = TRUE)    
        
## Load xwalk table to SQL ----
      # create last_run timestamp
      xwalk[, last_run := Sys.time()]
      
      # create table ID for SQL
      tbl_id <- DBI::Id(schema = "stage", 
                        table = "xwalk_05_linkage_mcaid_mcare_pha")  
      
      # Identify the column types to be created in SQL
      sql.columns <- c("pid" = "integer", "id_mcare" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", "id_mcaid" = "char(11)", "last_run" = "datetime")  
      
      # ensure column order in R is the same as that in SQL
      setcolorder(xwalk, names(sql.columns))
      
      # Write table to SQL
      dbWriteTable(db_claims, 
                   tbl_id, 
                   value = as.data.frame(xwalk),
                   overwrite = T, append = F, 
                   field.types = sql.columns)
      
      # Confirm that all rows were loaded to sql
      stage.count <- as.numeric(odbc::dbGetQuery(db_claims, 
                                                 "SELECT COUNT (*) FROM stage.xwalk_05_linkage_mcaid_mcare_pha"))
      if(stage.count != nrow(xwalk))
        stop("Mismatching row count, error writing or reading data")      
      
      dbDisconnect(db_claims)  
      
## Summary calculations ----      
      results <-  xwalk[, .(pid = length(unique(pid)), 
                            mcaid = length(unique(id_mcaid)), 
                            mcare = length(unique(id_mcare)),
                            dual = nrow(xwalk[!is.na(id_mcaid) & !is.na(id_mcare), ]) )]
      percentages <- results[, lapply(.SD, function(x){paste0(round(100*(x/pid), 1), "%")}), , .SDcols=c("mcaid", "mcare", "dual")]
      setnames(percentages, names(percentages), paste0(names(percentages), ".per"))
      results <- results[, lapply(.SD, function(x){prettyNum(x,big.mark=",", preserve.width="none")} )]
      results <- cbind(results, percentages)
      rm(percentages)
      setcolorder(results, c("pid", "mcaid", "mcaid.per", "mcare", "mcare.per", "dual", "dual.per"))
      
## The end ----      