## Header ----
# Author: Danny Colombara
# Date: 2020/01/22
# Purpose: Created cleanded versions of EDB (names), SSN, and HIC (alternate ID) tables
# Notes: 


## Set up environment----
    rm(list=ls())
    pacman::p_load(data.table, odbc, lubridate, glue, httr, rads)
    start.time <- Sys.time()
    options("scipen"=999) # turn off scientific notation  
    options(warning.length = 8170) # get lengthy warnings, needed for SQL
    setwd("C:/temp/")
    yaml.url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.mcare_xwalk_edb_user_view.yaml"
    
## (1) Connect to SQL Server & get YAML data ----    
    db.claims51 <- dbConnect(odbc(), "PHClaims51") 
    
    table_config <- yaml::yaml.load(RCurl::getURL(yaml.url))

## (2) Read in EDB (names) data ----
    edb <- data.table::setDT( dbGetQuery(conn = db.claims51, "SELECT DISTINCT * FROM [load_raw].[mcare_xwalk_edb_user_view]") )
    rads::sql_clean(edb)
    edb[, source := as.numeric(as.character(source))]
    edb[, c("bene_id", "bene_srnm_name", "bene_gvn_name", "bene_mdl_name") := lapply(.SD, as.character),,  .SDcols = c("bene_id", "bene_srnm_name", "bene_gvn_name", "bene_mdl_name")]
    
    id.count.orig <- as.numeric( dbGetQuery(conn = db.claims51, "SELECT count(DISTINCT(bene_id)) FROM [load_raw].[mcare_xwalk_edb_user_view]") )
    
## (3) Sort out duplicate ids ----
    # identify duplicate ids ----
      edb[, id.dup := .N, by = .(bene_id)] 
    
    # [edb.nodup] set aside non-duplicated rows for merging onto the final dataset ----
      edb.nodup <- edb[id.dup == 1][, id.dup := NULL] 
    
    # set aside duplicate ids for further processing ----
      edb.dups <- edb[id.dup !=1][, id.dup := NULL] 
      setorder(edb.dups, bene_id, -source) # sorting is critical to ensure most recent year is first for each bene_id
    
    # [edb.exact.dup] identify exact duplicates, for which we will keep the most recent year  ----
      edb.dups[, exact.dup := .N, by = c(setdiff(names(edb.dups), c("source", "crnt_rec_ind")))] # crnt_rec_ind only changes Y >> N, never other way around. So the most current is always what is of interest
      
      edb.exact.dup <- edb.dups[exact.dup != 1] # split off those with exact duplicates
      edb.exact.dup[, exact.dup := seq(1, .N), by = "bene_id"] # count number of repeats of id, with most recent == 1
      edb.exact.dup <- edb.exact.dup[exact.dup == 1][, c("exact.dup") := NULL] # keep only most recent year from exact duplicates
      
    # [edb.other.dups] process non-exact duplicates ----  
      edb.other.dups <- edb.dups[exact.dup == 1][, exact.dup := NULL]
      
        # Keep most recent last name  ---- 
          recent.srnm <- edb.other.dups[source == max(source), .(bene_srnm_name), by = .(bene_id)]
          edb.other.dups <- merge(edb.other.dups[, bene_srnm_name := NULL], recent.srnm[], by = "bene_id", all = T)
          
        # Keep most recent first name  ---- 
          recent.gvn <- edb.other.dups[source == max(source), .(bene_gvn_name), by = .(bene_id)]
          edb.other.dups <- merge(edb.other.dups[, bene_gvn_name := NULL], recent.gvn[], by = "bene_id", all = T)

        # Fill missing middle initial with the previous (if available) ----
          setorder(edb.other.dups, bene_id, source) # sort from oldest to newest so newer can inherit middle initial from previous
          edb.other.dups[, bene_mdl_name  := bene_mdl_name[1], by= .( bene_id , cumsum(!is.na(bene_mdl_name)) ) ] # fill forward / downward
          setorder(edb.other.dups, bene_id, -source) # resort with most recent first, by bene_id
          
        # Keep most recent middle initial when it is not missing ---- 
          recent.mdl <- edb.other.dups[source == max(source), .(bene_mdl_name), by = .(bene_id)]
          edb.other.dups <- merge(edb.other.dups[, bene_mdl_name := NULL], recent.mdl[], by = "bene_id", all = T)
          
        # keep most recent year ----
          edb.other.dups <- edb.other.dups[edb.other.dups[, .I[source == max(source)], by = bene_id]$V1]
          
## (4) Combine the three sub-tables ----
      edb.new <- rbindlist(list(edb.nodup, edb.exact.dup, edb.other.dups), use.names = T, fill = F)
      
## (5) Basic QA ----
      if(nrow(edb.new) != id.count.orig){stop("WARNING: The number of unique IDs in SQL's 'load_raw' should equal those in the final dataset (edb.new)")}
      if(nrow(edb.new) != length(unique(edb.new$bene_id))){stop("WARNING: At least one bene_id has been be duplicated in edb.new")}
          
## (6) Push to SQL ----
      # Drop source because no longer needed
          edb.new[, source := NULL]
          
      # create last_run timestamp
          edb.new[, last_run := Sys.time()]
        
      # create table ID for SQL
          tbl_id <- DBI::Id(schema = table_config$schema, 
                            table = table_config$table)  
        
      # ensure column order in R is the same as that in SQL
          setcolorder(edb.new, names(table_config$vars))          
          
      # delete table if it exists ----
          dbGetQuery(db.claims51, 
                     "if object_id('[stage].[mcare_xwalk_edb_user_view]', 'U') IS NOT NULL drop table [stage].[mcare_xwalk_edb_user_view]")
        
      # write table ----
          dbWriteTable(db.claims51, 
                       tbl_id, 
                       value = as.data.frame(edb.new),
                       overwrite = F, append = T, 
                       field.types = unlist(table_config$vars))
      
      # confirm that all rows were written to SQL ----
          id.count.final <- as.numeric( dbGetQuery(conn = db.claims51, "SELECT count(DISTINCT(bene_id)) FROM [stage].[mcare_xwalk_edb_user_view]") )
          if(nrow(edb.new) != id.count.final){stop("WARNING: The number of unique IDs SQL's 'stage' should equal those in the final dataset (edb.new)")}
          
          
## The end ----