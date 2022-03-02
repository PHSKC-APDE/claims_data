## Header ----
# Author: Danny Colombara
# Date: 2020/01/22
# Purpose: Created cleaned versions of EDB (names), SSN, and HIC (alternate ID) tables
# Notes: 


## Set up environment----
    rm(list=ls())
    pacman::p_load(data.table, odbc, lubridate, glue, httr, rads)
    start.time <- Sys.time()
    options("scipen"=999) # turn off scientific notation  
    options(warning.length = 8170) # get lengthy warnings, needed for SQL
    setwd("C:/temp/")
    yaml.url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_xwalk_edb_user_view.yaml"
    
## (1) Connect to SQL Server & get YAML data ----    
    db.claims51 <- dbConnect(odbc(), "PHClaims51") 
    
    table_config <- yaml::yaml.load(httr::GET(yaml.url))

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
      edb.nodup <- edb[id.dup == 1][, c("id.dup", "source") := NULL] 
    
    # set aside duplicate ids for further processing ----
      edb.dups <- edb[id.dup !=1] 
      setorder(edb.dups, bene_id, -source) # sorting is critical to ensure most recent year is first for each bene_id
    
    # [edb.exact.dup] identify exact duplicates, for which we will keep the most recent year  ----
      edb.dups[, exact.dup := .N, by = c(setdiff(names(edb.dups), c("crnt_rec_ind", "source")))] # crnt_rec_ind only changes Y >> N, never other way around. So the most current is always what is of interest
      edb.exact.dup <- copy(edb.dups[id.dup == exact.dup]) # keep when the # of times an ID occurs is the same as the # of perfect matches
      edb.exact.dup  <- edb.exact.dup[edb.exact.dup[, .I[which.max(source)], by = 'bene_id'][,V1], .(bene_srnm_name, bene_gvn_name, bene_mdl_name, crnt_rec_ind, bene_id)] # keep the row for the max year

    # [edb.other.dups] process non-exact duplicates ----  
      edb.other.dups <- copy(edb.dups[id.dup != exact.dup])[, c("id.dup", "exact.dup") := NULL]

        # Fill missing middle initial with the previous (if available) ----
          setorder(edb.other.dups, bene_id, source) # sort from oldest to newest so newer can inherit middle initial from previous year
          edb.other.dups[, bene_mdl_name  := bene_mdl_name[1], by= .( bene_id , cumsum(!is.na(bene_mdl_name)) ) ] # fill forward / downward
          setorder(edb.other.dups, bene_id, -source) # resort with most recent first, by bene_id
          
        # Keep the most recent last name, first name, and middle initial, by bene_id ----
          edb.other.dups  <- edb.other.dups[edb.other.dups[, .I[which.max(source)], by = 'bene_id'][,V1], .(bene_srnm_name, bene_gvn_name, bene_mdl_name, crnt_rec_ind, bene_id)] # keep the row for the max year
          
## (4) Combine the three sub-tables ----
      edb.new <- rbindlist(list(edb.nodup, edb.exact.dup, edb.other.dups), use.names = T, fill = F)
      
## (5) Basic QA ----
      if(nrow(edb.new) != id.count.orig){stop("WARNING: The number of unique IDs in SQL's 'load_raw' should equal those in the final dataset (edb.new)")}
      if(nrow(edb.new) != length(unique(edb.new$bene_id))){stop("WARNING: At least one bene_id has been be duplicated in edb.new")}
          
## (6) Push to SQL ----
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