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
    yaml.url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/danny/claims_db/phclaims/stage/tables/load_stage.mcare_xwalk_bene_ssn.yaml"

## (1) Connect to SQL Server & get YAML data ----    
    db.claims51 <- dbConnect(odbc(), "PHClaims51") 
    
    table_config <- yaml::yaml.load(httr::GET(yaml.url))

## (2) Read in ssn (names) data ----
    ssn <- data.table::setDT( dbGetQuery(conn = db.claims51, "SELECT DISTINCT * FROM [load_raw].[mcare_xwalk_bene_ssn]") )
    rads::sql_clean(ssn)
    ssn[, source := as.numeric(as.character(source))]
    ssn[, c("bene_id", "ssn") := lapply(.SD, as.character),,  .SDcols = c("bene_id", "ssn")]
    
    id.count.orig <- as.numeric( dbGetQuery(conn = db.claims51, "SELECT count(DISTINCT(bene_id)) FROM [load_raw].[mcare_xwalk_bene_ssn]") )
    
## (3) Remove duplicate ids ----
    # Order data by bene_id, then year, then ssn ----
      setorder(ssn, bene_id, source, ssn)
    
    # Identify, PER ID, the most recent data with the lowest SSN ----
      # most recent makes sense, because it could be a correction. However, lowest SSN is just a convenience
      # We have no thoughtful way to select among the SSN when an individual has more than one SSN for a given year
      # benefit of sorting and then keeping the first one is simply that we can replicate this data preparation
      ssn[, counter := seq(1, .N), by = "bene_id"]
    
    # Keep just just the first observation per bene_id ---
      ssn.new <- ssn[counter == 1][, c("source", "counter") := NULL]
    
## (4) Basic QA ----
      if(nrow(ssn.new) != id.count.orig){stop("WARNING: The number of unique IDs in SQL's 'load_raw' should equal those in the final dataset (ssn.new)")}
      if(nrow(ssn.new) != length(unique(ssn.new$bene_id))){stop("WARNING: At least one bene_id has been be duplicated in ssn.new")}
          
## (5) Push to SQL ----
      # create last_run timestamp
          ssn.new[, last_run := Sys.time()]
        
      # create table ID for SQL
          tbl_id <- DBI::Id(schema = table_config$schema, 
                            table = table_config$table)  
        
      # ensure column order in R is the same as that in SQL
          setcolorder(ssn.new, names(table_config$vars))          
          
      # delete table if it exists ----
          dbGetQuery(db.claims51, 
                     "if object_id('[stage].[mcare_xwalk_bene_ssn]', 'U') IS NOT NULL drop table [stage].[mcare_xwalk_bene_ssn]")
        
      # write table ----
          dbWriteTable(db.claims51, 
                       tbl_id, 
                       value = as.data.frame(ssn.new),
                       overwrite = F, append = T, 
                       field.types = unlist(table_config$vars))
      
      # confirm that all rows were written to SQL ----
          id.count.final <- as.numeric( dbGetQuery(conn = db.claims51, "SELECT count(DISTINCT(bene_id)) FROM [stage].[mcare_xwalk_bene_ssn]") )
          if(nrow(ssn.new) != id.count.final){stop("WARNING: The number of unique IDs SQL's 'stage' should equal those in the final dataset (ssn.new)")}
          
          
## The end ----