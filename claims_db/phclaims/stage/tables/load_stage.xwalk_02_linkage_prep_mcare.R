## Header ----
  # Author: Danny Colombara
  # Date: March 22, 2019
  # R version: 3.5.1
  # Purpose: Pull together Medicare patient identifiers into one place for eventual merging with Medicaid
  # Notes: Decided not to integrate HIC numbers because of no utility for Medicaid linkage
  #        
  #        Data dictonaries: https://www.ccwdata.org/web/guest/data-dictionaries
  # 
  # Notes: When MBSF file changes in recording of date of birth (dob), we will assume that the most recent is correct
  #        because it would normally remain static unless a person wanted to make a correction
  #
  #        When there is a chance in race, we will treat it like a change in dob, meaning we assuming the most recent is correct  
  #
  #        When there is a change in gender, we will record both (i.e., we'll have a male and a female column, both binary)
  #        this will allow us to have sex== unknown, male, female, mutliple

## Set up R Environment ----
    rm(list=ls())  # clear memory
    pacman::p_load(data.table, odbc, DBI, tidyr) # load packages
    options(scipen = 999) # set high threshhold for R to use scientific notation
    options(warning.length = 8170) # get lengthy warnings, needed for SQL
    
    start.time <- Sys.time()
    
## (1) Connect to SQL Server ----    
    db_claims <- dbConnect(odbc(), "PHClaims51")   
  
## (2) Load data from SQL ----  
    elig <- setDT(odbc::dbGetQuery(db_claims, "SELECT DISTINCT id_mcare, dob, gender_me, gender_female, gender_male FROM stage.mcare_elig_demo"))

    name <- setDT(odbc::dbGetQuery(db_claims, "SELECT DISTINCT bene_id, bene_srnm_name, bene_gvn_name, bene_mdl_name FROM load_raw.mcare_xwalk_edb_user_view"))
    setnames(name, names(name), c("id_mcare", "name_srnm", "name_gvn", "name_mdl"))    
    
    ssn <- setDT(odbc::dbGetQuery(db_claims, "SELECT DISTINCT * FROM load_raw.mcare_xwalk_bene_ssn"))
    setnames(ssn, names(ssn), c("id_mcare", "ssn"))

## (3) Tidy individual data files before merging ----
  # Keep only unique rows of identifiers within a file
      if(nrow(elig) - length(unique(elig$id_mcare)) != 0){
        stop('non-unique id_mcare in elig')
      } # confirm all ids are unique in names data
    
      name <- unique(name)
      if(nrow(name) - length(unique(name$id_mcare)) != 0){
        stop('non-unique id_mcare in name')
      } # confirm all ids are unique in names data
      
      ssn <- unique(ssn)
        ssn[, dup.id := .N, by = "id_mcare"] # identify duplicate ID
        ssn <- ssn[dup.id == 1, ][, c("dup.id"):=NULL] # No way to know which duplicate id pairing is correct, so drop them
        ssn[, dup.ssn := .N, by = "ssn"] # identify duplicate SSN
        ssn <- ssn[dup.ssn == 1, ][, c("dup.ssn"):=NULL] # No way to know which duplicate is correct, so drop them
      if(nrow(ssn) - length(unique(ssn$id_mcare)) >0){
        stop('non-unique id_mcare in ssn')
      } # confirm all id and ssn are unique

## (4) Merge Mcare identifiers together ----
    # for all of WA state, want the most complete dataset possible, regardless of whether missing SSN or any other bit of information
    id.key <- merge(ssn, name, by = "id_mcare", all.x=T, all.y = T)  
    if(nrow(id.key) - length(unique(id.key$id_mcare)) != 0){
      stop('non-unique id_mcare!')
    }
    
    id.key <- merge(id.key, elig, by = "id_mcare",  all.x=T, all.y = T)
    if(nrow(id.key) - length(unique(id.key$id_mcare)) != 0){
      stop('non-unique id_mcare!')
    }
    
## (5) Load Medicare id table to SQL ----
    # create last_run timestamp
    id.key[, last_run := Sys.time()]
    
    # create table ID for SQL
    tbl_id <- DBI::Id(schema = "stage", 
                      table = "xwalk_02_linkage_prep_mcare")  
    
    # column types for SQL
    sql.columns <- c("id_mcare" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", "ssn" = "char(9)", "dob" = "date", "name_srnm" = "varchar(255)", 
                     "name_gvn" = "varchar(255)", "name_mdl" = "varchar(255)", 
                     "gender_me" = "varchar(255)", "gender_female" = "integer", "gender_male" = "integer", "last_run" = "datetime")  
    
    # ensure column order in R is the same as that in SQL
    setcolorder(id.key, names(sql.columns))
    
    # Write table to SQL
    dbWriteTable(db_claims, 
                 tbl_id, 
                 value = as.data.frame(id.key),
                 overwrite = T, append = F, 
                 field.types = sql.columns)
    
    # Confirm that all rows were loaded to sql
    stage.count <- as.numeric(odbc::dbGetQuery(db_claims, 
                                               "SELECT COUNT (*) FROM stage.xwalk_02_linkage_prep_mcare"))
    if(stage.count != nrow(id.key))
      stop("Mismatching row count, error writing or reading data")
    
## (6) Close ODBC ----
    dbDisconnect(db_claims)        
    
## The end ----
    run.time <- Sys.time() - start.time  
    print(run.time)