## Header ----
  # Author: Danny Colombara
  # Date: 2019/03/22
  # Purpose: Push Medicare Cross-Walk data to SQL
  # Notes: 


## Clear memory and load packages ----
    rm(list=ls())
    pacman::p_load(data.table, odbc, DBI, tidyr)

## Prevent scientific notation except for huge numbers ----
  	options("scipen"=999) # turn off scientific notation

## Identify year for 'source' of raw data ----
    this.year <- 2017
  
## Load Medicare CSVs into memory ----
    edb <- fread(paste0("//phdata01/DROF_Data/DOH DATA/Medicare/CMS_Drive/CMS_Drive/4749/New data/", this.year, "/ebd_user_view_", substr(this.year, 3, 4),"/ebd_user_view.csv"), header = TRUE)
    hic <- fread(paste0("//phdata01/DROF_Data/DOH DATA/Medicare/CMS_Drive/CMS_Drive/4749/New data/", this.year, "/bene_hic_xwalk_", substr(this.year, 3, 4),"/bene_hic_xwalk.csv"), header = TRUE)
    ssn <- fread(paste0("//phdata01/DROF_Data/DOH DATA/Medicare/CMS_Drive/CMS_Drive/4749/New data/", this.year, "/bene_ssn_xwalk_", substr(this.year, 3, 4),"/bene_ssn_xwalk.csv"), header = TRUE, colClasses=c("character", "character"))
    # edb <- fread("//phdata01/DROF_Data/DOH DATA/Medicare/CMS_Drive/CMS_Drive/4749/New data/edb_user_view/edb_user_view.csv", header = TRUE) # 2016
    # hic <- fread("//phdata01/DROF_Data/DOH DATA/Medicare/CMS_Drive/CMS_Drive/4749/New data/bene_hic_xwalk/bene_hic_xwalk.csv", header = TRUE) # 2016
    # ssn <- fread("//phdata01/DROF_Data/DOH DATA/Medicare/CMS_Drive/CMS_Drive/4749/New data/bene_ssn_xwalk/bene_ssn_xwalk.csv", header = TRUE, colClasses=c("character", "character")) # 2016
  
## Connect to the servers ----
    sql_server = "KCITSQLUTPDBH51"
    sql_server_odbc_name = "PHClaims51"
    db.claims51 <- dbConnect(odbc(), sql_server_odbc_name) ##Connect to SQL server

## Create tables in SQL Server (only needed for the first year) ----  
  # # Create vectors with table column names
  #   edb.columns <- c("bene_id" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", "bene_srnm_name" = "varchar(255)", "bene_gvn_name" = "varchar(255)", "bene_mdl_name" = "varchar(255)", "crnt_rec_ind" = "varchar(255)")
  #   hic.columns <- c("bene_id" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", "hic" = "varchar(255)", "crnt_hic_sw" = "varchar(255)")
  #   ssn.columns <- c("bene_id"  = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", "ssn" = "varchar(255)")
  # 
  # # Create objects with database specifications
  #   sql_database_name <- "phclaims" ##Name of SQL database where table will be created
  #   sql_schema_name <- "load_raw" ##Name of schema where table will be created
  #   # basic table names
  #   edb.sql_table <- "mcare_xwalk_edb_user_view"
  #   hic.sql_table <- "mcare_xwalk_bene_hic"
  #   ssn.sql_table <- "mcare_xwalk_bene_ssn"
  #   # complete table paths for creation
  #   edb.create_table <- DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", edb.sql_table))
  #   hic.create_table <- DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", hic.sql_table))
  #   ssn.create_table <- DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", ssn.sql_table))
  # 
  # # Create the actual tables
  #   dbRemoveTable(conn = db.claims51, name = edb.create_table) # delete table if it exists
  #   dbCreateTable(conn = db.claims51, name = edb.create_table, fields = edb.columns, row.names = NULL)
  #   
  #   dbRemoveTable(conn = db.claims51, name = hic.create_table) # delete table if it exists
  #   dbCreateTable(conn = db.claims51, name = hic.create_table, fields = hic.columns, row.names = NULL)
  #   
  #   dbRemoveTable(conn = db.claims51, name = ssn.create_table) # delete table if it exists
  #   dbCreateTable(conn = db.claims51, name = ssn.create_table, fields = ssn.columns, row.names = NULL)
    
## Get column names from SQL ----
    edb.names <- tolower(names(dbGetQuery(db.claims51, "SELECT top (0) * from phclaims.load_raw.mcare_xwalk_edb_user_view")))
    hic.names <- tolower(names(dbGetQuery(db.claims51, "SELECT top (0) * from phclaims.load_raw.mcare_xwalk_bene_hic")))
    ssn.names <- tolower(names(dbGetQuery(db.claims51, "SELECT top (0) * from phclaims.load_raw.mcare_xwalk_bene_ssn")))
    
## Quick clean up of data from CSVs ----
    setnames(edb, names(edb), tolower(names(edb)))
    edb[, source := as.character(this.year)]
    suppressWarnings(edb[, c(grep("v1", names(edb), value = TRUE)) := NULL]) # drop column v1 if it exists. it is just the row number from SAS
    
    setnames(hic, names(hic), tolower(names(hic)))
    hic[, source := as.character(this.year)]
    suppressWarnings(hic[, c(grep("v1", names(hic), value = TRUE)) := NULL]) # drop column v1 if it exists. it is just the row number from SAS
    
    setnames(ssn, names(ssn), tolower(names(ssn)))
    ssn[, source := as.character(this.year)]
    suppressWarnings(ssn[, c(grep("v1", names(ssn), value = TRUE)) := NULL]) # drop column v1 if it exists. it is just the row number from SAS
    
    # Fix social security numbes so they are all 9 digits
    #ssn[, ssn := sprintf("%09d", ssn)]
  
## Order the data in R's memory to match the order in SQL database ----
    setcolorder(edb, edb.names)
    setcolorder(hic, hic.names)
    setcolorder(ssn, ssn.names)
    
## Load edb (member name) data & basic QA----
    # drop the current year's data if it exists to prevent duplication
      dbGetQuery(conn = db.claims51, glue::glue("DELETE FROM [PHClaims].[load_raw].[mcare_xwalk_edb_user_view] where source = {this.year}", .con = db.claims51))
    
    # count existing rows in SQL
      count.pre <- as.numeric(dbGetQuery(conn = db.claims51, "SELECT COUNT (*) FROM [PHClaims].[load_raw].[mcare_xwalk_edb_user_view]"))
      
    # write new data 
      tbl_id <- DBI::Id(schema = "load_raw", table = "mcare_xwalk_edb_user_view")
      dbWriteTable(db.claims51, 
                   tbl_id, 
                   value = as.data.frame(edb),
                   overwrite = F, append = T)
      
    # count final rows in SQL
      count.post <- as.numeric(dbGetQuery(conn = db.claims51, "SELECT COUNT (*) FROM [PHClaims].[load_raw].[mcare_xwalk_edb_user_view]"))  
      
    # QA
      if(count.post != count.pre + nrow(edb)){
        problem.edb <- "EDB: The final SQL row count should be equal to the original row count plus the new year's data."}else{problem.edb <- " "}
      
      
## Load hic (altername id) data ----
    # drop the current year's data if it exists to prevent duplication
      dbGetQuery(conn = db.claims51, glue::glue("DELETE FROM [PHClaims].[load_raw].[mcare_xwalk_bene_hic] where source = {this.year}", .con = db.claims51))
      
    # count existing rows in SQL
      count.pre <- as.numeric(dbGetQuery(conn = db.claims51, "SELECT COUNT (*) FROM [PHClaims].[load_raw].[mcare_xwalk_bene_hic]"))
      
    # write new data 
      tbl_id <- DBI::Id(schema = "load_raw", table = "mcare_xwalk_bene_hic")
      dbWriteTable(db.claims51, 
                   tbl_id, 
                   value = as.data.frame(hic),
                   overwrite = F, append = T)
      
    # count final rows in SQL
      count.post <- as.numeric(dbGetQuery(conn = db.claims51, "SELECT COUNT (*) FROM [PHClaims].[load_raw].[mcare_xwalk_bene_hic]"))  
      
    # QA
      if(count.post != count.pre + nrow(hic)){
        problem.hic <- "HIC: The final SQL row count should be equal to the original row count plus the new year's data."}else{problem.hic <-" "}

      
## Load ssn data ----
    # drop the current year's data if it exists to prevent duplication
      dbGetQuery(conn = db.claims51, glue::glue("DELETE FROM [PHClaims].[load_raw].[mcare_xwalk_bene_ssn] where source = {this.year}", .con = db.claims51))
      
    # count existing rows in SQL
      count.pre <- as.numeric(dbGetQuery(conn = db.claims51, "SELECT COUNT (*) FROM [PHClaims].[load_raw].[mcare_xwalk_bene_ssn]"))
      
    # write new data 
      tbl_id <- DBI::Id(schema = "load_raw", table = "mcare_xwalk_bene_ssn")
      dbWriteTable(db.claims51, 
                   tbl_id, 
                   value = as.data.frame(ssn),
                   overwrite = F, append = T)
      
    # count final rows in SQL
      count.post <- as.numeric(dbGetQuery(conn = db.claims51, "SELECT COUNT (*) FROM [PHClaims].[load_raw].[mcare_xwalk_bene_ssn]"))  
      
    # QA
      if(count.post != count.pre + nrow(ssn)){
        problem.ssn <- "SSN: The final SQL row count should be equal to the original row count plus the new year's data."}else{problem.ssn <- " "}
      
      
# create summary of errors ---- 
      problems <- glue::glue(
        problem.edb, "\n",
        problem.hic, "\n",
        problem.ssn)
      
      if(problems >1){
        message(glue::glue("WARNING ... MBSF ABCD FAILED AT LEAST ONE QA TEST", "\n",
                           "Summary of problems in loading xwalk files: ", "\n", 
                           problems))
      }else{message("Loading raw xwalk files passed all basic QA tests")}
      
# the end ----