## Header ----
  # Author: Danny Colombara
  # Date: 2019/03/22
  # Purpose: Push Medicare Cross-Walk data to SQL
  # Notes: 


## Clear memory and load packages ----
  rm(list=ls())
  pacman::p_load(data.table, odbc, DBI)

## Prevent scientific notation except for huge numbers ----
	options("scipen"=999) # turn off scientific notation

## Load Medicare CSVs into memory ----
    edb <- fread("//phdata01/DROF_Data/DOH DATA/Medicare/CMS_Drive/CMS_Drive/4749/New data/edb_user_view/edb_user_view.csv", header = TRUE)
    hic <- fread("//phdata01/DROF_Data/DOH DATA/Medicare/CMS_Drive/CMS_Drive/4749/New data/bene_hic_xwalk/bene_hic_xwalk.csv", header = TRUE)
    ssn <- fread("//phdata01/DROF_Data/DOH DATA/Medicare/CMS_Drive/CMS_Drive/4749/New data/bene_ssn_xwalk/bene_ssn_xwalk.csv", header = TRUE, colClasses=c("character", "character"))
  
## Connect to the servers ----
    sql_server = "KCITSQLUTPDBH51"
    sql_server_odbc_name = "PHClaims51"
    db.claims51 <- dbConnect(odbc(), sql_server_odbc_name) ##Connect to SQL server

## Create tables in SQL Server ----  
  # Create vectors with table column names
    edb.columns <- c("bene_id" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", "bene_srnm_name" = "varchar(255)", "bene_gvn_name" = "varchar(255)", "bene_mdl_name" = "varchar(255)", "crnt_rec_ind" = "varchar(255)")
    hic.columns <- c("bene_id" = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", "hic" = "varchar(255)", "crnt_hic_sw" = "varchar(255)")
    ssn.columns <- c("bene_id"  = "CHAR(15) collate SQL_Latin1_General_Cp1_CS_AS", "ssn" = "varchar(255)")
  
  # Create objects with database specifications
    sql_database_name <- "phclaims" ##Name of SQL database where table will be created
    sql_schema_name <- "load_raw" ##Name of schema where table will be created
    # basic table names
    edb.sql_table <- "mcare_xwalk_edb_user_view"
    hic.sql_table <- "mcare_xwalk_bene_hic"
    ssn.sql_table <- "mcare_xwalk_bene_ssn"
    # complete table paths for creation
    edb.create_table <- DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", edb.sql_table))
    hic.create_table <- DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", hic.sql_table))
    ssn.create_table <- DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", ssn.sql_table))

  # Create the actual tables
    dbRemoveTable(conn = db.claims51, name = edb.create_table) # delete table if it exists
    dbCreateTable(conn = db.claims51, name = edb.create_table, fields = edb.columns, row.names = NULL)
    
    dbRemoveTable(conn = db.claims51, name = hic.create_table) # delete table if it exists
    dbCreateTable(conn = db.claims51, name = hic.create_table, fields = hic.columns, row.names = NULL)
    
    dbRemoveTable(conn = db.claims51, name = ssn.create_table) # delete table if it exists
    dbCreateTable(conn = db.claims51, name = ssn.create_table, fields = ssn.columns, row.names = NULL)
    
## Get column names from SQL ----
    edb.names <- tolower(names(dbGetQuery(db.claims51, "SELECT top (0) * from phclaims.load_raw.mcare_xwalk_edb_user_view")))
    hic.names <- tolower(names(dbGetQuery(db.claims51, "SELECT top (0) * from phclaims.load_raw.mcare_xwalk_bene_hic")))
    ssn.names <- tolower(names(dbGetQuery(db.claims51, "SELECT top (0) * from phclaims.load_raw.mcare_xwalk_bene_ssn")))
    
## Quick clean up of data from CSVs ----
    setnames(edb, names(edb), tolower(names(edb)))
    edb[, c(grep("v1", names(edb), value = TRUE)) := NULL] # drop column v1 if it exists. it is just the row number from SAS
    
    setnames(hic, names(hic), tolower(names(hic)))
    hic[, c(grep("v1", names(hic), value = TRUE)) := NULL] # drop column v1 if it exists. it is just the row number from SAS
    
    setnames(ssn, names(ssn), tolower(names(ssn)))
    ssn[, c(grep("v1", names(ssn), value = TRUE)) := NULL] # drop column v1 if it exists. it is just the row number from SAS
    
    # Fix social security numbes so they are all 9 digits
    #ssn[, ssn := sprintf("%09d", ssn)]
  
## Order the data in R's memory to match the order in SQL database ----
    setcolorder(edb, edb.names)
    setcolorder(hic, hic.names)
    setcolorder(ssn, ssn.names)
    
## Create filepaths for writing to tables ----
    edb.write_table <- DBI::Id(schema = sql_schema_name, name = edb.sql_table)
    hic.write_table <- DBI::Id(schema = sql_schema_name, name = hic.sql_table)
    ssn.write_table <- DBI::Id(schema = sql_schema_name, name = ssn.sql_table)

## Loop for edb
      # set up parameters for loading data to SQL in chunks 
        max.row.num <- nrow(edb) # number of rows in the original R dataset
        chunk.size <- 10000 # number of rows uploaded per batch
        number.chunks <-  ceiling(max.row.num/chunk.size) # number of chunks to be uploaded
        starting.row <- 1 # the starting row number for each chunk to be uploaded. Will begin with 1 for the first chunk
        ending.row <- chunk.size  # the final row number for each chunk to be uploaded. Will begin with the overall chunk size for the first chunk
        
      for(i in 1:number.chunks){  
      
        # counter so we know it isn't broken
          print(paste0("edb: Loading chunk ", i, " of ", number.chunks, ": rows ", starting.row, "-", ending.row)) 
        
        # subset the data (i.e., create a data 'chunk')
          temp.dt<-edb[starting.row:ending.row,] 
        
        # load the data chunk into SQL
          dbWriteTable(conn = db.claims51, name = edb.write_table, value = as.data.frame(temp.dt), row.names = FALSE, header = T, append = T)

        # set the starting ane ending rows for the next chunk to be uploaded
          starting.row <- starting.row + chunk.size
          ifelse(ending.row + chunk.size < max.row.num, 
                 ending.row <- ending.row + chunk.size,
                 ending.row <- max.row.num)
      } # close the for loop that appends the chunks

## Loop for hic
      # set up parameters for loading data to SQL in chunks 
        max.row.num <- nrow(hic) # number of rows in the original R dataset
        chunk.size <- 10000 # number of rows uploaded per batch
        number.chunks <-  ceiling(max.row.num/chunk.size) # number of chunks to be uploaded
        starting.row <- 1 # the starting row number for each chunk to be uploaded. Will begin with 1 for the first chunk
        ending.row <- chunk.size  # the final row number for each chunk to be uploaded. Will begin with the overall chunk size for the first chunk
        
      for(i in 1:number.chunks){  
      
        # counter so we know it isn't broken
          print(paste0("hic: Loading chunk ", i, " of ", number.chunks, ": rows ", starting.row, "-", ending.row)) 
        
        # subset the data (i.e., create a data 'chunk')
          temp.dt<-hic[starting.row:ending.row,] 
        
        # load the data chunk into SQL
          dbWriteTable(conn = db.claims51, name = hic.write_table, value = as.data.frame(temp.dt), row.names = FALSE, header = T, append = T)

        # set the starting ane ending rows for the next chunk to be uploaded
          starting.row <- starting.row + chunk.size
          ifelse(ending.row + chunk.size < max.row.num, 
                 ending.row <- ending.row + chunk.size,
                 ending.row <- max.row.num)
      } # close the for loop that appends the chunks

## Loop for ssn
      # set up parameters for loading data to SQL in chunks 
        max.row.num <- nrow(ssn) # number of rows in the original R dataset
        chunk.size <- 10000 # number of rows uploaded per batch
        number.chunks <-  ceiling(max.row.num/chunk.size) # number of chunks to be uploaded
        starting.row <- 1 # the starting row number for each chunk to be uploaded. Will begin with 1 for the first chunk
        ending.row <- chunk.size  # the final row number for each chunk to be uploaded. Will begin with the overall chunk size for the first chunk
        
      for(i in 1:number.chunks){  
      
        # counter so we know it isn't broken
          print(paste0("ssn: Loading chunk ", i, " of ", number.chunks, ": rows ", starting.row, "-", ending.row)) 
        
        # subset the data (i.e., create a data 'chunk')
          temp.dt<-ssn[starting.row:ending.row,] 
        
        # load the data chunk into SQL
          dbWriteTable(conn = db.claims51, name = ssn.write_table, value = as.data.frame(temp.dt), row.names = FALSE, header = T, append = T)

        # set the starting ane ending rows for the next chunk to be uploaded
          starting.row <- starting.row + chunk.size
          ifelse(ending.row + chunk.size < max.row.num, 
                 ending.row <- ending.row + chunk.size,
                 ending.row <- max.row.num)
      } # close the for loop that appends the chunks

# the end