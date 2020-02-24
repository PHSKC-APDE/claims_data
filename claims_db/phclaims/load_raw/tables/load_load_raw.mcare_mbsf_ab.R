#################################################################################################
# Author: Danny Colombara
# Date: 2019/02/27
# Purpose: Push Medicare MBSF AB data to SQL
# Notes: Using odbc + sqldf packages rather than RODBC because the former are substantially (~20x) faster
#		 Empty SQL tables must be prepped in advance.
#################################################################################################

## Clear memory and load packages ----
  rm(list=ls())
  pacman::p_load(data.table, lubridate, stringr, readxl, readr, iotools, odbc, sqldf)

 ## Prevent scientific notation except for huge numbers ----
	options("scipen"=999) # turn off scientific notation

## Connect to the servers ----
  db.claims51 <- dbConnect(odbc(), "PH_PHClaims51") # using odbc/sqldf
  
## Identify tables and get column names from SQL ----
  mbsfab <- "raw_mcare_mbsf_ab" # will be referenced below
  mbsfab.names <- tolower(names(dbGetQuery(db.claims51, "SELECT top (0) * from PHClaims.dbo.raw_mcare_mbsf_ab")))

## Create Master loop MBSF_AB data ----
  for(yr in 2011:2014){
    # Import data####
      ifelse(yr %in% 2011:2013, 
      	mbsf <- readRDS(paste0("Y:/Medicare/CMS_Drive/CMS_Drive/4749/", yr, "/mbsf_ab_summary/mbsf_ab_summary_res000028029_req004749_", yr, ".Rds")), 
      	mbsf <- fread("Y:/Medicare/CMS_Drive/CMS_Drive/4749/New data/2014/mbsf_ab_summary_14/mbsf_ab_summary.csv")
      )
      if(yr==2014){
        mbsf[, c("age_cat", "age_cat_text", "NewId", "County", "race", "sex", "race_rti") := NULL] # these are dropped because they are not original variables
      }
      
    # Set column order to be sure that appending is correct
      setcolorder(mbsf, mbsfab.names)
      
    # set up parameters for loading data to SQL in chunks 
      max.row.num <- nrow(mbsf) # number of rows in the original R dataset
      chunk.size <- 10000 # number of rows uploaded per batch
      number.chunks <-  ceiling(max.row.num/chunk.size) # number of chunks to be uploaded
      starting.row <- 1 # the starting row number for each chunk to be uploaded. Will begin with 1 for the first chunk
      ending.row <- chunk.size  # the final row number for each chunk to be uploaded. Will begin with the overall chunk size for the first chunk
      
    # Create loop for appending new data
      for(i in 1:number.chunks){
        # counter so we know it isn't broken
          print(paste0(yr, ": Loading chunk ", i, " of ", number.chunks, ": rows ", starting.row, "-", ending.row))  
        
        # subset the data (i.e., create a data 'chunk')
          temp.dt<-mbsf[starting.row:ending.row,] 
        
        # load the data chunk into SQL
          dbWriteTable(conn = db.claims51, name = mbsfab, value = temp.dt, row.names = FALSE, header = FALSE, append = TRUE) # load data to SQL
        
        # set the starting ane ending rows for the next chunk to be uploaded
          starting.row <- starting.row + chunk.size
          ifelse(ending.row + chunk.size < max.row.num, 
                 ending.row <- ending.row + chunk.size,
                 ending.row <- max.row.num)
      } # close the for loop that appends the chunks
  } # close loop for each year

# the end