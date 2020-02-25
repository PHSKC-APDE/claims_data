# Susan Hernandez
# adapted code from Eli Kern
# APDE, PHSKC
# 2019-9-25
#

#### Import Medicare data from csv files, --Durable Medical Equipment (DME) ####

#### Step 1: clear memory; Load libraries; functions; and connect to servers #########


# clean memory ----
rm(list=ls())

# load libraries ----
options(max.print = 350, tibble.print_max = 50, scipen = 999)
origin <- "1970-01-01" # Date origin

# load libraries ----
library(pacman)
pacman::p_load(odbc, devtools, configr, glue, DBI, tidyverse, sqldf, methods, tibble, claims, data.table, lubricate)

##naming servers and file paths for our use 
sql_server = "KCITSQLUTPDBH51"
sql_server_odbc_name = "PH_PHClaims51"
#sql_database_conn <- dbConnect(odbc(), sql_server_odbc_name) ##Connect to SQL server
sql_database_name <- "phclaims" ##Name of SQL database where table will be created
sql_schema_name <- "load_raw" ##Name of schema where table will be created
table_name_part <- "dme_claims_k_15_seh"
sql_table <- paste0("mcare_", table_name_part) ##Name of SQL table to be created and loaded to
write_table_name <- DBI::Id(schema = sql_schema_name, name = sql_table)


##Base filepath for Medicare files----
basedrive <- "//phdata01/DROF_DATA/DOH DATA/Medicare/CMS_Drive/CMS_Drive/4749/"
setwd(basedrive)  


##Disconnect and reconnect to database--
#disconnect
db.Disconnect(sql_database_conn)

#Connect to PHClaims 51 Server
db.claims51 <- dbConnect(odbc(), "PH_APDEClaims51")


#### STEP 2: Create table shell ####
create_table_f(conn = db.claims51, 
               #config_file = file.path(git_path, "C:/Users/shernandez/code/claims_data/claims_db/phclaims/load_raw/tables/", "create_load_raw.mcare_dme_claims_k_15.yaml"),
               #config_file = file.path("C:/Users/shernandez/code/claims_data/claims_db/phclaims/load_raw/tables/", "create_load_raw.mcare_dme_claims_k_15.yaml"),
               config_file = file.path("C:/Users/shernandez/code/claims_data/claims_db/phclaims/load_raw/tables/", "create_load_raw.mcare_dme_claims_k_15_test.yaml"),
                                                            overall = T, ind_yr = F, test_mode = T)
## Identify tables and get column names from SQL ----
dmeclaimsk<- "dme_claims_k" # will be referenced below


## Create Master loop outpatient_base_claims_k data ----
for(yr in 15){
  # Import data####
  dmeclaimsk<- fread(paste0("//phdata01/DROF_DATA/DOH DATA/Medicare/CMS_Drive/CMS_Drive/4749/New data/20", yr, "/dme_claims_k_", yr, "/dme_claims_k_",yr,".csv"))

  # Drop the useless rownumber indicator that was made by SAS
  #outbaseclaimsk [, V1:=NULL]
  
  # Change column names to lower case
  setnames(dmeclaimsk, names(dmeclaimsk), tolower(names(dmeclaimsk)))
  
  # Change date variable to SQL friendly format
  #snfbaseclaimsk[, (date.vars) := lapply(.SD, dmy), .SDcols = date.vars]
  #snfbaseclaimsk[, (date.vars) := lapply(.SD, as.character), .SDcols = date.vars]
  
  # Add a variable to indicate the year of the data 
  dmeclaimsk[, data_year:=2000+yr]     
  
  # Set column order to match that in SQL to ensure proper appending
  setcolorder(dmeclaimsk, dmeclaimsk.names)      
  
  # set up parameters for loading data to SQL in chunks 
  max.row.num <- nrow(dmeclaimsk) # number of rows in the original R dataset
  chunk.size <- 10000 # number of rows uploaded per batch
  number.chunks <-  ceiling(max.row.num/chunk.size) # number of chunks to be uploaded
  starting.row <- 1 # the starting row number for each chunk to be uploaded. Will begin with 1 for the first chunk
  ending.row <- chunk.size  # the final row number for each chunk to be uploaded. Will begin with the overall chunk size for the first chunk
  
  # Create loop for appending new data
  for(i in 1:number.chunks){
    # counter so we know it isn't broken
    print(paste0("20",yr, ": Loading chunk ", i, " of ", number.chunks, ": rows ", starting.row, "-", ending.row))  
    
    # subset the data (i.e., create a data 'chunk')
    temp.dt<-dmeclaimsk[starting.row:ending.row,] 
    
    # load the data chunk into SQL
    dbWriteTable(conn = db.claims51, name = write_table_name, value = as.data.frame(temp.dt), row.names = FALSE, header = T, append = T)
    
    # set the starting ane ending rows for the next chunk to be uploaded
    starting.row <- starting.row + chunk.size
    ifelse(ending.row + chunk.size < max.row.num, 
           ending.row <- ending.row + chunk.size,
           ending.row <- max.row.num)
  } # close the for loop that appends the chunks
} # close loop for each year



