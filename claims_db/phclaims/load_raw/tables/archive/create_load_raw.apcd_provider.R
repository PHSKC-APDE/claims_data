# Eli Kern
# APDE, PHSKC
# 2019-6-29

#### Import APCD data from Amazon S3 bucket to SQL Server - load_raw.apcd_provider ####

##### Set up global parameters and call in libraries #####
options(max.print = 350, tibble.print_max = 50, scipen = 999)
origin <- "1970-01-01" # Date origin
library(odbc) # Work with SQL server
library(DBI) # Also work with SQL server
library(tidyverse) # Work with tidy data
library(rlang) # Work with core language features of R and tidyverse
library(usethis) # To easily modify R environment file
library(XML) # Work with XML files
library(methods) # Utility package
library(tibble) # Work with data frames

#### STEP 1: Define functions ####

## FUNCTION 1 - bcp_load_f: call the bulk copy program utility to insert data into existing SQL table
#Note this version does not use a format file and thus includes the -c parameter to specific use of character format
bcp_load_f <- function(server = NULL, table = NULL, read_file = NULL, format_file = NULL, nrow = NULL) {
  
  ##Disconnect and reconnect to database
  dbDisconnect(sql_database_conn)
  sql_database_conn <- dbConnect(odbc(), sql_server_odbc_name)
  
  #Set number of rows to load
  if(!is.null(nrow)){ 
    nrow = paste0("-L ", toString(nrow))
  }
  else {nrow = ""}
  
  #Load data file
  bcp_args <- c(table, "in", read_file, "-t ,", "-C 65001", "-F 2", paste0("-S ", server, " -T"), "-b 100000", nrow, "-c")
  system2(command = "bcp", args = c(bcp_args))
  #print(bcp_args)
}


#### STEP 2: Set universal parameters ####
write_path <- "\\\\phdata01/epe_data/APCD/Data_export/" ##Folder to save Amazon S3 files to
s3_folder <- "\"s3://waae-kc-ext/apcd_export/\"" ##Name of S3 folder containing data and format files

sql_server = "KCITSQLUTPDBH51"
sql_server_odbc_name = "PHClaims51"
sql_database_conn <- dbConnect(odbc(), sql_server_odbc_name) ##Connect to SQL server
sql_database_name <- "phclaims" ##Name of SQL database where table will be created


#### STEP 3: Create SQL table shell ####

##Set parameters specific to tables
read_path <- paste0(write_path, "provider_export/")
sql_schema_name <- "load_raw" ##Name of schema where table will be created
apcd_format_file <- list.files(path = file.path(read_path), pattern = "*format.xml", full.names = T)
long_file_list <- as.list(list.files(path = file.path(read_path), pattern = "*.csv", full.names = T))
short_file_list <- as.list(gsub(".csv", "", list.files(path = file.path(read_path), pattern = "*.csv", full.names = F)))

##Create tables, looping over file list
#Extract table name
table_name_part <- gsub("_1", "", short_file_list[[1]])
sql_table <- paste0("apcd_", table_name_part) ##Name of SQL table to be created and loaded to

#Extract column names and types from XML format file
format_xml <- xmlParse(apcd_format_file)
format_df <- xmlToDataFrame(nodes = xmlChildren(xmlRoot(format_xml)[["data"]]))
names <- xmlToDataFrame(nodes = xmlChildren(xmlRoot(format_xml)[["table-def"]]))
colNames <- (names$'column-name'[!is.na(names$'column-name')])
colnames(format_df) <- colNames
format_vector <- deframe(select(arrange(format_df, as.numeric(as.character(POSITION))), COLUMN_NAME, DATA_TYPE))  

#Drop table if it exists
if(dbExistsTable(sql_database_conn, name = DBI::Id(schema = sql_schema_name, table = sql_table)) == T) {
  dbRemoveTable(sql_database_conn, name = DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", sql_table)))}

#Create table shell using format file from APCD
dbCreateTable(sql_database_conn, name = DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", sql_table)), 
              fields = format_vector, row.names = NULL)


#### STEP 4: Load data to SQL table using BCP ####

## Copy CSV data files to SQL Server, looping over all files
system.time(lapply(seq_along(long_file_list), y=long_file_list, function(y, i) {
  
  #Load data using BCP
  file_name <- y[[i]]
  print(file_name)
  bcp_load_f(server = sql_server, table = paste0(sql_database_name, ".", sql_schema_name, ".", sql_table), read_file = file_name)
}))



