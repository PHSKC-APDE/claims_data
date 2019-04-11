# Eli Kern
# APDE, PHSKC
# 2019-1-28

#### Import APCD data from Amazon S3 bucket to SQL Server ####


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
  
  #Test SQL ODBC connection and if disconnected, reconnect
  ##if(dbIsValid(sql_database_conn) == FALSE) {
  ##  sql_database_conn <- dbConnect(odbc(), sql_server_odbc_name)
  ##}
  
  #Set number of rows to load
  if(!is.null(nrow)){ 
    nrow = paste0("-L ", toString(nrow))
  }
  else {nrow = ""}
  
  #Load data file
  bcp_args <- c(table, "in", read_file, "-t ,", "-C 65001", "-F 2",
                 paste0("-S ", server, " -T"), "-b 100000", nrow, "-c"
                #,"-e ", paste0("error_", table, ".log")
                )
  
  system2(command = "bcp", args = c(bcp_args))
}

#### STEP 2: Set universal parameters ####
write_path <- "\\\\phdata01/epe_data/APCD/Data_export/" ##Folder to save Amazon S3 files to
s3_folder <- "\"s3://waae-kc-ext/apcd_export/\"" ##Name of S3 folder containing data and format files

sql_server = "KCITSQLUTPDBH51"
sql_server_odbc_name = "PHClaims51"
sql_database_conn <- dbConnect(odbc(), sql_server_odbc_name) ##Connect to SQL server
sql_database_name <- "phclaims" ##Name of SQL database where table will be created


#### STEP 3: Save files from Amazon S3 bucket to drive ####

#List files in S3 folder
#system2(command = "aws", args = c("s3", "ls", s3_folder))

#Sync local folder to S3 bucket folder (this will download S3 files that have different size or modified date)
#system2(command = "aws", args = c("s3", "sync", s3_folder, write_path))


# #### STEP 4A: REFERENCE TABLES - Create SQL table shell ####
# 
# ##Set parameters specific to reference tables
# sql_schema_name <- "ref" ##Name of schema where table will be created
# read_path <- paste0(write_path, "small_table_reference_export/")
# format_file_list <- as.list(list.files(path = file.path(read_path), pattern = "*format.xml", full.names = T))
# long_file_list <- as.list(list.files(path = file.path(read_path), pattern = "*.csv", full.names = T))
# short_file_list <- as.list(gsub(".csv", "", list.files(path = file.path(read_path), pattern = "*.csv", full.names = F)))
# error_path <- "J:/APCD/Data_export/small_table_reference_export/error/"
# log_path <- "J:/APCD/Data_export/small_table_reference_export/log/"
# 
# ##Create tables, looping over file list
# #Run time for 36 tables: 7 seconds
# system.time(lapply(seq_along(format_file_list), y=format_file_list, function(y, i) {
#   
#   #Extract table name
#   table_name_part <- short_file_list[[i]]
#   sql_table <- paste0("apcd_", table_name_part) ##Name of SQL table to be created and loaded to
#   
#   #Extract column names and types from XML format file
#   apcd_format_file <- y[[i]]
#   format_xml <- xmlParse(apcd_format_file)
#   format_df <- xmlToDataFrame(nodes = xmlChildren(xmlRoot(format_xml)[["data"]]))
#   names <- xmlToDataFrame(nodes = xmlChildren(xmlRoot(format_xml)[["table-def"]]))
#   colNames <- (names$'column-name'[!is.na(names$'column-name')])
#   colnames(format_df) <- colNames
#   format_vector <- deframe(select(arrange(format_df, as.numeric(as.character(POSITION))), COLUMN_NAME, DATA_TYPE))
# 
#   #Drop table if it exists
#   if(dbExistsTable(sql_database_conn, name = sql_table) == T) {
#     dbRemoveTable(sql_database_conn, name = DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", sql_table)))}
# 
#   #Create table shell using format file from APCD
#   dbCreateTable(sql_database_conn, name = DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", sql_table)), 
#                 fields = format_vector, row.names = NULL)
# }))
# 
# #### STEP 4B: REFERENCE TABLES - Load data to SQL table ####
# #Run time for 36 tables: 2 min (via VPN) (takes longer with error logging)
# 
# ## Copy CSV data files to SQL Server, looping over all files
# system.time(lapply(seq_along(long_file_list), y=long_file_list, function(y, i) {
#   
#   #Extract table name
#   table_name_part <- short_file_list[[i]]
#   sql_table <- paste0("apcd_", table_name_part) ##Name of SQL table to be created and loaded to
#   print(table_name_part)
#   
#   #Load data using BCP
#   file_name <- y[[i]]
#   bcp_load_f(server = sql_server, table = paste0(sql_database_name, ".", sql_schema_name, ".", sql_table), read_file = file_name)
# }))
# 
# 
# #### STEP 5A: SMALL LDS_CAP TABLES - Create SQL table shell ####
# 
# ##Set parameters specific to tables
# sql_schema_name <- "stage" ##Name of schema where table will be created
# read_path <- paste0(write_path, "small_table_lds_cap_export/")
# format_file_list <- as.list(list.files(path = file.path(read_path), pattern = "*format.xml", full.names = T))
# long_file_list <- as.list(list.files(path = file.path(read_path), pattern = "*.csv", full.names = T))
# short_file_list <- as.list(gsub(".csv", "", list.files(path = file.path(read_path), pattern = "*.csv", full.names = F)))
# error_path <- "J:/APCD/Data_export/small_table_lds_cap_export/error/"
# #log_path <- "J:/APCD/Data_export/small_table_lds_cap_export/log/"
# 
# ##Create tables, looping over file list
# system.time(lapply(seq_along(format_file_list), y=format_file_list, function(y, i) {
#   
#   #Extract table name
#   table_name_part <- short_file_list[[i]]
#   sql_table <- paste0("apcd_", table_name_part) ##Name of SQL table to be created and loaded to
#   
#   #Extract column names and types from XML format file
#   apcd_format_file <- y[[i]]
#   format_xml <- xmlParse(apcd_format_file)
#   format_df <- xmlToDataFrame(nodes = xmlChildren(xmlRoot(format_xml)[["data"]]))
#   names <- xmlToDataFrame(nodes = xmlChildren(xmlRoot(format_xml)[["table-def"]]))
#   colNames <- (names$'column-name'[!is.na(names$'column-name')])
#   colnames(format_df) <- colNames
#   format_vector <- deframe(select(arrange(format_df, as.numeric(as.character(POSITION))), COLUMN_NAME, DATA_TYPE))
#   
#   #Drop table if it exists
#   if(dbExistsTable(sql_database_conn, name = sql_table) == T) {
#     dbRemoveTable(sql_database_conn, name = DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", sql_table)))}
#   
#   #Create table shell using format file from APCD
#   dbCreateTable(sql_database_conn, name = DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", sql_table)), 
#                 fields = format_vector, row.names = NULL)
# }))
# 
# #### STEP 5B: SMALL LDS_CAP TABLES - Load data to SQL table ####
# #Run time for 4 tables: XX min
# 
# ## Copy CSV data files to SQL Server, looping over all files
# #Run time: 3 minutes
# system.time(lapply(seq_along(long_file_list), y=long_file_list, function(y, i) {
#   
#   #Extract table name
#   table_name_part <- short_file_list[[i]]
#   sql_table <- paste0("apcd_", table_name_part) ##Name of SQL table to be created and loaded to
#   print(table_name_part)
#   
#   #Load data using BCP
#   file_name <- y[[i]]
#   bcp_load_f(server = sql_server, table = paste0(sql_database_name, ".", sql_schema_name, ".", sql_table), read_file = file_name)
# }))
# 

#### STEP 6: LARGE LDS_CAP TABLES ####

##Set parameters specific to tables
#List of large LDS_CAP table folders: dental_claim_export, eligibility_export, medical_claim_export, medical_claim_header_export,
  #medical_crosswalk_export, member_month_detail_export, pharmacy_claim_export, provider_export
read_path <- paste0(write_path, "medical_claim_export/")
sql_schema_name <- "stage" ##Name of schema where table will be created
apcd_format_file <- list.files(path = file.path(read_path), pattern = "*format.xml", full.names = T)
long_file_list <- as.list(list.files(path = file.path(read_path), pattern = "*.csv", full.names = T))
short_file_list <- as.list(gsub(".csv", "", list.files(path = file.path(read_path), pattern = "*.csv", full.names = F)))

  #For medical claim table only: break up ETL into 30GB each
  #i <- 1
  #j <- 6
  #long_file_list <- long_file_list[i:j]
  #short_file_list <- short_file_list[i:j]
    
##Create table shell

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
if(dbExistsTable(sql_database_conn, name = sql_table) == T) {
  dbRemoveTable(sql_database_conn, name = DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", sql_table)))}

#Create table shell using format file from APCD
dbCreateTable(sql_database_conn, name = DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", sql_table)), 
              fields = format_vector, row.names = NULL)

## Copy CSV data files to SQL Server, looping over all files
system.time(lapply(seq_along(long_file_list), y=long_file_list, function(y, i) {
  
  #Load data using BCP
  file_name <- y[[i]]
  print(file_name)
  bcp_load_f(server = sql_server, table = paste0(sql_database_name, ".", sql_schema_name, ".", sql_table), read_file = file_name)
}))



