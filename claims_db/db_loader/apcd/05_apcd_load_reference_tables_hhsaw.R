#### CODE TO LOAD APCD REFERENCE TABLES
# Eli Kern, PHSKC (APDE)
#
# 2019-10

#2022-04-26 update: Update to account for new format file structure
#2024-03-21 update: Update to push data to HHSAW with new naming syntax

## Set up global parameters and call in libraries
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)
origin <- "1970-01-01" # Date origin
pacman::p_load(tidyverse, odbc, configr, glue, keyring, svDialogs, R.utils)

## Connect to HHSAW
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")
interactive_auth <- TRUE #must be set to true if running from Azure VM
prod <- TRUE
db_claims <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)

#### STEP 1: Define inner bcp load function ####

## FUNCTION 1 - bcp_load_f: call the bulk copy program utility to insert data into existing SQL table
#Note this version does not use a format file and thus includes the -c parameter to specific use of character format
bcp_load_f <- function(server = NULL, table = NULL, read_file = NULL, format_file = NULL, nrow = NULL) {
  
  #Set number of rows to load
  if(!is.null(nrow)){ 
    nrow = paste0("-L ", toString(nrow))
  }
  else {nrow = ""}
  
  #Load data file
  bcp_args <- c(glue((table, "in", read_file, "-t ,", "-C 65001", "-F 2", glue("-S ", server, " -T"),

                "-b 100000", nrow, "-c")
  system2(command = "bcp", args = c(bcp_args), stdout = TRUE, stderr = TRUE)
}


#### STEP 2: Set universal parameters ####
write_path <- "//dphcifs/apde-cdip/apcd/apcd_data_import/" ##Folder where APCD data is downloaded from AWS
sql_server <- "HHSAW_prod" ##Name of ODBC connection set up on this machine
sql_server <- "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433"
sql_server <- "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov"
sql_server <- "kcitazrhpasqlprp16.azds.kingcounty.gov"
sql_database_name <- "hhs_analytics_workspace" ##Name of SQL database where table will be created


#### STEP 3: Create SQL table shell ####

##Set parameters specific to tables
sql_schema_name <- "claims" ##Name of schema where table will be created
read_path <- paste0(write_path, "small_table_reference_export/")

long_file_list <- as.list(list.files(path = file.path(read_path), pattern = "*.csv", full.names = T))
long_file_list <- long_file_list[!str_detect(long_file_list, pattern = "01_small_table_reference_format.csv")] # filter out format file

short_file_list <- as.list(gsub(".csv", "", list.files(path = file.path(read_path), pattern = "*.csv", full.names = F)))
short_file_list <- short_file_list[!str_detect(short_file_list, pattern = "01_small_table_reference_format")] # filter out format file

##Load format file
format_file <- read_csv(paste0(read_path, "01_small_table_reference_format.csv"), show_col_types = F)
table_list <- as.list(distinct(format_file, table_name)$table_name)

##Create tables, looping over table list
system.time(lapply(seq_along(table_list), y=table_list, function(y, i) {

  #Extract table name
  table_name_part <- table_list[[i]]
  sql_table <- paste0("ref_apcd_", table_name_part) ##Name of SQL table to be created and loaded to

  #Extract column names and types from format file
  format_file_subset <- filter(format_file, table_name == table_name_part)
  format_vector <- deframe(select(arrange(format_file_subset, as.numeric(as.character(column_position))), column_name, column_type))

  #Drop table if it exists
  if(dbExistsTable(db_claims, name = DBI::Id(schema = sql_schema_name, table = sql_table)) == T) {
    dbRemoveTable(db_claims, name = DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", sql_table)))}

  #Create table shell using format file from APCD
  DBI::dbCreateTable(db_claims, name = DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", sql_table)),
                fields = format_vector, row.names = NULL)
  
  #Print helpful message
  message(paste0("Table shell for reference table ", table_name_part, " successfully created."))
  
}))


#### STEP 4: Load data to SQL table using BCP ####
#Run time for 11 tables: 4 sec

## Copy CSV data files to SQL Server, looping over all files
system.time(lapply(seq_along(long_file_list), y=long_file_list, function(y, i) {

  #Extract table name
  table_name_part <- short_file_list[[i]]
  sql_table <- paste0("ref_apcd_", table_name_part) ##Name of SQL table to be created and loaded to
  print(table_name_part)

  #Prep BCP arguments
  file_name <- y[[i]]
  bcp_args <- c(glue('{sql_schema_name}.{sql_table} IN ',
                     '"{file_name}" ',
                     '-t , -C 65001 -F 2 ',
                     '-S "{sql_server}" -d "{sql_database_name}" ',
                     '-b 100000 -c ',
                     '-G -U {keyring::key_list("hhsaw")$username} -P {keyring::key_get("hhsaw", keyring::key_list("hhsaw")$username)} -D'))
  
  
  system2(command = "bcp", args = c(bcp_args), stdout = TRUE, stderr = TRUE)
  
  #Print helpful message
  print(paste0("Data for reference table ", table_name_part, " successfully loaded."))
}))
}