library(odbc) # Read to and write from SQL
library(curl) # Read files from FTP
library(keyring) # Access stored credentials
library(R.utils) # File and folder manipulation
library(zip) # Extract data from gzip
library(jsonlite) # Extract data from curl
library(tidyverse) # Manipulate data
library(dplyr) # Manipulate data
library(lubridate) # Manipulate data
library(glue) # Safely combine SQL code
library(configr) # Read in YAML files
library(xlsx) # Read in XLSX files
library(svDialogs) # Extra UI Elements

message("STEP 1: Loading Functions, Config File, Defining Variables, and Check SFTP Credentials...")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")
source(file.path(here::here(),"apcd_export_import/apcd_import_functions_test.R"))
config <- yaml::read_yaml(file.path(here::here(),"apcd_export_import/apcd_import_config_test.yaml"))
#Define directories for downloaded files and extracted files.
base_dir <- "//phcifs/SFTP_DATA/APDEDataExchange/WA-APCD/"
new_base_dir <- "//dphcifs.kc.kingcounty.lcl/APDE-CDIP/SFTP_APDEDATA/APDEDataExchange/WA-APCD/"
temp_dir <- "C:/temp/apcd/"
ref_dir <- paste0(base_dir, "ref_schema/")
stage_dir <- paste0(base_dir, "stage_schema/")
final_dir <- paste0(base_dir, "final_schema/")
config$table_file_path <- file.path(here::here(),paste0("apcd_export_import/", config$table_file_path))
apcd_etl_check_function_f(config)
files <- data.frame()

### STEP 2: REVIEW SFTP FILES AND CREATE ETL ENTRIES

message("STEP 2: Review SFTP Files and Create New ETL Entries")
message("Getting SFTP file list...")
files <- apcd_ftp_get_file_list_f(config)
message("Comparing current ETL log with SFTP file list...")
etl_list <- apcd_etl_get_list_f(config)
files <- files %>% 
  anti_join(etl_list, by = "file_name")

message("Create ETL entries for new SFTP files...")
if(nrow(files) > 0) {
  for(f in 1:nrow(files)) {
    files[f, "etl_id"] <- apcd_etl_entry_f(config,
                                           file_name = files[f,]$file_name,
                                           file_date = files[f,]$file_date,
                                           file_schema = files[f,]$schema,
                                           file_table = files[f,]$table,
                                           file_number = files[f,]$file_number)
  }
} else {
  message("No new SFTP files on server...")
}


### STEP 3: CHOOSE SCHEMAS AND TABLES TO DOWNLOAD, THEN DOWNLOAD FILES

# Select which schemas and tables to download the files
etl_list <- apcd_etl_get_list_f(config)
if(!is.Date(files$file_date)) {
  files$file_date <- as.Date(files$file_date)  
}
if(!is.Date(etl_list$file_date)) {
  etl_list$file_date <- as.Date(etl_list$file_date)  
}
if(nrow(files) > 0) {
  files <- files %>% left_join(etl_list) %>% filter(is.na(datetime_download))
} else {
  files <- etl_list %>% filter(is.na(datetime_download))
}
if(nrow(files) > 0) {
  files <- files %>% left_join(etl_list) %>% filter(is.na(datetime_download))
} else {
  files <- etl_list %>% filter(is.na(datetime_download))
}
if(nrow(files) > 0) {
  message(paste0("Begin Downloading ", nrow(files), " Files from SFTP..."))
  for(f in 1:nrow(files)) {
    message(paste0("...Downloading File: "  , f, ": ", files[f, "file_name"], "..."))
    if(files[f, "file_schema"] == "ref") {
      files[f, "file_path"] <- ref_dir
    } else if(files[f, "file_schema"] == "stage") {
      files[f, "file_path"] <- stage_dir
    } else {
      files[f, "file_path"] <- final_dir
    }
    files[f, "file_path"] <- paste0(files[f, "file_path"], "/", files[f, "file_name"])
    files[f, "datetime_download"] <- apcd_ftp_get_file_f(config, 
                                                         file = files[f, ])
    message(paste0("......Download Complete. ", nrow(files) - f, " of ", nrow(files), " left to download..."))
  }
  message("All Files Downloaded...")
} else {
  message("No files to Download...")
}


### STEP 4: EXTRACT AND LOAD DATA FROM FILES INTO SQL

# Select which schemas and tables to import
etl_list <- apcd_etl_get_list_f(config)
files <- etl_list %>% filter(is.na(datetime_load)) %>% filter(!is.na(datetime_download))
if(nrow(files) > 0) {
  message(paste0("Begin Loading ", nrow(files), " Files into SQL Server..."))
  import_errors <- list()
  for(f in 1:nrow(files)) {
    message(paste0("...Loading File: "  , f, ": ", files[f, "file_name"], "..."))
    result <- apcd_data_load_f(config, file = files[f, ])  
    message(paste0("......Loading Complete. ", nrow(files) - f, " of ", nrow(files), " left to import..."))
    if(!is.na(result)) {
      import_errors <- append(import_errors, result)
    } else {
      if(files[f, "file_number"] == files[f, "max_file_num"]) {
        conn <- create_db_connection(server = "hhsaw", prod = F, interactive = F)
        DBI::dbExecute(conn,
                       glue_sql("DROP TABLE {`config$schema_name`}.{`paste0(files[f, 'file_schema'], '_', files[f, 'file_table'])`}",
                                .con = conn))
      }
    }
    
  }
  message("All Files Loaded...")
  if(length(import_errors) == 0) {
    message("No errors to report...")
  } else {
    message(paste0("There were ", length(import_errors), " error(s):"))
    for(x in 1:length(import_errors)) {
      message(import_errors[x])
    }
  }
} else {
  message("No files to Load...")
}



