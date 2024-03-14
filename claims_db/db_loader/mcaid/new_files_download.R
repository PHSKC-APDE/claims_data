#### MASTER CODE TO DOWNLOAD NEW FILES FROM HCA FTP, QA THEM AND UPLOAD TO CORRECT LOCATIONS
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2023-06


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

library(tidyverse) # Manipulate data
library(dplyr) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(keyring) # Access stored credentials
library(stringr) # Various string functions
library(AzureStor)
library(AzureAuth)
library(svDialogs)
library(R.utils)
library(zip)
library(sftp)


#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")

#### CREATE CONNECTION ####
##interactive_auth <- dlg_list(c("TRUE", "FALSE"), title = "Interactive Authentication?")$res
interactive_auth <- FALSE
##prod <- dlg_list(c("TRUE", "FALSE"), title = "Production Server?")$res
prod <- TRUE

db_claims <- create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)

blob_token <- AzureAuth::get_azure_token(
  resource = "https://storage.azure.com", 
  tenant = keyring::key_get("adl_tenant", "dev"),
  app = keyring::key_get("adl_app", "dev"),
  auth_type = "authorization_code",
  use_cache = F
)
blob_endp <- storage_endpoint("https://inthealthdtalakegen2.blob.core.windows.net", token = blob_token)
cont <- storage_container(blob_endp, "inthealth")

#### Start File Processing
if(T) {
  #### Set sftp url, credentials and directories
  url <- "mft.wa.gov"
  basedir <- "C:/temp/mcaid/"
  dldir <- paste0(basedir, "download/")
  exdir <- paste0(basedir, "extract")
  gzdir <- paste0(basedir, "gzip")
  txtdir <- paste0(basedir, "txt")
  schema <- "claims"
  table <- "metadata_etl_log"
  
  ## Create SFTP/MFT connection
  sftp_con <- sftp_connect(server = url,   
                           username = key_list("hca_mft")[["username"]],   
                           password = key_get("hca_mft", key_list("hca_mft")[["username"]]))
  sftp_changedir(tofolder = "Claims", current_connection_name = "sftp_con")
  sftp_claims <- sftp_listfiles(sftp_con, recurse = F)
  sftp_changedir(tofolder = "../Eligibility", current_connection_name = "sftp_con")
  sftp_elig <- sftp_listfiles(sftp_con, recurse = F)
  sftp_file_cnt <- nrow(sftp_claims) + nrow(sftp_elig)
  ## CHECK FOR EXISTING - TO DO!
  etl_exists <- 0
  
  if (sftp_file_cnt > 0) {
    proceed_msg <- paste0("Download the ", sftp_file_cnt, " files?")
    proceed <- askYesNo(msg = proceed_msg)
  }
  
  if (proceed == T) {
    message(paste0("Downloading Files - ", Sys.time()))
    sftp_changedir(tofolder = "../Claims", current_connection_name = "sftp_con")
    sftp_download(file = sftp_claims$name, tofolder = paste0(dldir, "Claims"))
    sftp_changedir(tofolder = "../Eligibility", current_connection_name = "sftp_con")
    sftp_download(file = sftp_elig$name, tofolder = paste0(dldir, "Eligibility"))
    message(paste0("Download Completed - ", Sys.time()))
    
    zfiles <- data.frame("fileName" = list.files(dldir, pattern="*.gz", recursive = T))
    message(paste0("Extracting Files - ", Sys.time()))
    for (x in 1:nrow(zfiles)) {
      message(paste0("Begin Extracting ", zfiles[x, "fileName"], " - ", Sys.time()))
      ## Extract file to specified directory
      gunzip(paste0(dldir, "/", zfiles[x, "fileName"]), destname = paste0(exdir, "/", gsub("csv[.]gz$", "txt", zfiles[x, "fileName"])), remove = F)
      message(paste0("Extraction Completed - ", Sys.time()))
    }
    message("------------------------------")
    message(paste0("All Files Extracted - ", Sys.time()))
  }
 
  #### Consolidate each set of files into a single TXT file
  exdirs <- list.dirs(exdir)
  message(paste0("Consolodating Each Set of Files into Single TXT - ", Sys.time()))
  for(d in 1:length(exdirs)) {
    if(exdirs[d] != exdir) {
      efiles <- data.frame("fileName" = list.files(exdirs[d], pattern="*.txt"))
      tname <- paste0(substr(efiles[1, "fileName"], 1, str_locate(efiles[1, "fileName"], "[.]")[1, 1] - 1), ".txt")
      message(paste0("Begin Buidling ", tname, " - ", Sys.time()))
      for(i in 1:nrow(efiles)) {
        message(paste0("Reading File ", i, " of ", nrow(efiles), " - ", Sys.time()))
        con <- file(paste0(exdirs[d],"/",efiles[i, "fileName"]),"r")
        df <- readLines(con)
        close(con)
        if(i == 1) {
          message(paste0("Creating ", tname, " - ", Sys.time()))
        }
        message(paste0("Writing ", length(df) - 1, " Rows to ", tname, " - ", Sys.time()))
        for(x in 1:length(df)) {
          if(x == 1 && i == 1) {
            cat(df[x], file = paste0(txtdir, "/", tname), sep = "\n", append = F)
          } else if(x > 1) {
            cat(df[x], file = paste0(txtdir, "/", tname), sep = "\n", append = T)
          }
        }
      }
      message(paste0("File ", tname, " Complete - ", Sys.time()))
    }
    message(paste0("File Consolodation Complete - ", Sys.time()))
  }
  
  #### Check text files' rows, columns and dates
  tfiles <- data.frame("fileName" = list.files(txtdir, pattern="*.txt"))
  message("------------------------------")
  message(paste0("Performing QA Checks - ", Sys.time()))
  message("------------------------------")
  defaultW <- getOption("warn") 
  options(warn = -1)
  for (x in 1:nrow(tfiles)) {
    file_name <- tfiles[x, "fileName"]
    message(paste0("Begin QA Checks for ", file_name, " - ", Sys.time()))
    if(str_detect(tolower(file_name), "elig")) {
      tfiles[x, "type"] <- "elig"
    } else {
      tfiles[x, "type"] <- "claims"
    }
    if(tfiles[x, "type"] == "elig") {
      config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_elig_partial.yaml"))
      tfiles[x, "server_loc"] <- "//kcitsqlutpdbh51/importdata/Data/KC_Elig/"
    } else {
      config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_load_raw.mcaid_claim_partial.yaml"))
      tfiles[x, "server_loc"] <- "//kcitsqlutpdbh51/importdata/Data/KC_Claim/"
    }
    file_path = paste0(txtdir, "/", file_name)
    ### rows
    row_cnt <- R.utils::countLines(file_path) - 1
    ### columns
    load_table <- data.table::fread(file_path, nrow = 10)
    tbl_name <- as.list(names(load_table))
    tbl_vars <- as.list(names(config$vars))
    col_qa <- "PASS"
    for (v in 1:length(tbl_vars)) {
      if (tbl_vars[[v]] != tbl_name[[v]]) { col_qa <- "FAIL" }
    }
    ### dates
    ddate <- substr(file_name, nchar(file_name) - 11, nchar(file_name) - 4)
    ddate <- paste0(substr(ddate,1,4), "-", substr(ddate, 5, 6), "-", substr(ddate, 7, 8))
    del_date <- as.Date(ddate)
    memory.size(max = F)
    memory.limit(size = 128000)
    if(tfiles[x, "type"] == "elig") {
      dates <- read.delim(file_path, 
                          colClasses = c(rep("integer", 1), rep("NULL", 39)), 
                          sep = "\t",
                          header = T)
      min_date <- as.character(min(as.vector(dates$CLNDR_YEAR_MNTH)))
      max_date <- as.character(max(as.vector(dates$CLNDR_YEAR_MNTH)))
      min_date <- as.Date(paste0(substr(min_date, 1, 4), "-", substr(min_date, 5, 6), "-01"))
      max_date <- as.Date(ymd(paste0(substr(max_date, 1, 4), "-", substr(max_date, 5, 6), "-01")) %m+% months(1)) - 1
      db_claims <- create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)
      prev_rpm <- DBI::dbGetQuery(db_claims,
                                  glue::glue_sql(
                                    "SELECT TOP (1) row_count / (DATEDIFF(month, date_min, date_max) + 1) as rpm
                        FROM {`schema`}.{`table`}
                        WHERE row_count IS NOT NULL 
                        AND data_source = 'Medicaid' 
                        AND CHARINDEX('elig', file_name, 1) > 0
                        ORDER BY delivery_date DESC", .con = db_claims))[1,1]
      curr_rpm <- row_cnt / (interval(min_date, max_date) %/% months(1) + 1)
      rpm_diff <- (curr_rpm - prev_rpm) / prev_rpm
    } else {
      dates <- read.delim(file_path, 
                          colClasses = c(rep("NULL", 8), rep("character", 1), rep("NULL", 110)), 
                          sep = "\t",
                          header = T)
      dates <- as.Date(dates$FROM_SRVC_DATE)
      min_date <- min(dates)
      max_date <- max(dates)
      min_date <- as.Date(paste0(format(min_date, "%Y"), "-", format(min_date, "%m"), "-01"))
      max_date <- as.Date(ymd(paste0(format(max_date, "%Y"), "-", format(max_date, "%m"), "-01")) %m+% months(1)) - 1
      db_claims <- create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)
      prev_rpm <- DBI::dbGetQuery(db_claims,
                                  glue::glue_sql(
                                    "SELECT TOP (1) row_count / (DATEDIFF(month, date_min, date_max) + 1) as rpm
                        FROM {`schema`}.{`table`}
                        WHERE row_count IS NOT NULL 
                        AND data_source = 'Medicaid' 
                        AND CHARINDEX('claim', file_name, 1) > 0
                        ORDER BY delivery_date DESC", .con = db_claims))[1,1]
      curr_rpm <- row_cnt / (interval(min_date, max_date) %/% months(1) + 1)
      rpm_diff <- (curr_rpm - prev_rpm) / prev_rpm
    }  
    tfiles[x,"del_date"] <- del_date
    tfiles[x,"min_date"] <- min_date
    tfiles[x,"max_date"] <- max_date
    tfiles[x,"col_qa"] <- col_qa
    tfiles[x,"row_cnt"] <- row_cnt
    tfiles[x,"rpm_diff"] <- rpm_diff
    message(paste0("QA Checks Completed  - ", Sys.time()))
    rm(dates, load_table)
  }
  options(warn = defaultW)
  message("------------------------------")
  message(paste0("All QA Checks Completed - ", Sys.time()))
  message("------------------------------")
  ## Display QA Results
  for (x in 1:nrow(tfiles)) {
    message(glue("File: {tfiles[x, 'fileName']}
             Date Delivery: {tfiles[x, 'del_date']}
             Date Min: {tfiles[x, 'min_date']}
             Date Max: {tfiles[x, 'max_date']}
             Column QA: {tfiles[x, 'col_qa']}
             Row Count: {tfiles[x, 'row_cnt']}
             Rows vs Prev: {round(tfiles[x, 'rpm_diff'] * 100, 2)}%
             ------------------------------"))
  }
  
  
  if (nrow(tfiles) > 0) {
    proceed <- NA
    proceed_msg <- glue("Would you like to compress, upload and add these files to [{table}]?")
    proceed <- askYesNo(msg = proceed_msg)
  }  else { message("No files to process.") }
  
  if (proceed == T && nrow(tfiles) > 0) {
    ## Compress files into gz format
    message(paste0("Compressing Files - ", Sys.time()))
    for (x in 1:nrow(tfiles)) {
      ## Compress file to specified directory
      tfiles[x, "gzName"] <- paste0(tfiles[x, "fileName"], ".gz")
      message(paste0("Begin Compressing ", tfiles[x, "gzName"], " - ", Sys.time()))
      gzip(paste0(txtdir, "/", tfiles[x, "fileName"]), 
           destname = paste0(gzdir, "/", tfiles[x, "gzName"]), 
           remove = F)
      message(paste0("Compression Completed - ", Sys.time()))
    }
    message(paste0("All Files Compressed - ", Sys.time()))
    ## Compress files into gz format
    message(paste0("Uploading and Renaming Files - ", Sys.time()))
    for (x in 1:nrow(tfiles)) {
      ## Upload file to specified directory on Azure blob
      message(paste0("Begin Uploading/Renaming ", tfiles[x, "gzName"], " - ", Sys.time()))
      tfiles[x, "uploadName"] <- tfiles[x, "gzName"]
      storage_upload(cont, 
                     paste0(gzdir, "/", tfiles[x, "gzName"]), 
                     paste0("claims/mcaid/", tfiles[x, "type"], "/incr/", tfiles[x, "uploadName"]))
      message(paste0("Upload Completed - ", Sys.time()))
    }
    message(paste0("All Files Uploaded - ", Sys.time()))
    
    db_claims <- create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)
    for (x in 1:nrow(tfiles)) {
      tfiles[x, "batch_id_prod"] <- load_metadata_etl_log_file_f(conn = db_claims, 
                                                            server = "hhsaw",
                                                            batch_type = "incremental", 
                                                            data_source = "Medicaid", 
                                                            date_min = tfiles[x, "min_date"],
                                                            date_max = tfiles[x, "max_date"],
                                                            delivery_date = tfiles[x, "del_date"], 
                                                            file_name = tfiles[x, "uploadName"],
                                                            file_loc = paste0("claims/mcaid/", tfiles[x, "type"], "/incr/"),
                                                            row_cnt = tfiles[x, "row_cnt"], 
                                                            note = paste0("Partial refresh of Medicaid ", tfiles[x, "type"], " data"))
    }
    schema <- "metadata"
    table <- "etl_log"
    proceed_msg <- glue("Would you like to create ETL Log Entries on HHSAW Dev?")
    proceed <- askYesNo(msg = proceed_msg)
    if (proceed == T) {
      db_claims <- create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = F)
      for (x in 1:nrow(tfiles)) {
        tfiles[x, "batch_id_dev"] <- load_metadata_etl_log_file_f(conn = db_claims, 
                                                              server = "hhsaw",
                                                              batch_type = "incremental", 
                                                              data_source = "Medicaid", 
                                                              date_min = tfiles[x, "min_date"],
                                                              date_max = tfiles[x, "max_date"],
                                                              delivery_date = tfiles[x, "del_date"], 
                                                              file_name = tfiles[x, "uploadName"],
                                                              file_loc = paste0("claims/mcaid/", tfiles[x, "type"], "/incr/"),
                                                              row_cnt = tfiles[x, "row_cnt"], 
                                                              note = paste0("Partial refresh of Medicaid ", tfiles[x, "type"], " data"))
      }
    }
    message(paste0("All Entries Created - ", Sys.time()))
  }
  delete <- askYesNo("Delete Temporary Files?")
  if(delete == T) {
    files <- list.files(dldir, recursive = T)
    for (x in 1:length(files)) {
      file.remove(paste0(dldir, "/", files[x]))
    }
    files <- list.files(exdir, recursive = T)
    for (x in 1:length(files)) {
      file.remove(paste0(exdir, "/", files[x]))
    }
    files <- list.files(txtdir, recursive = T)
    for (x in 1:length(files)) {
      file.remove(paste0(txtdir, "/", files[x]))
    }
    files <- list.files(gzdir, recursive = T)
    for (x in 1:length(files)) {
      file.remove(paste0(gzdir, "/", files[x]))
    }
  }
}

rm(list=ls())
