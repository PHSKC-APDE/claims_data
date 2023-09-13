#### MASTER CODE TO DOWNLOAD NEW FILES FROM HCA FTP, QA THEM AND UPLOAD TO CORRECT LOCATIONS
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-05


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

library(tidyverse) # Manipulate data
library(dplyr) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(sf) # Read shape files
library(keyring) # Access stored credentials
library(stringr) # Various string functions
library(AzureStor)
library(AzureAuth)
library(svDialogs)
library(R.utils)
library(zip)
library(curl)
library(jsonlite)
library(httpuv)

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")

#### CREATE CONNECTION ####
interactive_auth <- dlg_list(c("TRUE", "FALSE"), title = "Interactive Authentication?")$res
prod <- dlg_list(c("TRUE", "FALSE"), title = "Production Server?")$res

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
  url <- "https://sft.wa.gov/api/v1.5/files"
  basedir <- "C:\\temp"
  zipdir <- paste0(basedir, "\\zip\\")
  exdir <- paste0(basedir, "\\extract\\")
  gzdir <- paste0(basedir, "\\gz\\")
  schema <- "claims"
  table <- "metadata_etl_log"
  
  h <- curl::new_handle()
  curl::handle_setopt(handle = h, httpauth = 1, userpwd = paste0(key_list("hca_sftp")[["username"]], ":", key_get("hca_sftp", key_list("hca_sftp")[["username"]])))
  
  ## Download JSON data of all zip files available in the home folder
  json <- curl::curl_fetch_memory(url, handle = h)
  ## Convert JSON to matrix
  ftpfiles <- fromJSON(rawToChar(json$content))
  zipfiles <- as.data.frame(ftpfiles[["files"]]["fileName"])
  zipfiles$shortName <- substr(zipfiles[, "fileName"], 1, nchar(zipfiles[, "fileName"]) - 4)
  etl_exists <- 0
  ## Compare files from ftp to files in etl_log
  for (x in 1:nrow(zipfiles)) {
    results <- DBI::dbGetQuery(db_claims,
                               glue::glue_sql("SELECT * FROM {`schema`}.{`table`}
                        WHERE CHARINDEX({zipfiles[x,'shortName']}, file_name, 1) > 0
                        ORDER BY delivery_date DESC", .con = db_claims))
    if (is.null(nrow(etl_exists)) == T) {
      etl_exists <- results
    } else {
      etl_exists <- rbind(etl_exists, results)
    }
  }
  
  ## Ask to remove existing file from ftp file list
  if (nrow(etl_exists) > 0) {
    for (x in 1:nrow(etl_exists)) {
      remove_msg <- glue::glue("The file: {etl_exists[x,'file_name']} \\
                              already exists in the [{table}] table \\
                              with etl_batch_id: {etl_exists[x,'etl_batch_id']}.
                              
                              Remove it from the list of files to download?")
      remove <- askYesNo(msg = remove_msg)
      if (remove == T) {
        zipfiles <- zipfiles[!zipfiles$shortName == substr(etl_exists[x, "file_name"], 1, nchar(etl_exists[x, "file_name"]) - 3), ]
      }
    }
  } 
  
  if (nrow(zipfiles) > 0) {
    proceed_msg <- "Download the following files?"
    for (x in 1:nrow(zipfiles)) {
      proceed_msg <- paste0(proceed_msg, "\n", zipfiles[x, "fileName"])
    }
    proceed <- askYesNo(msg = proceed_msg)
  }
  
  if (proceed == T) {
    message(paste0("Downloading Files - ", Sys.time()))
    ## Go through all of the filenames 
    for (x in 1:nrow(zipfiles)) {
      filename = zipfiles[x,"fileName"]
      ## Set the destination for the download
      zfile <- paste0(zipdir, filename)
      ## Set the url for the file to download
      message(paste0("Begin Downloading ", filename, " - ", Sys.time()))
      ## Download file and write it to a zip file in the specified directory  
      h <- curl::new_handle()
      curl::handle_setopt(handle = h, httpauth = 1, userpwd = paste0(key_list("hca_sftp")[["username"]], ":", key_get("hca_sftp", key_list("hca_sftp")[["username"]])))
      curl::curl_download(url = paste0(url,"/", filename), 
                          destfile = paste0(zipdir, filename), 
                          handle = h)
      message(paste0("Download Completed - ", Sys.time()))
    }
    message(paste0("All Files Downloaded - ", Sys.time()))
    
    zfiles <- data.frame("fileName" = list.files(zipdir, pattern="*.zip"))
    message(paste0("Extracting Files - ", Sys.time()))
    for (x in 1:nrow(zfiles)) {
      message(paste0("Begin Extracting ", zfiles[x, "fileName"], " - ", Sys.time()))
      ## Extract file to specified directory
      unzip(paste0(zipdir, zfiles[x, "fileName"]), exdir = exdir)
      message(paste0("Extraction Completed - ", Sys.time()))
    }
    message("------------------------------")
    message(paste0("All Files Extracted - ", Sys.time()))
  }
  
  #### Check text files' rows, columns and dates
  tfiles <- data.frame("fileName" = list.files(exdir, pattern="*.txt"))
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
    file_path = paste0(exdir, file_name)
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
      gzip(paste0(exdir, tfiles[x, "fileName"]), 
           destname = paste0(gzdir, tfiles[x, "gzName"]), 
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
                     paste0(gzdir, tfiles[x, "gzName"]), 
                     paste0("claims/mcaid/", tfiles[x, "type"], "/incr/", tfiles[x, "uploadName"]))
      message(paste0("Upload Completed - ", Sys.time()))
    }
    message(paste0("All Files Uploaded - ", Sys.time()))
    
    db_claims <- create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)
    for (x in 1:nrow(tfiles)) {
      tfiles[x, "batch_id"] <- load_metadata_etl_log_file_f(conn = db_claims, 
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
    proceed_msg <- glue("Would you like to copy to files to PHClaims?")
    proceed <- askYesNo(msg = proceed_msg)
    ## Copy files and data to PHClaims
    if (proceed == T) {
      blob_token <- AzureAuth::get_azure_token(
        resource = "https://storage.azure.com", 
        tenant = keyring::key_get("adl_tenant", "dev"),
        app = keyring::key_get("adl_app", "dev"),
        auth_type = "authorization_code",
        use_cache = F
      )
      blob_endp <- storage_endpoint("https://inthealthdtalakegen2.blob.core.windows.net", token = blob_token)
      cont <- storage_container(blob_endp, "inthealth")
      message(paste0("Downloading Files - ", Sys.time()))
      for (x in 1:nrow(tfiles)) {
        message(paste0("Begin Downloading ", tfiles[x, "uploadName"], 
                       " to ", tfiles[x, "server_loc"]," - ", Sys.time()))
        storage_download(cont, 
                         paste0("claims/mcaid/", tfiles[x, "type"], "/incr/", tfiles[x, "uploadName"]), 
                         paste0(tfiles[x, "server_loc"], tfiles[x, "uploadName"]))
        message(paste0("Download Completed - ", Sys.time()))
      }
      message(paste0("All Files Downloaded - ", Sys.time()))
      db_claims <- create_db_connection(server = "phclaims", interactive = interactive_auth, prod = prod)
      for (x in 1:nrow(tfiles)) {
        tfiles[x, "batch_id"] <- load_metadata_etl_log_file_f(conn = db_claims, 
                                                              server = "phclaims",
                                                              batch_type = "incremental", 
                                                              data_source = "Medicaid", 
                                                              date_min = tfiles[x, "min_date"],
                                                              date_max = tfiles[x, "max_date"],
                                                              delivery_date = tfiles[x, "del_date"], 
                                                              file_name = tfiles[x, "uploadName"],
                                                              file_loc = tfiles[x, "server_loc"],
                                                              row_cnt = tfiles[x, "row_cnt"], 
                                                              note = paste0("Partial refresh of Medicaid ", tfiles[x, "type"], " data"))
      }
    }
    message(paste0("All Files Processed - ", Sys.time()))
  }
  delete <- askYesNo("Delete Temporary Files?")
  if(delete == T) {
    files <- list.files(zipdir)
    for (x in 1:length(files)) {
      file.remove(paste0(zipdir,files[x]))
    }
    files <- list.files(exdir)
    for (x in 1:length(files)) {
      file.remove(paste0(exdir,files[x]))
    }
    files <- list.files(gzdir)
    for (x in 1:length(files)) {
      file.remove(paste0(gzdir,files[x]))
    }
  }
}

rm(list=ls())
