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
library(curl)
library(jsonlite)
library(readr)
library(apde.etl)


#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")

#### CREATE CONNECTION ####
##interactive_auth <- dlg_list(c("TRUE", "FALSE"), title = "Interactive Authentication?")$res
interactive_auth <- TRUE
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
  url <- "mft.wa.gov/"
  basedir <- "C:/temp/mcaid/"
  dldir <- paste0(basedir, "download/")
  exdir <- paste0(basedir, "extract")
  gzdir <- paste0(basedir, "gzip")
  txtdir <- paste0(basedir, "txt")
  schema <- "claims"
  table <- "metadata_etl_log"
  
  ## Create SFTP/MFT connection
  process_chunk <- function(chunk) {
    raw_text <- rawToChar(chunk)
    combined_text <- paste0(leftover, raw_text)
    lines <- strsplit(combined_text, "\n", fixed = TRUE)[[1]]
    last_char <- substr(combined_text, nchar(combined_text), nchar(combined_text))
    if (last_char == "\n") {
      processed_lines <- lines
      leftover <<- ""
    } else {
      processed_lines <- lines[-length(lines)]
      leftover <<- lines[length(lines)]
    }
    all_files <<- c(all_files, processed_lines)
    #message("Parsed ", length(processed_lines), " files in this chunk...")
  }
  h <- curl::new_handle()
  all_files <- character()
  leftover <- ""
  curl::handle_setopt(h, dirlistonly = TRUE, customrequest = "GET", httpauth = 1, userpwd = paste0(key_list("hca_mft")[["username"]], ":", key_get("hca_mft", key_list("hca_mft")[["username"]])))
  curl::curl_fetch_stream(paste0("sftp://", url, "Claims/"), handle = h, process_chunk)
  sftp_claims <- as.data.frame(list(file_name = all_files))
  sftp_claims$folder <- "Claims"
  all_files <- character()
  leftover <- ""
  curl::handle_reset(h)
  curl::handle_setopt(h, dirlistonly = TRUE, customrequest = "GET", httpauth = 1, userpwd = paste0(key_list("hca_mft")[["username"]], ":", key_get("hca_mft", key_list("hca_mft")[["username"]])))
  curl::curl_fetch_stream(paste0("sftp://", url, "Eligibility/"), handle = h, process_chunk)
  sftp_elig <- as.data.frame(list(file_name = all_files))
  sftp_elig$folder <- "Eligibility"
  rm(all_files, leftover, process_chunk)
  sftp_file_cnt <- nrow(sftp_claims) + nrow(sftp_elig)
  sftp_files <- rbind(sftp_claims, sftp_elig)
  sftp_files$url <- paste0("sftp://", key_list("hca_mft")[["username"]], "@", url, sftp_files$folder, "/", sftp_files$file_name)
  ## CHECK FOR EXISTING - TO DO!
  etl_exists <- 0
  
  if (sftp_file_cnt > 0) {
    proceed_msg <- paste0("Download the ", sftp_file_cnt, " files?")
    proceed <- askYesNo(msg = proceed_msg)
  }
  
  if (proceed == T) {
    message(paste0("Downloading Files - ", Sys.time()))
    for(f in 1:nrow(sftp_files)) {
      message(paste0("...Downloading file ", f, " of ", nrow(sftp_files), ": ", sftp_files[f, "file_name"], " - ", Sys.time()))
      curl::handle_reset(h)
      curl::handle_setopt(h, httpauth = 1, userpwd = paste0(key_list("hca_mft")[["username"]], ":", key_get("hca_mft", key_list("hca_mft")[["username"]])))
      start_time <- Sys.time()
      curl::curl_download(url = sftp_files[f, "url"], 
                          destfile = paste0(dldir, sftp_files[f, "folder"], "/", sftp_files[f, "file_name"]),
                          quiet = F,
                          handle = h)
      end_time <- Sys.time()
      if(nrow(sftp_files) > 1 && f < nrow(sftp_files) && end_time - start_time < 60) {
        Sys.sleep(as.integer(60 - (end_time - start_time)) + 1)
      }
    }
    message(paste0("Download Completed - ", Sys.time()))
    
    zfiles <- data.frame("fileName" = list.files(dldir, pattern="*.gz", recursive = T))
    compression <- "gz"
    if(nrow(zfiles) == 0) {
      zfiles <- data.frame("fileName" = list.files(dldir, pattern="*.zip", recursive = T))  
      compression <- "zip"
    }
    message(paste0("Extracting Files - ", Sys.time()))
    for (x in 1:nrow(zfiles)) {
      message(paste0("Begin Extracting ", zfiles[x, "fileName"], " - ", Sys.time()))
      ## Extract file to specified directory
      if(compression == "gz") {
        gunzip(paste0(dldir, "/", zfiles[x, "fileName"]), destname = paste0(exdir, "/", gsub("csv[.]gz$", "txt", zfiles[x, "fileName"])), remove = F)
      } else {
        unzip(paste0(dldir, "/", zfiles[x, "fileName"]), exdir = paste0(exdir, "/", substring(zfiles[x, "fileName"], 1, survPen::instr(zfiles[x, "fileName"], "/") - 1)))
      }
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
      if(nrow(efiles) == 0) {
        efiles <- data.frame("fileName" = list.files(exdirs[d], pattern="*.csv"))  
      }
      tname <- paste0(substr(efiles[1, "fileName"], 1, str_locate(efiles[1, "fileName"], "[.]")[1, 1] - 1), ".txt")
      message(paste0("Buidling ", tname, " - ", Sys.time()))
      file_path_e <- paste0(gsub("/", "\\\\", exdirs[d]), "\\")
      file_path_t <- paste0(gsub("/", "\\\\", txtdir), "\\")
      efiles$filepath <- paste0(file_path_e, efiles$fileName)
      files <- paste(efiles$filepath, collapse = ", ")
      ps_cmd <- paste0('Get-Content ', files, ' | ForEach-Object { $_ -replace "\\|", "`t" } | Set-Content ', file_path_t, tname)
      system(paste('powershell -Command', shQuote(ps_cmd)))
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
  month_count <- data.frame()
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
    memory.size(max = T)
    memory.limit(size = 128000)
    if(tfiles[x, "type"] == "elig") {
      df <- readr::read_delim(file_path, lazy = T, delim = "\t")
      dates <- as.data.frame(df$CLNDR_YEAR_MNTH)
      names(dates) <- c("CLNDR_YEAR_MNTH")
      min_date <- as.character(min(as.vector(dates$CLNDR_YEAR_MNTH)))
      max_date <- as.character(max(as.vector(dates$CLNDR_YEAR_MNTH)))
      min_date <- as.Date(paste0(substr(min_date, 1, 4), "-", substr(min_date, 5, 6), "-01"))
      max_date <- as.Date(ymd(paste0(substr(max_date, 1, 4), "-", substr(max_date, 5, 6), "-01")) %m+% months(1)) - 1
      db_claims <- create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)
      prev_data <- DBI::dbGetQuery(db_claims,
                                  glue::glue_sql(
                                    "SELECT TOP (1) row_count / (DATEDIFF(month, date_min, date_max) + 1) as rpm, 
                                    DATEDIFF(month, date_min, date_max) + 1 as num_mon, date_min, date_max
                        FROM {`schema`}.{`table`}
                        WHERE row_count IS NOT NULL 
                        AND data_source = 'Medicaid' 
                        AND CHARINDEX('elig', file_name, 1) > 0
                        ORDER BY delivery_date DESC", .con = db_claims))
      prev_rpm <- prev_data[1,1]
      prev_mon <- prev_data[1,2]
      curr_rpm <- row_cnt / (interval(min_date, max_date) %/% months(1) + 1)
      rpm_diff <- (curr_rpm - prev_rpm) / prev_rpm
      mcnt <- dates %>% count(CLNDR_YEAR_MNTH)
      mcnt$perc <- mcnt$n/row_cnt
      mcnt$x <- x
      month_count <- rbind(month_count, mcnt)
      rm(dates)
      rm(df)
    } else {
      
      df <- readr::read_delim(file_path, lazy = T, delim = "\t")
      dates <- as.Date(df$FROM_SRVC_DATE)
      min_date <- min(dates)
      max_date <- max(dates)
      min_date <- as.Date(paste0(format(min_date, "%Y"), "-", format(min_date, "%m"), "-01"))
      max_date <- as.Date(ymd(paste0(format(max_date, "%Y"), "-", format(max_date, "%m"), "-01")) %m+% months(1)) - 1
      db_claims <- create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)
      prev_data <- DBI::dbGetQuery(db_claims,
                                  glue::glue_sql(
                                    "SELECT TOP (1) row_count / (DATEDIFF(month, date_min, date_max) + 1) as rpm, 
                                    DATEDIFF(month, date_min, date_max) + 1 as num_mon, date_min, date_max
                        FROM {`schema`}.{`table`}
                        WHERE row_count IS NOT NULL 
                        AND data_source = 'Medicaid' 
                        AND CHARINDEX('claim', file_name, 1) > 0
                        ORDER BY delivery_date DESC", .con = db_claims))
      prev_rpm <- prev_data[1,1]
      prev_mon <- prev_data[1,2]
      curr_rpm <- row_cnt / (interval(min_date, max_date) %/% months(1) + 1)
      rpm_diff <- (curr_rpm - prev_rpm) / prev_rpm
      dates <- as.data.frame(dates)
      dates$CLNDR_YEAR_MNTH <- as.integer(format(dates$dates, "%Y%m"))
      mcnt <- dates %>% count(CLNDR_YEAR_MNTH)
      mcnt$perc <- mcnt$n/row_cnt
      mcnt$x <- x
      month_count <- rbind(month_count, mcnt)
      rm(dates)
      rm(df)
    }  
    tfiles[x,"del_date"] <- del_date
    tfiles[x,"min_date"] <- min_date
    tfiles[x,"max_date"] <- max_date
    tfiles[x,"col_qa"] <- col_qa
    tfiles[x,"row_cnt"] <- row_cnt
    tfiles[x,"rpm_diff"] <- rpm_diff
    tfiles[x,"mon_cnt"] <- nrow(mcnt)
    if(tfiles[x, 'mon_cnt'] == prev_mon) {
      tfiles[x, "monvprev"] <- "PASS"
    } else {
      tfiles[x, "monvprev"] <- "FAIL"
    }
    if(lubridate::interval(prev_data[1, "date_min"], tfiles[x,"min_date"]) %/% months(1) == 1
       && lubridate::interval(prev_data[1, "date_max"], tfiles[x,"max_date"]) %/% months(1) == 1) {
        tfiles[x, 'expdates'] <- "PASS"
    } else {
      tfiles[x, 'expdates'] <- "FAIL"
    }
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
             Months vs Prev: {tfiles[x, 'monvprev']}
             Expected Dates: {tfiles[x, 'expdates']}
             Column QA: {tfiles[x, 'col_qa']}
             Row Count: {tfiles[x, 'row_cnt']}
             Rows vs Prev: {round(tfiles[x, 'rpm_diff'] * 100, 2)}%
             CLNDR_YEAR_MNTH Percentages:"))
    mc <- month_count[month_count$x == x,]
    mc <- mc[order(mc$CLNDR_YEAR_MNTH),]
    for (i in 1:nrow(mc)) {
      message(glue("{mc[i, 'CLNDR_YEAR_MNTH']} - {mc[i, 'n']} - {round(mc[i, 'perc'] * 100, 2)}%"))
    }
    message("------------------------------")
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
      tfiles[x, "batch_id_prod"] <- load_metadata_etl_log_file(conn = db_claims, 
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
      db_claims <- create_db_connection(server = "hhsaw", interactive = F, prod = F)
      for (x in 1:nrow(tfiles)) {
        tfiles[x, "batch_id_dev"] <- load_metadata_etl_log_file(conn = db_claims, 
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
gc()
