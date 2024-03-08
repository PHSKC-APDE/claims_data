#### MASTER CODE TO UPLOAD WA-APCD GZIP files to Azure Blob Storage
#
# Eli Kern, PHSKC-APDE
#
# Adapted code from Jeremy Whitehurst, PHSKC (APDE)
#
# 2024-02


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

pacman::p_load(tidyverse, odbc, configr, glue, keyring, AzureStor, AzureAuth, svDialogs, R.utils, zip) # Load list of packages

#keyring::key_set('adl_tenant', username = 'dev')
#keyring::key_set('adl_app', username = 'dev')
keyring::key_list()

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")

#### STEP 1: CREATE CONNECTIONS ####

##Establish connection to HHSAW prod
#interactive_auth <- dlg_list(c("TRUE", "FALSE"), title = "Interactive Authentication?")$res
interactive_auth <- FALSE
#prod <- dlg_list(c("TRUE", "FALSE"), title = "Production Server?")$res
prod <- TRUE
db_claims <- create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)

##Establish connection to Azure Blob Storage
#This should create popup window in browser that automatically authenticates
#For first time only, you will have to submit an approval request to KCIT, follow up with Philip Sylling for help if needed
blob_token <- AzureAuth::get_azure_token(
  resource = "https://storage.azure.com", 
  tenant = keyring::key_get("adl_tenant", "dev"),
  app = keyring::key_get("adl_app", "dev"),
  auth_type = "authorization_code",
  use_cache = F
)
blob_endp <- storage_endpoint("https://inthealthdtalakegen2.blob.core.windows.net", token = blob_token)
cont <- storage_container(blob_endp, "inthealth")


#### STEP 2: UPLOAD GZIP FILES TO AZURE BLOG STORAGE ####

## Test a single file (provider master)
message(paste0("Uploading test WA-APCD file - ", Sys.time()))
file_read_folder <- "//dphcifs/apde-cdip/apcd/apcd_data_import/provider_master_export/"
#Note that for first time I had to add RStudio Session to Windows Firewall exception
system.time(
  AzureStor::storage_multiupload(cont,
                                 src = paste0(file_read_folder, "*.gz"),
                                 dest = paste0("claims/apcd/provider_master_import")))

##Test on dental data (multiple files) using parallel connections
#Run time: 126min
# message(paste0("Uploading test multiple WA-APCD files - ", Sys.time()))
# file_read_folder <- "//dphcifs/apde-cdip/apcd/apcd_data_import/dental_claim_export/"
# system.time(
#   AzureStor::storage_multiupload(cont,
#                                  src = paste0(file_read_folder, "*.gz"),
#                                  dest = paste0("claims/apcd")))

##Then test same code using a for loop to load one file at a time - this might end up being faster for large files like this
#For now use repetitive code but if this option is faster, then replace with a loop just as Jeremy does
file_read_folder <- "//dphcifs/apde-cdip/apcd/apcd_data_import/dental_claim_export/"
file_paths_list <- as.list(list.files(path = file.path(file_read_folder), full.names = F, pattern = "*.gz", all.files = F))
system.time(for (i in 1:length(file_paths_list)) {
 file_name <- file_paths_list[i]
 message(paste0("Begin Uploading/Renaming ", file_name, " - ", Sys.time()))
 AzureStor::storage_upload(cont,
                           src = paste0(file_read_folder, file_name),
                           dest = paste0("claims/apcd/dental_claim_import/", file_name))
 message(paste0("Upload Completed - ", Sys.time()))
})




system.time(AzureStor::storage_upload(cont,
                                      src = paste0(file_read_folder, "provider_master_000.csv.gz"),
                                      dest = paste0("claims/apcd/", "provider_master_000.csv.gz")))






#### Jeremy's code ####

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

rm(list=ls())
