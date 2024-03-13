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
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")

#### STEP 1: CREATE CONNECTIONS ####

##Establish connection to HHSAW prod
#interactive_auth <- dlg_list(c("TRUE", "FALSE"), title = "Interactive Authentication?")$res
interactive_auth <- FALSE #must be set to true if running from Azure VM
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

#Note that storage_upload (one file at a time) is faster than storage_multiupload (parallel uploads) for large files
#Run time for dental claim files: 104 min

## Beginning message (before loop begins)
message(paste0("Beginning process to load GZIP files to Azure Blob Storage - ", Sys.time()))

##Set up empty dataframe to hold QA results
file_count_qa_results <- data.frame(
  folder=as.character(),
  file_count=as.integer(),
  qa_result=as.character(),
  load_complete_time=as.character()
)

#Establish list of CIFS folders for which GZIP files will be loaded to Azure Blob Storage
folder_list <- list("claim_icdcm_raw", "claim_line_raw", "claim_procedure_raw", "claim_provider_raw", "dental_claim", "eligibility", "medical_claim_header",
                   "member_month_detail", "pharmacy_claim", "provider", "provider_master")

#Begin loop
lapply(folder_list, function(folder_list) {

  #Select table from list
  folder_selected <- folder_list
  message(paste0("Working on folder for: ", folder_selected, " - ", Sys.time()))
  
  #Create CIFS folder path, load list of GZIP files, and count GZIP files
  folder_path <- glue("//dphcifs/apde-cdip/apcd/apcd_data_import/", folder_selected, "_export/")
  file_paths_list <- as.list(list.files(path = file.path(folder_path), full.names = F, pattern = "*.gz", all.files = F))
  file_count_cifs <- length(file_paths_list)
  message(paste0("Number of GZIP files in CIFS folder for: ", folder_selected, " - ", file_count_cifs, " files"))
  
  #Load GZIP files to Azure Blob Storage using AzureStor package
  system.time(for (i in 1:length(file_paths_list)) {
   file_name <- file_paths_list[i]
   message(paste0("Begin Uploading ", file_name, " - ", Sys.time()))
   AzureStor::storage_upload(cont,
                             src = paste0(folder_path, file_name),
                             dest = paste0("claims/apcd/dental_claim_import/", file_name))
   message(paste0("Upload Completed - ", Sys.time()))})
   
   #Count number of GZIP files uploaded to Azure Blob Storage
   file_list_azure <- AzureStor::list_storage_files(cont, dir = glue("claims/apcd/", folder_selected, "_import/"))$name
   file_count_azure <- length(file_list_azure[grepl("*.gz$", file_list_azure)])
   message(paste0("Number of GZIP files in Azure for: ", folder_selected, " - ", file_count_azure, " files"))
   
   #QA check
   if(file_count_cifs == file_count_azure) {
     qa_files_azure <- "PASS"
   } else {
     qa_files_azure <- "FAIL"
   }
  
   if (qa_files_azure == "FAIL") {
     stop(glue::glue("Mismatching file count between CIFS and Azure Blob Storage for: ", folder_selected,
                     ". Rerun script to try loading files again."))
   } else if(qa_files_azure == "PASS") {
     message("Number of files loaded to Azure match number of files on CIFS for: ", folder_selected, ". Loading complete.")
   }
   
   #Write QA results to R dataframe
   qa_result_inner <- data.frame(folder=folder_selected, file_count = file_count_cifs, qa_result = qa_files_azure,
                                 load_complete_time = as.character(Sys.time()))
   file_count_qa_results <- bind_rows(file_count_qa_results, qa_result_inner)

})

## Closing message (after loop completes)
message(paste0("All files have been successfully loaded to Azure Blob Storage - ", Sys.time()))