### STEP 1: LOAD FUNCTIONS, CONFIG FILE, DEFINE DIRECTORY VARIABLES, AND CHECK CREDENTIALS
if(T) {
  message("STEP 1: Loading Functions, Config File, Defining Variables, and Check SFTP Credentials...")
  source("apcd_import_functions.R")
  config <- yaml::read_yaml("apcd_import_config.yaml")
  #Define directories for downloaded files and extracted files.
  base_dir <- config$base_dir
  ref_dir <- paste0(base_dir, "/ref_schema")
  stage_dir <- paste0(base_dir, "/stage_schema")
  final_dir <- paste0(base_dir, "/final_schema")
  apcd_prep_check_f(config)
  files <- data.frame()
  memory.limit(size = 56000)
}

### STEP 2: REVIEW SFTP FILES AND CREATE ETL ENTRIES
if(T) {
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
}

### STEP 3: CHOOSE SCHEMAS AND TABLES TO DOWNLOAD, THEN DOWNLOAD FILES
if(T) {
  # Select which schemas and tables to download the files
  etl_list <- apcd_etl_get_list_f(config)
  if(nrow(files) > 0) {
    files <- files %>% left_join(etl_list) %>% filter(is.na(datetime_download))
  } else {
    files <- etl_list %>% filter(is.na(datetime_download))
  }
  schemas <- dlg_list(unique(files$file_schema), 
                      multiple = T,
                      title = "Select File Schemas to Download")$res
  files <- files[files$file_schema %in% schemas, ]
  tables <- dlg_list(unique(files$file_table), 
                     multiple = T,
                     preselect = unique(files$file_table),
                     title = "Select File Tables to Download")$res
  files <- files[files$file_table %in% tables, ]
  
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
}

### STEP 4: EXTRACT AND LOAD DATA FROM FILES INTO SQL
if(T) {
  # Select which schemas and tables to import
  etl_list <- apcd_etl_get_list_f(config)
  files <- etl_list %>% filter(is.na(datetime_load)) %>% filter(!is.na(datetime_download))
  files$schema_table <- paste0(files$file_schema, ".", files$file_table)
  files <- files[order(files$schema_table), ]
  schemas <- dlg_list(unique(files$file_schema), 
                      multiple = T,
                      title = "Select File Schemas to Download")$res
  files <- files[files$file_schema %in% schemas, ]
  tables <- dlg_list(unique(files$schema_table), 
                     multiple = T,
                     preselect = unique(files$schema_table),
                     title = "Select File Tables to Download")$res
  files <- files[files$schema_table %in% tables, ]
  message(paste0("Begin Loading ", nrow(files), " Files into SQL Server..."))
  import_errors <- list()
  for(f in 1:nrow(files)) {
    message(paste0("...Loading File: "  , f, ": ", files[f, "file_name"], "..."))
    result <- apcd_data_load_f(config, file = files[f, ])  
    message(paste0("......Loading Complete. ", nrow(files) - f, " of ", nrow(files), " left to import..."))
    if(!is.na(result)) {
      import_errors <- append(import_errors, result)
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
}

