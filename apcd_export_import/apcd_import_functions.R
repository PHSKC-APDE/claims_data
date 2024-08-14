## Install Packages
if("odbc" %in% rownames(installed.packages()) == F) {
  install.packages("odbc")
}
library(odbc) # Read to and write from SQL
if("curl" %in% rownames(installed.packages()) == F) {
  install.packages("curl")
}
library(curl) # Read files from FTP
if("keyring" %in% rownames(installed.packages()) == F) {
  install.packages("keyring")
}
library(keyring) # Access stored credentials
if("R.utils" %in% rownames(installed.packages()) == F) {
  install.packages("R.utils")
}
library(R.utils) # File and folder manipulation
if("zip" %in% rownames(installed.packages()) == F) {
  install.packages("zip")
}
library(zip) # Extract data from gzip
if("jsonlite" %in% rownames(installed.packages()) == F) {
  install.packages("jsonlite")
}
library(jsonlite) # Extract data from curl
if("tidyverse" %in% rownames(installed.packages()) == F) {
  install.packages("tidyverse")
}
library(tidyverse) # Manipulate data
if("dplyr" %in% rownames(installed.packages()) == F) {
  install.packages("dplyr")
}
library(dplyr) # Manipulate data
if("lubridate" %in% rownames(installed.packages()) == F) {
  install.packages("lubridate")
}
library(lubridate) # Manipulate data
if("glue" %in% rownames(installed.packages()) == F) {
  install.packages("glue")
}
library(glue) # Safely combine SQL code
if("configr" %in% rownames(installed.packages()) == F) {
  install.packages("configr")
}
library(configr) # Read in YAML files
if("xlsx" %in% rownames(installed.packages()) == F) {
  install.packages("xlsx")
}
library(xlsx) # Read in XLSX files
if("svDialogs" %in% rownames(installed.packages()) == F) {
  install.packages("svDialogs")
}
library(svDialogs) # Extra UI Elements

## Pull in APDE Common Functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/load_table_from_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/add_index.R")

## Checks if all directories exist, checks if the SFTP keyring has been set and then set it, checks if keyring credentials are valid and prompts to reset if not
apcd_prep_check_f <- function(config) {
  # Check if directories exist. Create directories that do not exist.
  if(!file.exists(base_dir)) { 
    dir.create(base_dir)
    message(paste0("The base directory (", base_dir, ") does not exist. Creating directory."))
  }
  if(!file.exists(ref_dir)) { 
    dir.create(ref_dir)
    message(paste0("The extracted ref schema directory (", ref_dir, ") does not exist. Creating directory."))
  }
  if(!file.exists(stage_dir)) { 
    dir.create(stage_dir)
    message(paste0("The extracted stage schema directory (", stage_dir, ") does not exist. Creating directory."))
  }
  if(!file.exists(final_dir)) { 
    dir.create(final_dir)
    message(paste0("The extracted final schema directory (", final_dir, ") does not exist. Creating directory."))
  }
  
  # Check SFTP Keyring and Credentials
  cred_check <- 0
  while(cred_check == 0) {
    if(nrow(key_list(config$ftp_keyring)) == 0) {
      message(paste0("The keyring for the SFTP (", config$ftp_keyring, ") has not been created."))
      username <- dlgInput("Username:")$res
      keyring::key_set(service = config$ftp_keyring,
                       username = username)
    }
    h <- curl::new_handle()
    curl::handle_setopt(handle = h, httpauth = 1, userpwd = paste0(key_list(config$ftp_keyring)[["username"]], ":", key_get(config$ftp_keyring, key_list(config$ftp_keyring)[["username"]])))
    json <- curl::curl_fetch_memory(config$ftp_url, handle = h)
    if(str_detect(rawToChar(json$content), "Login failed")) {
      message(paste0("Current SFTP Credentials Invalid. Reset keyring (", config$ftp_keyring, ") for SFTP."))
      username <- dlgInput("Username:")$res
      keyring::key_set(service = config$ftp_keyring,
                       username = username)
    } else {
      cred_check <- 1
    }
  }
}

## Checks if ETL log table exists, creates table if not
apcd_etl_check_function_f <- function(config) {
  conn <- DBI::dbConnect(odbc::odbc(), config$odbc_name)
  if(!DBI::dbExistsTable(conn, DBI::Id(schema = config$ref_schema, table = config$etl_table))) {
    vars <- apcd_get_table_vars_f(table_file_path = config$table_file_path, 
                                  schema = config$ref_schema, 
                                  table = config$etl_table)
    create_table(conn, 
                 to_schema = config$ref_schema,
                 to_table = config$etl_table,
                 vars = vars)
  }
}

## Returns entire ETL log table along with the max file number for each table
apcd_etl_get_list_f <- function(config) {
  apcd_etl_check_function_f(config)
  conn <- DBI::dbConnect(odbc::odbc(), config$odbc_name)
  results <- DBI::dbGetQuery(conn,
                             glue::glue_sql("SELECT a.[etl_id], a.[file_date], a.[file_number], b.[max_file_num], 
             a.[rows_file], a.[rows_loaded], a.[datetime_etl_create], 
             a.[datetime_download], a.[datetime_load], a.[datetime_archive], 
             a.[datetime_delete], a.[file_schema], a.[file_table], 
             a.[file_name], a.[file_path] 
             FROM {`config$ref_schema`}.{`config$etl_table`} a 
             INNER JOIN (SELECT [file_date], [file_schema], [file_table], 
              MAX([file_number]) AS max_file_num 
              FROM {`config$ref_schema`}.{`config$etl_table`} 
              GROUP BY [file_date], [file_schema], [file_table]) b 
              ON a.[file_date] = b.file_date 
                AND a.[file_schema] = b.[file_schema] 
                AND a.[file_table] = b.[file_table] 
             ORDER BY a.[etl_id]",
                                            .con = conn))
  return(results)
}

## Creates a new ETL entry in the ETL log and returns the etl_id OR updates an ETL entry
apcd_etl_entry_f <- function(config,
                             etl_id = NULL,
                             file_name = NULL,
                             file_date = NULL,
                             file_schema = NULL,
                             file_table = NULL,
                             file_number = NULL,
                             column_name = NULL,
                             value = NULL) {
  conn <- DBI::dbConnect(odbc::odbc(), config$odbc_name)
  if(is.null(etl_id)) {
    if(is.null(file_name)) {
      stop("To create a new ETL entry, file_name is requried.")
    }
    if(is.null(file_date)) {
      stop("To create a new ETL entry, file_name is requried.")
    }
    if(is.null(file_schema)) {
      stop("To create a new ETL entry, file_name is requried.")
    }
    if(is.null(file_table)) {
      stop("To create a new ETL entry, file_name is requried.")
    }
    if(is.null(file_number)) {
      stop("To create a new ETL entry, file_name is requried.")
    }
    DBI::dbExecute(conn,
                   glue_sql("INSERT INTO {`config$ref_schema`}.{`config$etl_table`}
                            ([file_name], [file_date], [file_schema], [file_table], 
                            [file_number], [datetime_etl_create])
                            VALUES
                            ({file_name}, {file_date}, {file_schema}, {file_table}, 
                            {file_number}, GETDATE())",
                            .con = conn))
    results <- DBI::dbGetQuery(conn,
                               glue_sql("SELECT [etl_id] 
                                      FROM {`config$ref_schema`}.{`config$etl_table`}
                                      WHERE [file_name] = {file_name}",
                                        .con = conn))
    results <- as.numeric(results)
    message(paste0("ETL Entry created for ETL ID: ", results, " - ", file_name))
    return(results)
  } else {
    if(is.null(column_name)) {
      stop("Specify column_name to update ETL entry.")
    }
    if(is.null(value)) {
      value <- DBI::SQL("GETDATE()")
    }
    DBI::dbExecute(conn,
                   glue_sql("UPDATE {`config$ref_schema`}.{`config$etl_table`}
                            SET {`column_name`} = {value} 
                            WHERE [etl_id] = {etl_id}",
                            .con = conn))
    message(paste0("ETL ID: ", etl_id, " - Updated ", column_name))
  }
}

## Returns the data for a single ETL entry from the ETL log
apcd_etl_get_entry_f <- function(config,
                                 etl_id) {
  apcd_etl_check_function_f(config)
  conn <- DBI::dbConnect(odbc::odbc(), config$odbc_name)
  results <- DBI::dbGetQuery(conn,
                             glue_sql("SELECT [etl_id], [file_date], [file_number], 
                                      [rows_file], [rows_loaded], [datetime_etl_create], 
                                      [datetime_download], [datetime_load], 
                                      [datetime_archive], [datetime_delete], 
                                      [file_schema], [file_table], [file_name], [file_path]                                       
                                      FROM {`config$ref_schema`}.{`config$etl_table`}                                      
                                      WHERE [etl_id] = {etl_id}",
                                      .con = conn))
  return(results)
}

## Returns the value of a single column for a single ETL entry from the ETL log
apcd_etl_get_entry_value_f <- function(config,
                                       etl_id,
                                       column_name) {
  apcd_etl_check_function_f(config)
  conn <- DBI::dbConnect(odbc::odbc(), config$odbc_name)
  results <- DBI::dbGetQuery(conn,
                             glue_sql("SELECT {`column_name`}                                       
                                      FROM {`config$ref_schema`}.{`config$etl_table`}                                      
                                      WHERE [etl_id] = {etl_id}",
                                      .con = conn))
  return(results[1,1])
}

## Gets a list of table variables from the table list Excel file
apcd_get_table_vars_f <- function(table_file_path, 
                                  schema, 
                                  table) {
  tables <- read.xlsx(table_file_path, 1)
  sel_table <- tables %>%
    filter(schema_name == schema) %>%
    filter(table_name == table)
  cols <- sel_table[, c("column_name", "column_type")]
  vars <- list()
  for(c in 1:nrow(cols)) {
    vars[cols[c, "column_name"]] <- cols[c, "column_type"]
  }
  return(vars)
}

## Returns a list of files from the SFTP (all 3 directories), determines schema, table, file number and file date
apcd_ftp_get_file_list_f <- function(config) {
  # Get list of files from SFTP
  h <- curl::new_handle()
  curl::handle_setopt(handle = h, httpauth = 1, userpwd = paste0(key_list(config$ftp_keyring)[["username"]], ":", key_get(config$ftp_keyring, key_list(config$ftp_keyring)[["username"]])))
  url <- paste0(config$ftp_url, "ref_schema/")
  json <- curl::curl_fetch_memory(url, handle = h)
  ftpfiles <- fromJSON(rawToChar(json$content))
  rfiles <- cbind(ftpfiles[["files"]]["fileName"], ftpfiles[["files"]]["lastModifiedTime"])
  rfiles$url <- paste0(url,rfiles$fileName)
  rfiles$schema <- "ref"
  h <- curl::new_handle()
  curl::handle_setopt(handle = h, httpauth = 1, userpwd = paste0(key_list(config$ftp_keyring)[["username"]], ":", key_get(config$ftp_keyring, key_list(config$ftp_keyring)[["username"]])))
  url <- paste0(config$ftp_url, "stage_schema/")
  json <- curl::curl_fetch_memory(url, handle = h)
  ftpfiles <- fromJSON(rawToChar(json$content))
  sfiles <- cbind(ftpfiles[["files"]]["fileName"], ftpfiles[["files"]]["lastModifiedTime"])
  sfiles$url <- paste0(url,sfiles$fileName)
  sfiles$schema <- "stage"
  h <- curl::new_handle()
  curl::handle_setopt(handle = h, httpauth = 1, userpwd = paste0(key_list(config$ftp_keyring)[["username"]], ":", key_get(config$ftp_keyring, key_list(config$ftp_keyring)[["username"]])))
  url <- paste0(config$ftp_url, "final_schema/")
  json <- curl::curl_fetch_memory(url, handle = h)
  ftpfiles <- fromJSON(rawToChar(json$content))
  ffiles <- cbind(ftpfiles[["files"]]["fileName"], ftpfiles[["files"]]["lastModifiedTime"])
  ffiles$url <- paste0(url,ffiles$fileName)
  ffiles$schema <- "final"
  files <- rbind(rfiles, sfiles, ffiles)
  files <- files[, !(names(files) %in% c("lastModifiedTime"))]
  colnames(files) <- c("file_name", "url", "schema")
  for(f in 1:nrow(files)) {
    files[f, "table"] <- strsplit(files[f, "file_name"], "[.]")[[1]][2]
    files[f, "file_number"] <- as.numeric(substring(strsplit(files[f, "file_name"], "[.]")[[1]][3], 1, 3))
    files[f, "file_date"] <- substring(files[f, "file_name"], 
                                       nchar(files[f, "file_name"]) - 14, 
                                       nchar(files[f, "file_name"]) - 7)
  }
  files$file_date <- paste0(substring(files$file_date, 1, 4), 
                            "-",
                            substring(files$file_date, 5, 6), 
                            "-",
                            substring(files$file_date, 7, 8))
  return(files)
}

## Downloads file from SFTP and saves it to the specified directory. Updates the ETL log with file_path and datetime_download. Returns the datetime_download
apcd_ftp_get_file_f <- function(config,  
                                file) {
  h <- curl::new_handle()
  curl::handle_setopt(handle = h, httpauth = 1, userpwd = paste0(key_list(config$ftp_keyring)[["username"]], ":", key_get(config$ftp_keyring, key_list(config$ftp_keyring)[["username"]])))
  # Download file
  curl::curl_download(url = file$url, 
                      destfile = file$file_path,
                      quiet = T,
                      handle = h)
  # Update ETL log
  apcd_etl_entry_f(config,
                   etl_id = file$etl_id,
                   column_name = "datetime_download")
  apcd_etl_entry_f(config,
                   etl_id = file$etl_id,
                   column_name = "file_path",
                   value = file$file_path)
  return(apcd_etl_get_entry_value_f(config, file$etl_id, "datetime_download"))
}

## Extracts file with gzip, counts the files rows, updates ETL log, archives old data, creates new table (if needed), loads data via BCP, counts rows loaded, updates ETL log
apcd_data_load_f <- function(config,
                             file) {
  # Extract file
  message(paste0("......Extracting File: "  , file$file_name, "..."))
  if(file.exists(str_replace(file$file_path, '.gz', ''))) {
    file.remove(str_replace(file$file_path, '.gz', ''))
  }
  gunzip(file$file_path, remove = F)
  message("......Extracting Complete...")
  # Count rows in file and update ETL log
  message("......Counting Rows in File...")
  file_raw <- str_replace(file$file_path, ".gz", "")
  file$rows_file <- read.table(text = shell(paste("wc -l", file_raw), intern = T))[1,1]
  apcd_etl_entry_f(config,
                   etl_id = file$etl_id,
                   column_name = "rows_file",
                   value = file$rows_file)
  message("......Counting Complete... ")
  
  # Check for old table, archive table and delete previously archived table
  if(file$file_num == 1) {
    apcd_data_archive_f(config, 
                        schema_name = file$file_schema,
                        table_name = file$file_table)
  }
  conn <- DBI::dbConnect(odbc::odbc(), config$odbc_name)
  # Create table if the file is the first file for the table
  if(file$file_number == 1) {
    vars <- apcd_get_table_vars_f(table_file_path = config$table_file_path, 
                                  schema = file$file_schema, 
                                  table = file$file_table)
    
    create_table(conn, 
                 to_schema = file$file_schema, 
                 to_table = file$file_table,                    
                 vars = vars)
  }
  # Create table if the file is the first file for the table
  if(file$file_number == 1) {
    vars <- apcd_get_table_vars_f(table_file_path = config$table_file_path, 
                                  schema = file$file_schema, 
                                  table = file$file_table)
    
    create_table(conn, 
                 to_schema = file$file_schema, 
                 to_table = file$file_table,                    
                 vars = vars)
  }
  # Load data via BCP
  message("......Loading Data to SQL... ")
  load_table_from_file(conn = conn,
                       config = config,
                       server = "apcd",
                       to_schema = file$file_schema,
                       to_table = file$file_table,
                       file_path = file_raw,
                       truncate = F,
                       first_row = 1)
  message("......Loading Complete... ")
  
  # Count rows in table, subtract row counts from previously loaded files
  message("......Counting Rows in SQL Table...")
  file$rows_loaded <- DBI::dbGetQuery(conn, 
                                      glue::glue_sql("SELECT COUNT(*) 
                                                     FROM {`file$file_schema`}.{`file$file_table`}", 
                                                     .con = conn))[1,1]
  file$rows_loaded <- file$rows_loaded - DBI::dbGetQuery(conn, 
                                                         glue::glue_sql("SELECT SUM(ISNULL([rows_loaded], 0)) 
                   FROM {`config$ref_schema`}.{`config$etl_table`}
                   WHERE [file_date] = {file$file_date}
                    AND [file_schema] = {file$file_schema} 
                    AND [file_table] = {file$file_table}", 
                                                                        .con = conn))[1,1]
  # Update rows_loaded in ETL log
  apcd_etl_entry_f(config,
                   etl_id = file$etl_id,
                   column_name = "rows_loaded",
                   value = file$rows_loaded)
  message("......Counting Complete... ")
  # Compare rows loaded to the rows in the file
  if(file$rows_file == file$rows_loaded) {
    message("......All Rows Successfully Loaded to SQL Table...")
    # Update datetime_load in ETL log
    conn <- DBI::dbConnect(odbc::odbc(), config$odbc_name)
    apcd_etl_entry_f(config,
                     etl_id = file$etl_id,
                     column_name = "datetime_load")
    result <- NA
  } else {
    result <- paste0("ERROR: Row Count of File ", file$file_name, " (", file$rows_file, ") does NOT MATCH Rows Loaded to SQL Table (", file$rows_loaded, ")!!!")
  }
  # If the file is the last file for the table, add an index to the table
  if(file$file_schema != config$ref_schema && file$file_num == file$max_file_num && is.na(result)) {
    message("......Adding Index... ")
    conn <- DBI::dbConnect(odbc::odbc(), config$odbc_name)
    index_name <- paste0(config$index_prefix, 
                         file$file_schema,
                         "_",
                         file$file_table)
    add_index(conn = conn,
              config = config,
              server = "apcd",
              to_schema = file$file_schema,
              to_table = file$file_table,
              index_type = config$index_type,
              index_name = index_name)
    
    message("......Index Added... ")
  }
  # Remove extracted file
  message("......Deleting Extracted File...")
  unlink(file_raw)
  return(result)
}

## Check for Archive table, Delete Old Archive table, Check for Old table, Delete Index from Old table, Archive Old Table
apcd_data_archive_f <- function(config, 
                                schema_name,  
                                table_name) {
  conn <- DBI::dbConnect(odbc::odbc(), config$odbc_name)
  # Check for old archive table and delete it
  if(DBI::dbExistsTable(conn, DBI::Id(schema = schema_name, table = paste0(table_name, config$archive_suffix)))) {
    message("......Dropping Old Archived SQL Table...")
    DBI::dbExecute(conn,
                   glue::glue_sql("DROP TABLE {`schema_name`}.{`paste0(table_name, config$archive_suffix)`}",
                                  .con = conn))
    # Update ETL log datetime_delete for all entries affected by the deleting old archive table
    to_del <- DBI::dbGetQuery(conn,
                              glue::glue_sql("SELECT etl_id
                                             FROM {`config$ref_schema`}.{`config$etl_table`}
                                             WHERE datetime_delete IS NULL
                                              AND datetime_archive IS NOT NULL
                                              AND datetime_load IS NOT NULL
                                              AND file_schema = {schema_name}
                                              AND file_table = {table_name} ",
                                             .con = conn))
    if(nrow(to_del) > 0) {
      for(i in 1:nrow(to_del))
        apcd_etl_entry_f(config,
                         etl_id = as.numeric(to_del[i]$etl_id),
                         column_name = "datetime_delete")
    }
  }
  conn <- DBI::dbConnect(odbc::odbc(), config$odbc_name)
  # Check for old table and archive it by renaming table with the archive suffix
  if(DBI::dbExistsTable(conn, DBI::Id(schema = schema_name, table = table_name))) {
    message("......Archiving Old SQL Table...")
    # Check if old table has an index and delete it
    if(schema_name != config$ref_schema) {
      existing_index <- DBI::dbGetQuery(conn, 
                                        glue::glue_sql("SELECT DISTINCT a.index_name 
                                                       FROM 
                                                       (SELECT ind.name AS index_name                        
                                                       FROM                        
                                                       (SELECT object_id, name, type_desc FROM sys.indexes  
                                                       WHERE type_desc LIKE 'CLUSTERED%') ind               
                                                       INNER JOIN                   
                                                       (SELECT name, schema_id, object_id FROM sys.tables   
                                                       WHERE name = {table_name}) t                    
                                                       ON ind.object_id = t.object_id          
                                                       INNER JOIN                     
                                                       (SELECT name, schema_id FROM sys.schemas   
                                                       WHERE name = {schema_name}) s                
                                                       ON t.schema_id = s.schema_id) a",      
                                                       .con = conn))
      
      if (nrow(existing_index) != 0) {
        message(".........Dropping Index on Old SQL Table...")
        lapply(seq_along(existing_index), function(i) {
          DBI::dbExecute(conn,
                         glue::glue_sql("DROP INDEX {`existing_index[['index_name']][[i]]`} 
                                        ON {`schema_name`}.{`table_name`}", 
                                        .con = conn))
        })
      }
    }
    # Rename table
    conn <- DBI::dbConnect(odbc::odbc(), config$odbc_name)
    DBI::dbExecute(conn,
                   glue::glue_sql("EXEC sp_rename 
                                    {paste0(schema_name, '.', table_name)}, 
                                    {paste0(table_name, config$archive_suffix)}",
                                  .con = conn))
    # Update ETL log datetime_archive for all entries affected by the archive old table
    to_archive <- DBI::dbGetQuery(conn,
                                  glue::glue_sql("SELECT etl_id
                                             FROM {`config$ref_schema`}.{`config$etl_table`}
                                             WHERE datetime_delete IS NULL
                                              AND datetime_archive IS NULL
                                              AND datetime_load IS NOT NULL
                                              AND file_schema = {schema_name}
                                              AND file_table = {table_name} ",
                                                 .con = conn))
    if(nrow(to_archive) > 0) {
      for(i in 1:nrow(to_archive))
        apcd_etl_entry_f(config,
                         etl_id = as.numeric(to_archive[i]$etl_id),
                         column_name = "datetime_archive")
    }
    message(paste0("......Old Sql Table (", 
                   schema_name, ".", table_name,
                   ") has been Archived (",
                   schema_name, ".", table_name, config$archive_suffix, ")..."))
    
  }
}