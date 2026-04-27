pacman::p_load(odbc, keyring, R.utils, zip, tidyverse, dplyr, lubridate, glue, configr, openxlsx, apde.etl)

### SET DIRECTORIES AND TABLE INFO
if(T) {
  export_dir <- "//dphcifs/APDE-CDIP/Seattle_FreshBucks/touw/"
  temp_dir <- "C:/temp/cdr/export_test/"
  source_tables <- read.xlsx("//dphcifs/APDE-CDIP/Seattle_FreshBucks/touw/cdr_mcaid_table_metadata_2026-04-23.xlsx", 1)
  columns <- read.xlsx("//dphcifs/APDE-CDIP/Seattle_FreshBucks/touw/cdr_mcaid_table_metadata_2026-04-23.xlsx", 2)
  etl_log <- read.xlsx(file.path(here::here(),"apcd_export_import/apcd_etl_log.xlsx"), 1)
  etl_table <- "cdr_etl_log"
  batch_date <- "20260423"
  schema <- "jwhitehurst"
  config <- list()
  config[["db_name"]] <- "hhs_analytics_workspace"
  config[["server_path"]] <- "hhsaw_dev"
  config[["odbc_name"]] <- "hhsaw_dev"
}

### DROP/CREATE ETL LOG
if(T) {
  conn <- create_db_connection("hhsaw", prod = F, interactive = T)
  cols <- etl_log[, c("column_name", "column_type")]
  vars <- list()
  for(c in 1:nrow(cols)) {
    vars[cols[c, "column_name"]] <- cols[c, "column_type"]
  }
  create_table(conn, 
               to_schema = schema, 
               to_table = etl_table,
               vars = vars)
}

### POPULATE ETL LOG
if(T) {
  conn <- create_db_connection("hhsaw", prod = F, interactive = T)
  files <- data.frame(file_name = list.files(path = export_dir, pattern = paste0(batch_date, ".csv.gz")))
  for(i in 1:nrow(files)) {
    files[i, "file_schema"] <- schema
    files[i, "file_table"] <- paste0(strsplit(files[i, "file_name"], "[.]")[[1]][1], "_", strsplit(files[i, "file_name"], "[.]")[[1]][2])
    files[i, "file_number"] <- as.numeric(substring(strsplit(files[i, "file_name"], "[.]")[[1]][3], 1, 3))
    files[i, "file_date"] <- substring(files[i, "file_name"], 
                                       nchar(files[i, "file_name"]) - 14, 
                                       nchar(files[i, "file_name"]) - 7)
    files[i, "file_path"] <- paste0(export_dir, files[i, "file_name"])
  }
  files$file_date <- paste0(substring(files$file_date, 1, 4), 
                            "-",
                            substring(files$file_date, 5, 6), 
                            "-",
                            substring(files$file_date, 7, 8))
  for(i in 1:nrow(files)) {
    DBI::dbExecute(conn,
                   glue_sql("INSERT INTO {`schema`}.{`etl_table`}
                            ([file_name], [file_date], [file_schema], [file_table], 
                            [file_number], [file_path], [datetime_etl_create])
                            VALUES
                            ({files[i, 'file_name']}, {files[i, 'file_date']}, {files[i, 'file_schema']}, 
                            {files[i, 'file_table']}, {files[i, 'file_number']}, {files[i, 'file_path']},
                            GETDATE())",
                            .con = conn))
  }
}

### COPY FILE TO TEMP, EXTRACT, CREATE TABLE, LOAD, DELETE FILE
if(T) {
  conn <- create_db_connection("hhsaw", prod = F, interactive = T)
  files <- DBI::dbGetQuery(conn, 
                           glue::glue_sql("SELECT * 
                                          FROM {`schema`}.{`etl_table`}
                                          WHERE datetime_load IS NULL
                                          ORDER BY file_table, file_number",
                                          .con = conn))
  message(glue::glue("{nrow(files)} files to load..."))
  for(i in 1:nrow(files)) {
    file <- files[i,]
    message(glue::glue("{i}: Processing file - {file$file_name} - {Sys.time()}"))
    message(glue::glue("{i}: Copying file - {Sys.time()}"))
    file.copy(from = file$file_path, 
              to = paste0(temp_dir, file$file_name),
              overwrite = T)
    message(glue::glue("{i}: Extracting file - {Sys.time()}"))
    gunzip(paste0(temp_dir, file$file_name), remove = F)
    file_raw <- str_replace(paste0(temp_dir, file$file_name), ".gz", "")
    message(glue::glue("{i}: Counting rows in file - {Sys.time()}"))
    file$rows_file <- read.table(text = shell(paste("wc -l", file_raw), intern = T))[1,1]
    files[i, "rows_file"] <- file$rows_file
    DBI::dbExecute(conn,
                   glue::glue_sql("UPDATE {`schema`}.{`etl_table`}
                                  SET rows_file = {file$rows_file}
                                  WHERE etl_id = {file$etl_id}",
                                  .con = conn))
    if(file$file_number == 1) {
      vars <- list()
      cols <- columns %>% 
        filter(schema_name == strsplit(file$file_name, "[.]")[[1]][1]) %>% 
        filter(table_name == strsplit(file$file_name, "[.]")[[1]][2])
      for(c in 1:nrow(cols)) {
        vars[cols[c, "column_name"]] <- cols[c, "column_type"]
      }
      apde.etl::create_table(conn, 
                   to_schema = file$file_schema, 
                   to_table =  file$file_table,                    
                   vars = vars)
    }
    message(glue::glue("{i}: Loading data to SQL - {Sys.time()}"))
    load_table_from_file(conn = conn,
                         config = config,
                         server = config$odbc_name,
                         to_schema = file$file_schema,
                         to_table = file$file_table,
                         file_path = file_raw,
                         truncate = F,
                         first_row = 1,
                         azure = T,
                         azure_uid = keyring::key_list("hhsaw")[["username"]],
                         azure_pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]])
    )
    message(glue::glue("{i}: Counting rows in table - {Sys.time()}"))
    file$rows_loaded <- DBI::dbGetQuery(conn, 
                                        glue::glue_sql("SELECT COUNT(*) 
                                                     FROM {`file$file_schema`}.{`file$file_table`}", 
                                                       .con = conn))[1,1]
    max_file_num <- DBI::dbGetQuery(conn,
                                    glue::glue_sql("SELECT MAX(file_number)
                                                   FROM {`schema`}.{`etl_table`}
                                                   WHERE [file_date] = {file$file_date}
                                                    AND [file_schema] = {file$file_schema} 
                                                    AND [file_table] = {file$file_table}",
                                                   .con = conn))[1,1]
    if(max_file_num > 1) {
      file$rows_loaded <- file$rows_loaded - DBI::dbGetQuery(conn, 
                                                             glue::glue_sql("SELECT SUM(ISNULL([rows_loaded], 0)) 
                   FROM {`schema`}.{`etl_table`}
                   WHERE [file_date] = {file$file_date}
                    AND [file_schema] = {file$file_schema} 
                    AND [file_table] = {file$file_table}", 
                                                                            .con = conn))[1,1]
    }
    files[i, "rows_loaded"] <- file$rows_loaded
    if(file$rows_file == file$rows_loaded) {
      message(glue::glue("{i}: All rows loaded - {Sys.time()}"))
      DBI::dbExecute(conn,
                   glue::glue_sql("UPDATE {`schema`}.{`etl_table`}
                                  SET rows_loaded = {file$rows_loaded},
                                  datetime_load = GETDATE()
                                  WHERE etl_id = {file$etl_id}",
                                  .con = conn))
    } else {
      result <- paste0("ERROR: Row Count of File ", file$file_name, " (", file$rows_file, ") does NOT MATCH Rows Loaded to SQL Table (", file$rows_loaded, ")!!!")
    }
    message(glue::glue("{i}: Deleting files - {Sys.time()}"))
    unlink(paste0(temp_dir, file$file_name))
    unlink(file_raw)
  }
  message(glue::glue("Completed - {Sys.time()}"))
}
