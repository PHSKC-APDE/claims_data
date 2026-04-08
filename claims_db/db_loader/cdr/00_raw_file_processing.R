pacman::p_load(tidyverse, odbc, configr, glue, keyring, AzureStor, AzureAuth, svDialogs, R.utils, zip, apde.etl, fpeek, tibble, xlsx, utils, stringr, readr, lubridate, dplyr, data.table, gpg) # Load list of packages

devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/copy_into.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/load_table_from_file.R")
interactive_auth <- FALSE
prod <- TRUE
server <- "hhsaw"

conn_db <- create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)
conn_dw <- create_db_connection(server = "inthealth", interactive = interactive_auth, prod = prod)

batch <- "20260319"
dir_raw <- "//dphcifs/APDE-CDIP/Mcaid-Mcare/cdr_raw/"
dir_batch <- paste0(dir_raw, batch, "/")
dir_txt <- paste0(dir_batch, "txt/")
dir_clean <- paste0(dir_batch, "txt_clean/")
dir_gz <- paste0(dir_batch, "gz/")
base_path <- paste0("https://inthealthdtalakegen2.blob.core.windows.net/inthealth/cdr/", batch, "/")
schema <- "cdr"
stg_schema <- "stg_cdr"

files <- data.frame("fileName" = list.files(paste0(dir_txt), pattern="*.txt"))
files$table_name <- substring(files$fileName, 5)
for(i in 1:nrow(files)) {
  files[i, "table_name"] <- substring(files[i, "table_name"], 1, survPen::instr(files[i, "table_name"], "\\.", n = 1) - 1)
}

headers <- files[substr(files$fileName, nchar(files$fileName) - 13, nchar(files$fileName)) == "HeaderOnly.txt",]
files <- files[substr(files$fileName, nchar(files$fileName) - 13, nchar(files$fileName)) != "HeaderOnly.txt",]

### GET Column Names
columns <- data.frame(matrix(ncol = 6, nrow = 0))
colnames(columns) <- c("table_name", "field_name", "column_name", "column_desc", "column_type", "column_order")
c <- 1
for(i in 1:nrow(headers)) {
  con <- file(paste0(dir_txt, headers[i, "fileName"]))
  suppressWarnings(txt <- readLines(con))
  close(con)
  cols <- stringr::str_split(stringr::str_replace_all(txt, "~@~", ""), "\\|\\@\\|", simplify = T)
  for(x in 1:length(cols)) {
    columns[c, "table_name"] <- headers[i, "table_name"]
    columns[c, "field_name"] <- cols[1, x]
    columns[c, "column_name"] <- stringr::str_replace_all(
                                    stringr::str_replace_all(
                                        stringr::str_replace_all(
                                          stringr::str_replace_all(
                                            gsub("\\(|\\)", "", 
                                                tolower(cols[1, x])), 
                                            ",", ""),
                                          " ", "_"), 
                                        "-", "_"), 
                                    "/", "_")
    columns[c, "column_type"] <- "VARCHAR(255)"
    columns[c, "column_order"] <- x
    c <- c + 1
  }
  columns[c, "table_name"] <- headers[i, "table_name"]
  columns[c, "field_name"] <- "ETL ID"
  columns[c, "column_name"] <- "etl_id"
  columns[c, "column_desc"] <- "ETL ID"
  columns[c, "column_type"] <- "INTEGER"
  columns[c, "column_order"] <- x + 1
  c <- c + 1
}

write.csv(columns, "C:/temp/columns.csv")

## Get row counts from provided file and column counts based on header files
row_cnt <- xlsx::read.xlsx(paste0(dir_batch, "DataValidationSummary.xlsx"), sheetIndex = 1)
colnames(row_cnt) <- c("fileName", "row_cnt")
files <- inner_join(files, row_cnt)
col_cnt <- columns %>% dplyr::count(table_name)
colnames(col_cnt) <- c("table_name", "col_cnt")
col_cnt$col_cnt <- col_cnt$col_cnt - 1
files <- inner_join(files, col_cnt)

# Clean GZIP file in chunks: remove all bytes that are not printable ASCII (raw-based)
clean_ascii_only_gzip <- function(src_dir, src_file, chunk_size = 50*1024*1024) {
  # Clean file name and create empty temp file
  clean_file <- paste0(substring(src_file, 1, nchar(src_file) - 7), ".clean.txt.gz")
  tmp_out <- tempfile(fileext = ".gz")
  # Open connections for streaming
  con_in  <- gzfile(paste0(src_dir, src_file), "rb")
  con_out <- gzfile(tmp_out, "wb")
  # Read chunks
  repeat {
    chunk <- readBin(con_in, "raw", n = chunk_size)
    if(length(chunk) == 0) break
    # Replace all non-printable ASCII bytes (outside 0x20–0x7E) with space
    chunk[chunk < as.raw(0x20) | chunk > as.raw(0x7E)] <- as.raw(0x20)
    writeBin(chunk, con_out)
  }
  # Close connections
  close(con_in)
  close(con_out)
  # Move temp to gz directory
  file.rename(from = tmp_out,  to = paste0(src_dir, clean_file))
}

for(i in 1:nrow(files)) {
  system.time(
    clean_ascii_only_gzip(src_dir = dir_gz, 
                          src_file = paste0(files[i, "table_name"], 
                                            "_", batch, ".txt.gz")))
}


## Split files and compress with GZip and upload to Azure blob
maxid <- DBI::dbGetQuery(conn_db,
                         glue::glue_sql("SELECT MAX(etl_id) FROM {`schema`}.[metadata_etl_log]",
                                        .con = conn_db))[1,1]
if(is.na(maxid) == T) {
  maxid <- 0
}

blob_token <- AzureAuth::get_azure_token(
  resource = "https://storage.azure.com", 
  tenant = keyring::key_get("adl_tenant", "dev"),
  app = keyring::key_get("adl_app", "dev"),
  auth_type = "authorization_code",
  use_cache = F)
blob_endp <- storage_endpoint("https://inthealthdtalakegen2.blob.core.windows.net", token = blob_token)
cont <- storage_container(blob_endp, "inthealth")

for(i in 1:nrow(files)) {
    file_name <- paste0(files[i, "table_name"], "_", batch, ".clean.txt.gz")
    file_path <- paste0(dir_gz, file_name)
  
  files[i, "file_path"] <- paste0(base_path, file_name)
  storage_upload(cont, 
                 file_path, 
                 paste0("cdr/", batch, "/", file_name))
  
}


for(i in 1:nrow(files)) {
  files[i, "etl_id"] <- maxid + i
  file_name <- paste0(dir_gz, files[i, "table_name"], "_", batch, ".txt.gz")
  gzip(paste0(dir_txt, files[i, "fileName"]), 
         destname = file_name,
         remove = F)
  files[i, "file_path"] <- paste0(base_path, file_name)
  storage_upload(cont, 
                 file_name, 
                 paste0("cdr/", batch, "/", files[i, "table_name"], "_", batch, ".txt.gz"))
  DBI::dbExecute(conn_db,
                 glue::glue_sql("INSERT INTO {`schema`}.[metadata_etl_log]
                                (etl_id, batch_date, file_name,
                                table_name, file_location, file_path, 
                                file_qa_col_cnt, file_qa_row_cnt, 
                                etl_entry_datetime, last_update_datetime)
                                VALUES
                                ({files[i, 'etl_id']},
                                {batch},
                                {files[i, 'fileName']},
                                {files[i, 'table_name']},
                                {dir_txt},
                                {files[i, 'file_path']},
                                {files[i, 'col_cnt']},
                                {files[i, 'row_cnt']},
                                GETDATE(), GETDATE())",
                                .con = conn_db))
}



if(T) {
  conn_db <- create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)
  conn_dw <- create_db_connection(server = "inthealth", interactive = interactive_auth, prod = prod)
  batches <- DBI::dbGetQuery(conn_db, 
                             "SELECT DISTINCT
                             batch_date
                             FROM cdr.metadata_etl_log 
                             batch_date")
  
  batch <- dlg_list(batches[,"batch_date"], title = "Select Batch to Load Raw Files")$res
  if(T == T) {
    conn_db <- create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)
    conn_dw <- create_db_connection(server = "inthealth", interactive = interactive_auth, prod = prod)
    files <- DBI::dbGetQuery(conn_db,
                             glue::glue_sql("SELECT *
                                          FROM cdr.metadata_etl_log 
                                          WHERE batch_date = {batch}
                                          ORDER BY etl_id",
                                            .con = conn_db))

    for(i in 1:nrow(files)) {
      file <- files[i,]
      table <- file$table_name
      table_raw <- paste0("raw_", table, '_', batch)
      table_archive <- paste0("archive_", table)
      message(paste0(Sys.time(), " - ", i, " : ", file$etl_id))
      message(paste0("...Begin loading ", file$file_name, " to [", stg_schema, "].[", table_raw, "]..."))
      vars <- DBI::dbGetQuery(conn_db,
                              glue::glue_sql("SELECT * 
                                             FROM {`schema`}.[ref_tables]
                                             WHERE etl_id = {file$etl_id}
                                             ORDER BY column_order",
                                             .con = conn_db))
      vars <- vars[vars$column_name != "etl_id",]
      raw_table_config <- list()
      #for(v in 1:nrow(vars)) {
      #  raw_table_config$vars[vars[v, "column_name"]] <- "VARCHAR(255)"
      #}
      raw_table_config$hhsaw$to_schema <- stg_schema
      raw_table_config$hhsaw$to_table <- table_raw
      raw_table_config$hhsaw$base_url <- "https://inthealthdtalakegen2.dfs.core.windows.net/inthealth/"
      
      if (DBI::dbExistsTable(conn_dw, DBI::Id( schema = stg_schema, table = table_raw))) {
        DBI::dbExecute(conn_dw, 
                       glue::glue_sql("DROP TABLE {`stg_schema`}.{`table_raw`}",
                                      .con = conn_dw))
      }
      
      
      if(sum(str_detect(vars$column_name, "patient_id")) > 0) {
        index_sql <- " WITH (CLUSTERED INDEX (patient_id));"
      } else if(sum(str_detect(vars$column_name, "patientid")) > 0) {
        index_sql <- " WITH (CLUSTERED INDEX (patientid));"
      } else {
        index_sql <- ""
      }
      
      sql <- glue::glue_sql("CREATE TABLE {`stg_schema`}.{`table_raw`} (
                            {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`vars$column_name`} {DBI::SQL(vars$column_type)} NULL', 
                            .con = conn_dw), sep = ', \n'))}) {DBI::SQL(index_sql)}", .con = conn_dw)
      message(sql)
      DBI::dbExecute(conn_dw, sql)
      
      sql <- glue::glue_sql(
"TRUNCATE TABLE {`stg_schema`}.{`table_raw`};
COPY INTO {`stg_schema`}.{`table_raw`}
(
{DBI::SQL(glue::glue_collapse(glue::glue_sql('{`vars$column_name`} DEFAULT NULL {vars$column_order}', .con = conn_dw), sep = ', \n'))}
)
FROM {file$file_path}
WITH (
FILE_TYPE = 'CSV',
MAXERRORS = 100,
COMPRESSION = 'GZIP',
FIELDTERMINATOR = '|@|',
ROWTERMINATOR = '~@~',
FIELDQUOTE = '',
FIRSTROW = 1,
ERRORFILE = {paste0(base_path, 'error')}
);", .con = conn_dw)
      message("----------------------------------------------------------------------------------------------------")
      message(sql)
      DBI::dbExecute(conn_dw, sql)
      
      raw_count <- DBI::dbGetQuery(conn_dw,
                                   glue::glue_sql("SELECT COUNT(*) FROM {`stg_schema`}.{`table_raw`}",
                                                  .con = conn_dw))[1,1]
      if(file$file_qa_row_cnt == raw_count) {
        message("...QA: Success - All rows loaded...")
        DBI::dbExecute(conn_db,
                       glue::glue_sql("UPDATE {`schema`}.[metadata_etl_log]
                               SET load_raw_datetime = GETDATE() 
                               WHERE etl_id = {file$etl_id}",
                                      .con = conn_db))
      } else {
        stop("QA: ERROR - Not all rows loaded!")
      }

      
    }   
      
  }  
      
   
  
###### - ATTENTION ELI: This is where I stopped. #######
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
     message(paste0("...Copying data from ", table_raw, " to ", table))
      
        table_vars <- DBI::dbGetQuery(conn_db,
                                      glue::glue_sql("SELECT * 
                                                     FROM {`schema`}.[ref_tables]
                                                     WHERE table_name = {table}
                                                     ORDER BY [column_order]",
                                                     .con = conn_db))
        table_config <- list()
        for(v in 1:nrow(table_vars)) {
          table_config$vars[table_vars[v, "column_name"]] <- table_vars[v, "column_type"]
        }
        table_config$hhsaw$to_schema <- stg_schema
        table_config$hhsaw$to_table <- table
      if (!DBI::dbExistsTable(conn_dw, DBI::Id(schema = stg_schema, table = table))) {  
        create_table(conn_dw,
                     server,
                     config = table_config) 
      }
      
      
      cols <- DBI::dbGetQuery(conn_dw,
                              glue::glue_sql("SELECT LOWER([COLUMN_NAME]) AS 'column_name'
                                                FROM [INFORMATION_SCHEMA].[COLUMNS]
                                                WHERE [TABLE_NAME] = {table} AND [TABLE_SCHEMA] = {stg_schema}
                                                ORDER BY [ORDINAL_POSITION]",
                                             .con = conn_dw))
      
      
      new_cols <- dplyr::anti_join(vars, cols)
      if(nrow(new_cols) > 0) {
        for(c in 1:nrow(new_cols)) {
          DBI::dbExecute(conn_dw, 
                         glue::glue_sql("ALTER TABLE {`stg_schema`}.{`table`} 
                                        ADD {`new_cols[c,'column_name']`} {DBI::SQL(new_cols[c,'column_type'])} NULL;",
                                        .con = conn_dw))
        }
      }
      
      DBI::dbExecute(conn_dw, glue::glue_sql("INSERT INTO {`stg_schema`}.{`table`}      
                                             ({`names(table_config$vars)`*})   
                                             SELECT       
                                             {`names(raw_table_config$vars)`*},
                                             {file$etl_id}
                                             FROM {`stg_schema`}.{`table_raw`}",  
                                             .con = conn_dw))
      copy_count <- DBI::dbGetQuery(conn_dw,
                                    glue::glue_sql("SELECT COUNT(*) FROM {`stg_schema`}.{`table`} 
                                                  WHERE etl_id = {file$etl_id}",
                                                   .con = conn_dw))[1,1]
      if(file$file_qa_row_cnt == copy_count) {
        message("...QA: Success - All rows copied...")
        DBI::dbExecute(conn_dw, glue::glue_sql("DROP TABLE {`stg_schema`}.{`table_raw`}",
                                               .con = conn_dw))
        DBI::dbExecute(conn_db,
                       glue::glue_sql("UPDATE {`schema`}.[metadata_etl_log]
                               SET load_raw_datetime = GETDATE() 
                               WHERE etl_id = {file$etl_id}",
                                      .con = conn_db))
      } else {
        stop("QA: ERROR - Not all rows copied!")
      }
    }
  }
  message("LOADING RAW DATA COMPLETE!")
}

