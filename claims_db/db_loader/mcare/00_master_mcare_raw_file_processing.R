######## MCARE FILE FIX
library(tidyverse) # Manipulate data
library(dplyr) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(keyring) # Access stored credentials
library(stringr) # Various string functions
library(R.utils)
library(utils)
library(zip)
library(xlsx)
library(tibble)
library(AzureStor)
library(AzureAuth)
library(svDialogs)

devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/copy_into.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/table_duplicate.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/load_df_bcp.R")

interactive_auth <- FALSE
prod <- TRUE

conn_db <- create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)
conn_dw <- create_db_connection(server = "inthealth", interactive = interactive_auth, prod = prod)

table_duplicate_f(conn_from = conn_db, 
                  conn_to = conn_dw,
                  server_to = "inthealth", 
                  db_to = "inthealth_edw",
                  from_schema = "claims",
                  from_table = "ref_mcare_files_data_dictionary",
                  to_schema = "stg_claims",
                  to_table = "ref_mcare_files_data_dictionary",
                  confirm_tables = F,
                  delete_table = T)

rawdir <- "//dphcifs/APDE-CDIP/Mcaid-Mcare/medicare_raw/"
batchdir <- "Files_20250523"
batchyear <- 2022
basedir <- "C:/temp/mcare"
exdir <- paste0(basedir, "/ex/")
fixdir <- paste0(basedir, "/fixed/")
gzdir <- paste0(basedir, "/gz/")
files <- data.frame("fileName" = list.files(paste0(rawdir, batchdir), pattern="*.csv"))
for(i in 1:nrow(files)) {
  if(survPen::instr(paste0(tolower(files[i, "fileName"]), " "), ".gz") > 0) {
    gunzip(paste0(rawdir, batchdir, "/", files[i, "fileName"]), paste0(exdir, gsub("[.]gz$", "", files[i, "fileName"])), remove = F)
  } else {
    file.copy(paste0(rawdir, batchdir, "/", files[i, "fileName"]), exdir)
  }
}


files <- data.frame("fileName" = list.files(exdir, pattern="*.csv"))
tablefile <- DBI::dbGetQuery(conn_db, "SELECT DISTINCT 
                                      table_name, REPLACE(table_name, 'mcare_', '') AS file_name
                                      FROM claims.ref_mcare_files_data_dictionary")

get_mcare_table_columns_f <- function(conn, table) {
  x <- DBI::dbGetQuery(conn, glue::glue_sql("SELECT *
                                            FROM claims.ref_mcare_files_data_dictionary
                                            WHERE table_name = {table}
                                            ORDER BY column_order", .con = conn))
  return(x)
}

columns <- data.frame(matrix(ncol = 4, nrow = 0))
colnames(columns) <- c("table_name", "column_name", "column_type", "column_order")
allcolumns <- data.frame(matrix(ncol = 4, nrow = 0))
colnames(allcolumns) <- c("table_name", "column_name", "column_type", "column_order")
a <- 1
for(i in 1:nrow(files)) {
  con <- file(paste0(exdir, files[i, "fileName"]), "r")
  first_line <- readLines(con, n = 1)
  close(con)
  if(survPen::instr(first_line, ",") > 0) {
    sep <- ","
  } else {
    sep <- "|"
  }
  df <- read.csv(paste0(exdir, files[i, "fileName"]), sep = sep, nrows = 2)
  fcols <- tolower(colnames(df))
  for(t in 1:nrow(tablefile)) {
    if(survPen::instr(tolower(files[i, "fileName"]), tablefile[t, "file_name"]) > 0) {
      files[i, "table_name"] <- tablefile[t, "table_name"]
    }
  }
  tcols <- get_mcare_table_columns_f(conn_db, files[i, "table_name"])
  new_order <- DBI::dbGetQuery(conn_db, glue::glue_sql("SELECT MAX(column_order)
                                            FROM claims.ref_mcare_files_data_dictionary
                                            WHERE table_name = {files[i, 'table_name']}", .con = conn_db))[1,1] + 1
  cols <- data.frame(matrix(ncol = 4, nrow = 0))
  colnames(cols) <- c("table_name", "column_name", "column_type", "column_order")
  c <- 1
  t <- 1
  for(col in fcols) {
    if(any(tcols == col) == F) {
      message(paste0(files[i, "table_name"], " - ", col))
      cols[c, "table_name"] <- files[i, "table_name"]
      cols[c, "column_name"] <- col
      cols[c, "column_type"] <- "VARCHAR(255)"
      cols[c, "column_order"] <- new_order
      new_order <- new_order + 1
      c <- c + 1
    }
    allcolumns[a, "table_name"] <- files[i, "table_name"]
    allcolumns[a, "column_name"] <- col
    allcolumns[a, "column_type"] <- "VARCHAR(255)"
    allcolumns[a, "column_order"] <- t
    a <- a + 1
    t <- t + 1
  }
  if(nrow(cols) > 0) {
    if(nrow(columns) > 0) {
      columns <- bind_rows(columns, cols) 
    } else {
      columns <- cols
    }
  }
}
## Check columns variable before writing to table
if(nrow(columns > 0)) {
  message(paste0("There are ", nrow(columns), " column(s) to add to [claims].[ref_mcare_tables]."))
  print(columns)
  addcol <- askYesNo("Add New Column(s)?")
  if(addcol == T) {
    DBI::dbAppendTable(conn_db, DBI::Id(schema = "claims", table = "ref_mcare_tables"), columns)
    message("Columns added.")
  }
} else {
  message("No new columns.")
}

conn_db <- create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)
conn_dw <- create_db_connection(server = "inthealth", interactive = interactive_auth, prod = prod)

# reorder columns and write fixed files then compress
for(i in 1:nrow(files)) {
  message(paste0(i, " - reading ", files[i, "fileName"]))
  con <- file(paste0(exdir, files[i, "fileName"]), "r")
  first_line <- readLines(con, n = 1)
  close(con)
  if(survPen::instr(first_line, ",") > 0) {
    sep <- ","
  } else {
    sep <- "|"
  }
  df <- read.csv(paste0(exdir, files[i, "fileName"]), sep = sep)
  files[i, "row_count"] <- nrow(df)
  colnames(df) <- tolower(colnames(df))
  fcols <- tolower(colnames(df))
  tcols <- get_mcare_table_columns_f(conn_db, files[i, "table_name"])
  tcols <- tcols[2:nrow(tcols),]
  torder <- data.frame(matrix(ncol = nrow(tcols), nrow = 0))
  colnames(torder) <- tcols[,"column_name"]
  for(c in 1:length(colnames(df))) {
    col <- colnames(df)[[c]]
    for(t in 1:nrow(tcols)) {
      if(col == tcols[t, "column_name"]) { 
      } else if(col == tcols[t, "column_name_long"]) {
        colnames(df)[[c]] <- tcols[t, "column_name"]
        message(paste0("COLUMN CHANGE: ", c, " - ", tcols[t, "column_name"], " = ", tcols[t, "column_name_long"], " - long"))
      } else if(is.na(tcols[t, "column_name_alt"]) == F) { 
        if(col == tcols[t, "column_name_alt"]) {
          colnames(df)[[c]] <- tcols[t, "column_name"]
          message(paste0("COLUMN CHANGE: ", c, " - ", tcols[t, "column_name"], " = ", tcols[t, "column_name_alt"], " - alt"))
        }
      }
    }
  }
  df <- plyr::rbind.fill(torder, df)
  message(paste0(i, " - writing ", files[i, "fileName"]))
  write.table(df, paste0(fixdir, files[i, "fileName"]), sep = "|", quote = F, na = "", row.names = F)
  files[i, "gzName"] <- paste0(files[i, "fileName"], ".gz")
  gzip(paste0(fixdir, files[i, "fileName"]), destname = paste0(gzdir, files[i, "gzName"]), remove = F)
}

if(T) {
  etl <- askYesNo("Upload files and create ETL log entries?")
  if(etl == T) {
    batch_date <- substring(batchdir, 7, 15)
    batch_date <- paste0(substring(batch_date, 1, 4), 
                         "-", substring(batch_date, 5, 6), 
                         "-", substring(batch_date, 7, 8))
    blob_token <- AzureAuth::get_azure_token(
      resource = "https://storage.azure.com", 
      tenant = keyring::key_get("adl_tenant", "dev"),
      app = keyring::key_get("adl_app", "dev"),
      auth_type = "authorization_code",
      use_cache = F
    )
    blob_endp <- storage_endpoint("https://inthealthdtalakegen2.blob.core.windows.net", token = blob_token)
    cont <- storage_container(blob_endp, "inthealth")
    uploaddir <- paste0("claims/mcare/fixed/", batchdir, "/")
    for(i in 1:nrow(files)) {
      storage_upload(cont, 
                     paste0(gzdir, "/", files[i, "gzName"]), 
                     paste0(uploaddir, files[i, "gzName"]))
    }
    for(i in 1:nrow(files)) {
      load_metadata_etl_log_file_f(conn = conn_db, 
                                   server = "hhsaw",
                                   batch_type = "full", 
                                   data_source = "Medicare", 
                                   date_min = paste0(batchyear, "-01-01"),
                                   date_max = paste0(batchyear, "-12-31"),
                                   delivery_date = batch_date, 
                                   file_name = files[i, "gzName"],
                                   file_loc = uploaddir,
                                   row_cnt = files[i, "row_count"], 
                                   note = files[i, "table_name"])
    }
    message("Files uploaded and ETL log entries created...")
  }
  rm(blob_endp, blob_token, cols, columns, cont, df, tablefile, files, tcols, torder,
     c, col, con, etl, exdir, basedir, gzdir, first_line, fcols, i, new_order,
     rawdir, sep, t, uploaddir, fixdir, batch_date, batchdir)
}


if(T) {
  conn_db <- create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)
  conn_dw <- create_db_connection(server = "inthealth", interactive = interactive_auth, prod = prod)
  batches <- DBI::dbGetQuery(conn_db, 
                             "SELECT 
                             YEAR(date_min) AS 'year',
                             COUNT(*) AS 'cnt'
                             FROM claims.metadata_etl_log 
                             WHERE data_source = 'Medicare' AND date_load_raw IS NULL 
                             GROUP BY YEAR(date_min)
                             ORDER BY YEAR(date_min)")
  
  year_select <- dlg_list(batches[,"year"], title = "Select Year to Load Raw Files")$res
  confirm <- askYesNo(paste0("Confirm loading ", batches[batches$year == year_select,"cnt"], " file(s)?"))
  if(confirm == T) {
    files <- DBI::dbGetQuery(conn_db,
                           glue::glue_sql("SELECT *
                                          FROM claims.metadata_etl_log 
                                          WHERE data_source = 'Medicare' 
                                            AND date_load_raw IS NULL 
                                            AND YEAR(date_min) = {year_select}
                                          ORDER BY etl_batch_id",
                                          .con = conn_db))
    
    for(i in 1:nrow(files)) {
      file <- files[i,]
      schema <- "stg_claims"
      table <- file$note
      table_raw <- paste0("raw_", table)
      table_archive <- paste0("archive_", table)
      message(paste0(Sys.time(), " - ", i, " : ", file$etl_batch_id))
      message(paste0("...Begin loading ", file$file_name, " to [", schema, "].[", table_raw, "]..."))
      v <- get_mcare_table_columns_f(conn_db, table)
      vars <- v[2:nrow(v),]
      table_config <- list()
      for(v in 1:nrow(vars)) {
        table_config$vars[vars[v, "column_name"]] <- "VARCHAR(255)"
      }
      table_config$hhsaw$to_schema <- schema
      table_config$hhsaw$to_table <- table_raw
      table_config$hhsaw$base_url <- "https://inthealthdtalakegen2.dfs.core.windows.net/inthealth/"
      
      copy_into_f(conn = conn_dw, 
                  server = "hhsaw",
                  config = table_config,
                  dl_path = paste0(table_config[["hhsaw"]][["base_url"]], file["file_location"], file["file_name"]),
                  file_type = "csv", compression = "gzip",
                  identity = "Storage Account Key", secret = key_get("inthealth_edw"),
                  overwrite = T,
                  first_row = 2,
                  field_term = "|",
                  row_term = "\\n",
                  rodbc = F)
      raw_count <- DBI::dbGetQuery(conn_dw,
                                   glue::glue_sql("SELECT COUNT(*) FROM {`schema`}.{`table_raw`}",
                                                  .con = conn_db))[1,1]
      if(file$row_count == raw_count) {
        message("...QA: Success - All rows loaded...")
      } else {
        stop("QA: ERROR - Not all rows loaded!")
      }
      message(paste0("...Copying data from ", table_raw, " to ", table))
      cols <- DBI::dbGetQuery(conn_dw,
                                   glue::glue_sql("select a.column_name as 'to_col', b.column_name as 'from_col'
                                                    from information_schema.columns a 
                                                    left join stg_claims.ref_mcare_files_data_dictionary b on a.table_name = b.table_name and 
                                                      (a.column_name = b.column_name or a.column_name = b.column_name_long or a.column_name = b.column_name_alt)
                                                    where a.table_schema = {schema} and a.table_name = {table} and a.column_name <> 'etl_batch_id'
                                                    order by a.ordinal_position",
                                                  .con = conn_dw))
      
      DBI::dbExecute(conn_dw, glue::glue_sql("INSERT INTO {`schema`}.{`table`}      
                                             ([etl_batch_id], {`cols$to_col`*})   
                                             SELECT {file$etl_batch_id},      
                                             {`cols$from_col`*}      
                                             FROM {`schema`}.{`table_raw`}",  
                                             .con = conn_dw))
      copy_count <- DBI::dbGetQuery(conn_dw,
                                   glue::glue_sql("SELECT COUNT(*) FROM {`schema`}.{`table`} 
                                                  WHERE etl_batch_id = {file$etl_batch_id}",
                                                  .con = conn_db))[1,1]
      if(file$row_count == copy_count) {
        message("...QA: Success - All rows copied...")
        DBI::dbExecute(conn_dw, glue::glue_sql("DROP TABLE {`schema`}.{`table_raw`}",
                                               .con = conn_dw))
        DBI::dbExecute(conn_db,
                       glue::glue_sql("UPDATE claims.metadata_etl_log  
                               SET date_load_raw = GETDATE() 
                               WHERE etl_batch_id = {file$etl_batch_id}",
                                      .con = conn_db))
      } else {
        stop("QA: ERROR - Not all rows copied!")
      }
    }
  }
  message("LOADING RAW DATA COMPLETE!")
}


# Add option to delete files

rm(list=ls())

