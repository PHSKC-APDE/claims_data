#### cODE TO COPY FULL AND CLEAN CHR_VITALS TABLE FROM AZURE BLOB TO sYNAPSE
# This script will sanitize Azure Blob GZIP files by removing all non-printable ASCII characters
# (loading data chunk by chunk in RAM) before writing back to Azure and copying to Synapse
#
# Eli Kern, PHSKC-APDE
#
# Adapted code from Jeremy Whitehurst, PHSKC (APDE)
#

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

pacman::p_load(tidyverse, odbc, configr, glue, keyring, AzureStor, AzureAuth, svDialogs, R.utils, zip, apde.etl) # Load list of packages

#Needs to be run once unmodifed and needs secret password for user names 
#keyring::key_set('adl_tenant', username = 'dev')
#keyring::key_set('adl_app', username = 'dev')
keyring::key_list()

#### STEP 1: CREATE CONNECTIONS ####

##Establish connection to HHSAW prod
interactive_auth <- FALSE
prod <- TRUE
db_claims <- apde.etl::create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)

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


#Check contents
file_list_azure <- AzureStor::list_storage_files(cont, dir = glue("cdr/"))
file_list_azure <- AzureStor::list_storage_files(cont, dir = glue("cdr/20250421/"))


#### STEP 2: COUNT ROWS IN AZURE FILE ####

# Download GZIP file to a temp file (still compressed)
tmpfile <- tempfile(fileext = ".gz")
AzureStor::storage_download(cont, "cdr/20250421/oCHR_VitalSigns_20250421.txt.gz", dest = tmpfile, overwrite = T)

# Open gz connection in binary mode
con <- gzfile(tmpfile, open = "rb")

# Convert "~@~" to raw bytes for binary search
pattern <- charToRaw("~@~")
pat_len <- length(pattern)

# 1–10 MB per read depending on memory
buf_size <- 1e6
count <- 0L
remainder <- raw(0)

repeat {
  chunk <- readBin(con, what = "raw", n = buf_size)
  if (length(chunk) == 0) break
  
  # Combine remainder (last bytes from previous chunk) with current chunk
  data <- c(remainder, chunk)
  
  # Find indices where first byte of pattern appears
  hits <- which(data == pattern[1])
  if (length(hits)) {
    for (h in hits) {
      if (h + pat_len - 1 <= length(data)) {
        if (all(data[h:(h + pat_len - 1)] == pattern)) {
          count <- count + 1L
        }
      }
    }
  }
  
  # Keep last (pat_len - 1) bytes for next loop (in case delimiter spans chunks)
  if (length(data) >= (pat_len - 1)) {
    remainder <- tail(data, pat_len - 1)
  } else {
    remainder <- data
  }
}

close(con)
unlink(tmpfile)

cat("Row count:", count, "\n")

#Success! Row count: --expected 47,179,873


#### STEP 3: PRE-PROCESS GZIP DATA TO REMOVE CRs, LFs, FFs, and all non-printable control characters ####

# Clean GZIP file in chunks: remove all bytes that are not printable ASCII (raw-based)
clean_blob_ascii_only_gzip <- function(blob_container, src_blob, dest_blob, chunk_size = 50*1024*1024) {
  
  tmp_in  <- tempfile(fileext = ".gz")
  tmp_out <- tempfile(fileext = ".gz")
  
  # Download blob to temp file
  download_blob(blob_container, src_blob, tmp_in)
  
  # Open connections for streaming
  con_in  <- gzfile(tmp_in, "rb")
  con_out <- gzfile(tmp_out, "wb")
  
  repeat {
    chunk <- readBin(con_in, "raw", n = chunk_size)
    if(length(chunk) == 0) break
    
    # Replace all non-printable ASCII bytes (outside 0x20–0x7E) with space
    chunk[chunk < as.raw(0x20) | chunk > as.raw(0x7E)] <- as.raw(0x20)
    
    writeBin(chunk, con_out)
  }
  
  close(con_in)
  close(con_out)
  
  # Upload cleaned file back to blob
  upload_blob(blob_container, src = tmp_out, dest = dest_blob)
  
  # Clean up
  file.remove(tmp_in, tmp_out)
  
  message("Cleaning complete: ", dest_blob)
}

# Run cleaning function
# Run time: 3 min
system.time(clean_blob_ascii_only_gzip(
  blob_container = cont,
  src_blob  = "cdr/20250421/oCHR_VitalSigns_20250421.txt.gz",
  dest_blob = "cdr/20250421/oCHR_VitalSigns_20250421_cleaned.txt.gz"
))


#### STEP 4: USE COPY INTO COMMAND TO COPY DATA FROM AZURE BLOB TO SYNAPSE ####

conn_db <- apde.etl::create_db_connection(server = "hhsaw", interactive = interactive_auth, prod = prod)
conn_dw <- apde.etl::create_db_connection(server = "inthealth", interactive = interactive_auth, prod = prod)

batch <- "20250421"
dir_raw <- "//dphcifs/APDE-CDIP/Mcaid-Mcare/cdr_raw/"
dir_batch <- paste0(dir_raw, batch, "/")
dir_txt <- paste0(dir_batch, "txt/")
dir_gz <- paste0(dir_batch, "gz/")
base_path <- paste0("https://inthealthdtalakegen2.blob.core.windows.net/inthealth/cdr/", batch, "/")
schema <- "cdr"
stg_schema <- "stg_cdr"

batches <- DBI::dbGetQuery(conn_db, 
                           "SELECT DISTINCT
                             batch_date
                             FROM cdr.metadata_etl_log 
                             batch_date")

batch <- dlg_list(batches[,"batch_date"], title = "Select Batch to Load Raw Files")$res

files <- DBI::dbGetQuery(conn_db,
                         glue::glue_sql("SELECT *
                                          FROM cdr.metadata_etl_log 
                                          WHERE batch_date = {batch}
                                          ORDER BY etl_id",
                                        .con = conn_db))

file <- filter(files, table_name == "CHR_VitalSigns")
table <- file$table_name
table_raw <- paste0("raw_", table, "_ek_test")
#table_raw <- paste0(table, "_ek_test")

vars <- DBI::dbGetQuery(conn_db,
                        glue::glue_sql(
                          "SELECT * 
     FROM {`schema`}.[ref_tables]
     WHERE table_name = {table}
     ORDER BY column_order",
                          .con = conn_db))

vars <- vars[vars$column_name != "etl_id",]

raw_table_config <- list()
for(v in 1:nrow(vars)) {
  #raw_table_config$vars[vars[v, "column_name"]] <- "VARCHAR(255)"
  raw_table_config$vars[vars[v, "column_name"]] <- "NVARCHAR(2000)"
}
raw_table_config$hhsaw$to_schema <- stg_schema
raw_table_config$hhsaw$to_table <- table_raw
raw_table_config$hhsaw$base_url <- "https://inthealthdtalakegen2.dfs.core.windows.net/inthealth/"

# Create table shell
apde.etl::create_table(conn_dw,
                       to_schema = stg_schema,
                       to_table = table_raw,
                       vars = raw_table_config$vars)

# Create SQL code copying data from Azure Blob to Synapse

#temp overwrite of file_path to use cleaned GZIP file in place of original
file$file_path <- gsub("CHR_VitalSigns_20250421", "oCHR_VitalSigns_20250421_cleaned", file$file_path)

#note that FIELDQUOTE = '' turns off quoting, meaning that SQL will escape content within value
#the reason i need this is that double quotes are in the text (") and are screwing up the COPY INTO command
#the other option here would be to add a double quote to the list of things to remove in the sanitize code, replacing with blank
sql <- glue::glue_sql(
  "TRUNCATE TABLE {`stg_schema`}.{`table_raw`};
COPY INTO {`stg_schema`}.{`table_raw`}
(
{DBI::SQL(glue::glue_collapse(glue::glue_sql('{`vars$column_name`} DEFAULT NULL {vars$column_order}', .con = conn_dw), sep = ', \n'))}
)
FROM {file$file_path}
WITH (
FILE_TYPE = 'CSV',
MAXERRORS = 10,
COMPRESSION = 'GZIP',
FIELDTERMINATOR = '|@|',
ROWTERMINATOR = '~@~',
FIELDQUOTE = '',
FIRSTROW = 1,
ERRORFILE = {paste0(base_path, 'error')}
);", .con = conn_dw)

cat(sql)

# Run SQL command
# Run time: 9 min
system.time(DBI::dbExecute(conn_dw, sql))

# Number of rows in my SQL table vs current table
dbGetQuery(con = conn_dw, "select count(*) as row_count from stg_cdr.raw_CHR_VitalSigns;")$row_count #expect 4,179,869
dbGetQuery(con = conn_dw, "select count(*) as row_count from stg_cdr.raw_CHR_VitalSigns_ek_test;")$row_count #expect 47,179,873

## SUCCESS ##