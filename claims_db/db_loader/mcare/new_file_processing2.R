##mcare raw and file fixing

library(tidyverse) # Manipulate data
library(dplyr) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Githuba
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(keyring) # Access stored credentials
library(stringr) # Various string functions
library(svDialogs)
library(R.utils)
library(pool)
library(AzureStor)
library(AzureAuth)
library(xlsx)

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/copy_into.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_table.R")
#### CREATE CONNECTION ####
interactive_auth <- TRUE
prod <- TRUE

conn_db <- create_db_connection(server = "hhsaw", interactive = F, prod = T)
conn_dw <- create_db_connection(server = "inthealth", interactive = F, prod = T)

blob_token <- AzureAuth::get_azure_token(
  resource = "https://storage.azure.com", 
  tenant = keyring::key_get("adl_tenant", "dev"),
  app = keyring::key_get("adl_app", "dev"),
  auth_type = "authorization_code",
  use_cache = F
)
blob_endp <- storage_endpoint("https://inthealthdtalakegen2.blob.core.windows.net", token = blob_token)
cont <- storage_container(blob_endp, "inthealth")

##x <- read.xlsx("C:/Users/jwhitehurst/OneDrive - King County/Medicare/mcare_crosswalk_fields.xlsx",sheetName = "Sheet1")
##DBI::dbAppendTable(conn_db, DBI::Id(schema = "claims", table = "ref_mcare_tables"), x)

get_mcare_table_columns_f <- function(conn, table, year = 9999) {
  x <- DBI::dbGetQuery(conn, glue::glue_sql("SELECT column_name, column_type
                                            FROM claims.ref_mcare_tables
                                            WHERE min_year <= {year} AND table_name = {table}
                                            ORDER BY column_order", .con = conn))
  return(x)
}

files <- DBI::dbGetQuery(conn_db, "SELECT * FROM claims.metadata_etl_log WHERE data_source = 'Medicare' AND date_load_raw IS NULL ORDER BY etl_batch_id")



conn_db <- create_db_connection(server = "hhsaw", interactive = F, prod = T)
conn_dw <- create_db_connection(server = "inthealth", interactive = F, prod = T)
i <- 1
for(i in 1:nrow(files)) {
vars <- get_mcare_table_columns_f(conn_db, files[i, "note"])
table_vars <- list()
for(v in 1:nrow(vars)) {
  table_vars[vars[v,1]] <- vars[v,2]
}
create_table(conn_dw, server = "inthealth", vars = table_vars,
             to_schema = "stg_claims",
             to_table = table)

}
i <- 1

for(i in 1:nrow(files)) {
file <- files[i,]
message(paste0(Sys.time(), " - ", i, " : ", file$etl_batch_id))
table <- file$note
table_raw <- paste0("raw_", table)
table_archive <- paste0("archive_", table)
vars <- get_mcare_table_columns_f(conn_db, table, year(file$date_max))
table_config <- list()
for(v in 1:nrow(vars)) {
  table_config$vars[vars[v,1]] <- "VARCHAR(255)"
}
table_config$hhsaw$to_schema <- "stg_claims"
table_config$hhsaw$to_table <- table_raw
table_config$hhsaw$base_url <- "https://inthealthdtalakegen2.dfs.core.windows.net/inthealth/"


#create_table(conn_dw, server = "inthealth", vars = table_config$vars,
#             to_schema = "stg_claims",
#             to_table = table_raw)

copy_into_f(conn = conn_dw, 
            server = "hhsaw",
            config = table_config,
            dl_path = paste0(table_config[["hhsaw"]][["base_url"]], file["file_location"], file["file_name"]),
            file_type = "csv", compression = "gzip",
            identity = "Storage Account Key", secret = key_get("inthealth_edw"),
            overwrite = T,
            first_row = 2,
            field_terminator = "|",
            rodbc = F)
message(paste0("Copying data from ", table_raw, " to ", table))
DBI::dbExecute(conn_dw, glue::glue_sql("INSERT INTO [stg_claims].{`table`}
                ([etl_batch_id], {`names(table_config$vars)`*})
               SELECT {file$etl_batch_id},
               {`names(table_config$vars)`*}
               FROM [stg_claims].{`table_raw`}
               WHERE {`names(table_config$vars[1])`} <> {names(table_config$vars[1])}",
                                       .con = conn_dw))

DBI::dbExecute(conn_dw, glue::glue_sql("DROP TABLE [stg_claims].{`table_raw`}",
                                       .con = conn_dw))
}


i <- 1
conn_db <- create_db_connection(server = "hhsaw", interactive = F, prod = T)
conn_dw <- create_db_connection(server = "inthealth", interactive = F, prod = T)
tables <- DBI::dbGetQuery(conn_db, "SELECT table_name FROM claims.ref_mcare_tables GROUP BY table_name")
for(i in 1:nrow(tables)) {
  message(tables[i,1])
  conn_dw <- create_db_connection(server = "inthealth", interactive = F, prod = T)
  conn_db <- create_db_connection(server = "hhsaw", interactive = F, prod = T)
vars <- DBI::dbGetQuery(conn_db, glue::glue_sql("SELECT column_name
                                            FROM claims.ref_mcare_tables
                                            WHERE table_name = {tables[i,1]}
                                            ORDER BY column_order", .con = conn_db))

  DBI::dbExecute(conn_dw, glue::glue_sql("INSERT INTO stg_claims.{`paste0('archive_', tables[i,1])`}
                 ({DBI::SQL(glue_collapse(vars$column_name, sep = ', \n'))})
                 SELECT {DBI::SQL(glue_collapse(vars$column_name, sep = ', \n'))}
                 FROM stg_claims.{`tables[i,1]`}", .con = conn_dw))
  DBI::dbDisconnect(conn_dw)

}
⌡





