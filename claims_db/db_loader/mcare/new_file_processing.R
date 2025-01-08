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

basedir <- "C:/temp/mcare"
exdir <- paste0(basedir, "/ex/")
gzdir <- paste0(basedir, "/fixed/")
files <- data.frame("fileName" = list.files(exdir, pattern="*.csv"))

for(i in 1:nrow(files)) {
  df <- read.csv(paste0(exdir, files[i, "fileName"]), sep = "|", nrows = 2)
  cols <- data.frame(matrix(ncol = 2, nrow = 0))
  colnames(cols) <- c("file", "column")
  for(c in 1:length(names(df))) {
    cols[c, 2] <- names(df)[c]
    cols[c, 1] <- files[i, "fileName"]
  }
  if(i == 1) { columns <- cols }
  else { columns <- bind_rows(columns, cols) }
}

write.csv(columns, paste0(basedir, "/columns.csv"))
write.csv(files, paste0(basedir, "/files.csv"))
files <- read.csv(paste0(basedir, "/files.csv"))



get_mcare_table_columns_f <- function(conn, table, year = 9999) {
  x <- DBI::dbGetQuery(conn, glue::glue_sql("SELECT column_name, column_type
                                            FROM claims.ref_mcare_tables
                                            WHERE min_year <= {year} AND table_name = {table}
                                            ORDER BY column_order", .con = conn))
  return(x)
}


devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")
interactive_auth <- TRUE
prod <- TRUE

conn_db <- create_db_connection(server = "hhsaw", interactive = F, prod = T)
conn_dw <- create_db_connection(server = "inthealth", interactive = F, prod = T)


# just rename files
for(i in 1:nrow(files)) {
  message(paste0(i, " - reading ", files[i, "fileName"]))
  df <- read.csv(paste0(exdir, files[i, "fileName"]), sep = "|")
#  conn_db <- create_db_connection(server = "hhsaw", interactive = F, prod = T)
#  vars <- get_mcare_table_columns_f(conn_db, files[i,2], 2100)
#  colnames(df) <- as.vector(vars$column_name)
  message(paste0(i, " - writing ", files[i, "fileName"]))
  write.table(df, paste0(gzdir, files[i, "fileName"]), sep = "|", quote = F, na = "", row.names = F)
}



# write fixed files
files <- data.frame("fileName" = list.files(paste0(basedir, "/fixed/"), pattern="*.csv"))
for( i in 1:nrow(files)) {
  gzip(paste0(basedir, "/fixed/", files[i, "fileName"]), destname = paste0(basedir, "/gz/", files[i, "fileName"], ".gz"), remove = F)
  
}


# manual column reordering
i <- 4
df <- read.csv(paste0(exdir, files[i, "fileName"]), sep = "|")
df <- df[,c(1:34,38,39,35,36,40,37)]
conn_db <- create_db_connection(server = "hhsaw", interactive = F, prod = T)
vars <- get_mcare_table_columns_f(conn_db, "mcare_hha_revenue_center", 2100)
colnames(df) <- as.vector(vars$column_name)
message(paste0(i, " - writing ", files[i, "fileName"]))
write.table(df, paste0(gzdir, files[i, "fileName"]), sep = "|", quote = F, na = "", row.names = F)

















rm(list=ls())

