setwd("apcd_export_import")
source("apcd_import_functions.R")
config <- yaml::read_yaml("apcd_import_config.yaml")

### GET CURRENT TABLE COLUMN LIST AND ORDER
tables <- read.xlsx(config$table_file_path, 1)
tables <- tables %>%
  filter(table_name != config$etl_table)
otables <- data.frame()

tlist <- unique(tables[,c("schema_name", "table_name")])
conn <- DBI::dbConnect(odbc::odbc(), "PHClaims51")
for(t in 1:nrow(tlist)) {
  columns <- DBI::dbGetQuery(conn,
                             glue_sql("
SELECT 
TABLE_SCHEMA AS 'schema_name',
TABLE_NAME AS 'table_name',
COLUMN_NAME AS 'column_name', 
CONCAT(UPPER(DATA_TYPE), IIF(CHARACTER_MAXIMUM_LENGTH IS NULL, '', CONCAT('(', CHARACTER_MAXIMUM_LENGTH, ')')),IIF(ISNULL(NUMERIC_SCALE, 0) = 0, '', CONCAT('(', NUMERIC_PRECISION, ',', NUMERIC_SCALE, ')'))) AS 'column_type',    
ORDINAL_POSITION AS 'column_position'
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = {tlist[t,'table_name']} AND TABLE_SCHEMA = {tlist[t, 'schema_name']}
ORDER BY ORDINAL_POSITION;",
                                      .con = conn))
  otables <- rbind(otables, columns)
}
write.xlsx(otables, "Table_Column_List.xlsx", sheetName = "Sheet 1")
rm(tables, otables, columns, tlist, t, conn)


### RENAME FILES WITH DATES
base_dir <- "//phcifs/SFTP_DATA/APDEDataExchange/WA-APCD/"
export_dir <- paste0(base_dir,"export/")
# export_dir <- "c:/temp/apcd/"
load_date <- "20220609"
file_list <- list.files(path = export_dir,
                        recursive = T,
                        full.names = T)
for(file in file_list) {
  new_file <- file
  if(!is.na(str_locate(file, "part")[1,1])) {
    new_file <- str_replace_all(file, ".txt", "")
    new_file <- str_replace(new_file, ".gz.0", ".0")
    new_file <- str_replace(new_file, ".part", ".txt")
    new_file <- str_replace(new_file, ".txt", paste0("_", load_date, ".txt"))
  }
  file.rename(file, new_file)
}
rm(base_dir, export_dir, load_date, file_list, file, new_file)


### DROP ALL TABLES AND CLEAR ETL LOG
df <- read.xlsx(config$table_file_path, 1)
df <- df %>% filter (table_name != config$etl_table)
df <- unique(paste0(df$schema_name, ".", df$table_name))
conn <- DBI::dbConnect(odbc::odbc(), config$odbc_name)
for (x in 1:length(df)) {
  DBI::dbExecute(conn, 
                 glue_sql("IF OBJECT_ID({df[x]}, 'u') IS NOT NULL 
                          DROP TABLE {DBI::SQL(df[x])};
                          ",
                          .con = conn))
}
conn <- DBI::dbConnect(odbc::odbc(), config$odbc_name)
df <- DBI::dbGetQuery(conn, "SELECT file_schema, file_table FROM [ref].[apcd_etl_log] GROUP BY file_schema, file_table")
for (x in 1:nrow(df)) {
  if(df[x,]$file_schema != 'ref') {
    DBI::dbExecute(conn, 
                   glue_sql("UPDATE [ref].[apcd_etl_log]
                          SET datetime_load = NULL, rows_loaded = NULL
                          WHERE file_schema = {df[x,]$file_schema}
                          AND file_table = {df[x,]$file_table}
                          AND datetime_load IS NOT NULL", 
                            .con = conn))
  }
}
rm(conn, df, x)
