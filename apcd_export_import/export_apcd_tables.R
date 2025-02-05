library(odbc) # Read to and write from SQL
library(curl) # Read files from FTP
library(keyring) # Access stored credentials
library(R.utils) # File and folder manipulation
library(zip) # Extract data from gzip
library(jsonlite) # Extract data from curl
library(tidyverse) # Manipulate data
library(dplyr) # Manipulate data
library(lubridate) # Manipulate data
library(glue) # Safely combine SQL code
library(configr) # Read in YAML files
library(xlsx) # Read in XLSX files
library(svDialogs) # Extra UI Elements

devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")
source(file.path(here::here(),"apcd_export_import/apcd_import_functions.R"))
config <- yaml::read_yaml(file.path(here::here(),"apcd_export_import/apcd_import_config.yaml"))

export_dir <- "//phcifs/SFTP_DATA/APDEDataExchange/WA-APCD/export/"
export_dir_new <- "//dphcifs.kc.kingcounty.lcl/APDE-CDIP/SFTP_APDEDATA/APDEDataExchange/WA-APCD/export/"
temp_dir <- "C:/temp/apcd/"
ref_dir <- paste0(temp_dir, "ref_schema/")
stage_dir <- paste0(temp_dir, "stage_schema/")
final_dir <- paste0(temp_dir, "final_schema/")
source_tables <- read.xlsx(file.path(here::here(),"apcd_export_import/apcd_source_tables.xlsx"), sheetIndex = 1)
batch_date <- "20241217"

### GET COLUMNS FOR TABLES
table_list <- data.frame()
for(i in 1:nrow(source_tables)) {
  if(source_tables[i,"schema_name"] == 'stg_claims') {
    conn <- create_db_connection("inthealth", interactive = F, prod = T)  
  } else {
    conn <- create_db_connection("hhsaw", interactive = F, prod = T)
  }
  columns <- DBI::dbGetQuery(conn, glue::glue_sql(
"SELECT 
CASE
	WHEN [TABLE_NAME] LIKE 'final%' THEN 'final'
	WHEN [TABLE_SCHEMA] = 'stg_claims' THEN 'stage'
	ELSE 'ref'
END AS 'schema_name',
REPLACE(REPLACE([TABLE_NAME], 'ref_', ''), 'final_', '') AS 'table_name',
[COLUMN_NAME] AS 'column_name',
CONCAT(
  UPPER([DATA_TYPE]), 
	CASE
		WHEN [DATA_TYPE] IN('VARCHAR', 'CHAR', 'NVARCHAR') THEN CONCAT('(',CASE
		                  WHEN [CHARACTER_MAXIMUM_LENGTH] = -1 THEN 'MAX'
		                  ELSE CAST([CHARACTER_MAXIMUM_LENGTH] AS VARCHAR(4))
		                END
		                , ') COLLATE ', [COLLATION_NAME])
		WHEN [DATA_TYPE] IN('DECIMAL', 'NUMERIC') THEN CONCAT('(', [NUMERIC_PRECISION], ',', [NUMERIC_SCALE], ')')
		ELSE ''
	END) AS 'column_type',
[ORDINAL_POSITION] AS 'column_position',
[TABLE_SCHEMA] AS 'source_schema',
[TABLE_NAME] AS 'source_table'
FROM [INFORMATION_SCHEMA].[COLUMNS]
WHERE [TABLE_SCHEMA] = {source_tables[i,'schema_name']}
AND [TABLE_NAME] = {source_tables[i,'table_name']}
AND [COLUMN_NAME] <> 'etl_batch_id'
ORDER BY schema_name, table_name, column_position", .con = conn))

  table_list <- rbind(table_list, columns)
}

i <- 8
### EXPORT TABLES  
for(i in 1:nrow(source_tables)) {
  message(glue::glue("{i}: Begin table {source_tables[i,'schema_name']}.{source_tables[i,'table_name']} - {Sys.time()}"))
  if(source_tables[i,"schema_name"] == 'stg_claims') {
    server <- "inthealth"
    db_name <- "inthealth_edw"
    conn <- create_db_connection(server, interactive = F, prod = T)
    batches <- DBI::dbGetQuery(conn, glue::glue_sql(
      "SELECT CAST(ROUND(SUM(nps.reserved_page_count) * 8.0 / 1024 / 1000 / 1.5, 0) AS INTEGER)
FROM sys.schemas s
INNER JOIN sys.tables t ON s.schema_id = t.schema_id
INNER JOIN sys.indexes i ON  t.object_id = i.object_id AND i.index_id <= 1
INNER JOIN sys.pdw_table_distribution_properties tp ON t.object_id = tp.object_id
INNER JOIN sys.pdw_table_mappings tm ON t.object_id = tm.object_id
INNER JOIN sys.pdw_nodes_tables nt ON tm.physical_name = nt.name
INNER JOIN sys.dm_pdw_nodes pn ON  nt.pdw_node_id = pn.pdw_node_id
INNER JOIN sys.pdw_distributions di ON  nt.distribution_id = di.distribution_id
INNER JOIN sys.dm_pdw_nodes_db_partition_stats nps ON nt.object_id = nps.object_id AND nt.pdw_node_id = nps.pdw_node_id AND nt.distribution_id = nps.distribution_id AND i.index_id = nps.index_id
WHERE pn.type = 'COMPUTE' AND s.name = {source_tables[i,'schema_name']} AND t.name = {source_tables[i,'table_name']}
GROUP BY s.name, t.name", .con = conn))[1,1]
  } else {
    server <- "hhsaw"
    db_name <- "hhs_analytics_workspace"
    conn <- create_db_connection(server, interactive = F, prod = T)
    batches <- DBI::dbGetQuery(conn, glue::glue_sql(
"SELECT CAST(ROUND(SUM(reserved_page_count) * 8.0 / 1024 / 1000 / 1.5, 0) AS INTEGER)
FROM sys.dm_db_partition_stats, sys.objects, sys.schemas
WHERE sys.dm_db_partition_stats.object_id = sys.objects.object_id
	AND sys.objects.schema_id = sys.schemas.schema_id
	AND sys.schemas.name = {source_tables[i,'schema_name']}
	AND sys.objects.name = {source_tables[i,'table_name']}
GROUP BY sys.schemas.name, sys.objects.name", .con = conn))[1,1]
  }
  cols <- table_list %>% 
    filter(source_schema == source_tables[i,'schema_name']) %>%
    filter(source_table == source_tables[i,'table_name'])
  if(batches > 1) {
    id_col <- cols[1, "column_name"]
    row_cnt <- DBI::dbGetQuery(conn, glue::glue_sql(
      "SELECT COUNT(*) FROM {`source_tables[i,'schema_name']`}.{`source_tables[i,'table_name']`}", .con = conn))[1,1]
    batch_size <- round(row_cnt / batches, 0)
    cur_row <- 1
    message(glue::glue("...{i}: Table will be split into {batches} file(s) - {Sys.time()}"))
    message(glue::glue("...{i}: Adding row number to table - {Sys.time()}"))
    DBI::dbExecute(conn, glue::glue_sql(
      "ALTER TABLE {`source_tables[i,'schema_name']`}.{`source_tables[i,'table_name']`} ADD rownum BIGINT IDENTITY(1,1)", .con = conn))
  } else { batches <- 1 }
  blank <- "''"
  for(x in 1:batches) {
    if(batches > 1) {
      sql <- glue::glue("SELECT {glue::glue_collapse(glue::glue('REPLACE([{cols$column_name}], CHAR(9), {blank})'), sep = ', ')} FROM [{source_tables[i,'schema_name']}].[{source_tables[i,'table_name']}] WHERE [rownum] BETWEEN {cur_row} AND {cur_row + batch_size} order by rownum")
      cur_row <- cur_row + batch_size + 1
    } else {
      sql <- glue::glue("SELECT {glue::glue_collapse(glue::glue('REPLACE([{cols$column_name}], CHAR(9), {blank})'), sep = ', ')} FROM [{source_tables[i,'schema_name']}].[{source_tables[i,'table_name']}]")
    }
    filename <- glue::glue("{cols[1, 'schema_name']}.{cols[1, 'table_name']}.{str_pad(x, 3, pad = '0')}_{batch_date}.csv")
    filepath <- paste0(temp_dir, filename)
    user <-keyring::key_list(server)[["username"]]
    pass <- keyring::key_get(server, keyring::key_list(server)[["username"]])
    bcp_args <- c(glue::glue('"{sql}" ',
                           'queryout ',
                           '"{filepath}" ',
                           '-r \\n ',
                           '-t \\t ',
                           '-C 65001 ',
                           '-S "{server}" ',
                           '-d {db_name} ', 
                           '-b 100000 ',
                           '-c ',
                           '-G ',
                           '-U {user} ',
                           '-P {pass} ',
                           '-q ',
                           '-D '))
    message(glue::glue("...{i} - {x}: Writing {filename} - {Sys.time()}"))
    a = system2(command = "bcp", args = c(bcp_args), stdout = TRUE, stderr = TRUE)
    message(glue::glue("...{i} - {x}: Compressing file - {Sys.time()}"))
    gzip(filepath)
    message(glue::glue("...{i} - {x}: Moving file {filename} - {Sys.time()}"))
    file.rename(from = paste0(filepath, ".gz"), 
                to = paste0(export_dir, filename, ".gz"))
  }
  if(batches > 1) {
    conn <- create_db_connection(server, interactive = F, prod = T)
    DBI::dbExecute(conn, glue::glue_sql(
      "ALTER TABLE {`source_tables[i,'schema_name']`}.{`source_tables[i,'table_name']`} DROP COLUMN rownum", .con = conn))
    DBI::dbDisconnect(conn)
  }
}
table_list <- subset(table_list, select = -c(source_schema, source_table))
etl_log <- read.xlsx(file.path(here::here(),"apcd_export_import/apcd_etl_log.xlsx"), sheetIndex = 1)
table_list <- rbind(table_list, etl_log)
write.xlsx(table_list, file.path(here::here(),paste0("apcd_export_import/APCD_Tables_", batch_date, ".xlsx")), row.names = F, append = F)  
