pacman::p_load(odbc, keyring, R.utils, zip, tidyverse, dplyr, lubridate, glue, configr, openxlsx, apde.etl)

### SET DIRECTORIES AND TABLE INFO
export_dir <- "//dphcifs/APDE-CDIP/Seattle_FreshBucks/touw/"
temp_dir <- "C:/temp/cdr/export/"
source_tables <- read.xlsx("//dphcifs/APDE-CDIP/Seattle_FreshBucks/touw/cdr_mcaid_table_metadata_2026-04-23.xlsx", 1)
batch_date <- "20260423"

### GET COLUMNS FOR TABLES
table_list <- data.frame()
for(i in 1:nrow(source_tables)) {
  conn <- create_db_connection("inthealth", interactive = T, prod = T)  
  columns <- DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT 
{source_tables[i,'schema_name']} AS 'schema_name',
{source_tables[i,'table_name']} AS 'table_name',
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
[ORDINAL_POSITION] AS 'column_position'
FROM [INFORMATION_SCHEMA].[COLUMNS]
WHERE [TABLE_SCHEMA] = {source_tables[i,'schema_name']}
AND [TABLE_NAME] = {source_tables[i,'table_name']}
AND [COLUMN_NAME] <> 'etl_batch_id'
ORDER BY schema_name, table_name, column_position", .con = conn))
  
  table_list <- rbind(table_list, columns)
}
### GET ROW AND BATCH COUNTS
for(i in 1:nrow(source_tables)) {
  message(glue::glue("{i}: Getting info from {source_tables[i,'schema_name']}.{source_tables[i,'table_name']} - {Sys.time()}"))
  server <- "inthealth"
  db_name <- "inthealth_edw"
  conn <- create_db_connection(server, interactive = T, prod = T)
  batches <- DBI::dbGetQuery(conn, glue::glue_sql(
      "SELECT CAST(ROUND(SUM(nps.reserved_page_count) * 8.0 / 1024 / 1000 / 1.75, 0) AS INTEGER)
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
  row_cnt <- DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT COUNT(*) FROM {`source_tables[i,'schema_name']`}.{`source_tables[i,'table_name']`}", .con = conn))[1,1]
  if(batches > 0) {
    batch_size <- round(row_cnt / batches, 0)
  } else { 
    batches <- 1 
    batch_size <- as.numeric(row_cnt)
  }
  source_tables[i,"rows"] <- row_cnt
  source_tables[i,"batches"] <- batches
  source_tables[i,"batch_size"] <- as.numeric(batch_size)
}
### EXPORT TABLES  
for(i in 1:nrow(source_tables)) {
  server <- "inthealth"
  db_name <- "inthealth_edw"
  conn <- create_db_connection(server, interactive = T, prod = T)
  message(glue::glue("{i}: Begin table {source_tables[i,'schema_name']}.{source_tables[i,'table_name']} - {Sys.time()}"))
  row_cnt <- source_tables[i,"rows"]
  batches <- source_tables[i,"batches"]
  batch_size <- source_tables[i,"batch_size"]
  cols <- table_list %>% 
    filter(schema_name == source_tables[i,'schema_name']) %>%
    filter(table_name == source_tables[i,'table_name'])
  if(batches > 1) {
    id_col <- cols[1, "column_name"]
    cur_row <- 1
    message(glue::glue("...{i}: Table will be split into {batches} file(s) - {Sys.time()}"))
    message(glue::glue("...{i}: Adding row number to table - {Sys.time()}"))
    DBI::dbExecute(conn, glue::glue_sql(
      "ALTER TABLE {`source_tables[i,'schema_name']`}.{`source_tables[i,'table_name']`} ADD rownum BIGINT IDENTITY(1,1)", .con = conn))
  }
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
    conn <- create_db_connection(server, interactive = T, prod = T)
    DBI::dbExecute(conn, glue::glue_sql(
      "ALTER TABLE {`source_tables[i,'schema_name']`}.{`source_tables[i,'table_name']`} DROP COLUMN rownum", .con = conn))
    DBI::dbDisconnect(conn)
  }
}

