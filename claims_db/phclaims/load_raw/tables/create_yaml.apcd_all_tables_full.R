# Eli Kern
# APDE, PHSKC
# 2019-6-29

#### Import APCD data from Amazon S3 bucket to SQL Server - load_raw.apcd_eligibility ####

##### Set up global parameters and call in libraries #####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)
origin <- "1970-01-01" # Date origin

library(pacman)
pacman::p_load(tidyverse, glue)

#### STEP 1: Set universal parameters ####
read_path <- "\\\\kcitsqlutpdbh51/ImportData/Data/APCD_data_import/" #Folder containing exported format files
write_path <- "C:/Users/kerneli/OneDrive - King County/GitHub/claims_data/claims_db/phclaims/load_raw/tables/" #Local GitHub folder

sql_schema_name <- "load_raw" ##Name of schema where table will be created
table_list <- list("dental_claim", "eligibility", "medical_claim", "member_month_detail", "pharmacy_claim", "provider",
                   "provider_master", "provider_practice_roster")

#### STEP 2: Loop over APCD tables, saving create YAML file for each table ####

lapply(table_list, function(table_list) {
  
  #Read table path from list
  table_path <- glue(read_path, table_list, "_export")
  
  #Extract table name
  table_name_part <- gsub("_format.xml", "", list.files(path = file.path(table_path), pattern = "*format.xml", full.names = F))
  sql_table <- glue("apcd_", table_name_part)
  
  #Extract table chunk names
  long_file_list <- as.list(list.files(path = file.path(table_path), pattern = "*.csv", full.names = T))
  short_file_list <- as.list(gsub(".csv", "", list.files(path = file.path(table_path), pattern = "*.csv", full.names = F)))
  file_df <- cbind(plyr::ldply(short_file_list), plyr::ldply(long_file_list)) #Bind file path and table names
  colnames(file_df) <- c("table_name", "file_path") #Name variables
  file_df <- file_df %>% #Normalize contents
    mutate(table_name = paste0("table_", gsub(glue(table_name_part, "_"), "", table_name)),
           file_path = gsub("\\\\", "/", file_path))
  file_list <- as.list(deframe(file_df)) #Convert to list
  
  #Add additional levels (lists) to specify other parameters for YAML file
  file_list <- lapply(file_list, function(x) { 
    list(file_path = x,
         field_term = "\\t",
         row_term = "\\n")
  }) 
  
  #For tables with only 1 table chunk, change list name to "overall"
  if (length(long_file_list) == 1) {
    names(file_list) <- "overall"
  }
  
  #For tables with multiple chunks, create list of table chunk suffixes
  if (length(long_file_list) > 1) {
    combine_years <- list(years = short_file_list)
    file_list <- append(file_list, combine_years[1])
  }
  
  #Extract column names, positions and data types from XML format file, convert to YAML and write to file
  apcd_format_file <- list.files(path = file.path(table_path), pattern = "*format.xml", full.names = T)
  format_xml <- XML::xmlParse(apcd_format_file)
  format_df <- XML::xmlToDataFrame(nodes = XML::xmlChildren(XML::xmlRoot(format_xml)[["data"]]), stringsAsFactors = F)
  names <- XML::xmlToDataFrame(nodes = XML::xmlChildren(XML::xmlRoot(format_xml)[["table-def"]]))
  colNames <- (names$'column-name'[!is.na(names$'column-name')])
  colnames(format_df) <- colNames
  vars_list <- as.list(deframe(select(arrange(format_df, as.numeric(as.character(POSITION))), COLUMN_NAME, DATA_TYPE)))
  format_list <- list("schema" = sql_schema_name, "table" = sql_table, "vars" = vars_list)
  yaml::write_yaml(append(format_list, file_list), glue(write_path, "load_", sql_schema_name, ".", sql_table, "_full", ".yaml"), indent = 4,
                   indent.mapping.sequence = T)
  
  glue(sql_table, " format file successfully converted to YAML file")
})

