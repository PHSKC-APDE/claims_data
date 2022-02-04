# Eli Kern
# APDE, PHSKC
# 2019-6-29

#2/1/22 update: Added virtual desktop path, removed provider roster table, updated YAML parameters per Alastair's new load_table_from_file function

#### Create YAML files from XML format files for all non-reference files ####

##### Set up global parameters and call in libraries #####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)
origin <- "1970-01-01" # Date origin

library(pacman)
pacman::p_load(tidyverse, glue)

#### STEP 1: Set universal parameters ####
read_path <- "\\\\kcitsqlutpdbh51/ImportData/Data/APCD_data_import/" #Folder containing exported format files
#write_path <- "C:/Users/kerneli/OneDrive - King County/GitHub/claims_data/claims_db/phclaims/load_raw/tables/" #Eli's Local GitHub folder on KC laptop
write_path <- "C:/Users/kerneli.PH/Documents/GitHub/claims_data/claims_db/phclaims/load_raw/tables/" #Eli's Local GitHub folder on KCITENGPRRSTUD00.kc.kingcounty.lcl
write_path <- "C:/Users/SHERNANDEZ/OneDrive - King County/Documents/GitHub/claims_data/claims_db/phclaims/load_raw/tables/" #Susan's Local GitHub folder on KCITENGPRRSTUD00.kc.kingcounty.lcl



server_path <- "KCITSQLUTPDBH51"
db_name <- "PHClaims"
sql_schema_name <- "load_raw" ##Name of schema where table will be created
table_list <- list("claim_icdcm_raw", "claim_line_raw", "claim_procedure_raw", "claim_provider_raw", "dental_claim", "eligibility", "medical_claim_header",
                   "member_month_detail", "pharmacy_claim", "provider", "provider_master")


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
    mutate(table_name = paste0("table_", "part", gsub(glue(table_name_part, "_"), "", table_name)),
           file_path = gsub("\\\\", "/", file_path))
  file_list <- as.list(deframe(file_df)) #Convert to list
  
  #Add additional levels (lists) to specify other parameters for YAML file
  file_list <- lapply(file_list, function(x) { 
    list(file_path = x,
         field_term = ',',
         row_term = "\\n")
  }) 
  
  #For tables with only 1 table chunk, change list name to "overall"
  if (length(long_file_list) == 1) {
    names(file_list) <- "overall"
  }
  
  #For tables with multiple chunks, create list of table chunk suffixes
  if (length(long_file_list) > 1) {
    table_index <- as.list(paste0("part", gsub(glue(table_name_part, "_"), "", short_file_list)))
    combine_years <- list(years = table_index)
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
  format_list <- list("server" = server_path, "db_name" = db_name, "to_schema" = sql_schema_name, "to_table" = sql_table,"vars" = vars_list)
  yaml::write_yaml(append(format_list, file_list), glue(write_path, "load_", sql_schema_name, ".", sql_table, "_full", ".yaml"), indent = 4,
                   indent.mapping.sequence = T)
  
  glue(sql_table, " format file successfully converted to YAML file")
})