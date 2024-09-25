# Eli Kern
# APDE, PHSKC
# 2019-6-29

#2/1/22 update: Added virtual desktop path, removed provider roster table, updated YAML parameters per Alastair's new load_table_from_file function
#4/26/22 update:  Modify code to use CSV instead of XML format files from Enclave, change row terminator to '0x0A' (used by UNLOAD function, includes quotes)
#6/13/23 update: File path for virtual machine DPHXPHAAPR5EBYK
#9/20/23 update: Read path changed to CIFS folder
#3/11/24 update: Added smart selection for write path
#3/11/24 update: Modified YAML creation code to align with COPY_INTO function in Azure environment

#### Create YAML files from CSV format files for all non-reference files ####

##### Set up global parameters and call in libraries #####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)
origin <- "1970-01-01" # Date origin

library(pacman)
pacman::p_load(tidyverse, glue)

#### STEP 1: Set universal parameters ####
read_path <- "//dphcifs/apde-cdip/apcd/apcd_data_import/" #Folder containing exported format files

##Smart selection for write path for YAML files
if(file.exists("C:/Users/SHERNANDEZ.KC/Documents/GitHub/claims_data/claims_db/phclaims/load_raw/tables")){ #Susan on DPHXPHAAPR5EBYK
  write_path <- "C:/Users/SHERNANDEZ.KC/Documents/GitHub/claims_data/claims_db/phclaims/load_raw/tables"
} else if(file.exists("C:/Users/SHERNANDEZ/OneDrive - King County/Documents/GitHub/claims_db/phclaims/load_raw/tables/")){ #Susan on KCITENGPRRSTUD00
  write_path <- "C:/Users/SHERNANDEZ/OneDrive - King County/Documents/GitHub/claims_db/phclaims/load_raw/tables/"
} else if(file.exists("C:/Users/SHERNANDEZ/OneDrive - King County/Documents/GitHub/claims_db/phclaims/load_raw/tables/")){ #Eli on KCITENGPRRSTUD00
  write_path <- "C:/Users/kerneli.PH/Documents/GitHub/claims_data/claims_db/phclaims/load_raw/tables/"
} else if(file.exists("C:/Users/kerneli/GitHub/claims_data/claims_db/phclaims/load_raw/tables/")){ #Eli on KC laptop
  write_path <- "C:/Users/kerneli/GitHub/claims_data/claims_db/phclaims/load_raw/tables/"
}

#Set static parameters for YAML file
to_schema <- "stg_claims"
qa_schema <- "claims"
qa_table <- "metadata_qa_apcd"
ext_data_source <- "datascr_WS_EDW"
ext_schema <- "stg_claims"
dl_path_base <- "https://inthealthdtalakegen2.dfs.core.windows.net/inthealth/claims/apcd/"
base_url <- "https://inthealthdtalakegen2.dfs.core.windows.net/inthealth/"

#Set extract-specific parameters for YAML file
date_min <- as.Date("2014-01-01")
date_max <- as.Date("2023-03-31")
date_delivery <- as.Date("2024-06-28")
apcd_extract_number <- "10023"

#Establish list of tables for which YAML format files will be created
table_list <- list("claim_icdcm_raw", "claim_line_raw", "claim_procedure_raw", "claim_provider_raw", "dental_claim", "eligibility", "medical_claim_header",
                   "member_month_detail", "pharmacy_claim", "provider", "provider_master")


#### STEP 2: Loop over APCD tables, saving create YAML file for each table ####

lapply(table_list, function(table_list) {
  
  #Read table path from list
  table_path <- glue(read_path, table_list, "_export")
  
  #Extract table name
  table_name_part <- gsub("_format.csv", "", list.files(path = file.path(table_path), pattern = "*format.csv", full.names = F))
  sql_table <- glue("apcd_", table_name_part)
  
  #Extract column names, positions and data types from XML format file, convert to YAML and write to file
  apcd_format_file <- list.files(path = file.path(table_path), pattern = "*format.csv", full.names = T)
  format_df <- read_csv(apcd_format_file, show_col_types = F)
  vars_list <- as.list(deframe(select(arrange(format_df, as.numeric(as.character(column_position))), column_name, column_type)))
  server_parameter_list <- list("to_schema" = to_schema, "to_table" = sql_table, "qa_schema" = qa_schema, "qa_table" = qa_table,
                                   "ext_data_source" = ext_data_source, "ext_schema" = ext_schema, "ext_table" = sql_table,
                                   "dl_path" = glue(dl_path_base, table_name_part, "_import/"),
                                   "base_url" = base_url)
  row_count <- as.integer(unique(format_df$row_count))
  col_count <- as.integer(unique(format_df$column_count))
  format_list <- list("hhsaw" = server_parameter_list, "row_count" = row_count, "col_count" = col_count, "date_min" = date_min,
                      "date_max" = date_max, "date_delivery" = date_delivery, "note_delivery" = glue(sql_table, ", extract ", apcd_extract_number),
                      "vars" = vars_list)
  yaml::write_yaml(x = format_list,
                   file = glue(write_path, "load_", to_schema, ".", sql_table, "_full", ".yaml"),
                   indent = 4,
                   indent.mapping.sequence = T,
                   handlers = list(
                     Date = function(x) format(x, "%Y-%m-%d")
                   ))
  
  glue(sql_table, " format file successfully converted to YAML file")
})