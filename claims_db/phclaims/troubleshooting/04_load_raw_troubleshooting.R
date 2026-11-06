#By: Susan Hernandez
#Purpose: APCD troubleshooting script 04_copy_raw

rm()
#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

#these are APDE packages relied upon for the script. You will need to install them following the instructions from their git
#install.packages("remotes")
# Store the token locally (paste when prompted)
#gitcreds::gitcreds_set() #update token if necessary, get email

#remotes::install_github("PHSKC-APDE/rads", auth_token = NULL)
#remotes::install_github("PHSKC-APDE/apde.etl", auth_token = NULL)
pacman::p_load(tidyverse, odbc, configr, glue, keyring, svDialogs, R.utils, apde.etl, arrow, dplyr,purr, glue, tibble, yaml, httr) # Load list of packages



#### SET UP FUNCTIONS NOT INCLUDED IN APDE.ETL PACKAGE ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_sql.R")


#### STEP 1: CREATE CONNECTIONS ####

##Establish connection to inthealth_edw prod
#Enter credentials for HHSAW
#key_set("hhsaw", username = "shernandez@kingcounty.gov") #Only run this each time password is changed
#key_set("inthealth_edw_prod", username = "shernandez@kingcounty.gov") #Only run this each time password is changed
keyring::key_list() #Run this to list all the stored usernames


interactive_auth <- FALSE
prod <- TRUE
server <- "hhsaw"
dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)


##2. Double check columns

library(arrow)
                                                        "Z:\medical_claim_header\medical_claim_header0000_part_00.parquet"
#read_path <- "//dphcifs/apde-cdip/apcd/apcd_data_import/medical_claim/medical_claim0000_part_00.parquet"
read_path <- "//dphcifs/apde-cdip/apcd/apcd_data_import/medical_claim_header/medical_claim_header0000_part_00.parquet"

ds <- arrow::open_dataset(read_path)

# List column names
names(ds$schema)

#to compare the columns in raw file and yaml file
 library(arrow)
parquet_cols <- names(arrow::open_dataset(read_path)$schema)
library(yaml)
#yaml_url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_stg_claims.apcd_medical_claim_full.yaml"
yaml_url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/refs/heads/main/claims_db/phclaims/load_raw/tables/load_stg_claims.apcd_medical_claim_header_full.yaml"
yaml_content <- yaml::read_yaml(yaml_url)

# Extract YAML-defined column names:
yaml_cols <- names(yaml_content$vars)

yaml_cols

library(dplyr)
library(arrow)
library(yaml)

# --- INPUTS ---------------------------------------------------------

#read_path <- "//dphcifs/apde-cdip/apcd/apcd_data_import/medical_claim/medical_claim0000_part_00.parquet"
#read_path <- "//dphcifs/apde-cdip/apcd/apcd_data_import/medical_claim_diagnosis/medical_claim_diagnosis0000_part_00.parquet"
read_path <- "//dphcifs/apde-cdip/apcd/apcd_data_import/medical_claim_header/medical_claim_header0000_part_00.parquet"

# parquet file column order & types
ds <- arrow::open_dataset(read_path)
parquet_cols <- names(ds$schema)
parquet_types <- sapply(ds$schema$fields, function(f) f$type$ToString())

# YAML column order & types
#yaml_url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_stg_claims.apcd_medical_claim_full.yaml"
#yaml_url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_stg_claims.apcd_medical_claim_diagnosis_full.yaml"
yaml_url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_stg_claims.apcd_medical_claim_header_full.yaml"

yaml_content <- yaml::read_yaml(yaml_url)


yaml_cols <- names(yaml_content$vars)
yaml_types <- unlist(yaml_content$vars)

# --- CREATE COMPARISON TABLE ----------------------------------------

# Create a unified list of all columns present anywhere
all_cols <- union(parquet_cols, yaml_cols)

comparison_df <- data.frame(
  column = all_cols,
  parquet_index = match(all_cols, parquet_cols),
  yaml_index = match(all_cols, yaml_cols),
  parquet_type = parquet_types[match(all_cols, parquet_cols)],
  yaml_type = yaml_types[match(all_cols, yaml_cols)],
  stringsAsFactors = FALSE
)

# Determine status
comparison_df$status <- case_when(
  is.na(comparison_df$parquet_index) ~ "deleted (in YAML, not in parquet)",
  is.na(comparison_df$yaml_index) ~ "added (in parquet, not in YAML)",
  comparison_df$parquet_index != comparison_df$yaml_index ~ "moved",
  TRUE ~ "same"
)

# Detect type mismatches
#Add check.names = FALSE and row.names = NULL
comparison_df$status <- dplyr::case_when(
  is.na(comparison_df$parquet_index) ~ "deleted (in YAML, not in parquet)",
  is.na(comparison_df$yaml_index) ~ "added (in parquet, not in YAML)",
  comparison_df$parquet_index != comparison_df$yaml_index ~ "moved",
  TRUE ~ "same"
)

comparison_df

#1. Added columns
#Columns in parquet but not YAML:
library(dplyr)
  added <- comparison_df %>% 
  filter(status == "added (in parquet, not in YAML)")


#2. Deleted columns
#Columns in YAML but not parquet:
  deleted <- comparison_df %>% 
  filter(status == "deleted (in YAML, not in parquet)")


#3. Moved columns
#Present in both, but with different positions:
  moved <- comparison_df %>% 
  filter(status == "moved")

#5. Summary list (optional)
#If you want all results at once:
  changes <- list(
    added = added,
    deleted = deleted,
    moved = moved
  )

changes


#6. Quick peek
#If you want a compact printout:
  lapply(changes, function(x) x[, c("column","parquet_index","yaml_index","parquet_type","yaml_type","status")])


#figure out column length
#✔ Full function definition: get_max_string_lengths_duckdb
get_max_string_lengths_duckdb <- function(parquet_files) {
  
  # Connect to DuckDB (in-memory)
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  
  # Build DuckDB file list syntax: ['file1', 'file2', ...]
  file_list_sql <- paste0(
    "['",
    paste(parquet_files, collapse = "', '"),
    "']"
  )
  
  parquet_sql <- paste0("read_parquet(", file_list_sql, ")")
  
  # Use Arrow to inspect schema
  ds <- arrow::open_dataset(parquet_files)
  schema <- ds$schema
  
  # Identify string columns (utf8)
  string_cols <- names(schema)[
    sapply(schema$fields, function(f) grepl("string|utf8", f$type$ToString()))
  ]
  
  if (length(string_cols) == 0) {
    return(list())
  }
  
  # Build SQL query to compute max string lengths
  select_sql <- paste0(
    "SELECT ",
    paste0("MAX(LENGTH(", string_cols, ")) AS ", string_cols, collapse = ", "),
    " FROM ",
    parquet_sql
  )
  
  # Execute SQL
  res <- DBI::dbGetQuery(con, select_sql)
  
  # Convert row into a clean named list
  max_lengths <- as.list(res[1, ])
  names(max_lengths) <- string_cols
  
  return(max_lengths)
}

#read_path <-"Z:/medical_claim_header/medical_claim_header0000_part_00.parquet"
read_path <- "//dphcifs/apde-cdip/apcd/apcd_data_import/medical_claim_header/medical_claim_header0000_part_00.parquet"
max_lengths <- get_max_string_lengths_duckdb(read_path)
#max_lengths[["type_of_bill_desc"]]


