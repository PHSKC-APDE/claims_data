#By: Susan Hernandez
#Purpose: APCD troubleshooting script 04_copy_raw

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


#### STEP 2: Function: Schema Drift Checker ####

check_schema_drift <- function(base_path) {
  
  files <- list.files(base_path, pattern = "\\.parquet$", full.names = TRUE)
  
  if (length(files) == 0) {
    stop(glue("No Parquet files found in: {base_path}"))
  }
  
  message(glue("Found {length(files)} parquet files. Checking schemas..."))
  
  get_schema <- function(file) {
    s <- open_dataset(file) %>% schema()
    tibble(
      file = basename(file),
      schema = paste(format(s), collapse = "\n")
    )
  }
  
  schemas <- map_df(files, get_schema)
  
  drift_groups <- schemas %>%
    group_by(schema) %>%
    summarise(files = paste(file, collapse = ", "),
              file_count = n(),
              .groups = "drop")
  
  if (nrow(drift_groups) > 1) {
    message("SCHEMA DRIFT DETECTED — files do NOT share the same structure.")
    print(drift_groups)
  } else {
    message("No schema drift detected. All files share the same schema.")
  }
  
  return(drift_groups)
}

# ----------------------------------------------------
# ETL LOOP
# ----------------------------------------------------

folder_list <- list("provider_master")

lapply(folder_list, function(folder) {
  
  # Update this to your REAL local or mounted ADLS path
  base_path <- glue("Z:/provider_master/")
  
  # Run schema drift check BEFORE ETL
  drift <- check_schema_drift(base_path)
  
  if (nrow(drift) > 1) {
    stop("Schema drift detected — ETL stopped to prevent COPY INTO failure.")
  }
  
  # Load YAML configuration
  table_name <- folder
  
  message("Loading YAML config file for: ", table_name, " - ", Sys.time())
  
  url <- glue("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_stg_claims.apcd_{table_name}_full.yaml")
  
  table_config <- yaml::yaml.load(httr::GET(url))
  
  # ----------------------------------------------------
  # Remainder of your ETL code — unchanged
  # ----------------------------------------------------
  
  message("Creating ETL batch ID for: ", table_name, " - ", Sys.time())
  current_batch_id <- load_metadata_etl_log_file(
    conn = db_claims,
    batch_type = "full",
    data_source = "APCD",
    date_min = table_config$date_min,
    date_max = table_config$date_max,
    delivery_date = table_config$date_delivery, 
    note = table_config$note_delivery,
    row_cnt = table_config$row_count,
    file_name = table_config[[server]][["to_table"]],
    file_loc = table_config[[server]][["dl_path"]],
    server = server,
    auto_proceed = TRUE
  )
  
  # Load data using COPY INTO
  message("Loading data for: ", table_config[[server]][["to_table"]], " - ", Sys.time())
  
  to_schema <- table_config[[server]][["to_schema"]]
  to_table  <- table_config[[server]][["to_table"]]
  dl_path   <- table_config[[server]][["dl_path"]]
  table_distribution <- table_config[[server]][["table_distribution"]]
  
  system.time(
    apde.etl::copy_into(
      conn = dw_inthealth, 
      server = server,
      config = table_config,
      dl_path = dl_path,
      file_type = "parquet",
      with = table_distribution,
      overwrite = TRUE
    )
  )
  
  ##QA row count
  message("Running row count comparison QA for: ", table_config[[server]][["to_table"]], " - ", Sys.time())
  qa_rows_sql <- qa_load_row_count(conn = dw_inthealth,
                                   server = server,
                                   config = table_config,
                                   row_count = table_config$row_count,
                                   overall = T,
                                   ind_yr = F)
  
  # Report individual results out to SQL table
  qa_schema <- table_config[[server]][["qa_schema"]]
  qa_table <- table_config[[server]][["qa_table"]]
  
  DBI::dbExecute(conn = db_claims,
                 glue::glue_sql("INSERT INTO {`qa_schema`}.{`qa_table`}
                                  (etl_batch_id, table_name, qa_item, qa_result, qa_date, note) 
                                  VALUES ({current_batch_id}, 
                                          '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                                          'Number rows loaded to SQL vs. expected value(s)', 
                                          {qa_rows_sql$outcome[1]},
                                          {format(Sys.time(), usetz = FALSE)},
                                          {qa_rows_sql$note[1]})",
                                .con = db_claims))
  
  if (qa_rows_sql$outcome[1] == "FAIL") {
    stop(glue::glue("Mismatching row count between source file and SQL table. 
                    Check {qa_schema}.{qa_table} for details (etl_batch_id = {current_batch_id}"))
  }
  
  ## Add batch ID column to data table, using current batch ID
  DBI::dbExecute(dw_inthealth,
                 glue::glue_sql("ALTER TABLE {`to_schema`}.{`to_table`} 
                  ADD etl_batch_id INTEGER DEFAULT {current_batch_id}",
                                .con = dw_inthealth))
  DBI::dbExecute(dw_inthealth,
                 glue::glue_sql("UPDATE {`to_schema`}.{`to_table`} 
                  SET etl_batch_id = {current_batch_id}",
                                .con = dw_inthealth))
  
  ## Add date_load_raw to metadata_etl_log table
  message(paste0("Adding date_load_raw to metadata_etl_log for: ", table_config[[server]][["to_table"]]), " - ", Sys.time())
  DBI::dbExecute(db_claims,
                 glue::glue_sql("UPDATE claims.metadata_etl_log 
                                 SET date_load_raw = GETDATE() 
                                 WHERE etl_batch_id = {current_batch_id}
                                 AND file_name = {to_table}",
                                .con = db_claims))
})



library(arrow)
library(dplyr)
library(purrr)
library(stringr)
library(tibble)

# Path to your parquet files (your mapped ADLS path)
base_path <- "Z:/provider_master/"

# List parquet files
files <- list.files(base_path, pattern = "\\.parquet$", full.names = TRUE)

# Function to extract bad values from a single file
find_bad_values <- function(file) {
  df <- read_parquet(file) %>%
    select(state_physical = 18)   # column ordinal 18
  
  df %>%
    mutate(file = basename(file),
           value_length = nchar(state_physical)) %>%
    filter(
      is.na(state_physical) |
        value_length > 4 |
        !str_detect(state_physical, "^[A-Za-z]*$")  # unexpected characters
    )
}

# Apply to all files and bind results
bad_values <- map_df(files, find_bad_values)

bad_values




# Path to parquet files

base_path <- "Z:/provider_master/"
files <- list.files(base_path, pattern = "\\.parquet$", full.names = TRUE)

inspect_column18 <- function(file) {
  df <- read_parquet(file)
  col <- df[[18]]   # state_physical
  
  tibble(
    file = basename(file),
    index = seq_along(col),
    raw_value = col,
    r_class = map_chr(col, ~ class(.x)[1]),
    as_char = map_chr(col, ~ {
      tryCatch(
        as.character(.x),
        error = function(e) "<cannot-convert>"
      )
    })
  ) %>%
    filter(
      r_class != "character" |
        as_char == "<cannot-convert>" |
        grepl("[^A-Za-z]", as_char)    # punctuation, numbers, odd chars
    )
}

problems <- map_df(files, inspect_column18)

problems



Even after you expanded state_physical to VARCHAR(10), SQL Server still needs to CAST the parquet values into VARCHAR(10). But here is the catch:
  COPY INTO does not reject because of string length
It rejects because of parquet field type mismatch.
Apache Arrow often encodes these odd strings as:
  
  “binary view”
untyped nulls
non-UTF8 strings
or mixed-type columns

So SQL Server sees these values not as text but as something like:
  BINARY / VARBINARY → cannot CAST to VARCHAR(10)

This matches your diagnostic output:
  r_class != "character"
as_char == "<cannot-convert>"


##Double check columns

library(arrow)

read_path <- "//dphcifs/apde-cdip/apcd/apcd_data_import/medical_claim/medical_claim0000_part_00.parquet"

ds <- arrow::open_dataset(read_path)

# List column names
names(ds$schema)

#to compare the columns in raw file and yaml file
> library(arrow)
> parquet_cols <- names(arrow::open_dataset(read_path)$schema)
> library(yaml)
yaml_url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/load_raw/tables/load_stg_claims.apcd_medical_claim_full.yaml"

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


6. Quick peek
If you want a compact printout:
  lapply(changes, function(x) x[, c("column","parquet_index","yaml_index","parquet_type","yaml_type","status")])


#figure out column length
✔ Full function definition: get_max_string_lengths_duckdb
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

read_path <-"Z:/medical_claim_header/medical_claim_header0000_part_00.parquet"
max_lengths <- get_max_string_lengths_duckdb(read_path)
max_lengths[["type_of_bill_desc"]]
