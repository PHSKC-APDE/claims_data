# Eli Kern
# APDE, PHSKC
# 2019-6-29

#2/1/22 update: Added virtual desktop path, removed provider roster table, updated YAML parameters per Alastair's new load_table_from_file function
#4/26/22 update:  Modify code to use CSV instead of XML format files from Enclave, change row terminator to '0x0A' (used by UNLOAD function, includes quotes)
#6/13/23 update: File path for virtual machine DPHXPHAAPR5EBYK
#9/20/23 update: Read path changed to CIFS folder
#3/11/24 update: Added smart selection for write path
#3/11/24 update: Modified YAML creation code to align with COPY_INTO function in Azure environment
#9/25/24 update:YAML file path
#1/16/26 update: Change row count to numeric to handle values in excess of 2.1 billion (leading to QA fail)
#1/27/26 update: Change row count to character to avoid scientific notation in YAML files (leading to QA fail)
#7/6/26 update: Adapt to pull info from PARQUET files and add table distribution parameter for inthealth_edw, including reference tables

#### Create YAML files from CSV format files for all non-reference files ####

##### Set up global parameters and call in libraries #####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)
origin <- "1970-01-01" # Date origin

library(pacman)
pacman::p_load(tidyverse, glue, arrow, duckdb)

#### STEP 0: Define custom functions ####

#Function to determine max string length by column using duckdb
get_max_string_lengths_duckdb <- function(parquet_files) {
  
  # DuckDB connection
  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  
  # Build DuckDB list-of-files syntax:
  # read_parquet(['file1','file2',...])

  file_list_sql <- paste0(
    "['",
    paste(parquet_files, collapse = "', '"),
    "']"
  )
  parquet_sql <- paste0("read_parquet(", file_list_sql, ")")
  
  # Get schema using Arrow
  ds <- arrow::open_dataset(parquet_files)
  schema <- ds$schema
  
  string_cols <- names(schema)[
    sapply(schema$fields, function(f) grepl("string|utf8", f$type$ToString()))
  ]
  
  if (length(string_cols) == 0) return(list())
  
  # Build SQL: SELECT MAX(LENGTH(col1)), MAX(LENGTH(col2)), ...
  select_sql <- paste0(
    "SELECT ",
    paste0("MAX(LENGTH(", string_cols, ")) AS ", string_cols, collapse = ", "),
    " FROM ",
    parquet_sql
  )
  
  # Execute single SQL query
  res <- DBI::dbGetQuery(con, select_sql)
  
  # Convert row of results into a named list
  max_lengths <- as.list(res[1, ])
  names(max_lengths) <- string_cols
  
  return(max_lengths)
}

#Function to convert Arrow types → SQL types (using lengths)
convert_arrow_to_sql <- function(arrow_type) {
  arrow_type <- tolower(arrow_type)
  
  # STRING → VARCHAR(n)
  if (arrow_type == "string") {
      return("VARCHAR(272)")
    }
  
  # NUMERIC TYPES
  if (arrow_type == "int32") return("INT")
  if (arrow_type == "int64") return("BIGINT")
  
  # DECIMAL
  if (grepl("^decimal128", arrow_type)) {
    ps <- gsub("decimal128\\(|\\)", "", arrow_type)
    return(paste0("DECIMAL(", ps, ")"))
  }
  
  # FLOATS
  if (arrow_type == "double") return("FLOAT")
  if (arrow_type == "float")  return("REAL")
  
  # DATES & TIMESTAMPS
  if (grepl("^timestamp", arrow_type)) return("DATETIME2")
  if (grepl("^date", arrow_type)) return("DATE")
  
  # FALLBACK
  return("VARCHAR(272)")
}


#### STEP 1: Set universal parameters for data tables ####
read_path <- "//dphcifs/apde-cdip/apcd/apcd_data_import/" #Folder containing files exported from Analytic Enclave

##Smart selection for write path for YAML files
if(file.exists("C:/GitHub/claims_data/claims_db/phclaims/load_raw/tables/")){ #Eli on KC laptop
  write_path <- "C:/GitHub/claims_data/claims_db/phclaims/load_raw/tables/"
} else if(file.exists("C:/Users/SHERNANDEZ.KC/Documents/GitHub/claims_data/claims_db/phclaims/load_raw/tables/")){ #Susan on DPHXPHAAPR5EBYK
  write_path <- "C:/Users/SHERNANDEZ.KC/Documents/GitHub/claims_data/claims_db/phclaims/load_raw/tables/"
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
date_max <- as.Date("2025-12-31")
date_delivery <- as.Date("2026-05-07")
apcd_extract_number <- "10037"

#Establish list of tables for which YAML format files will be created
table_list <- list("cmsdrg_output_multi_ver", "dental_claim", "eligibility", "inpatient_stay_summary_ltd", "medical_claim",
                   "medical_claim_diagnosis", "medical_claim_header", "medical_claim_icd_procedure",
                   "member_month_detail", "pharmacy_claim", "provider", "provider_master")


#### STEP 2: Loop over APCD data tables, saving YAML file for each table ####
lapply(table_list, function(table_list) {
  
  #Read table path from list
  table_path <- glue(read_path, table_list)
  parquet_files <- list.files(
    table_path,
    pattern = "\\.parquet$",
    full.names = TRUE,
    recursive = FALSE
  )
  
  #Extract table name
  table_name_part <- table_list
  sql_table <- glue("apcd_", table_name_part)
  
  #Assign Synapse table DISTRIBUTION to each table
  if(table_list %in% c("cmsdrg_output_multi_ver", "inpatient_stay_summary_ltd")) {
    table_dist <- "DISTRIBUTION = HASH(inpatient_discharge_id)" 
  } else if(table_list %in% c("eligibility", "member_month_detail", "dental_claim", "pharmacy_claim")) {
    table_dist <- "DISTRIBUTION = HASH(internal_member_id)"
  } else if(table_list %in% c("medical_claim", "medical_claim_diagnosis", "medical_claim_icd_procedure")) {
    table_dist <- "DISTRIBUTION = HASH(medical_claim_service_line_id)"
  } else if(table_list %in% c("medical_claim_header")) {
    table_dist <- "DISTRIBUTION = HASH(medical_claim_header_id)"
  } else if(table_list %in% c("provider")) {
    table_dist <- "DISTRIBUTION = HASH(internal_provider_id)"
  } else if(table_list %in% c("provider_master")) {
    table_dist <- "DISTRIBUTION = REPLICATE"
  } else {
    table_dist <- "DISTRIBUTION = ROUND_ROBIN"
  }
  
  #Extract column names, data types, and column count
  ds <- open_dataset(table_path, format="parquet")
  vars_list <- names(ds)
  dtypes_arrow <- sapply(ds$schema, function(x) x$ToString())
  dtypes_clean <- sub(".*: ", "", dtypes_arrow)
  col_count <- length(vars_list)
  
  #Scan columns to identify max length for string columns
  max_lengths <- get_max_string_lengths_duckdb(table_path)
  
  #Convert arrow data types to SQL data type
  sql_types <- mapply(
    function(col, type) {
      if (type == "string") {
        ml <- max_lengths[[col]]
        # Add your safety buffer: e.g. *2 for short strings, constant for longer strings
        if (is.na(ml) || ml == 0) ml <- 1  # set width of 1 for null string cols
        
        if (ml < 10) {
          safe_len <- ml * 2
        } else {
          safe_len <- ml + 50
        }
        return(paste0("VARCHAR(", safe_len, ")"))
      } else {
        convert_arrow_to_sql(type)   # your numeric + date mappings
      }
    },
    col = vars_list,
    type = dtypes_clean,
    USE.NAMES = TRUE
  )
  sql_types <- as.list(sql_types)
  
  #Use duckdb to get row count
  con <- dbConnect(duckdb())
  row_count_num <- sum(sapply(parquet_files, function(fp) {
    as.numeric(dbGetQuery(con, paste0(
      "SELECT count(*) FROM read_parquet('", fp, "')"
    ))$count)
  }))
  row_count <- as.character(row_count_num) #convert to string to avoid scientific notation
  dbDisconnect(con, shutdown=TRUE)
  
  #Set up static parameters
  server_parameter_list <- list("to_schema" = to_schema, "to_table" = sql_table, "qa_schema" = qa_schema, "qa_table" = qa_table,
                                   "ext_data_source" = ext_data_source, "ext_schema" = ext_schema, "ext_table" = sql_table,
                                   "table_distribution" = table_dist,
                                   "dl_path" = glue(dl_path_base, table_name_part, "_import/"),
                                   "base_url" = base_url)

  format_list <- list("hhsaw" = server_parameter_list, "row_count" = row_count, "col_count" = col_count, "date_min" = date_min,
                      "date_max" = date_max, "date_delivery" = date_delivery, "note_delivery" = glue(sql_table, ", extract ", apcd_extract_number),
                      "vars" = sql_types)
  yaml::write_yaml(x = format_list,
                   file = glue(write_path, "load_", to_schema, ".", sql_table, "_full", ".yaml"),
                   indent = 4,
                   indent.mapping.sequence = T,
                   handlers = list(
                     Date = function(x) format(x, "%Y-%m-%d")
                   ))
  
  glue(sql_table, " YAML file successfully created.")
})


#### STEP 3: Set universal parameters for reference tables ####
read_path <- "//dphcifs/apde-cdip/apcd/apcd_data_import/reference_tables" #Folder containing ref tables exported from Analytic Enclave

##Smart selection for write path for YAML files
if(file.exists("C:/Users/SHERNANDEZ.KC/Documents/GitHub/claims_data/claims_db/phclaims/ref/tables/")){ #Susan on DPHXPHAAPR5EBYK
  write_path <- "C:/Users/SHERNANDEZ.KC/Documents/GitHub/claims_data/claims_db/phclaims/ref/tables/"
} else if(file.exists("C:/GitHub/claims_data/claims_db/phclaims/ref/tables/")){ #Eli on KC laptop
  write_path <- "C:/GitHub/claims_data/claims_db/phclaims/ref/tables/"
}

#Set static parameters for YAML file
to_schema <- "stg_claims"
dl_path_base <- "https://inthealthdtalakegen2.dfs.core.windows.net/inthealth/claims/apcd/reference_tables_import/"
base_url <- "https://inthealthdtalakegen2.dfs.core.windows.net/inthealth/"


#Establish list of tables for which YAML format files will be created
table_list <-  list.files(
  read_path,
    pattern = "\\.parquet$",
    full.names = TRUE
)

#### STEP 4: Loop over APCD ref tables, saving YAML file for each table ####
lapply(table_list, function(table_list) {
  
  #Read table path from list
  table_path <- table_list

  #Extract table name
  table_name_part <- tools::file_path_sans_ext(basename(table_list))
  table_name_clean <- gsub("000", "", table_name_part)
  sql_table <- glue("ref_apcd_", table_name_clean)
  
  #Extract column names, data types, and column count
  ds <- open_dataset(table_path, format="parquet")
  vars_list <- names(ds)
  dtypes_arrow <- sapply(ds$schema, function(x) x$ToString())
  dtypes_clean <- sub(".*: ", "", dtypes_arrow)
  col_count <- length(vars_list)
  
  #Scan columns to identify max length for string columns
  max_lengths <- get_max_string_lengths_duckdb(table_path)
  
  #Convert arrow data types to SQL data type
  sql_types <- mapply(
    function(col, type) {
      if (type == "string") {
        ml <- max_lengths[[col]]
        # Add your safety buffer: e.g. *2 for short strings, constant for longer strings
        if (ml < 10) {
          safe_len <- ml * 2
        } else {
          safe_len <- ml + 50
        }
        return(paste0("VARCHAR(", safe_len, ")"))
      } else {
        convert_arrow_to_sql(type)   # your numeric + date mappings
      }
    },
    col = vars_list,
    type = dtypes_clean,
    USE.NAMES = TRUE
  )
  sql_types <- as.list(sql_types)
  
  #Set up static parameters
  server_parameter_list <- list("to_schema" = to_schema, "to_table" = sql_table,
                                "dl_path" = glue(dl_path_base, table_name_part, ".parquet"),
                                "base_url" = base_url)
  
  format_list <- list("hhsaw" = server_parameter_list, "col_count" = col_count, "vars" = sql_types)
  yaml::write_yaml(x = format_list,
                   file = glue(write_path, "load_", to_schema, ".", sql_table, ".yaml"),
                   indent = 4,
                   indent.mapping.sequence = T,
                   handlers = list(
                     Date = function(x) format(x, "%Y-%m-%d")
                   ))
  
  glue(sql_table, " YAML file successfully created.")
})
