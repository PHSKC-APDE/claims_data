##Script to load QRS measure value sets to SQL for 2024
##QRS = Quality Rating System, maintained by CMS, includes HEDIS and Pharmacy Quality Alliance (PQA) measures, available to public
##This also includes Medications List Directories (MLD), which are used by HEDIS measures
##Eli Kern, March 2026

#### Setup ####

##Load packages and set defaults
pacman::p_load(tidyverse, openxlsx, odbc) # Load list of packages
options(max.print = 350) # Limit # of rows to show when printing/showing a data.frame
options(tibble.print_max = 50) # Limit # of rows to show when printing/showing a tibble (a tidyverse-flavored data.frame)
options(scipen = 999) # Avoid scientific notation
origin <- "1970-01-01" # Set the origin date, which is needed for many data/time functions

#Set path for secure drive where HEDIS and QRS value sets are stored - update this each year
hedis_file_path <- "//dphcifs/APDE-CDIP/HEDIS/2024_QRS_Only_Free/"
hedis_year <- 2024L

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/load_table_from_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")

## Connect to HHSAW
interactive_auth <- FALSE
prod <- TRUE
db_claims <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)


#### Step 1: Load HEDIS and QRS value sets to R ####

hedis_measure <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "HEDIS for QRS MY 2024 VSD 2024-04-01.xlsx"),
  sheet = "Measures to Value Sets",
  colNames = TRUE,
  detectDates = TRUE,
  skipEmptyCols = TRUE,
  skipEmptyRows = TRUE) %>%
  
  rename(
    measure_id = Measure.ID,
    measure_name = Measure.Name,
    value_set_name = Value.Set.Name,
    value_set_oid = Value.Set.OID) %>%
  
  mutate(year = hedis_year) %>%
  relocate(year, .before = everything())

hedis_value_set <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "HEDIS for QRS MY 2024 VSD 2024-04-01.xlsx"),
  sheet = "Value Sets to Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skipEmptyCols = TRUE,
  skipEmptyRows = TRUE) %>%
  
  rename(
    value_set_name = Value.Set.Name,
    value_set_oid = Value.Set.OID,
    value_set_version = Value.Set.Version,
    code = Code,
    definition = Definition,
    code_system = Code.System,
    code_system_oid = Code.System.OID,
    code_system_version = Code.System.Version) %>%
  
  mutate(year = hedis_year) %>%
  relocate(year, .before = everything())

hedis_medication_measure <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "HEDIS MY 2024 Medication List Directory 2025-01-15.xlsx"),
  sheet = "Measures to Medication Lists",
  colNames = TRUE,
  detectDates = TRUE,
  skipEmptyCols = TRUE,
  skipEmptyRows = TRUE) %>%
  
  rename(
    measure_id = Measure.ID,
    measure_name = Measure.Name,
    medication_list_name = Medication.List.Name,
    medication_list_oid = Medication.List.OID) %>%
  
  mutate(year = hedis_year) %>%
  relocate(year, .before = everything())

hedis_medication_list <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "HEDIS MY 2024 Medication List Directory 2025-01-15.xlsx"),
  sheet = "Medication Lists to Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skipEmptyCols = TRUE,
  skipEmptyRows = TRUE) %>%
  
  rename(
    medication_list_name = Medication.List.Name,
    medication_list_oid = Medication.List.OID,
    medication_list_version = Medication.List.Version,
    code = Code,
    generic_product_name = Generic.Product.Name,
    brand_name = Brand.Name,
    route = Route,
    package_size = Package.Size,
    unit = Unit,
    code_system = Code.System,
    code_system_oid = Code.System.OID,
    code_system_version = Code.System.Version) %>%
  
  mutate(
    drug_class = NA_character_,
    drug_id = NA_character_,
    drug_name = NA_character_,
    form = NA_character_,
    med_conversion_factor = NA_character_,
    year = hedis_year) %>%
  
  select(
    year,
    medication_list_name,
    medication_list_oid,
    medication_list_version,
    code,
    generic_product_name,
    brand_name,
    route,
    package_size,
    unit,
    code_system,
    code_system_oid,
    code_system_version,
    drug_class,
    drug_id,
    drug_name,
    form,
    med_conversion_factor)


#### Step 2: Add last run column ####

hedis_measure <- hedis_measure %>%
  mutate(last_run = Sys.time())

hedis_value_set <- hedis_value_set %>%
  mutate(last_run = Sys.time())

hedis_medication_measure <- hedis_medication_measure %>%
  mutate(last_run = Sys.time())

hedis_medication_list <- hedis_medication_list %>%
  mutate(last_run = Sys.time())


#### Step 3: Normalize ICD-CM codes to align with claims data structure ####

#Strip punctuation from ICD-10-CM codes
#Strip punctuation from ICD-9-CM codes and pad to 5 digits with trailing zeroes
hedis_value_set <- hedis_value_set %>%
  
  mutate(
    code = case_when(
      code_system %in% c("ICD10CM", "ICD9CM") ~ str_replace_all(code, "[:punct:]", ""),
      TRUE ~ code),
    
    code = case_when(
      code_system == "ICD9CM" & str_length(code) == 3 ~ paste0(code, "00"),
      code_system == "ICD9CM" & str_length(code) == 4 ~ paste0(code, "0"),
      TRUE ~ code))

#Confirm that all ICD9CM codes are 5 digits long
hedis_value_set %>% filter(code_system == "ICD9CM") %>% distinct(str_length(code))

#Confirm that all ICD10CM codes are between 3-7 digits long
hedis_value_set %>% filter(code_system == "ICD10CM") %>% distinct(str_length(code))

#Confirm that all NDC codes are 11-digits long
hedis_medication_list %>% filter(code_system == "NDC") %>% distinct(str_length(code))


#### Step 4: Upload reference tables to HHSAW ####

to_schema <- "claims"
to_table_measures <- "ref_hedis_measures_apde"
to_table_value_sets <- "ref_hedis_value_sets_apde"
to_table_med_measures <- "ref_hedis_medication_measures_apde"
to_table_med_lists <- "ref_hedis_medication_lists_apde"

## Load data to HHSAW

#HEDIS measures (<1 min)
system.time(dbAppendTable(
  db_claims,
  name = DBI::Id(schema = to_schema, table = to_table_measures),
  value = hedis_measure
))

#HEDIS value sets (<1 min)
system.time(dbAppendTable(
  db_claims,
  name = DBI::Id(schema = to_schema, table = to_table_value_sets),
  value = hedis_value_set
))

#HEDIS medication measures (<1 min)
system.time(dbAppendTable(
  db_claims,
  name = DBI::Id(schema = to_schema, table = to_table_med_measures),
  value = hedis_medication_measure
))

#HEDIS medication lists (<1 min)
system.time(dbAppendTable(
  db_claims,
  name = DBI::Id(schema = to_schema, table = to_table_med_lists),
  value = hedis_medication_list
))