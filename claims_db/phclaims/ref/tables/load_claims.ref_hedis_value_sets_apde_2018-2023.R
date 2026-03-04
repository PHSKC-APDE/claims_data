##Script to load all HEDIS and QRS measure value sets to SQL for 2018-2023
##HEDIS = Healthcare Effectiveness Data & Information Set, maintained by NCQA, proprietary
##QRS = Quality Rating System, maintained by CMS, includes HEDIS and Pharmacy Quality Alliance (PQA) measures, available to public
##This also includes Medications List Directories (MLD), which are used by HEDIS measures
##Eli Kern, September 2023

## 2026-03 update: Modify code to work with current SQL standards and file paths

#### Setup ####

##Load packages and set defaults
pacman::p_load(tidyverse, openxlsx, odbc) # Load list of packages
options(max.print = 350) # Limit # of rows to show when printing/showing a data.frame
options(tibble.print_max = 50) # Limit # of rows to show when printing/showing a tibble (a tidyverse-flavored data.frame)
options(scipen = 999) # Avoid scientific notation
origin <- "1970-01-01" # Set the origin date, which is needed for many data/time functions

#Set path for secure drive where HEDIS and QRS value sets are stored
hedis_file_path <- "//dphcifs/APDE-CDIP/HEDIS/"

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/load_table_from_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")

## Connect to HHSAW
interactive_auth <- FALSE
prod <- TRUE
db_claims <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)


#### Step 1: Load HEDIS and QRS value sets to R, one year at a time, 2018-2023 ####

##2023 QRS

hedis_measure_2023 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2023_QRS_Only_Free/", "MY 2023 HEDIS for QRS VSD 2023-03-31.xlsx"),
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
  
  mutate(year = 2023) %>%
  relocate(year, .before = everything())

hedis_value_set_2023 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2023_QRS_Only_Free/", "MY 2023 HEDIS for QRS VSD 2023-03-31.xlsx"),
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
  
  mutate(year = 2023) %>%
  relocate(year, .before = everything())

hedis_medication_measure_2023 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2023_QRS_Only_Free/", "HEDIS MY 2023 Medication List Directory 2023-03-31.xlsx"),
  sheet = "Measures to Medication Lists",
  colNames = TRUE,
  detectDates = TRUE,
  skipEmptyCols = TRUE,
  skipEmptyRows = TRUE) %>%
  
  rename(
    measure_id = Measure.ID,
    measure_name = Measure.Name,
    medication_list_name = Medication.List.Name,
    medication_list_oid = Value.Set.OID) %>%
  
  mutate(year = 2023) %>%
  relocate(year, .before = everything())

hedis_medication_list_2023 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2023_QRS_Only_Free/", "HEDIS MY 2023 Medication List Directory 2023-03-31.xlsx"),
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
    year = 2023) %>%
  
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


#2022 QRS

hedis_measure_2022 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2022_QRS_Only_Free/", "MY 2022 HEDIS for QRS VSD 2022-10-12.xlsx"),
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
  
  mutate(year = 2022) %>%
  relocate(year, .before = everything())

hedis_value_set_2022 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2022_QRS_Only_Free/", "MY 2022 HEDIS for QRS VSD 2022-10-12.xlsx"),
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
  
  mutate(year = 2022) %>%
  relocate(year, .before = everything())

hedis_medication_measure_2022 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2022_QRS_Only_Free/", "HEDIS MY 2022 Medication List Directory 2022-03-31.xlsx"),
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
  
  mutate(year = 2022) %>%
  relocate(year, .before = everything())

hedis_medication_list_2022 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2022_QRS_Only_Free/", "HEDIS MY 2022 Medication List Directory 2022-03-31.xlsx"),
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
    year = 2022) %>%
  
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


#2021 QRS

hedis_measure_2021 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2021_QRS_Only_Free/", "MY 2021 HEDIS for QRS VSD 2021-03-31.xlsx"),
  sheet = "Measure ID to Value Sets",
  colNames = TRUE,
  detectDates = TRUE,
  skipEmptyCols = TRUE,
  skipEmptyRows = TRUE) %>%
  
  rename(
    measure_id = Measure.ID,
    measure_name = Measure.Name,
    value_set_name = Value.Set.Name,
    value_set_oid = Value.Set.OID) %>%
  
  mutate(year = 2021) %>%
  relocate(year, .before = everything())

hedis_value_set_2021 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2021_QRS_Only_Free/", "MY 2021 HEDIS for QRS VSD 2021-03-31.xlsx"),
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
  
  mutate(year = 2021) %>%
  relocate(year, .before = everything())

hedis_medication_measure_2021 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2021_QRS_Only_Free/", "HEDIS MY 2021 Medication List Directory 2021-03-31.xlsx"),
  sheet = "Measures to Medication Lists",
  colNames = TRUE,
  detectDates = TRUE,
  skipEmptyCols = TRUE,
  skipEmptyRows = TRUE) %>%
  
  rename(
    measure_id = Measure.ID,
    measure_name = Measure.Name,
    medication_list_name = Medication.List.Name,
    medication_list_oid = Value.Set.OID) %>%
  
  mutate(year = 2021) %>%
  relocate(year, .before = everything())

hedis_medication_list_2021 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2021_QRS_Only_Free/", "HEDIS MY 2021 Medication List Directory 2021-03-31.xlsx"),
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
    code_system_version = Code.System.Version,
    drug_class = Drug.Class) %>%
  
  mutate(
    drug_id = NA_character_,
    drug_name = NA_character_,
    form = NA_character_,
    med_conversion_factor = NA_character_,
    year = 2021) %>%
  
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


#2020 HEDIS

hedis_measure_2020 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2020/", "N. HEDIS 2020 Volume 2 Value Set Directory 10-01-2019.xlsx"),
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
  
  mutate(year = 2020) %>%
  relocate(year, .before = everything())

hedis_value_set_2020 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2020/", "N. HEDIS 2020 Volume 2 Value Set Directory 10-01-2019.xlsx"),
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
  
  mutate(year = 2020) %>%
  relocate(year, .before = everything())

hedis_medication_measure_2020 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2020/", "HEDIS MY 2020 Medication List Directory 2020-11-02.xlsx"),
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
  
  mutate(year = 2020) %>%
  relocate(year, .before = everything())


hedis_medication_list_2020 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2020/", "HEDIS MY 2020 Medication List Directory 2020-11-02.xlsx"),
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
    code_system_version = Code.System.Version,
    drug_class = Drug.Class) %>%
  
  mutate(
    drug_id = NA_character_,
    drug_name = NA_character_,
    form = NA_character_,
    med_conversion_factor = NA_character_,
    year = 2020) %>%
  
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


#2019 HEDIS

hedis_measure_2019 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2019/", "M. HEDIS 2019 Volume 2 VSD 11_05_2018.xlsx"),
  sheet = "Volume 2 Measures to Value Sets",
  colNames = TRUE,
  detectDates = TRUE,
  skipEmptyCols = TRUE,
  skipEmptyRows = TRUE) %>%
  
  rename(
    measure_id = Measure.ID,
    measure_name = Measure.Name,
    value_set_name = Value.Set.Name,
    value_set_oid = Value.Set.OID) %>%
  
  mutate(year = 2019) %>%
  relocate(year, .before = everything())

hedis_value_set_2019 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2019/", "M. HEDIS 2019 Volume 2 VSD 11_05_2018.xlsx"),
  sheet = "Volume 2 Value Sets to Codes",
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
  
  mutate(year = 2019) %>%
  relocate(year, .before = everything())

hedis_medication_measure_2019 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2019/", "HEDIS-2019-NDC-MLD-Directory-Complete-Workbook-FINAL-11-1-2018-2.xlsx"),
  sheet = "Measure ID to Medications List",
  colNames = TRUE,
  detectDates = TRUE,
  skipEmptyCols = TRUE,
  skipEmptyRows = TRUE) %>%
  
  rename(
    measure_id = Measure.ID,
    measure_name = Measure.Name,
    medication_list_name = Medication.List.Name) %>%
  
  mutate(
    medication_list_oid = NA_character_,
    year = 2019) %>%
  relocate(year, .before = everything())

hedis_medication_list_2019 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2019/", "HEDIS-2019-NDC-MLD-Directory-Complete-Workbook-FINAL-11-1-2018-2.xlsx"),
  sheet = "Medications List to NDC Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skipEmptyCols = TRUE,
  skipEmptyRows = TRUE) %>%
  
  rename(
    medication_list_name = Medication.List,
    code = NDC.Code,
    brand_name = Brand.Name,
    generic_product_name = Generic.Product.Name,
    route = Route,
    drug_class = Description,
    drug_id = Drug.ID,
    drug_name = Drug.Name,
    package_size = Package.Size,
    unit = Unit,
    dose = Dose,
    form = Form,
    med_conversion_factor = MED.Conversion.Factor) %>%
  
  mutate(
    medication_list_oid = NA_character_,
    medication_list_version = NA_character_,
    code_system = "NDC",
    code_system_oid = NA_character_,
    code_system_version = NA_character_,
    year = 2019) %>%
  
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


#2018 HEDIS

hedis_measure_2018 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2018/", "2018 Volume 2 Value Set Directory 07_03_2017.xlsx"),
  sheet = "Volume 2 Measures to Value Sets",
  colNames = TRUE,
  detectDates = TRUE,
  skipEmptyCols = TRUE,
  skipEmptyRows = TRUE) %>%
  
  rename(
    measure_id = Measure.ID,
    measure_name = Measure.Name,
    value_set_name = Value.Set.Name,
    value_set_oid = Value.Set.OID) %>%
  
  mutate(year = 2018) %>%
  relocate(year, .before = everything())

hedis_value_set_2018 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2018/", "2018 Volume 2 Value Set Directory 07_03_2017.xlsx"),
  sheet = "Volume 2 Value Sets to Codes",
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
  
  mutate(year = 2018) %>%
  relocate(year, .before = everything())

hedis_medication_measure_2018 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2018/", "20180208_HEDIS_NDC_MLD_CompleteDirectory_Workbook_2018.xlsx"),
  sheet = "Measure ID to Medications List",
  colNames = TRUE,
  detectDates = TRUE,
  skipEmptyCols = TRUE,
  skipEmptyRows = TRUE) %>%
  
  rename(
    measure_id = Measure.ID,
    measure_name = Measure.Name,
    medication_list_name = Medication.List.Name) %>%
  
  mutate(
    medication_list_oid = NA_character_,
    year = 2018) %>%
  relocate(year, .before = everything())

hedis_medication_list_2018 <- read.xlsx(
  xlsxFile= paste0(hedis_file_path, "2018/", "20180208_HEDIS_NDC_MLD_CompleteDirectory_Workbook_2018.xlsx"),
  sheet = "Medications List to NDC Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skipEmptyCols = TRUE,
  skipEmptyRows = TRUE) %>%
  
  rename(
    medication_list_name = Medication.List,
    code = NDC.Code,
    brand_name = Brand.Name,
    generic_product_name = Generic.Product.Name,
    route = Route,
    drug_class = Description,
    drug_id = Drug.ID,
    drug_name = Drug.Name,
    package_size = Package.Size,
    unit = Unit,
    dose = Dose,
    form = Form,
    med_conversion_factor = MED.Conversion.Factor) %>%
  
  mutate(
    medication_list_oid = NA_character_,
    medication_list_version = NA_character_,
    code_system = "NDC",
    code_system_oid = NA_character_,
    code_system_version = NA_character_,
    year = 2018) %>%
  
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


#### Step 2: Bind all years into one data frame for each reference table and add date-time for last_run column ####

hedis_measures <- ls(pattern = "^hedis_measure_\\d{4}$") |>
  mget(envir = .GlobalEnv) |>
  bind_rows() %>%
  mutate(last_run = Sys.time())

hedis_value_sets <- ls(pattern = "^hedis_value_set_\\d{4}$") |>
  mget(envir = .GlobalEnv) |>
  bind_rows() %>%
  mutate(last_run = Sys.time())

hedis_medication_measures <- ls(pattern = "^hedis_medication_measure_\\d{4}$") |>
  mget(envir = .GlobalEnv) |>
  bind_rows() %>%
  mutate(last_run = Sys.time())

hedis_medication_lists <- ls(pattern = "^hedis_medication_list_\\d{4}$") |>
  mget(envir = .GlobalEnv) |>
  bind_rows() %>%
  mutate(last_run = Sys.time())

#Remove interim data frames
rm(list = ls(pattern = "^hedis_measure_\\d{4}$"))
rm(list = ls(pattern = "^hedis_value_set_\\d{4}$"))
rm(list = ls(pattern = "^hedis_medication_measure_\\d{4}$"))
rm(list = ls(pattern = "^hedis_medication_list_\\d{4}$"))


#### Step 3: Normalize ICD-CM codes to align with claims data structure ####

#Strip punctuation from ICD-10-CM codes
#Strip punctuation from ICD-9-CM codes and pad to 5 digits with trailing zeroes
hedis_value_sets <- hedis_value_sets %>%
  
  mutate(
    code = case_when(
      code_system %in% c("ICD10CM", "ICD9CM") ~ str_replace_all(code, "[:punct:]", ""),
      TRUE ~ code),
    
    code = case_when(
      code_system == "ICD9CM" & str_length(code) == 3 ~ paste0(code, "00"),
      code_system == "ICD9CM" & str_length(code) == 4 ~ paste0(code, "0"),
      TRUE ~ code))

#Confirm that all ICD9CM codes are 5 digits long
hedis_value_sets %>% filter(code_system == "ICD9CM") %>% distinct(str_length(code))

#Confirm that all ICD10CM codes are between 3-7 digits long
hedis_value_sets %>% filter(code_system == "ICD10CM") %>% distinct(str_length(code))

#Confirm that all NDC codes are 11-digits long
hedis_medication_lists %>% filter(code_system == "NDC") %>% distinct(str_length(code))


#### Step 4: Upload reference tables to HHSAW ####

to_schema <- "claims"
to_table_measures <- "ref_hedis_measures_apde"
to_table_value_sets <- "ref_hedis_value_sets_apde"
to_table_med_measures <- "ref_hedis_medication_measures_apde"
to_table_med_lists <- "ref_hedis_medication_lists_apde"

## Load data to HHSAW

#HEDIS measures (<1 min)
system.time(dbWriteTable(db_hhsaw, name = DBI::Id(schema = to_schema, table = to_table_measures), 
             value = as.data.frame(hedis_measures), 
             overwrite = T))

#HEDIS value sets (69 min)
system.time(dbWriteTable(db_hhsaw, name = DBI::Id(schema = to_schema, table = to_table_value_sets), 
             value = as.data.frame(hedis_value_sets), 
             overwrite = T))

#HEDIS medication measures (<1 min)
system.time(dbWriteTable(db_hhsaw, name = DBI::Id(schema = to_schema, table = to_table_med_measures), 
             value = as.data.frame(hedis_medication_measures), 
             overwrite = T))

#HEDIS medication lists (225 min)
system.time(dbWriteTable(db_hhsaw, name = DBI::Id(schema = to_schema, table = to_table_med_lists), 
             value = as.data.frame(hedis_medication_lists), 
             overwrite = T))

# Add index on HEDIS value sets table (<1 min)
system.time(DBI::dbExecute(db_hhsaw, 
  glue::glue_sql(
  "CREATE CLUSTERED INDEX [idx_cl_code_system_code] ON {`to_schema`}.{`to_table_value_sets`}(code_system, code)",
  .con = db_hhsaw)))