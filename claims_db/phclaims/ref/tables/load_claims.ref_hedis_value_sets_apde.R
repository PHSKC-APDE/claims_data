##Script to load all HEDIS and QRS measure value sets to SQL
##HEDIS = Healthcare Effectiveness Data & Information Set, maintained by NCQA, proprietary
##QRS = Quality Rating System, maintained by CMS, includes HEDIS and Pharmacy Quality Alliance (PQA) measures, available to public
##This also includes Medications List Directories (MLD), which are used by HEDIS measures
##Eli Kern, September 2023

#### Setup ####

##Load packages and set defaults
pacman::p_load(tidyverse, openxlsx2, data.table, lubridate, odbc) # Load list of packages
options(max.print = 350) # Limit # of rows to show when printing/showing a data.frame
options(tibble.print_max = 50) # Limit # of rows to show when printing/showing a tibble (a tidyverse-flavored data.frame)
options(scipen = 999) # Avoid scientific notation
origin <- "1970-01-01" # Set the origin date, which is needed for many data/time functions

#Set path for secure drive where HEDIS and QRS value sets are stored
hedis_file_path <- "//dchs-shares01/dchsdata/dchsphclaimsdata/hedis/"
hedis_write_path <- "//dchs-shares01/dchsdata/dchsphclaimsdata/hedis/hedis_value_sets_for_sql_load/"

#Connect to HHSAW using ODBC driver
db_hhsaw <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433",
                           database = "hhs_analytics_workspace",
                           uid = keyring::key_list("hhsaw")[["username"]],
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")


#### Step 1: Load HEDIS and QRS value sets to R ####

#2018 HEDIS

hedis_measure_2018 <- read_xlsx(
  file = paste0(hedis_file_path, "2018/", "2018 Volume 2 Value Set Directory 07_03_2017.xlsx"),
  sheet = "Volume 2 Measures to Value Sets",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    measure_id = `Measure ID`,
    measure_name = `Measure Name`,
    value_set_name = `Value Set Name`,
    value_set_oid = `Value Set OID`) %>%
  
  mutate(year = 2018) %>%
  relocate(year, .before = everything())

hedis_value_set_2018 <- read_xlsx(
  file = paste0(hedis_file_path, "2018/", "2018 Volume 2 Value Set Directory 07_03_2017.xlsx"),
  sheet = "Volume 2 Value Sets to Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    value_set_name = `Value Set Name`,
    value_set_oid = `Value Set OID`,
    value_set_version = `Value Set Version`,
    code = Code,
    definition = Definition,
    code_system = `Code System`,
    code_system_oid = `Code System OID`,
    code_system_version = `Code System Version`) %>%
  
  mutate(year = 2018) %>%
  relocate(year, .before = everything())

hedis_medication_measure_2018 <- read_xlsx(
  file = paste0(hedis_file_path, "2018/", "20180208_HEDIS_NDC_MLD_CompleteDirectory_Workbook_2018.xlsx"),
  sheet = "Measure ID to Medications List",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    measure_id = `Measure ID`,
    measure_name = `Measure Name`,
    medication_list_name = `Medication List Name`) %>%
  
  mutate(
    medication_list_oid = NA_character_,
    year = 2018) %>%
  relocate(year, .before = everything())

hedis_medication_list_2018 <- read_xlsx(
  file = paste0(hedis_file_path, "2018/", "20180208_HEDIS_NDC_MLD_CompleteDirectory_Workbook_2018.xlsx"),
  sheet = "Medications List to NDC Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    medication_list_name = `Medication List`,
    code = `NDC Code`,
    brand_name = `Brand Name`,
    generic_product_name = `Generic Product Name`,
    route = Route,
    drug_class = Description,
    drug_id = `Drug ID`,
    drug_name = `Drug Name`,
    package_size = `Package Size`,
    unit = Unit,
    dose = Dose,
    form = Form,
    med_conversion_factor = `MED Conversion Factor`) %>%
  
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

#2019 HEDIS

hedis_measure_2019 <- read_xlsx(
  file = paste0(hedis_file_path, "2019/", "M. HEDIS 2019 Volume 2 VSD 11_05_2018.xlsx"),
  sheet = "Volume 2 Measures to Value Sets",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    measure_id = `Measure ID`,
    measure_name = `Measure Name`,
    value_set_name = `Value Set Name`,
    value_set_oid = `Value Set OID`) %>%
  
  mutate(year = 2019) %>%
  relocate(year, .before = everything())

hedis_value_set_2019 <- read_xlsx(
  file = paste0(hedis_file_path, "2019/", "M. HEDIS 2019 Volume 2 VSD 11_05_2018.xlsx"),
  sheet = "Volume 2 Value Sets to Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    value_set_name = `Value Set Name`,
    value_set_oid = `Value Set OID`,
    value_set_version = `Value Set Version`,
    code = Code,
    definition = Definition,
    code_system = `Code System`,
    code_system_oid = `Code System OID`,
    code_system_version = `Code System Version`) %>%
  
  mutate(year = 2019) %>%
  relocate(year, .before = everything())

hedis_medication_measure_2019 <- read_xlsx(
  file = paste0(hedis_file_path, "2019/", "HEDIS-2019-NDC-MLD-Directory-Complete-Workbook-FINAL-11-1-2018-2.xlsx"),
  sheet = "Measure ID to Medications List",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    measure_id = `Measure ID`,
    measure_name = `Measure Name`,
    medication_list_name = `Medication List Name`) %>%
  
  mutate(
    medication_list_oid = NA_character_,
    year = 2019) %>%
  relocate(year, .before = everything())

hedis_medication_list_2019 <- read_xlsx(
  file = paste0(hedis_file_path, "2019/", "HEDIS-2019-NDC-MLD-Directory-Complete-Workbook-FINAL-11-1-2018-2.xlsx"),
  sheet = "Medications List to NDC Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    medication_list_name = `Medication List`,
    code = `NDC Code`,
    brand_name = `Brand Name`,
    generic_product_name = `Generic Product Name`,
    route = Route,
    drug_class = Description,
    drug_id = `Drug ID`,
    drug_name = `Drug Name`,
    package_size = `Package Size`,
    unit = Unit,
    dose = Dose,
    form = Form,
    med_conversion_factor = `MED Conversion Factor`) %>%
  
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

#2020 HEDIS

hedis_measure_2020 <- read_xlsx(
  file = paste0(hedis_file_path, "2020/", "N. HEDIS 2020 Volume 2 Value Set Directory 10-01-2019.xlsx"),
  sheet = "Measures to Value Sets",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    measure_id = `Measure ID`,
    measure_name = `Measure Name`,
    value_set_name = `Value Set Name`,
    value_set_oid = `Value Set OID`) %>%
  
  mutate(year = 2020) %>%
  relocate(year, .before = everything())

hedis_value_set_2020 <- read_xlsx(
  file = paste0(hedis_file_path, "2020/", "N. HEDIS 2020 Volume 2 Value Set Directory 10-01-2019.xlsx"),
  sheet = "Value Sets to Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    value_set_name = `Value Set Name`,
    value_set_oid = `Value Set OID`,
    value_set_version = `Value Set Version`,
    code = Code,
    definition = Definition,
    code_system = `Code System`,
    code_system_oid = `Code System OID`,
    code_system_version = `Code System Version`) %>%
  
  mutate(year = 2020) %>%
  relocate(year, .before = everything())

hedis_medication_measure_2020 <- read_xlsx(
  file = paste0(hedis_file_path, "2020/", "HEDIS MY 2020 Medication List Directory 2020-11-02.xlsx"),
  sheet = "Measures to Medication Lists",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    measure_id = `Measure ID`,
    measure_name = `Measure Name`,
    medication_list_name = `Medication List Name`,
    medication_list_oid = `Medication List OID`) %>%

  mutate(year = 2020) %>%
  relocate(year, .before = everything())


hedis_medication_list_2020 <- read_xlsx(
  file = paste0(hedis_file_path, "2020/", "HEDIS MY 2020 Medication List Directory 2020-11-02.xlsx"),
  sheet = "Medication Lists to Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    medication_list_name = `Medication List Name`,
    medication_list_oid = `Medication List OID`,
    medication_list_version = `Medication List Version`,
    code = Code,
    generic_product_name = `Generic Product Name`,
    brand_name = `Brand Name`,
    route = Route,
    package_size = `Package Size`,
    unit = Unit,
    code_system = `Code System`,
    code_system_oid = `Code System OID`,
    code_system_version = `Code System Version`,
    drug_class = `Drug Class`) %>%
  
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

#2021 QRS

hedis_measure_2021 <- read_xlsx(
  file = paste0(hedis_file_path, "2021_QRS_Only_Free/", "MY 2021 HEDIS for QRS VSD 2021-03-31.xlsx"),
  sheet = "Measure ID to Value Sets",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    measure_id = `Measure ID`,
    measure_name = `Measure Name`,
    value_set_name = `Value Set Name`,
    value_set_oid = `Value Set OID`) %>%
  
  mutate(year = 2021) %>%
  relocate(year, .before = everything())

hedis_value_set_2021 <- read_xlsx(
  file = paste0(hedis_file_path, "2021_QRS_Only_Free/", "MY 2021 HEDIS for QRS VSD 2021-03-31.xlsx"),
  sheet = "Value Sets to Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    value_set_name = `Value Set Name`,
    value_set_oid = `Value Set OID`,
    value_set_version = `Value Set Version`,
    code = Code,
    definition = Definition,
    code_system = `Code System`,
    code_system_oid = `Code System OID`,
    code_system_version = `Code System Version`) %>%
  
  mutate(year = 2021) %>%
  relocate(year, .before = everything())

hedis_medication_measure_2021 <- read_xlsx(
  file = paste0(hedis_file_path, "2021_QRS_Only_Free/", "HEDIS MY 2021 Medication List Directory 2021-03-31.xlsx"),
  sheet = "Measures to Medication Lists",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    measure_id = `Measure ID`,
    measure_name = `Measure Name`,
    medication_list_name = `Medication List Name`,
    medication_list_oid = `Value Set OID`) %>%
  
  mutate(year = 2021) %>%
  relocate(year, .before = everything())

hedis_medication_list_2021 <- read_xlsx(
  file = paste0(hedis_file_path, "2021_QRS_Only_Free/", "HEDIS MY 2021 Medication List Directory 2021-03-31.xlsx"),
  sheet = "Medication Lists to Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    medication_list_name = `Medication List Name`,
    medication_list_oid = `Medication List OID`,
    medication_list_version = `Medication List Version`,
    code = Code,
    generic_product_name = `Generic Product Name`,
    brand_name = `Brand Name`,
    route = Route,
    package_size = `Package Size`,
    unit = Unit,
    code_system = `Code System`,
    code_system_oid = `Code System OID`,
    code_system_version = `Code System Version`,
    drug_class = `Drug Class`) %>%
  
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

#2022 QRS

hedis_measure_2022 <- read_xlsx(
  file = paste0(hedis_file_path, "2022_QRS_Only_Free/", "MY 2022 HEDIS for QRS VSD 2022-10-12.xlsx"),
  sheet = "Measures to Value Sets",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    measure_id = `Measure ID`,
    measure_name = `Measure Name`,
    value_set_name = `Value Set Name`,
    value_set_oid = `Value Set OID`) %>%
  
  mutate(year = 2022) %>%
  relocate(year, .before = everything())

hedis_value_set_2022 <- read_xlsx(
  file = paste0(hedis_file_path, "2022_QRS_Only_Free/", "MY 2022 HEDIS for QRS VSD 2022-10-12.xlsx"),
  sheet = "Value Sets to Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    value_set_name = `Value Set Name`,
    value_set_oid = `Value Set OID`,
    value_set_version = `Value Set Version`,
    code = Code,
    definition = Definition,
    code_system = `Code System`,
    code_system_oid = `Code System OID`,
    code_system_version = `Code System Version`) %>%
  
  mutate(year = 2022) %>%
  relocate(year, .before = everything())

hedis_medication_measure_2022 <- read_xlsx(
  file = paste0(hedis_file_path, "2022_QRS_Only_Free/", "HEDIS MY 2022 Medication List Directory 2022-03-31.xlsx"),
  sheet = "Measures to Medication Lists",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    measure_id = `Measure ID`,
    measure_name = `Measure Name`,
    medication_list_name = `Medication List Name`,
    medication_list_oid = `Medication List OID`) %>%
  
  mutate(year = 2022) %>%
  relocate(year, .before = everything())

hedis_medication_list_2022 <- read_xlsx(
  file = paste0(hedis_file_path, "2022_QRS_Only_Free/", "HEDIS MY 2022 Medication List Directory 2022-03-31.xlsx"),
  sheet = "Medication Lists to Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    medication_list_name = `Medication List Name`,
    medication_list_oid = `Medication List OID`,
    medication_list_version = `Medication List Version`,
    code = Code,
    generic_product_name = `Generic Product Name`,
    brand_name = `Brand Name`,
    route = Route,
    package_size = `Package Size`,
    unit = Unit,
    code_system = `Code System`,
    code_system_oid = `Code System OID`,
    code_system_version = `Code System Version`) %>%
  
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

#2023 QRS

hedis_measure_2023 <- read_xlsx(
  file = paste0(hedis_file_path, "2023_QRS_Only_Free/", "MY 2023 HEDIS for QRS VSD 2023-03-31.xlsx"),
  sheet = "Measures to Value Sets",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    measure_id = `Measure ID`,
    measure_name = `Measure Name`,
    value_set_name = `Value Set Name`,
    value_set_oid = `Value Set OID`) %>%
  
  mutate(year = 2023) %>%
  relocate(year, .before = everything())

hedis_value_set_2023 <- read_xlsx(
  file = paste0(hedis_file_path, "2023_QRS_Only_Free/", "MY 2023 HEDIS for QRS VSD 2023-03-31.xlsx"),
  sheet = "Value Sets to Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    value_set_name = `Value Set Name`,
    value_set_oid = `Value Set OID`,
    value_set_version = `Value Set Version`,
    code = Code,
    definition = Definition,
    code_system = `Code System`,
    code_system_oid = `Code System OID`,
    code_system_version = `Code System Version`) %>%
  
  mutate(year = 2023) %>%
  relocate(year, .before = everything())

hedis_medication_measure_2023 <- read_xlsx(
  file = paste0(hedis_file_path, "2023_QRS_Only_Free/", "HEDIS MY 2023 Medication List Directory 2023-03-31.xlsx"),
  sheet = "Measures to Medication Lists",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    measure_id = `Measure ID`,
    measure_name = `Measure Name`,
    medication_list_name = `Medication List Name`,
    medication_list_oid = `Value Set OID`) %>%
  
  mutate(year = 2023) %>%
  relocate(year, .before = everything())

hedis_medication_list_2023 <- read_xlsx(
  file = paste0(hedis_file_path, "2023_QRS_Only_Free/", "HEDIS MY 2023 Medication List Directory 2023-03-31.xlsx"),
  sheet = "Medication Lists to Codes",
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  rename(
    medication_list_name = `Medication List Name`,
    medication_list_oid = `Medication List OID`,
    medication_list_version = `Medication List Version`,
    code = Code,
    generic_product_name = `Generic Product Name`,
    brand_name = `Brand Name`,
    route = Route,
    package_size = `Package Size`,
    unit = Unit,
    code_system = `Code System`,
    code_system_oid = `Code System OID`,
    code_system_version = `Code System Version`) %>%
  
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

#### Step 2: Bind all years into one data frame for each reference table ####

hedis_measures <- bind_rows(
  hedis_measure_2018,
  hedis_measure_2019,
  hedis_measure_2020,
  hedis_measure_2021,
  hedis_measure_2022,
  hedis_measure_2023)

hedis_value_sets <- bind_rows(
  hedis_value_set_2018,
  hedis_value_set_2019,
  hedis_value_set_2020,
  hedis_value_set_2021,
  hedis_value_set_2022,
  hedis_value_set_2023)

hedis_medication_measures <- bind_rows(
  hedis_medication_measure_2018,
  hedis_medication_measure_2019,
  hedis_medication_measure_2020,
  hedis_medication_measure_2021,
  hedis_medication_measure_2022,
  hedis_medication_measure_2023)

hedis_medication_lists <- bind_rows(
  hedis_medication_list_2018,
  hedis_medication_list_2019,
  hedis_medication_list_2020,
  hedis_medication_list_2021,
  hedis_medication_list_2022,
  hedis_medication_list_2023)

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


#### Step 4: Export HEDIS reference tables as RData file to secure folder ####
save(hedis_measures, hedis_value_sets, hedis_medication_measures, hedis_medication_lists,
     file = paste0(hedis_write_path, "hedis_value_sets_apde.Rdata"))

save(hedis_measures, hedis_value_sets, hedis_medication_measures, hedis_medication_lists,
     file = paste0(hedis_write_path, "hedis_value_sets_apde_backup_", Sys.Date(), ".Rdata"))


#### Step 5: Upload updated reference tables to HHSAW and PHClaims ####

to_schema <- "claims"
to_table_measures <- "ref_hedis_measures_apde"
to_table_value_sets <- "ref_hedis_value_sets_apde"
to_table_med_measures <- "ref_hedis_medication_measures_apde"
to_table_med_lists <- "ref_hedis_medication_lists_apde"

## Load data to HHSAW

#HEDIS measures
system.time(dbWriteTable(db_hhsaw, name = DBI::Id(schema = to_schema, table = to_table_measures), 
             value = as.data.frame(hedis_measures), 
             overwrite = T))

#HEDIS value sets
system.time(dbWriteTable(db_hhsaw, name = DBI::Id(schema = to_schema, table = to_table_value_sets), 
             value = as.data.frame(hedis_value_sets), 
             overwrite = T))

#HEDIS medication measures
system.time(dbWriteTable(db_hhsaw, name = DBI::Id(schema = to_schema, table = to_table_med_measures), 
             value = as.data.frame(hedis_medication_measures), 
             overwrite = T))

#HEDIS medication lists
system.time(dbWriteTable(db_hhsaw, name = DBI::Id(schema = to_schema, table = to_table_med_lists), 
             value = as.data.frame(hedis_medication_lists), 
             overwrite = T))

# Add index on HEDIS value sets table
system.time(DBI::dbExecute(db_hhsaw, 
  glue::glue_sql(
  "CREATE CLUSTERED INDEX [idx_cl_code_system_code] ON {`to_schema`}.{`to_table_value_sets`}(code_system, code)",
  .con = db_hhsaw)))

#PLACEHOLDER FOR WRITING TABLES TO PHCLAIMS ONCE SQL MIGRATION IS COMPLETE