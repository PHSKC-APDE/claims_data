## Script to update RDA value sets for SQL upload
## Eli Kern, September 2023
## Use "APDE protocol for updating DSHS Research and Data Analysis"

##2025-1-7 updates JL:
#Updated SharePoint folders to reflect current organization
#In step 1 when loading existing RDA value set, updated the name of the dataset
#Added code to remove row that's just the date range and not ICD codes for "mi-diagnosis" and "sud-diagnosis"
#Created new sub_group_condition category of "mh_other" for new ICD codes and moved ccs detail code 5.9 to mh_other
#Updated coding in step 6 when collapsing to distinct rows to not account for desc variable b/c many ICD-CM codes had different descriptions between this update and previous update
#Removed uploading table to PHClaims as it's no longer needed

##2025-6-2 updates JL:
#Added MOUD procedure codes to the reference table so an updated OUD definition can be created
# (these are not from the RDA)

#2025-6-9 updates JL:
#Adding another variable as flag for more general MOUD procedure codes (e.g. 96372) that require primary OUD diagnosis

#2025-6-12 updates JL:
#From NO HARMS, there were 66 ICD-10-CM codes associated with self-harm but not mapped to mh_any = 1
    #-adding them to this table so Kai can then update the icdcm_codes table

#### Setup ####

##Load packages and set defaults
pacman::p_load(tidyverse, openxlsx2, data.table, lubridate, Microsoft365R, odbc) # Load list of packages
options(max.print = 350) # Limit # of rows to show when printing/showing a data.frame
options(tibble.print_max = 50) # Limit # of rows to show when printing/showing a tibble (a tidyverse-flavored data.frame)
options(scipen = 999) # Avoid scientific notation
origin <- "1970-01-01" # Set the origin date, which is needed for many data/time functions

##Set keyring for SharePoint account
#keyring::key_set("sharepoint", username = "REPLACE TEXT WITH YOUR EMAIL ADDRESS") #Will prompt for password
#keyring::key_list()

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


#### Step 1: Load existing reference table and new value sets ####

mh_value_set_new_version <- "2024-07-31" ##UPDATE EACH TIME THIS SCRIPT IS RUN
sud_value_set_new_version <- "2024-07-31" ##UPDATE EACH TIME THIS SCRIPT IS R

##Connect to SharePoint/TEAMS site
myteam <- get_team(team_name = "DPH-KCCross-SectorData",
                   username = keyring::key_list("sharepoint")$username,
                   password = keyring::key_get("sharepoint", keyring::key_list("sharepoint")$username),
                   auth_type = "resource_owner",
                   tenant = "kingcounty.gov")

##Connect to drive (i.e., Documents-General document library) and navigate to desired subfolder
myteam$list_drives() #lists all available document libraries
myteamdrive = myteam$get_drive("Documents") #connect to document library named "Documents"
#myteamfolder = myteamdrive$get_item("General")
myteamfolder = myteamdrive$get_item("References")
myteamfolder = myteamfolder$get_item("RDA_measures")
myteamfolder_rda_value_set_existing = myteamfolder$get_item("rda_value_sets_for_sql_load")
myteamfolder_rda_value_set_existing$list_items()

myteamfolder_rda_mh_value_sets = myteamfolder$get_item("mh_service_penetration_measure")
myteamfolder_rda_mh_value_sets = myteamfolder_rda_mh_value_sets$get_item(paste0("mh_", mh_value_set_new_version))
myteamfolder_rda_mh_value_sets$list_items()

myteamfolder_rda_sud_value_sets = myteamfolder$get_item("sud_tx_penetration_measure")
myteamfolder_rda_sud_value_sets = myteamfolder_rda_sud_value_sets$get_item(paste0("sud_", sud_value_set_new_version))
myteamfolder_rda_sud_value_sets$list_items()

## Load sub_group_pharmacy reference table from metadata file files to temp location
temp_rda_vs_metadata <- tempfile(fileext = ".xlsx") #Create temp file to hold contents of SP file
myteamfolder_rda_value_set_existing$get_item("rda_value_sets_metadata.xlsx")$download(dest = temp_rda_vs_metadata)
sub_group_pharmacy <- read_xlsx(
  file = temp_rda_vs_metadata,
  sheet = "sub_group_pharmacy",
  colNames = TRUE,
  detectDates = TRUE)

## Load existing RDA value set, dropping last_run variable and MOUD procedure codes (will be added later)
myteamfolder_rda_value_set_existing$get_item("rda_value_sets_current.rdata")$load_rdata()
rda_value_sets_existing <- rda_value_sets_updated_final %>% 
  filter(value_set_name != "apde-moud-procedure") %>% 
  select(-c(last_run, oud_dx1_flag))
rm(rda_value_sets_updated_final)

## Load new value sets for MH and SUD measures
temp_mh_vs_new <- tempfile(fileext = ".xlsx")
temp_sud_vs_new <- tempfile(fileext = ".xlsx")
myteamfolder_rda_mh_value_sets$get_item(paste0("mhsr-value-sets_",mh_value_set_new_version,".xlsx"))$download(
  dest = temp_mh_vs_new)
myteamfolder_rda_sud_value_sets$get_item(paste0("sud-tx-rate-value-sets_",sud_value_set_new_version,".xlsx"))$download(
  dest = temp_sud_vs_new)

#mh-proc1
mh_vs_new_proc1 <- read_xlsx(
  file = temp_mh_vs_new,
  sheet = "MH-Proc1-MCG261",
  start_row = 2,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "mh",
    value_set_name = "mh-proc1",
    data_source_type = "procedure",
    code_set = "CPT-HCPCS",
    code = Code,
    desc = CodeDescription,
    mcg_code = "261") %>%
  
  select(value_set_group:mcg_code)

#mh-proc2
mh_vs_new_proc2 <- read_xlsx(
  file = temp_mh_vs_new,
  sheet = "MH-Proc2-MCG4947",
  start_row = 2,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "mh",
    value_set_name = "mh-proc2",
    data_source_type = "procedure",
    code_set = "CPT-HCPCS",
    code = Code,
    desc = CodeDescription,
    mcg_code = "4947") %>%
  
  select(value_set_group:mcg_code)

#mh-proc3
mh_vs_new_proc3 <- read_xlsx(
  file = temp_mh_vs_new,
  sheet = "MH-Proc3-MCG3117",
  start_row = 2,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "mh",
    value_set_name = "mh-proc3",
    data_source_type = "procedure",
    code_set = "CPT-HCPCS",
    code = Code,
    desc = CodeDescription,
    mcg_code = "3117") %>%
  
  select(value_set_group:mcg_code)

#mh-proc4
mh_vs_new_proc4 <- read_xlsx(
  file = temp_mh_vs_new,
  sheet = "MH-Proc4-MCG4491",
  start_row = 2,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "mh",
    value_set_name = "mh-proc4",
    data_source_type = "procedure",
    code_set = "CPT-HCPCS",
    code = `CPT or HCPC Procedure Code`,
    desc = CodeDescription,
    mcg_code = "4491") %>%
  
  select(value_set_group:mcg_code)

#mh-proc5
mh_vs_new_proc5 <- read_xlsx(
  file = temp_mh_vs_new,
  sheet = "MH-Proc5-MCG4948",
  start_row = 2,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "mh",
    value_set_name = "mh-proc5",
    data_source_type = "procedure",
    code_set = "CPT",
    code = `CPT Procedure Code`,
    desc = CodeDescription,
    mcg_code = "4948") %>%
  
  select(value_set_group:mcg_code)

#mh-taxonomy
mh_vs_new_taxonomy <- read_xlsx(
  file = temp_mh_vs_new,
  sheet = "MH-Taxonomy-MCG262",
  start_row = 2,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "mh",
    value_set_name = "mh-taxonomy",
    data_source_type = "taxonomy",
    code_set = "HPT",
    code = `Taxonomy Code`,
    desc = CodeDescription,
    mcg_code = "262") %>%
  
  select(value_set_group:mcg_code)

#mi-diagnosis
mh_vs_new_diagnosis <- read_xlsx(
  file = temp_mh_vs_new,
  sheet = "MI-Diagnosis",
  start_row = 1,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "mh",
    value_set_name = "mi-diagnosis",
    data_source_type = "diagnosis",
    code_set = "ICDCM",
    code = DxCode,
    desc = CodeDescription,
    mcg_code = "7MCGs") %>%
  
  select(value_set_group:mcg_code) %>% 
  filter(toupper(desc) != 'DATE RANGE CODE')

#psychotropic-ndc
mh_vs_new_ndc <- read_xlsx(
  file = temp_mh_vs_new,
  sheet = "Psychotropic-NDC-5MCGs",
  start_row = 1,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "mh",
    value_set_name = "psychotropic-ndc",
    data_source_type = "pharmacy",
    code_set = "NDC",
    code = as.character(NDCExpansion),
    desc = NDCLabel,
    mcg_code = "5MCGs") %>%
  
  #remove meds with missing NCD
  filter(code != "NULL") %>%
  
  select(value_set_group:mcg_code)

#sud-dx-value-set
sud_vs_new_diagnosis <- read_xlsx(
  file = temp_sud_vs_new,
  sheet = "SUD-Dx-Value-Set",
  start_row = 1,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "sud",
    value_set_name = "sud-dx-value-set",
    data_source_type = "diagnosis",
    code_set = "ICDCM",
    code = `ICD-9 or ICD-10 Diagnosis Code`,
    desc = CodeDescription,
    mcg_code = NA_character_) %>%
  
  select(value_set_group:mcg_code) %>% 
  filter(toupper(desc) != 'DATE RANGE CODE')

#sbirt-proc
sud_vs_new_sbirt <- read_xlsx(
  file = temp_sud_vs_new,
  sheet = "SBIRT-Proc-Value-Set (3169)",
  start_row = 2,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "sud",
    value_set_name = "sbirt-proc",
    data_source_type = "procedure",
    code_set = "CPT-HCPCS",
    code = `CPT or HCPC Procedure Code`,
    desc = CodeDescription,
    mcg_code = "3169") %>%
  
  select(value_set_group:mcg_code)

#detox
sud_vs_new_detox <- read_xlsx(
  file = temp_sud_vs_new,
  sheet = "Detox-Value-Set",
  start_row = 1,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "sud",
    value_set_name = "detox",
    data_source_type = case_when(
      CodeSet == "HCPC" ~ "procedure",
      CodeSet == "ICD-9 procedure code" ~ "procedure",
      CodeSet == "ICD-10 procedure code" ~ "procedure",
      CodeSet == "revenue code" ~ "billing",
      TRUE ~ NA_character_),
    code_set = case_when(
      CodeSet == "HCPC" ~ "HCPCS",
      CodeSet == "ICD-9 procedure code" ~ "ICD9PCS",
      CodeSet == "ICD-10 procedure code" ~ "ICD10PCS",
      CodeSet == "revenue code" ~ "UBREV",
      TRUE ~ NA_character_),
    code = Code,
    desc = CodeDescription,
    mcg_code = NA_character_) %>%
  
  select(value_set_group:mcg_code)

#sud-op-tx-proc
sud_vs_new_sud_op_tx_proc <- read_xlsx(
  file = temp_sud_vs_new,
  sheet = "SUD-OP-Tx-Proc-Value-Set (3156)",
  start_row = 2,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "sud",
    value_set_name = "sud-op-tx-proc",
    data_source_type = "procedure",
    code_set = "HCPCS",
    code = `HCPC Procedure Code`,
    desc = CodeDescription,
    mcg_code = "3156") %>%
  
  select(value_set_group:mcg_code)

#sud-ost
sud_vs_new_sud_ost <- read_xlsx(
  file = temp_sud_vs_new,
  sheet = "SUD-OST-Value-Set (3148)",
  start_row = 2,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "sud",
    value_set_name = "sud-ost",
    data_source_type = "procedure",
    code_set = "HCPCS",
    code = `HCPC Procedure Code`,
    desc = CodeDescription,
    mcg_code = "3148") %>%
  
  select(value_set_group:mcg_code)

#sud-ip-res
sud_vs_new_ip_res <- read_xlsx(
  file = temp_sud_vs_new,
  sheet = "SUD-IP-RES-Value-Set",
  start_row = 1,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "sud",
    value_set_name = "sud-ip-res",
    data_source_type = case_when(
      CodeSet == "HCPC" ~ "procedure",
      CodeSet == "DRG" ~ "diagnosis",
      TRUE ~ NA_character_),
    code_set = case_when(
      CodeSet == "HCPC" ~ "HCPCS",
      CodeSet == "DRG" ~ "DRG",
      TRUE ~ NA_character_),
    code = Code,
    desc = CodeDescription,
    mcg_code = NA_character_) %>%
  
  select(value_set_group:mcg_code)

#sud-asmt
sud_vs_new_sud_asmt <- read_xlsx(
  file = temp_sud_vs_new,
  sheet = "SUD-ASMT-Value-Set (3149)",
  start_row = 2,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "sud",
    value_set_name = "sud-asmt",
    data_source_type = "procedure",
    code_set = "HCPCS",
    code = `HCPC Procedure Code`,
    desc = CodeDescription,
    mcg_code = "3149") %>%
  
  select(value_set_group:mcg_code)

#sud-taxonomy
sud_vs_new_sud_taxonomy <- read_xlsx(
  file = temp_sud_vs_new,
  sheet = "SUD-Taxonomy-Value-Set (3170)",
  start_row = 2,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "sud",
    value_set_name = "sud-taxonomy",
    data_source_type = "taxonomy",
    code_set = "HPT",
    code = `Taxonomy Code`,
    desc = CodeDescription,
    mcg_code = "3170") %>%
  
  select(value_set_group:mcg_code)

#proc-w-prim-sud-dx
sud_vs_new_proc_prim_sud_dx <- read_xlsx(
  file = temp_sud_vs_new,
  sheet = "proc-w-prim-SUD-Dx-vs (3324)",
  start_row = 2,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "sud",
    value_set_name = "proc-w-prim-SUD-Dx",
    data_source_type = "procedure",
    code_set = "HCPCS",
    code = `HCPC Procedure Code`,
    desc = CodeDescription,
    mcg_code = "3324") %>%
  
  select(value_set_group:mcg_code)

#proc-w-any-sud-dx
sud_vs_new_proc_any_sud_dx <- read_xlsx(
  file = temp_sud_vs_new,
  sheet = "proc-w-any-SUD-Dx-vs (4881)",
  start_row = 2,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "sud",
    value_set_name = "proc-w-any-SUD-Dx",
    data_source_type = "procedure",
    code_set = "CPT-HCPCS",
    code = `CPT or HCPC Procedure Code`,
    desc = CodeDescription,
    mcg_code = "4881") %>%
  
  select(value_set_group:mcg_code)

#moud-maud
sud_vs_new_moud_maud <- read_xlsx(
  file = temp_sud_vs_new,
  sheet = "MOUD-MAUD-Value-Set",
  start_row = 1,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "sud",
    value_set_name = "moud-maud",
    data_source_type = "pharmacy",
    code_set = "NDC",
    code = as.character(NDCExpansion),
    desc = NDCLabel,
    mcg_code = NA_character_) %>%
  
  select(value_set_group:mcg_code)

#moud-procedure
sud_vs_new_moud_proc <- read_xlsx(
  file = temp_sud_vs_new,
  sheet = "MOUD-Procedure-Value-Set",
  start_row = 2,
  colNames = TRUE,
  detectDates = TRUE,
  skip_empty_cols = TRUE,
  skip_empty_rows = TRUE) %>%
  
  mutate(
    value_set_group = "sud",
    value_set_name = "moud-procedure",
    data_source_type = "procedure",
    code_set = "HCPCS",
    code = `HCPC Procedure Code`,
    desc = `Short Description`,
    mcg_code = NA_character_) %>%
  
  select(value_set_group:mcg_code)


#### Step 2: Bind all new value sets ####

rda_value_sets_new_raw <- bind_rows(
  mh_vs_new_diagnosis,
  mh_vs_new_ndc,
  mh_vs_new_proc1,
  mh_vs_new_proc2,
  mh_vs_new_proc3,
  mh_vs_new_proc4,
  mh_vs_new_proc5,
  mh_vs_new_taxonomy,
  sud_vs_new_detox,
  sud_vs_new_diagnosis,
  sud_vs_new_ip_res,
  sud_vs_new_moud_maud,
  sud_vs_new_moud_proc,
  sud_vs_new_proc_any_sud_dx,
  sud_vs_new_proc_prim_sud_dx,
  sud_vs_new_sbirt,
  sud_vs_new_sud_asmt,
  sud_vs_new_sud_op_tx_proc,
  sud_vs_new_sud_ost,
  sud_vs_new_sud_taxonomy)

rm(mh_vs_new_diagnosis,
   mh_vs_new_ndc,
   mh_vs_new_proc1,
   mh_vs_new_proc2,
   mh_vs_new_proc3,
   mh_vs_new_proc4,
   mh_vs_new_proc5,
   mh_vs_new_taxonomy,
   sud_vs_new_detox,
   sud_vs_new_diagnosis,
   sud_vs_new_ip_res,
   sud_vs_new_moud_maud,
   sud_vs_new_moud_proc,
   sud_vs_new_proc_any_sud_dx,
   sud_vs_new_proc_prim_sud_dx,
   sud_vs_new_sbirt,
   sud_vs_new_sud_asmt,
   sud_vs_new_sud_op_tx_proc,
   sud_vs_new_sud_ost,
   sud_vs_new_sud_taxonomy)


#### Step 3: Normalize certain code sets and create ICDCM version ####

rda_value_sets_new <- rda_value_sets_new_raw %>%
  
  mutate(
    
    #Set description column to upper case and trim all white space
    desc = str_squish(str_to_upper(desc)),
    
    #Create ICDCM version
    code_set = case_when(
      code_set == "ICDCM" & str_detect(code,"^[:digit:]") ~ "ICD9CM",
      code_set == "ICDCM" & str_detect(code, "^E") & str_detect(desc, "POISON|INJURY|INJURIES|
                                                                INJ|INJU|POIS|SELF") ~ "ICD9CM",
      code_set == "ICDCM" & str_detect(code, "^V") ~ "ICD9CM",
      code_set == "ICDCM" & str_detect(code, "^[:alpha:]") ~ "ICD10CM",
      TRUE ~ code_set),
    
    #Normalize ICD9CM values by padding to 5 digits with trailing zeroes
    code_raw = code,
    code = case_when(
      code_set == "ICD9CM" & str_length(code) == 3 ~ paste0(code, "00"),
      code_set == "ICD9CM" & str_length(code) == 4 ~ paste0(code, "0"),
      TRUE ~ code),
    
    #Normalize NDC codes by padding to 11 digits with leading zeroes
    code = case_when(
      code_set == "NDC" & str_length(code) == 7 ~ paste0("0000", code),
      code_set == "NDC" & str_length(code) == 8 ~ paste0("000", code),
      code_set == "NDC" & str_length(code) == 9 ~ paste0("00", code),
      code_set == "NDC" & str_length(code) == 10 ~ paste0("0", code),
      TRUE ~ code)) %>%
  
  distinct() # collapse to distinct rows after transformation

#All ICD9CM codes should be 5 digits long
rda_value_sets_new %>%
  mutate(code_len = str_length(code)) %>%
  filter(code_set == "ICD9CM") %>%
  count(code_len)

#All NDC codes should be 11 digits long
rda_value_sets_new %>%
  mutate(code_len = str_length(code)) %>%
  filter(code_set == "NDC") %>%
  count(code_len)

#For ICD-CM codes that have more than 1 row (this really only happens when padding ICD-9-CM codes to 5 digits), select longest
rda_value_sets_new <- rda_value_sets_new %>%
  mutate(
    code_raw_len = case_when(
      code_set %in% c("ICD9CM", "ICD10CM") ~ str_length(code_raw),
      TRUE ~ NA_integer_)) %>%
  group_by(code) %>%
  mutate(
    row_count = n(),
    code_raw_len_rank = rank(-code_raw_len, ties.method = c("first"))) %>%
  ungroup() %>%
  filter(is.na(code_raw_len) | code_raw_len_rank == 1L) %>%
  select(-code_raw:-code_raw_len_rank)

#Confirm that no ICD-CM codes have more than 1 row
rda_value_sets_new %>% filter(code_set %in% c("ICD9CM", "ICD10CM")) %>% group_by(code) %>%
  mutate(row_count = n()) %>% ungroup() %>% count(row_count)

#Confirm that no NDC codes have more than 1 row
rda_value_sets_new %>% filter(code_set %in% c("NDC")) %>% group_by(code) %>%
  mutate(row_count = n()) %>% ungroup() %>% count(row_count)


#### Step 4: Populate sub_group variable based on ICD-CM codes ####

#Import ICD-CM reference table from HHSAW
ref.icdcm_codes <- dbGetQuery(
  conn = db_hhsaw,
  statement = "select * from hhs_analytics_workspace.ref.icdcm_codes;") %>%
  select(icdcm:icdcm_version, ccs_broad_desc:ccs_catch_all)

#Prep RDA value sets for linkage
rda_value_sets_new <- rda_value_sets_new %>%
  mutate(icdcm_version = case_when(
    code_set == "ICD9CM" ~ 9,
    code_set == "ICD10CM" ~ 10,
    TRUE ~ NA_real_
  ))

rda_value_sets_new <- left_join(rda_value_sets_new, ref.icdcm_codes, by = c("code" = "icdcm", "icdcm_version" = "icdcm_version"))

#Check to make sure all ICDCM codes in RDA value sets join to ICDCM ref table - should be 0
count(filter(rda_value_sets_new, code_set %in% c("ICD9CM", "ICD10CM") & is.na(ccs_detail_desc)))

#Check for duplicate rows (which is okay for procedure codes that exist in more than one value set)
rda_value_sets_new %>%
  group_by(code_set, code) %>%
  summarise(row_count = n()) %>%
  filter(row_count >1)

#Use CCS detail categories from ref.icdcm_codes table to group ICDCM codes in RDA value sets into BH condition categories
rda_value_sets_new <- rda_value_sets_new %>%
  
  mutate(sub_group_condition = case_when(
    
    #Assignments based on CCS detail categories
    ccs_detail_code %in% c("5.1") ~ "mh_adjustment",
    ccs_detail_code %in% c("MBD005", "5.2", "5.6", "SKN002") ~ "mh_anxiety",
    ccs_detail_code %in% c("MBD002", "INJ074", "INJ058", "EXT012", "EXT001", "EXT002", "EXT003", "EXT004",
                           "EXT005", "EXT007", "EXT011", "EXT018", "EXT014", "EXT030", "EXT016", "EXT029",
                           "EXT017", "EXT010", "EXT019", "10.3", "INJ073", "GEN025", "INJ064", "INJ059", "MBD012",
                           "5.13") ~ "mh_depression",
    ccs_detail_code %in% c("MBD008", "5.7", "MBD013") ~ "mh_disrupt",
    ccs_detail_code %in% c("MBD003", "5.8") ~ "mh_mania_bipolar",
    ccs_detail_code %in% c("12.2", "5.10", "MBD001") ~ "mh_psychotic",
    ccs_detail_code %in% c("5.11", "MBD017", "DIG007", "DIG018", "INF007", "CIR005", "MAL010", "DIG019",
                           "16.11", "2613") ~ "sud_alcohol",
    ccs_detail_code %in% c("MBD019") ~ "sud_cannabis",
    ccs_detail_code %in% c("MBD022") ~ "sud_hallucinogen",
    ccs_detail_code %in% c("MBD023") ~ "sud_inhalant",
    ccs_detail_code %in% c("MBD018") ~ "sud_opioid",
    ccs_detail_code %in% c("MBD018") ~ "sud_opioid",
    ccs_detail_code %in% c("INJ030", "6.9", "MBD025") ~ "sud_other_substance",
    ccs_detail_code %in% c("MBD020") ~ "sud_sedative",
    
    #Assignments based on ICDCM codes (where CCS categories have to be disaggregated)
    ccs_detail_code == "5.3" & code %in% c("31400", "31401") ~ "mh_adhd",
    ccs_detail_code == "5.3" ~ "mh_disrupt",
    
    ccs_detail_code %in% c("INJ075") & code %in% c("T510X1S", "T511X1S", "T512X1S", "T513X1S",
                                                   "T518X1S", "T5191XS") ~ "sud_alcohol",
    ccs_detail_code %in% c("INJ075") ~ "mh_depression",
    
    ccs_detail_code %in% c("INJ060") & code %in% c("T510X1D", "T511X1D", "T512X1D", "T513X1D",
                                                   "T518X1D", "T5191XD") ~ "sud_alcohol",
    ccs_detail_code %in% c("INJ060") ~ "mh_depression",
    
    ccs_detail_code %in% c("EXT015") & code %in% c("T51", "T510", "T510X", "T510X1", "T510X1A",
                                                   "T511", "T511X", "T511X1", "T511X1A", "T512", "T512X",
                                                   "T512X1", "T512X1A", "T513", "T513X", "T513X1", "T513X1A",
                                                   "T518", "T518X", "T518X1", "T518X1A", "T519", "T5191",
                                                   "T5191XA") ~ "sud_alcohol",
    ccs_detail_code %in% c("EXT015") ~ "mh_depression",
    
    ccs_detail_code %in% c("MBD026") & code %in% c("F304", "F317", "F3170", "F3172", "F3174", "F3176", "F3178") ~ "mh_mania_bipolar",
    ccs_detail_code %in% c("MBD026") & code %in% c("F325", "F334", "F3340", "F3342") ~ "mh_depression",
    ccs_detail_code %in% c("MBD026") & code %in% c("F1011", "F1021") ~ "sud_alcohol",
    ccs_detail_code %in% c("MBD026") & code %in% c("F1111", "F1121") ~ "sud_opioid",
    ccs_detail_code %in% c("MBD026") & code %in% c("F1211", "F1221") ~ "sud_cannabis",
    ccs_detail_code %in% c("MBD026") & code %in% c("F1311", "F1321") ~ "sud_sedative",
    ccs_detail_code %in% c("MBD026") & code %in% c("F1411", "F1421") ~ "sud_cocaine",
    ccs_detail_code %in% c("MBD026") & code %in% c("F1511", "F1521") ~ "sud_other_stimulant",
    ccs_detail_code %in% c("MBD026") & code %in% c("F1611", "F1621") ~ "sud_hallucinogen",
    ccs_detail_code %in% c("MBD026") & code %in% c("F1811", "F1821") ~ "sud_inhalant",
    ccs_detail_code %in% c("MBD026") & code %in% c("F1911", "F1921") ~ "sud_other_substance",
    
    ccs_detail_code %in% c("MBD014") & code %in% c("F90", "F900", "F901", "F902", "F908", "F909") ~ "mh_adhd",
    ccs_detail_code %in% c("MBD014") & code %in% c("F948", "F949") ~ "mh_anxiety",
    
    ccs_detail_code %in% c("PNL010") & code %in% c("P961", "P0449", "P0440", "P044") ~ "sud_other_substance",
    ccs_detail_code %in% c("PNL010") & code %in% c("P0481") ~ "sud_cannabis",
    ccs_detail_code %in% c("PNL010") & code %in% c("P0442") ~ "sud_hallucinogen",
    ccs_detail_code %in% c("PNL010") & code %in% c("P0441") ~ "sud_cocaine",
    ccs_detail_code %in% c("PNL010") & code %in% c("P043") ~ "sud_alcohol",
    ccs_detail_code %in% c("PNL010") & code %in% c("P0417") ~ "sud_sedative",
    ccs_detail_code %in% c("PNL010") & code %in% c("P0416") ~ "sud_other_stimulant",
    ccs_detail_code %in% c("PNL010") & code %in% c("P0414") ~ "sud_opioid",
    
    ccs_detail_code %in% c("MBD006") & code %in% c("F42", "F428", "F429") ~ "mh_anxiety",
    ccs_detail_code %in% c("MBD006") & code %in% c("F422") ~ "mh_mania_bipolar",
    ccs_detail_code %in% c("MBD006") & code %in% c("F423", "F424", "F633") ~ "mh_disrupt",
    
    ccs_detail_code %in% c("MBD004") & code %in% c("F063", "F0630", "F348", "F349", "F39") ~ "mh_depression",
    ccs_detail_code %in% c("MBD004") & code %in% c("F3481") ~ "mh_disrupt",
    ccs_detail_code %in% c("MBD004") & code %in% c("F3489") ~ "mh_mania_bipolar",
    
    ccs_detail_code %in% c("FAC012") & code %in% c("Z714", "Z7141") ~ "sud_alcohol",
    ccs_detail_code %in% c("FAC012") & code %in% c("Z715", "Z7151") ~ "sud_other_substance",
    
    ccs_detail_code %in% c("5.9") ~ "mh_other",

    ccs_detail_code %in% c("MBD021") & str_detect(desc, "COCAINE") ~ "sud_cocaine",
    ccs_detail_code %in% c("MBD021") ~ "sud_other_stimulant",
    
    ccs_detail_code %in% c("5.12") & str_detect(code, "^3040") ~ "sud_opioid",
    ccs_detail_code %in% c("5.12") & str_detect(code, "^3041") ~ "sud_sedative",
    ccs_detail_code %in% c("5.12") & str_detect(code, "^3042") ~ "sud_cocaine",
    ccs_detail_code %in% c("5.12") & str_detect(code, "^3043") ~ "sud_cannabis",
    ccs_detail_code %in% c("5.12") & str_detect(code, "^3044") ~ "sud_other_stimulant",
    ccs_detail_code %in% c("5.12") & str_detect(code, "^3045") ~ "sud_hallucinogen",
    ccs_detail_code %in% c("5.12") & str_detect(code, "^3047") ~ "sud_opioid",
    ccs_detail_code %in% c("5.12") & str_detect(code, "^3052") ~ "sud_cannabis",
    ccs_detail_code %in% c("5.12") & str_detect(code, "^3053") ~ "sud_hallucinogen",
    ccs_detail_code %in% c("5.12") & str_detect(code, "^3054") ~ "sud_sedative",
    ccs_detail_code %in% c("5.12") & str_detect(code, "^3055") ~ "sud_opioid",
    ccs_detail_code %in% c("5.12") & str_detect(code, "^3056") ~ "sud_cocaine",
    ccs_detail_code %in% c("5.12") & str_detect(code, "^3057") ~ "sud_other_stimulant",
    ccs_detail_code %in% c("5.12") & code %in% c("76072") ~ "sud_opioid",
    ccs_detail_code %in% c("5.12") & code %in% c("76073") ~ "sud_hallucinogen",
    ccs_detail_code %in% c("5.12") & code %in% c("76075") ~ "sud_cocaine",
    ccs_detail_code %in% c("5.12") ~ "sud_other_substance",
    
    ccs_detail_code %in% c("MBD007") & code %in% c("F43", "F430", "F941", "F942") ~ "mh_anxiety",
    ccs_detail_code %in% c("MBD007") & str_detect(code, "^F431") ~ "mh_anxiety",
    ccs_detail_code %in% c("MBD007") & str_detect(code, "^F438") ~ "mh_anxiety",
    ccs_detail_code %in% c("MBD007") & str_detect(code, "^F439") ~ "mh_anxiety",
    ccs_detail_code %in% c("MBD007") & str_detect(code, "^F432") ~ "mh_adjustment",
    
    
    TRUE ~ NA_character_
  ))
  

#Make sure there are no diagnosis code rows with a null sub_group_apde value, expect 0
count(filter(rda_value_sets_new, code_set %in% c("ICD9CM", "ICD10CM") & is.na(sub_group_condition)) %>%
        select(code_set, code, desc, sub_group_condition))

#Identify missing and fill in blanks
blanks <- rda_value_sets_new %>%
  filter(code_set %in% c("ICD9CM", "ICD10CM") & is.na(sub_group_condition)) %>%
  distinct(code_set, code, desc, sub_group_condition)
#there were 109 missing in 1/7/2025 update

#Manual recoding
rda_value_sets_new <- rda_value_sets_new %>% 
  mutate(sub_group_condition = case_when(ccs_detail_code %in% c("MBD010", "5.15", 
                                                                "MBD011", "MBD009", 
                                                                "NVS011", "SYM008",
                                                                "SYM016", "5.4",
                                                                "5.5", "15.7", "5.14") ~ "mh_other",
                                         ccs_detail_code == "MBD007" & str_detect(code, "^F48|^F44") ~ "mh_other",
                                         ccs_detail_code == "MBD014" & code == "F988" ~ "mh_other",
                                         ccs_detail_code == "MBD006" & code == "F4522" ~ "mh_other",
                                         TRUE ~ sub_group_condition)) %>% 
  select(-ccs_broad_desc:-ccs_catch_all) #remove variables from ref.icdcm_codes table  



#### Step 5: Populate sub_group variable based on NDC/pharmacy codes ####

#Use sub_group_pharmacy table (which I created from 2021 version of RDA value sets) to create vectors of drug names
  #for each sub_group_pharmacy

acamprosate_rx <- sub_group_pharmacy$desc_1[sub_group_pharmacy$sub_group_pharmacy=="Acamprosate"]
adhd_rx <- sub_group_pharmacy$desc_1[sub_group_pharmacy$sub_group_pharmacy=="ADHD Rx"]
antianxiety_rx <- sub_group_pharmacy$desc_1[sub_group_pharmacy$sub_group_pharmacy=="Antianxiety Rx"]
antidepressant_rx <- sub_group_pharmacy$desc_1[sub_group_pharmacy$sub_group_pharmacy=="Antidepressants Rx"]
antimania_rx <- sub_group_pharmacy$desc_1[sub_group_pharmacy$sub_group_pharmacy=="Antimania Rx"]
antipsychotic_rx <- sub_group_pharmacy$desc_1[sub_group_pharmacy$sub_group_pharmacy=="Antipsychotic Rx"]
buprenorphine_naloxone_rx <- sub_group_pharmacy$desc_1[sub_group_pharmacy$sub_group_pharmacy=="Buprenorphine-Naloxone"]
buprenorphine_rx <- sub_group_pharmacy$desc_1[sub_group_pharmacy$sub_group_pharmacy=="Buprenorphine"]
naltrexone_rx <- sub_group_pharmacy$desc_1[sub_group_pharmacy$sub_group_pharmacy=="Naltrexone"]
disulfiram_rx <- sub_group_pharmacy$desc_1[sub_group_pharmacy$sub_group_pharmacy=="Disulfiram"]

#Then use these vectors in a case_when statement to assign a sub_group_pharmacy to each NDC
rda_value_sets_new_rx <- rda_value_sets_new %>%
  mutate(sub_group_pharmacy = case_when(
    data_source_type == "pharmacy" & desc %in% acamprosate_rx ~ "pharm_acamprosate",
    data_source_type == "pharmacy" & desc %in% disulfiram_rx ~ "pharm_disulfiram",
    data_source_type == "pharmacy" & desc %in% adhd_rx ~ "pharm_adhd",
    data_source_type == "pharmacy" & desc %in% antianxiety_rx ~ "pharm_antianxiety",
    data_source_type == "pharmacy" & desc %in% antidepressant_rx ~ "pharm_antidepressant",
    data_source_type == "pharmacy" & desc %in% antimania_rx ~ "pharm_antimania",
    data_source_type == "pharmacy" & desc %in% antipsychotic_rx ~ "pharm_antipsychotic",
    data_source_type == "pharmacy" & desc %in% buprenorphine_naloxone_rx ~ "pharm_buprenorphine_naloxone",
    data_source_type == "pharmacy" & desc %in% buprenorphine_rx ~ "pharm_buprenorphine",
    data_source_type == "pharmacy" & desc %in% naltrexone_rx ~ "pharm_naltrexone_rx",
    TRUE ~ NA_character_
  ))

#Manual recoding based on cumulative value sets to date
rda_value_sets_new_rx <- rda_value_sets_new_rx %>%
  mutate(sub_group_pharmacy = case_when(
    data_source_type == "pharmacy" & is.na(sub_group_pharmacy) &
      desc %in% c("METHYLPHENIDATE", "DICLOFENAC SODIUM DR", "LISDEXAMFETAMINE DIMESYLATE", 
                  "GUANFACINE HYDROCHLORIDE ER", "RELEXXII", "AMPHETAMINE/DEXTROAMPHETAMINE ER") ~ "pharm_adhd",
    data_source_type == "pharmacy" & is.na(sub_group_pharmacy) &
      desc %in% c("VILAZODONE HYDROCHLORIDE", "VENLAFAXINE BESYLATE ER", "ABILIFY MYCITE STARTER KIT",
                  "ABILIFY MYCITE MAINTENANCE KIT", "AUVELITY", "ZURZUVAE") ~ "pharm_antidepressant",
    data_source_type == "pharmacy" & is.na(sub_group_pharmacy) & 
      desc %in% c("INVEGA HAFYERA", "LURASIDONE HYDROCHLORIDE", "RYKINDO", "RISPERIDONE ER", 
                  "UZEDY", "ABILIFY ASIMTUFII") ~ "pharm_antipsychotic",
    data_source_type == "pharmacy" & is.na(sub_group_pharmacy) & desc %in% c("LOREEV XR") ~ "pharm_antianxiety",
    data_source_type == "pharmacy" & is.na(sub_group_pharmacy) & str_detect(desc, "NALTREXONE") ~ "pharm_naltrexone_rx",
    data_source_type == "pharmacy" & is.na(sub_group_pharmacy) & str_detect(desc, "DISULFIRAM") ~ "pharm_disulfiram",
    data_source_type == "pharmacy" & is.na(sub_group_pharmacy) & desc %in% c("BRIXADI") ~ "pharm_buprenorphine",
    
    TRUE ~ sub_group_pharmacy
  ))

#Identify missing and fill in blanks
rda_value_sets_new_rx %>%
  filter(data_source_type == "pharmacy" & is.na(sub_group_pharmacy)) %>%
  distinct(desc)

#Check to make sure all drugs have been assigned a sub_group_pharmacy value - this query should return nothing
rda_value_sets_new_rx %>%
  filter(data_source_type == "pharmacy" & is.na(sub_group_pharmacy)) %>%
  distinct(desc)

#Assign a sub_group_condition value based on sub_group_pharmacy table
rda_value_sets_new_rx <- rda_value_sets_new_rx %>%
  mutate(sub_group_condition = case_when(
    data_source_type == "pharmacy" & sub_group_pharmacy %in% c("pharm_acamprosate", "pharm_disulfiram") ~ "sud_alcohol",
    data_source_type == "pharmacy" & sub_group_pharmacy %in% c("pharm_adhd") ~ "mh_adhd",
    data_source_type == "pharmacy" & sub_group_pharmacy %in% c("pharm_antianxiety") ~ "mh_anxiety",
    data_source_type == "pharmacy" & sub_group_pharmacy %in% c("pharm_antidepressant") ~ "mh_depression",
    data_source_type == "pharmacy" & sub_group_pharmacy %in% c("pharm_antimania") ~ "mh_mania_bipolar",
    data_source_type == "pharmacy" & sub_group_pharmacy %in% c("pharm_antipsychotic") ~ "mh_psychotic",
    data_source_type == "pharmacy" & sub_group_pharmacy %in% c("pharm_buprenorphine_naloxone") ~ "sud_opioid",
    data_source_type == "pharmacy" & sub_group_pharmacy %in% c("pharm_buprenorphine") ~ "sud_opioid",
    data_source_type == "pharmacy" & sub_group_pharmacy %in% c("pharm_naltrexone_rx") ~ "sud_opioid",
    TRUE ~ sub_group_condition
  ))

#Verify that all drugs have been assigned to a sub_group_condition, should return 0
rda_value_sets_new_rx %>% filter(data_source_type == "pharmacy" & is.na(sub_group_condition)) %>% count()


#### Step 5b: Add in 66 ICD-10-CM codes that were identified through NO HARMS ####
extra_idc <- data.frame(value_set_group = "mh",
                        value_set_name = "apde-added-diagnosis",
                        data_source_type = "diagnosis",
                        code_set = "ICD10CM",
                        code = c("T43652", "T43652A", "T43652D", "T43652S", "T45AX2A", 
                                 "T45AX2D", "T45AX2S", "T4792X", "X738XX", "X739XX", 
                                 "X7401X", "X7402X", "X7409X", "X748XX", "X749XX", "X75XXX",
                                 "X76XXX", "X770XX", "X771XX", "T4592X", "X710XX", "X711XX", 
                                 "X712XX", "X713XX", "X718XX", "X719XX", "X72XXX", "X730XX",
                                 "T3692X", "T3792X", "T3992X", "X781XX", "X782XX", "X788XX",
                                 "X789XX", "X79XXX", "X80XXX", "X810XX", "X811XX", "X818XX", 
                                 "X820XX", "X821XX", "X822XX", "X828XX", "X830XX", "X731XX", 
                                 "X732XX", "X838XX", "X772XX", "X773XX", "X778XX", "X779XX", 
                                 "X780XX", "T1491X", "T56822A", "T56822D", "T56822S", "X831XX",
                                 "X832XX", "T40412", "T40422", "T40492", "T4272X", "T4392X", 
                                 "T4142X", "T4992X"),
                        icdcm_version = 10,
                        sub_group_condition = "mh_other") 

# Join to ref.icdcm_codes table in order to get the description
extra_idc_final <- dbGetQuery(conn = db_hhsaw, 
                              statement = "select distinct icdcm, icdcm_version, icdcm_description
                                            from ref.icdcm_codes
                                            WHERE (SUBSTRING(icdcm, 1, 3) IN ('X60', 'X61', 'X62', 'X63', 'X64', 'X65', 'X66', 'X67', 'X68', 'X69', 'X70', 'X71', 'X72', 'X73', 'X74', 'X75', 'X76', 'X77', 'X78', 'X79', 'X80', 'X81', 'X82', 'X83', 'X84')  
                                              OR (SUBSTRING(icdcm, 1, 3) IN ('T36', 'T37', 'T38', 'T39', 'T40', 'T41', 'T42', 'T43', 'T44', 'T45', 'T46', 'T47', 'T48', 'T49', 'T50', 'T51', 'T52', 'T53', 'T54', 'T55', 'T56', 'T57', 'T58', 'T59', 'T60', 'T61', 'T62', 'T63', 'T64', 'T65') 
                                                AND SUBSTRING(icdcm, 6, 1) = '2'
                                                AND SUBSTRING(icdcm, 1, 4) NOT IN ('T369', 'T379', 'T399', 'T414', 'T427', 'T439', 'T459', 'T479', 'T499')) 
                                              OR (SUBSTRING(icdcm, 1, 4) IN ('T369', 'T379', 'T399', 'T414', 'T427', 'T439', 'T459', 'T479', 'T499') 
                                                AND SUBSTRING(icdcm, 5, 1) = '2') 
                                              OR (SUBSTRING(icdcm, 1, 3) = 'T71' AND SUBSTRING(icdcm, 6, 1) = '2') OR SUBSTRING(icdcm, 1, 5) = 'T1491' ) 
                                              AND mh_any IS NULL") %>% 
  right_join(extra_idc, by = c("icdcm" = "code", "icdcm_version")) %>% 
  mutate(desc = toupper(icdcm_description)) %>% 
  rename(code = icdcm) %>% 
  select(value_set_group, value_set_name, data_source_type, code_set, code, desc, icdcm_version, 
         sub_group_condition)

#### Step 6: Bind to existing RDA value set and collapse to distinct rows ####

rda_value_sets_updated <- bind_rows(rda_value_sets_existing, rda_value_sets_new_rx, extra_idc_final) %>% 
  distinct(across(-desc), .keep_all = TRUE)

#Confirm that no ICD-CM codes have more than 1 row
rda_value_sets_updated %>% filter(code_set %in% c("ICD9CM", "ICD10CM")) %>% group_by(code) %>%
  mutate(row_count = n()) %>% ungroup() %>% count(row_count)

#no need to run the below if there are no ICD-CM codes with >1 row (most likely not needed after Jan 2025 update)
rda_value_sets_updated <- subset(rda_value_sets_updated, !(code %in% c("30113", "30122") & 
                                                             sub_group_condition %in% c("mh_mania_bipolar", 
                                                                                        "mh_psychotic")))
#these were previously classified as mh_mania_polar and mh_psychotic but are now mh_other

#Confirm that no NDC codes have more than 1 row
rda_value_sets_updated %>% filter(code_set %in% c("NDC")) %>% group_by(code) %>%
  mutate(row_count = n()) %>% ungroup() %>% count(row_count)

#Check for duplicate rows (which is okay for procedure codes that exist in more than one value set)
#Also some taxonomy codes are duplicated even after stripping white space because of slight changes in wording - ignore
rda_value_sets_updated %>%
  group_by(code_set, code) %>%
  summarise(row_count = n()) %>%
  filter(row_count >1) 


#### Step 6b: Add in MOUD procedure codes (these are not from RDA) ####

#Load in MOUD procedure code reference table that was previously created
moud_proc <- dbGetQuery(statement = "select * from claims.ref_moud_procedure_code;", 
                        conn = db_hhsaw) %>% 
  mutate(value_set_group = "sud",
         value_set_name = "apde-moud-procedure",
         data_source_type = "procedure",
         code_set = "HCPCS",
         code = procedure_code,
         desc = toupper(desc),
         sub_group_condition = "sud_opioid",
         oud_dx1_flag = ifelse(procedure_code %in% c("H0033", "96372", "11981", "11983", 
                                                     "G0516", "G0518", "G2073", "J2315"), 
                               1, 0)) %>% 
  select(value_set_group, value_set_name, data_source_type, code_set, code, desc, sub_group_condition,
         oud_dx1_flag)

#Bind the MOUD procedure codes to RDA reference table
rda_value_sets_updated_final <- plyr::rbind.fill(rda_value_sets_updated, moud_proc)


#### Step 7: Export initial version of reference table ####

#Add last run date/time
rda_value_sets_updated_final <- rda_value_sets_updated_final %>% mutate(last_run = Sys.time())

#Load Rdata file to SP site
myteamfolder_rda_value_set_existing$save_rdata(rda_value_sets_updated_final, file = "rda_value_sets_current.rdata")

#Save backup file with today's date
myteamfolder_rda_value_set_existing$
  save_rdata(rda_value_sets_updated_final,file = paste0("rda_value_sets_current_backup_", Sys.Date(), ".rdata"))


#### Step 8: Upload updated reference table to HHSAW ####

to_schema <- "ref"
to_table <- "rda_value_sets_apde"

# Load data
dbWriteTable(db_hhsaw, name = DBI::Id(schema = to_schema, table = to_table), 
             value = as.data.frame(rda_value_sets_updated_final), 
             overwrite = T)

# Add index
DBI::dbExecute(db_hhsaw, 
               glue::glue_sql("CREATE CLUSTERED INDEX [idx_cl_codeset_code] ON {`to_schema`}.{`to_table`} (code_set, code)",
                              .con = db_hhsaw))


