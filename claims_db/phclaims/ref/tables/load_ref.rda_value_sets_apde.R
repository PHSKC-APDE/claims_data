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

#2026-1-9 updates JL:
#ICD-CM codes with ccs_detail_code of 'MBD013' changed to be in the 'mh_other' category, except for F63 and F630

#2026-10-19 updates JL:
#1) updating SharePoint site to APDE Healthcare Data 
#2) new value set names that correspond to the new RDA format
#3) allow for duplicated codes, as long as value set names are different 


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
db_hhsaw <- apde.data::authenticate_hhsaw()



#### Step 1: Load existing reference table and new value sets ####

value_set_new_version_mh <- "2026-07-31" ##UPDATE EACH TIME THIS SCRIPT IS RUN
value_set_new_version_sud <- "2026-07-31" ##UPDATE EACH TIME THIS SCRIPT IS RUN

##Connect to SharePoint/TEAMS site
myteam <- get_team(team_name = "DPH-APDE-Healthcare-Data",
                   username = keyring::key_list("sharepoint")$username,
                   password = keyring::key_get("sharepoint", keyring::key_list("sharepoint")$username),
                   auth_type = "resource_owner",
                   tenant = "kingcounty.gov")

##Connect to drive (i.e., Documents-General document library) and navigate to desired subfolder
channel_internal <- myteam$get_channel("Internal Ops Shared Channel")
# channel_internal$list_drives() #lists all available document libraries
# myteamdrive = myteam$get_drive("Documents") #connect to document library named "Documents"
# #myteamfolder = myteamdrive$get_item("General")
myteamfolder = channel_internal$get_folder()
myteamfolder = myteamfolder$get_item("References/RDA_measures")
myteamfolder_rda_value_set_existing = myteamfolder$get_item("rda_value_sets_for_sql_load")
myteamfolder_rda_value_set_existing$list_items()

myteamfolder_rda_mh_value_sets = myteamfolder$get_item("mh_service_penetration_measure")
myteamfolder_rda_mh_value_sets = myteamfolder_rda_mh_value_sets$get_item(paste0("mh_", value_set_new_version_mh))
myteamfolder_rda_mh_value_sets$list_items()

myteamfolder_rda_sud_value_sets = myteamfolder$get_item("sud_tx_penetration_measure")
myteamfolder_rda_sud_value_sets = myteamfolder_rda_sud_value_sets$get_item(paste0("sud_", value_set_new_version_sud))
myteamfolder_rda_sud_value_sets$list_items()

## Load sub_group_pharmacy reference table from metadata file files to temp location
temp_rda_vs_metadata <- tempfile(fileext = ".xlsx") #Create temp file to hold contents of SP file
myteamfolder_rda_value_set_existing$get_item("rda_value_sets_metadata.xlsx")$download(dest = temp_rda_vs_metadata)
sub_group_pharmacy <- read_xlsx(
  file = temp_rda_vs_metadata,
  sheet = "sub_group_pharmacy",
  colNames = TRUE,
  detectDates = TRUE)

## Load existing RDA value set, dropping last_run variable, OUD diagnosis flag, and MOUD procedure codes (will be added later)
myteamfolder_rda_value_set_existing$get_item("rda_value_sets_current.rdata")$load_rdata()
rda_value_sets_existing <- rda_value_sets_updated_final %>% 
  filter_out(value_set_name == "apde-moud-procedure") %>% 
  select(-c(last_run, oud_dx1_flag))
rm(rda_value_sets_updated_final)

## Load new value sets for MH and SUD measures
temp_mh_vs_new <- tempfile(fileext = ".xlsx")
temp_sud_vs_new <- tempfile(fileext = ".xlsx")
myteamfolder_rda_mh_value_sets$get_item(paste0("mhsr-value-sets_",value_set_new_version_mh,".xlsx"))$download(
  dest = temp_mh_vs_new)
myteamfolder_rda_sud_value_sets$get_item(paste0("sud-tx-rate-value-denominator_",value_set_new_version_sud,".xlsx"))$download(
  dest = temp_sud_vs_new)
#for the SUD measures, the denominator and numerator excels have the same exact values and only the formula part is different

#MH value sets
#FllyQlfngPrcdr
mh_qual_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "FllyQlfngPrcdr", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "fllyqlfngprcdr",
         code_set = case_when(CodeTypeName == "CPT_Proc" ~ "CPT-HCPCS",
                              CodeTypeName == "HCPCS_Proc" ~ "HCPCS",
                              TRUE ~ NA),
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcNotHW
mh_nothw_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcNotHW", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procnothw",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ModHW
mh_hw_mod <- read_xlsx(file = temp_mh_vs_new, sheet = "ModHW", colNames = T) %>% 
  mutate(code = as.character(Code),
         value_set_group = "mh",
         value_set_name = "modhw",
         data_source_type = "modifier",
         desc = toupper(CodeDesc),
         code_set = tolower(CodeTypeName)) %>% 
  select(-c(Code, CodeDesc, CodeTypeName))

#ProcWithHW
mh_hw_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcWithHW", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procwithhw",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcWithHD
mh_hd_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcWithHD", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procwithhd",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ModHD
mh_hd_mod <- read_xlsx(file = temp_mh_vs_new, sheet = "ModHD", colNames = T) %>% 
  mutate(code = as.character(Code),
         value_set_group = "mh",
         value_set_name = "modhd",
         data_source_type = "modifier",
         desc = toupper(CodeDesc),
         code_set = tolower(CodeTypeName)) %>% 
  select(-c(Code, CodeDesc, CodeTypeName))

#ProcNotHFDiag
mh_nothf_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcNotHFDiag", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procnothfdiag",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ModHF
mh_hf_mod <- read_xlsx(file = temp_mh_vs_new, sheet = "ModHF", colNames = T) %>% 
  mutate(code = as.character(Code),
         value_set_group = "mh",
         value_set_name = "modhf",
         data_source_type = "modifier",
         desc = toupper(CodeDesc),
         code_set = tolower(CodeTypeName)) %>% 
  select(-c(Code, CodeDesc, CodeTypeName))

#MHDiag9_10
mh_dx <- read_xlsx(file = temp_mh_vs_new, sheet = "MHDiag9_10", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(code_set = case_when(CodeTypeName == "ICD9_Diag" ~ "ICD9CM",
                              CodeTypeName == "ICD10_Diag" ~ "ICD10CM",
                              TRUE ~ NA),
         desc = toupper(CodeDesc),
         value_set_group = "mh",
         value_set_name = "mhdiag9_10",
         data_source_type = "diagnosis") %>% 
  select(-c(CodeDesc, CodeTypeName))

#MHDateLimDiag9
mh_datelim_dx <- read_xlsx(file = temp_mh_vs_new, sheet = "MHDateLimDiag9", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(code_set = "ICD9CM",
         desc = toupper(CodeDesc),
         value_set_group = "mh",
         value_set_name = "mhdatelimdiag9",
         data_source_type = "diagnosis") %>% 
  select(-c(CodeDesc, CodeTypeName))

#ProcNotUB
mh_notub_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcNotUB", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procnotub",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ModUB
mh_ub_mod <- read_xlsx(file = temp_mh_vs_new, sheet = "ModUB", colNames = T) %>% 
  mutate(code = as.character(Code),
         value_set_group = "mh",
         value_set_name = "modub",
         data_source_type = "modifier",
         desc = toupper(CodeDesc),
         code_set = tolower(CodeTypeName)) %>% 
  select(-c(Code, CodeDesc, CodeTypeName))

#ProcNotHF
mh_nothf_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcNotHF", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procnothf",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcNotHFWithHW
mh_nothfwithhw_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcNotHFWithHW", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procnothfwithhw",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcNotHFHW
mh_nothfhw_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcNotHFHW", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procnothfhw",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcInpatFullQual
mh_inpat_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcInpatFullQual", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procinpatfullqual",
         code_set = case_when(CodeTypeName == "CPT_Proc" ~ "CPT-HCPCS",
                              CodeTypeName == "HCPCS_Proc" ~ "HCPCS",
                              CodeTypeName == "Mdfr" ~ "Mdfr",
                              TRUE ~ NA),
         desc = toupper(CodeDesc),
         data_source_type = ifelse(str_detect(CodeTypeName, "Proc"), "procedure", "modifier")) %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcInpatWithHH
mh_inpatwithhh_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcInpatWithHH", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procinpatwithhh",
         code_set = case_when(CodeTypeName == "CPT_Proc" ~ "CPT-HCPCS",
                              CodeTypeName == "HCPCS_Proc" ~ "HCPCS",
                              TRUE ~ NA),
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ModHH
mh_hh_mod <- read_xlsx(file = temp_mh_vs_new, sheet = "ModHH", colNames = T) %>% 
  mutate(code = as.character(Code),
         value_set_group = "mh",
         value_set_name = "modhh",
         data_source_type = "modifier",
         desc = toupper(CodeDesc),
         code_set = tolower(CodeTypeName)) %>% 
  select(-c(Code, CodeDesc, CodeTypeName))

#ProcInpatWIthHHnoHF
mh_inpatwithhhnohf_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcInpatWIthHHnoHF", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procinpatwithhhnohf",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcInpatNotHW
mh_inpatnothw_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcInpatNotHW", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procinpatnothw",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcInpatHForHW
mh_inpathforhw_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcInpatHForHW", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procinpathforhw",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcInpatNotHH
mh_inpatnothh_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcInpatNotHH", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procinpatnothh",
         code_set = case_when(CodeTypeName == "CPT_Proc" ~ "CPT-HCPCS",
                              CodeTypeName == "HCPCS_Proc" ~ "HCPCS",
                              TRUE ~ NA),
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcInpatNotHHHF
mh_inpatnothhhf_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcInpatNotHHHF", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procinpatnothhhf",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcInpatNotHD
mh_inpatnothd_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcInpatNotHD", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procinpatnothd",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcInpatNotUB
mh_inpatnotub_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcInpatNotUB", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procinpatnotub",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcInpatNotHF
mh_inpatnothf_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcInpatNotHF", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procinpatnothf",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcInpatNotHFHW
mh_inpatnothfhw_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcInpatNotHFHW", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procinpatnothfhw",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ClmTypeInpat
mh_inpat_claimtype <- read_xlsx(file = temp_mh_vs_new, sheet = "ClmTypeInpat", colNames = T) %>% 
  mutate(code = as.character(Code),
         value_set_group = "mh",
         value_set_name = "clmtypeinpat",
         data_source_type = "claim_type",
         desc = toupper(CodeDesc),
         code_set = tolower(CodeTypeName)) %>% 
  select(-c(Code, CodeDesc, CodeTypeName))

#ClmTypeOutPat
mh_outpat_claimtype <- read_xlsx(file = temp_mh_vs_new, sheet = "ClmTypeOutPat", colNames = T) %>% 
  mutate(code = as.character(Code),
         value_set_group = "mh",
         value_set_name = "clmtypeoutpat",
         data_source_type = "claim_type",
         desc = toupper(CodeDesc),
         code_set = tolower(CodeTypeName)) %>% 
  select(-c(Code, CodeDesc, CodeTypeName))

#ProcOutPatPrmryDx
mh_outpat_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcOutPatPrmryDx", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procoutpatprmrydx",
         code_set = case_when(CodeTypeName == "CPT_Proc" ~ "CPT-HCPCS",
                              CodeTypeName == "HCPCS_Proc" ~ "HCPCS",
                              TRUE ~ NA),
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#MHPrmyDiag9_10
mh_primary_dx <- read_xlsx(file = temp_mh_vs_new, sheet = "MHPrmyDiag9_10", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(code_set = case_when(CodeTypeName == "ICD9_Diag" ~ "ICD9CM",
                              CodeTypeName == "ICD10_Diag" ~ "ICD10CM",
                              TRUE ~ NA),
         desc = toupper(CodeDesc),
         value_set_group = "mh",
         value_set_name = "mhprmydiag9_10",
         data_source_type = "diagnosis") %>% 
  select(-c(CodeDesc, CodeTypeName))

#MHPrmryDateLimDiag9
mh_primarydatelim_dx <- read_xlsx(file = temp_mh_vs_new, sheet = "MHPrmryDateLimDiag9", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(code_set = "ICD9CM",
         desc = toupper(CodeDesc),
         value_set_group = "mh",
         value_set_name = "mhprmrydatelimdiag9",
         data_source_type = "diagnosis") %>% 
  select(-c(CodeDesc, CodeTypeName))

#ProcOutPatFullQual
mh_outpatfullqual_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcOutPatFullQual", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procoutpatfullqual",
         code_set = case_when(CodeTypeName == "CPT_Proc" ~ "CPT-HCPCS",
                              CodeTypeName == "HCPCS_Proc" ~ "HCPCS",
                              TRUE ~ NA),
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcOutPatNotHW
mh_outpatnothw_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcOutPatNotHW", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procoutpatnothw",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcOutPatWithQ2
mh_outpatwithq2_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcOutPatWithQ2", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procoutpatwithq2",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ModQ2
mh_q2_mod <- read_xlsx(file = temp_mh_vs_new, sheet = "ModQ2", colNames = T) %>% 
  mutate(code = as.character(Code),
         value_set_group = "mh",
         value_set_name = "modq2",
         data_source_type = "modifier",
         desc = toupper(CodeDesc),
         code_set = tolower(CodeTypeName)) %>% 
  select(-c(Code, CodeDesc, CodeTypeName))

#ProcOutPatNotUB
mh_outpatnotub_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcOutPatNotUB", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procoutpatnotub",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#TxnmyOutPat
mh_outpat_taxonomy <- read_xlsx(file = temp_mh_vs_new, sheet = "TxnmyOutPat", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "txnmyoutpat",
         data_source_type = "taxonomy", 
         code_set = "HPT",
         desc = toupper(CodeDesc)) %>% 
  select(-c(CodeDesc, CodeTypeName))

#ProcOutPatTxnmyPrmryDx
mh_outpattaxonomy_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcOutPatTxnmyPrmryDx", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procoutpattxnmyprmrydx",
         code_set = case_when(CodeTypeName == "CPT_Proc" ~ "CPT-HCPCS",
                              CodeTypeName == "HCPCS_Proc" ~ "HCPCS",
                              TRUE ~ NA),
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcOutPatMHDx
mh_outpatmhdx_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcOutPatMHDx", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procoutpatmhdx",
         code_set = case_when(CodeTypeName == "CPT_Proc" ~ "CPT-HCPCS",
                              CodeTypeName == "HCPCS_Proc" ~ "HCPCS",
                              TRUE ~ NA),
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ProcOutPatTxnmyMHDx
mh_outpattxnmymhdx_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "ProcOutPatTxnmyMHDx", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "procoutpattxnmymhdx",
         code_set = case_when(CodeTypeName == "CPT_Proc" ~ "CPT-HCPCS",
                              CodeTypeName == "HCPCS_Proc" ~ "HCPCS",
                              TRUE ~ NA),
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#DRGPriorJuly2014
mh_priorjuly2014_drg <- read_xlsx(file = temp_mh_vs_new, sheet = "DRGPriorJuly2014", colNames = T) %>% 
  rename(code = Code,
         desc = CodeDesc) %>% 
  mutate(code_set = "DRG",
         value_set_group = "mh",
         value_set_name = "drgpriorjuly2014",
         data_source_type = "diagnosis") %>% 
  select(-CodeTypeName)

#DRGJuly2014Forward
mh_july2014forward_drg <- read_xlsx(file = temp_mh_vs_new, sheet = "DRGJuly2014Forward", colNames = T) %>% 
  rename(code = Code,
         desc = CodeDesc) %>% 
  mutate(code_set = "DRG",
         value_set_group = "mh",
         value_set_name = "drgjuly2014forward",
         data_source_type = "diagnosis") %>% 
  select(-CodeTypeName)

#NeedProcIPWithHWNotHF
mh_needprocipwithhwnothf_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "NeedProcIPWithHWNotHF", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "needprocipwithhwnothf",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#NeedProcWithH9
mh_needprocwithh9_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "NeedProcWithH9", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "needprocwithh9",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#ModH9
mh_h9_mod <- read_xlsx(file = temp_mh_vs_new, sheet = "ModH9", colNames = T) %>% 
  mutate(code = as.character(Code),
         value_set_group = "mh",
         value_set_name = "modh9",
         data_source_type = "modifier",
         desc = toupper(CodeDesc),
         code_set = tolower(CodeTypeName)) %>% 
  select(-c(Code, CodeDesc, CodeTypeName))

#NeedProcWithUB
mh_needprocwithub_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "NeedProcWithUB", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "needprocwithub",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#NeedProcWithHWNotHF
mh_needprocwithhwnothf_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "NeedProcWithHWNotHF", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "needprocwithhwnothf",
         code_set = "HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#NeedProcWithH9NotHF
mh_needprocwithh9nothf_proc <- read_xlsx(file = temp_mh_vs_new, sheet = "NeedProcWithH9NotHF", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "needprocwithh9nothf",
         code_set = "CPT-HCPCS",
         desc = toupper(CodeDesc),
         data_source_type = "procedure") %>% 
  select(-c(CodeTypeName, CodeDesc))

#MH_NDCElement
mh_ndc <- read_xlsx(file = temp_mh_vs_new, sheet = "MH_NDCElement", colNames = T) %>% 
  rename(code = Code, 
         desc = CodeDesc, 
         code_set = CodeTypeName) %>% 
  mutate(value_set_group = "mh",
         value_set_name = "mh_ndcelement",
         data_source_type = "pharmacy")

#sud value sets
#NDCElement
sud_ndcelement <- read_xlsx(file = temp_sud_vs_new, sheet = "NDCElement", colNames = T) %>% 
  rename(code = Code, 
         desc = CodeDesc, 
         code_set = CodeTypeName) %>% 
  mutate(value_set_group = "sud",
         value_set_name = "ndcelement",
         data_source_type = "pharmacy")

#InpatientLineFlag
sud_inpatientline <- read_xlsx(file = temp_sud_vs_new, sheet = "InpatientLineFlag ", colNames = T) %>% 
  mutate(code = as.character(Code),
         value_set_group = "sud",
         value_set_name = "inpatientlineflag",
         data_source_type = "claim_type",
         desc = toupper(CodeDesc),
         code_set = tolower(CodeTypeName)) %>% 
  select(-c(Code, CodeDesc, CodeTypeName))

#SUD_IpSvcsPreJly2014AP_DR
sud_pre2014_drg <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_IpSvcsPreJly2014AP_DR", colNames = T) %>% 
  rename(code = Code,
         desc = CodeDesc) %>% 
  mutate(code_set = "DRG",
         value_set_name = "sud_ipsvcsprejly2014ap_dr",
         value_set_group = "sud",
         data_source_type = "diagnosis") %>% 
  select(-CodeTypeName)

#SUD_IpSvcsJly2014FrwrdAPR_DRG
sud_post2014_drg <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_IpSvcsJly2014FrwrdAPR_DRG", colNames = T) %>% 
  rename(code = Code,
         desc = CodeDesc) %>% 
  mutate(code_set = "DRG",
         value_set_name = "sud_ipsvcsjly2014frwrdapr_drg",
         value_set_group = "sud",
         data_source_type = "diagnosis") %>% 
  select(-CodeTypeName)

#SUD_WthdrwlMngmnt_RevCodes
sud_rev <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_WthdrwlMngmnt_RevCodes", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(code_set = "UBREV",
         value_set_group = "sud",
         value_set_name = "sud_wthdrwlmngmnt_revcodes",
         data_source_type = "billing",
         desc = toupper(CodeDesc)) %>% 
  select(-c(CodeDesc, CodeTypeName))

# SUD_WthdrwlMngmnt_ICDProcCodes
sud_icdproc <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_WthdrwlMngmnt_ICDProcCodes", colNames = T) %>% 
  rename(code = Code,
         desc = CodeDesc) %>% 
  mutate(code_set = case_when(CodeTypeName == "ICD9_Proc" ~ "ICD9PCS",
                              CodeTypeName == "ICD10_Proc" ~ "ICD10PCS",
                              TRUE ~ NA),
         value_set_group = "sud",
         value_set_name = "sud_wthdrwlmngmnt_icdproccodes",
         data_source_type = "procedure") %>% 
  select(-CodeTypeName)

#SUD_WthdrwlMngmnt_PrcdrCodes
sud_withdrawal_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_WthdrwlMngmnt_PrcdrCodes", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(desc = toupper(CodeDesc),
         value_set_group = "sud",
         value_set_name = "sud_wthdrwlmngmnt_prcdrcodes",
         code_set = "HCPCS",
         data_source_type = "procedure") %>% 
  select(-c(CodeDesc, CodeTypeName))

#SUD_InpatResid_PrcdrCodes
sud_inpat_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_InpatResid_PrcdrCodes", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(desc = toupper(CodeDesc),
         value_set_group = "sud",
         value_set_name = "sud_inpatresid_prcdrcodes",
         code_set = "HCPCS",
         data_source_type = "procedure") %>% 
  select(-c(CodeDesc, CodeTypeName))

#SUDAnyQlfyng_ICD_9_10Dx
sud_dx <- read_xlsx(file = temp_sud_vs_new, sheet = "SUDAnyQlfyng_ICD_9_10Dx", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(code_set = case_when(CodeTypeName == "ICD9_Diag" ~ "ICD9CM",
                              CodeTypeName == "ICD10_Diag" ~ "ICD10CM",
                              TRUE ~ NA),
         desc = toupper(CodeDesc),
         value_set_group = "sud",
         value_set_name = "sudanyqlfyng_icd_9_10dx",
         data_source_type = "diagnosis") %>% 
  select(-c(CodeDesc, CodeTypeName))

#SUDDateLimAnyQlfyng_ICD_9Dx
sud_datelim_dx <- read_xlsx(file = temp_sud_vs_new, sheet = "SUDDateLimAnyQlfyng_ICD_9Dx", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(code_set = "ICD9CM",
         desc = toupper(CodeDesc),
         value_set_group = "sud",
         value_set_name = "suddatelimanyqlfyng_icd_9dx",
         data_source_type = "diagnosis") %>% 
  select(-c(CodeDesc, CodeTypeName))

#SUD_RecHsResidServicesPrcdrCd 
sud_res_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_RecHsResidServicesPrcdrCd", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(code_set = "HCPCS",
         desc = toupper(CodeDesc),
         value_set_group = "sud",
         value_set_name = "sud_rechsresidservicesprcdrcd",
         data_source_type = "procedure") %>% 
  select(-c(CodeDesc, CodeTypeName))

#ResidSUDTreatmentFcltyLocCd
sud_res_location <- read_xlsx(file = temp_sud_vs_new, sheet = "ResidSUDTreatmentFcltyLocCd", colNames = T) %>% 
  rename(code = Code,
         desc = CodeDesc) %>% 
  mutate(code_set = tolower(CodeTypeName),
         value_set_group = "sud",
         value_set_name = "residsudtreatmentfcltyloccd",
         data_source_type = "place_of_service") %>% 
  select(-CodeTypeName)

#SUD_OpiateSubPrcdrCd
sud_opiatesub_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_OpiateSubPrcdrCd", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(desc = toupper(CodeDesc),
         code_set = "HCPCS",
         value_set_group = "sud",
         value_set_name = "sud_opiatesubprcdrcd",
         data_source_type = "procedure") %>% 
  select(-c(CodeDesc, CodeTypeName))

#ProfAndOutpatLineFlag
sud_profoutpat_claimtype <- read_xlsx(file = temp_sud_vs_new, sheet = "ProfAndOutpatLineFlag", colNames = T) %>% 
  mutate(code = as.character(Code),
         value_set_group = "sud",
         value_set_name = "profandoutpatlineflag",
         data_source_type = "claim_type",
         desc = toupper(CodeDesc),
         code_set = tolower(CodeTypeName)) %>% 
  select(-c(Code, CodeDesc, CodeTypeName))

#SUD_OutpatPrcdrCd 
sud_oupat_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_OutpatPrcdrCd", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "sud",
         value_set_name = "sud_oupatprcdrcd",
         data_source_type = "procedure",
         desc = toupper(CodeDesc),
         code_set = "HCPCS") %>% 
  select(-c(CodeDesc, CodeTypeName))

#SUD_PrvdrTxnmy 
sud_taxonomy <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_PrvdrTxnmy", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "sud",
         value_set_name = "sud_prvdrtxnmy",
         data_source_type = "taxonomy", 
         code_set = "HPT",
         desc = toupper(CodeDesc)) %>% 
  select(-c(CodeDesc, CodeTypeName))

#SUD_InpatResidServiceC 
sud_inpatresid_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_InpatResidServiceC", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(value_set_group = "sud",
         value_set_name = "sud_inpatresidservicec",
         data_source_type = "procedure",
         desc = toupper(CodeDesc),
         code_set = "HCPCS") %>% 
  select(-c(CodeDesc, CodeTypeName))

#SUD_OpiateSubstitutionServices
sud_opiatesubsvs_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_OpiateSubstitutionServices", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(desc = toupper(CodeDesc),
         code_set = "HCPCS",
         value_set_group = "sud",
         value_set_name = "sud_opiatesubstitutionservices",
         data_source_type = "procedure") %>% 
  select(-c(CodeDesc, CodeTypeName))

#SUD_WithdrawalManagementC
sud_withdrawalmanagement_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_WithdrawalManagementC", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(desc = toupper(CodeDesc),
         code_set = "HCPCS",
         value_set_group = "sud",
         value_set_name = "sud_withdrawalmanagementc",
         data_source_type = "procedure") %>% 
  select(-c(CodeDesc, CodeTypeName))

#SUD_OutpatTxServicesC
sud_outpatsvs_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_OutpatTxServicesC", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(desc = toupper(CodeDesc),
         code_set = "HCPCS",
         value_set_group = "sud",
         value_set_name = "sud_oupattxservicesc",
         data_source_type = "procedure") %>% 
  select(-c(CodeDesc, CodeTypeName))

#PrimaryQualifying_ICD_9_10Dx
sud_primary_dx <- read_xlsx(file = temp_sud_vs_new, sheet = "PrimaryQualifying_ICD_9_10Dx", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(code_set = case_when(CodeTypeName == "ICD9_Diag" ~ "ICD9CM",
                              CodeTypeName == "ICD10_Diag" ~ "ICD10CM",
                              TRUE ~ NA),
         desc = toupper(CodeDesc),
         value_set_group = "sud",
         value_set_name = "primaryqualifying_icd_9_10dx",
         data_source_type = "diagnosis") %>% 
  select(-c(CodeDesc, CodeTypeName))

#PrmryDateLimQlfyng_ICD_9Dx
sud_primarydatelim_dx <- read_xlsx(file = temp_sud_vs_new, sheet = "PrmryDateLimQlfyng_ICD_9Dx", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(code_set = "ICD9CM",
         desc = toupper(CodeDesc),
         value_set_group = "sud",
         value_set_name = "prmrydatelimqlfyng_icd_9dx",
         data_source_type = "diagnosis") %>% 
  select(-c(CodeDesc, CodeTypeName))

#SUDOutpatientServicesD
sud_outpatsvsd_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "SUDOutpatientServicesD", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(desc = toupper(CodeDesc),
         code_set = case_when(CodeTypeName == "CPT_Proc" ~ "CPT-HCPCS",
                              CodeTypeName == "HCPCS_Proc" ~ "HCPCS",
                              TRUE ~ NA),
         value_set_group = "sud",
         value_set_name = "sudoutpatientservicesd",
         data_source_type = "procedure") %>% 
  select(-c(CodeDesc, CodeTypeName))

#InpatientLineBroadFlag
sud_inpatline_claimtype <- read_xlsx(file = temp_sud_vs_new, sheet = "InpatientLineBroadFlag", colNames = T) %>% 
  mutate(code = as.character(Code),
         value_set_group = "sud",
         value_set_name = "inpatientlinebroadflag",
         data_source_type = "claim_type",
         desc = toupper(CodeDesc),
         code_set = tolower(CodeTypeName)) %>% 
  select(-c(Code, CodeDesc, CodeTypeName))

#SUD_IntensiveWithdrawalMngmnt
sud_withdrawal_rev <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_IntensiveWithdrawalMngmnt", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(code_set = "UBREV",
         value_set_group = "sud",
         value_set_name = "sud_intensivewithdrawalmngmnt",
         data_source_type = "billing",
         desc = toupper(CodeDesc)) %>% 
  select(-c(CodeDesc, CodeTypeName))

#SUD_Assessment 
sud_assessment_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_Assessment", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(desc = toupper(CodeDesc),
         code_set = "HCPCS",
         value_set_group = "sud",
         value_set_name = "sud_assessment",
         data_source_type = "procedure") %>% 
  select(-c(CodeDesc, CodeTypeName))

#SUD_SBIRT
sud_sbirt_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "SUD_SBIRT", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(desc = toupper(CodeDesc),
         code_set = case_when(CodeTypeName == "CPT_Proc" ~ "CPT-HCPCS",
                              CodeTypeName == "HCPCS_Proc" ~ "HCPCS",
                              TRUE ~ NA),
         value_set_group = "sud",
         value_set_name = "sud_sbirt",
         data_source_type = "procedure") %>% 
  select(-c(CodeDesc, CodeTypeName))

#CrisisA
sud_crisisa_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "CrisisA", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(desc = toupper(CodeDesc),
         code_set = case_when(CodeTypeName == "CPT_Proc" ~ "CPT-HCPCS",
                              CodeTypeName == "HCPCS_Proc" ~ "HCPCS",
                              TRUE ~ NA),
         value_set_group = "sud",
         value_set_name = "crisisa",
         data_source_type = "procedure") %>% 
  select(-c(CodeDesc, CodeTypeName))

#CrisisB
sud_crisisb_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "CrisisB", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(desc = toupper(CodeDesc),
         code_set = "HCPCS",
         value_set_group = "sud",
         value_set_name = "crisisb",
         data_source_type = "procedure") %>% 
  select(-c(CodeDesc, CodeTypeName))

#ModifierA
sud_modifiera_mod <- read_xlsx(file = temp_sud_vs_new, sheet = "ModifierA", colNames = T) %>% 
  mutate(code = as.character(Code),
         value_set_group = "sud",
         value_set_name = "modifiera",
         data_source_type = "modifier",
         desc = toupper(CodeDesc),
         code_set = tolower(CodeTypeName)) %>% 
  select(-c(Code, CodeDesc, CodeTypeName))

#ModifierC
sud_modifierc_mod <- read_xlsx(file = temp_sud_vs_new, sheet = "ModifierC", colNames = T) %>% 
  mutate(code = as.character(Code),
         value_set_group = "sud",
         value_set_name = "modifierc",
         data_source_type = "modifier",
         desc = toupper(CodeDesc),
         code_set = tolower(CodeTypeName)) %>% 
  select(-c(Code, CodeDesc, CodeTypeName))

#CrisisC
sud_crisisc_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "CrisisC", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(desc = toupper(CodeDesc),
         code_set = "HCPCS",
         value_set_group = "sud",
         value_set_name = "crisisc",
         data_source_type = "procedure") %>% 
  select(-c(CodeDesc, CodeTypeName))

#CrisisD
sud_crisisd_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "CrisisD ", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(desc = toupper(CodeDesc),
         code_set = "HCPCS",
         value_set_group = "sud",
         value_set_name = "crisisd",
         data_source_type = "procedure") %>% 
  select(-c(CodeDesc, CodeTypeName))

#CrisisE
sud_crisise_proc <- read_xlsx(file = temp_sud_vs_new, sheet = "CrisisE", colNames = T) %>% 
  rename(code = Code) %>% 
  mutate(desc = toupper(CodeDesc),
         code_set = "HCPCS",
         value_set_group = "sud",
         value_set_name = "crisise",
         data_source_type = "procedure") %>% 
  select(-c(CodeDesc, CodeTypeName))

#ModifierB
sud_modifierb_mod <- read_xlsx(file = temp_sud_vs_new, sheet = "ModifierB", colNames = T) %>% 
  mutate(code = as.character(Code),
         value_set_group = "sud",
         value_set_name = "modifierb",
         data_source_type = "modifier",
         desc = toupper(CodeDesc),
         code_set = tolower(CodeTypeName)) %>% 
  select(-c(Code, CodeDesc, CodeTypeName))


#### Step 2: Bind all new value sets ####
new_mh <- rbindlist(mget(ls(pattern = "^mh_")), fill = T)
new_sud <- rbindlist(mget(ls(pattern = "^sud_")), fill = T)

rda_value_sets_new_raw <- bind_rows(new_mh, new_sud) %>% 
  select(value_set_group, value_set_name, data_source_type, code_set, code, desc)

rm(list = ls(pattern = "^mh_"))
rm(list = ls(pattern = "^sud_"))


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
#Specifically making sure that there are not duplicate diagnosis codes per value_set_name
rda_value_sets_new <- rda_value_sets_new %>%
  mutate(
    code_raw_len = case_when(
      code_set %in% c("ICD9CM", "ICD10CM") ~ str_length(code_raw),
      TRUE ~ NA_integer_)) %>%
  group_by(value_set_name, code) %>%
  mutate(
    row_count = n(),
    code_raw_len_rank = rank(-code_raw_len, ties.method = c("first"))) %>%
  ungroup() %>%
  filter(is.na(code_raw_len) | code_raw_len_rank == 1L) %>%
  select(-code_raw:-code_raw_len_rank)

#Confirm that no ICD-CM codes have more than 1 row per value_set_name
rda_value_sets_new %>% filter(code_set %in% c("ICD9CM", "ICD10CM")) %>% group_by(value_set_name, code) %>%
  mutate(row_count = n()) %>% ungroup() %>% count(row_count)

#Confirm that no NDC codes have more than 1 row per value_set_name
rda_value_sets_new %>% filter(code_set %in% c("NDC")) %>% group_by(value_set_name, code) %>%
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

#Check for duplicate rows per code set and value set name
rda_value_sets_new %>%
  group_by(code_set, value_set_name, code) %>%
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
    ccs_detail_code %in% c("MBD008", "5.7") ~ "mh_disrupt",
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
    
    ccs_detail_code == "MBD013" & code %in% c("F63", "F630") ~ "mh_disrupt",
    ccs_detail_code == "MBD013" ~ "mh_other",
    
    ccs_detail_code %in% c("MBD010", "5.15", "MBD011", "MBD009", "NVS011", "SYM008",
                           "SYM016", "5.4", "5.5", "15.7", "5.14") ~ "mh_other",
    ccs_detail_code == "MBD007" & str_detect(code, "^F48|^F44") ~ "mh_other",
    ccs_detail_code == "MBD014" & code == "F988" ~ "mh_other",
    ccs_detail_code == "MBD006" & code == "F4522" ~ "mh_other",
    
    code %in% c("F50014", "F50024", "F5025", "F5025", "F50814") ~ "mh_other",
    code %in% c("F1091") ~ "sud_alcohol",
    code %in% c("F1191") ~ "sud_opioid",
    code %in% c("F1291") ~ "sud_cannabis", 
    code %in% c("F1391") ~ "sud_sedative",
    code %in% c("F1491") ~ "sud_cocaine",
    code %in% c("F1591") ~ "sud_other_stimulant",
    code %in% c("F1691") ~ "sud_hallucinogen",
    code %in% c("F1891") ~ "sud_inhalant",
    code %in% c("F1991") ~ "sud_other_substance",
     
    TRUE ~ NA_character_
  ))
  

#Make sure there are no diagnosis code rows with a null sub_group_apde value, expect 0
count(filter(rda_value_sets_new, code_set %in% c("ICD9CM", "ICD10CM") & is.na(sub_group_condition)) %>%
        select(code_set, code, desc, sub_group_condition))

#If there are rows missing, then run the code and manually add them in to the code above that categorizes sub_group_condition: 
#Identify missing and fill in blanks
blanks <- rda_value_sets_new %>%
  filter(code_set %in% c("ICD9CM", "ICD10CM") & is.na(sub_group_condition)) %>%
  distinct(code_set, code, desc, sub_group_condition, ccs_detail_code)
view(blanks)
#there were 13 missing in 10/16/2026 update

#Remove variables from ref.icdcm_codes table
rda_value_sets_new <- rda_value_sets_new %>% 
  select(-ccs_broad_desc:-ccs_catch_all) 



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
    data_source_type == "pharmacy" & code_set == "NDC" & desc %in% acamprosate_rx ~ "pharm_acamprosate",
    data_source_type == "pharmacy" & code_set == "NDC" & desc %in% disulfiram_rx ~ "pharm_disulfiram",
    data_source_type == "pharmacy" & code_set == "NDC" & desc %in% adhd_rx ~ "pharm_adhd",
    data_source_type == "pharmacy" & code_set == "NDC" & desc %in% antianxiety_rx ~ "pharm_antianxiety",
    data_source_type == "pharmacy" & code_set == "NDC" & desc %in% antidepressant_rx ~ "pharm_antidepressant",
    data_source_type == "pharmacy" & code_set == "NDC" & desc %in% antimania_rx ~ "pharm_antimania",
    data_source_type == "pharmacy" & code_set == "NDC" & desc %in% antipsychotic_rx ~ "pharm_antipsychotic",
    data_source_type == "pharmacy" & code_set == "NDC" & desc %in% buprenorphine_naloxone_rx ~ "pharm_buprenorphine_naloxone",
    data_source_type == "pharmacy" & code_set == "NDC" & desc %in% buprenorphine_rx ~ "pharm_buprenorphine",
    data_source_type == "pharmacy" & code_set == "NDC" & desc %in% naltrexone_rx ~ "pharm_naltrexone_rx",
    TRUE ~ NA_character_
  ))

#Manual recoding based on cumulative value sets to date
rda_value_sets_new_rx <- rda_value_sets_new_rx %>%
  mutate(sub_group_pharmacy = case_when(
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) &
      desc %in% c("METHYLPHENIDATE", "DICLOFENAC SODIUM DR", "LISDEXAMFETAMINE DIMESYLATE", 
                  "GUANFACINE HYDROCHLORIDE ER", "RELEXXII", "AMPHETAMINE/DEXTROAMPHETAMINE ER") ~ "pharm_adhd",
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) &
      desc %in% c("VILAZODONE HYDROCHLORIDE", "VENLAFAXINE BESYLATE ER", "ABILIFY MYCITE STARTER KIT",
                  "ABILIFY MYCITE MAINTENANCE KIT", "AUVELITY", "ZURZUVAE") ~ "pharm_antidepressant",
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) & 
      desc %in% c("INVEGA HAFYERA", "LURASIDONE HYDROCHLORIDE", "RYKINDO", "RISPERIDONE ER", 
                  "UZEDY", "ABILIFY ASIMTUFII") ~ "pharm_antipsychotic",
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) & desc %in% c("LOREEV XR") ~ "pharm_antianxiety",
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) & 
      (str_detect(desc, "NALTREX") | str_detect(desc, "LOTREXONE") | str_detect(desc, "VIVITROL")) ~ "pharm_naltrexone_rx",
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) & str_detect(desc, "DISULFIRAM") ~ "pharm_disulfiram",
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) & 
      (str_detect(desc, "BUPRENORPHINE") | str_detect(desc, "BRIXADI") | 
         str_detect(desc, "SUBOXONE") | str_detect(desc, "ZUBSOLV") | str_detect(desc, "SUBLOCADE"))~ "pharm_buprenorphine",
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) & str_detect(desc, "ACAMPROSATE") ~ "pharm_acamprosate",
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) & 
      (str_detect(desc, "CHLORPROMAZINE") | str_detect(desc, "FLUPHENAZINE") | str_detect(desc, "RISPERIDONE") | 
         str_detect(desc, "PROCHLORPERAZINE") | str_detect(desc, "ARIPIPRAZOLE") | 
         str_detect(desc, "LURASIDONE") | str_detect(desc, "QUETIAPINE") | str_detect(desc, "LOXAPINE") | 
         str_detect(desc, "HALOPERIDOL") | str_detect(desc, "ZYPREXA") | str_detect(desc, "PALIPERIDONE") | 
         str_detect(desc, "CLOZAPINE") | str_detect(desc, "RYKINDO") | str_detect(desc, "ERZOFRI") | 
         str_detect(desc, "ZIPRASIDONE") | str_detect(desc, "PERPHENAZINE") | str_detect(desc, "TRIFLUOPERAZINE") | 
         str_detect(desc, "CAPLYTA") | str_detect(desc, "THIOTHIXENE") | str_detect(desc, "ABILIFY") |
         str_detect(desc, "ADASUVE") | str_detect(desc, "ARISTADA") | str_detect(desc, "ASENAPINE") | 
         str_detect(desc, "BYSANTI") | str_detect(desc, "CLOZARIL") | str_detect(desc, "COBENFY") |
         str_detect(desc, "VRAYLAR") | str_detect(desc, "VERSACLOZ") | str_detect(desc, "UZEDY") |
         str_detect(desc, "THIORIDAZINE") | str_detect(desc, "SEROQUEL") | 
         str_detect(desc, "GEODON") | str_detect(desc, "COMPAZINE") | str_detect(desc, "COMPRO") | 
         str_detect(desc, "EQUETRO") | str_detect(desc, "SECUADO") | str_detect(desc, "FANAPT") |
         str_detect(desc, "HALDOL") | str_detect(desc, "INVEGA") | str_detect(desc, "LATUDA") |
         str_detect(desc, "LYBALVI") | str_detect(desc, "MOLINDONE") | str_detect(desc, "NUPLAZID") |
         str_detect(desc, "OPIPZA") | str_detect(desc, "PERPHEN") | str_detect(desc, "PERSERIS") |
         str_detect(desc, "REXULTI") | str_detect(desc, "RISPERDAL") | str_detect(desc, "SAPHRIS")) ~ "pharm_antipsychotic",
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) & 
      (str_detect(desc, "HYDROXYZINE") | str_detect(desc, "BUSPIRONE") | str_detect(desc, "ALPRAZOLAM") | 
         str_detect(desc, "DIAZEPAM") | str_detect(desc, "LORAZEPAM") | str_detect(desc, "BUCAPSOL") |
         str_detect(desc, "VALIUM") | str_detect(desc, "CLORAZEPATE") | str_detect(desc, "OXAZEPAM") | 
         str_detect(desc, "CHLORDIAZEPOXIDE") | str_detect(desc, "ATIVAN") | 
         str_detect(desc, "CHLORDIAZEPO") | str_detect(desc, "XANAX") | str_detect(desc, "VISTARIL") | 
         str_detect(desc, "TRANXENE") | str_detect(desc, "DROPERIDOL") | str_detect(desc, "LOREEV") |
         str_detect(desc, "MEPROBAMATE")) ~ "pharm_antianxiety",
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) & 
      (str_detect(desc, "BUPROPION") | str_detect(desc, "FLUOXETINE") | str_detect(desc, "IMIPRAMINE") | 
         str_detect(desc, "NORTRIPTYLINE") | str_detect(desc, "AMITRIPTYLINE") | 
         str_detect(desc, "OLANZAPINE") | str_detect(desc, "VILAZODONE") | str_detect(desc, "DULOXETINE") | 
         str_detect(desc, "DOXEPIN") | str_detect(desc, "DULOXETINE") | str_detect(desc, "CITALOPRAM") | 
         str_detect(desc, "VENLAFAXINE") | str_detect(desc, "SERTRALINE") | str_detect(desc, "ESCITALOPRAM") | 
         str_detect(desc, "ZURZUVAE") | str_detect(desc, "AUVELITY") | str_detect(desc, "TRAZODONE") | 
         str_detect(desc, "REMERON") | str_detect(desc, "FLUVOXAMINE") | str_detect(desc, "PAROXETINE") |
         str_detect(desc, "CLOMIPRAMINE") | str_detect(desc, "MIRTAZAPINE") | str_detect(desc, "MARPLAN") |
         str_detect(desc, "DESIPRAMINE") | str_detect(desc, "ZULRESSO") | str_detect(desc, "TRANYLCYPROMINE") |
         str_detect(desc, "AMOXAPINE") | str_detect(desc, "ANAFRANIL") | str_detect(desc, "APLENZIN") | 
         str_detect(desc, "CELEXA") | str_detect(desc, "ZOLOFT") | str_detect(desc, "WELLBUTRIN") |
         str_detect(desc, "VIIBRYD") | str_detect(desc, "TRINTELLIX") | str_detect(desc, "SYMBYAX") | 
         str_detect(desc, "SPRAVATO") | str_detect(desc, "CYMBALTA") | str_detect(desc, "EFFEXOR") | 
         str_detect(desc, "PRISTIQ") | str_detect(desc, "PROTRIPTYLINE") | 
         str_detect(desc, "NARDIL") | str_detect(desc, "DRIZALMA") | str_detect(desc, "EMSAM") | 
         str_detect(desc, "EXXUA") | str_detect(desc, "FETZIMA") | str_detect(desc, "LEXAPRO") |
         str_detect(desc, "NEFAZODONE") | str_detect(desc, "NORPRAMIN") | str_detect(desc, "PAMELOR") |
         str_detect(desc, "PARNATE") | str_detect(desc, "PAXIL") | str_detect(desc, "PEXEVA") |
         str_detect(desc, "PHENELZINE") | str_detect(desc, "PROZAC") | str_detect(desc, "RALDESY") |
         str_detect(desc, "FORFIVO")) ~ "pharm_antidepressant",
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) & 
      (str_detect(desc, "DEXTROAMPHETAMINE") | str_detect(desc, "METHAMPHETAMINE") |
         str_detect(desc, "METHYLPHENIDATE") | str_detect(desc, "DEXMETHYLPHENIDATE") |
         str_detect(desc, "DEXTROAMPHETAMINE") | str_detect(desc, "ADHANSIA") | 
         str_detect(desc, "ATOMOXETINE") | str_detect(desc, "GUANFACINE") | str_detect(desc, "DEXTROAMP") | 
         str_detect(desc, "LISDEXAMFETAMINE") | str_detect(desc, "AMPHETAMINE") | 
         str_detect(desc, "CLONIDINE") | str_detect(desc, "JORNAY") | str_detect(desc, "ADDERALL") |
         str_detect(desc, "ADZENYS") | str_detect(desc, "APTENSIO") | str_detect(desc, "ARYNTA") |
         str_detect(desc, "ATONCY") | str_detect(desc, "AZSTARYS") | str_detect(desc, "ZENZEDI") |
         str_detect(desc, "XELSTRYM") | str_detect(desc, "VYVANSE") | str_detect(desc, "STRATTERA") |
         str_detect(desc, "RITALIN") | str_detect(desc, "CONCERTA") | str_detect(desc, "COTEMPLA") |
         str_detect(desc, "DAYTRANA") | str_detect(desc, "DESOXYN") | str_detect(desc, "DEXEDRINE") |
         str_detect(desc, "DYANAVEL") | str_detect(desc, "EVEKEO") | str_detect(desc, "FOCALIN") |
         str_detect(desc, "INTUNIV") | str_detect(desc, "KAPVAY") | str_detect(desc, "METADATE") |
         str_detect(desc, "METHYLIN") | str_detect(desc, "MYDAYIS") | str_detect(desc, "NUVIGIL") |
         str_detect(desc, "ONYDA") | str_detect(desc, "PROCENTRA") | str_detect(desc, "PROVIGIL") |
         str_detect(desc, "QELBREE") | str_detect(desc, "QUILLICHEW") | str_detect(desc, "QUILLIVANT") |
         str_detect(desc, "RELEXXII")) ~ "pharm_adhd",
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) & 
      (str_detect(desc, "LITHIUM") | str_detect(desc, "LITHOBID")) ~ "pharm_antimania",
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) & str_detect(desc, "MODAFINIL") ~ "pharm_armodafinil",
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) & str_detect(desc, "FLUOCINONIDE") ~ "pharm_antiinflammatory",
    data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy) & str_detect(desc, "PRAMIPEXOLE") ~ "pharm_parkinsons",
    
    TRUE ~ sub_group_pharmacy
  ))

#Check to make sure all drugs have been assigned a sub_group_pharmacy value - this query should return nothing
#if it returns something, then need to add it to the manual recoding above
missing_rx <- rda_value_sets_new_rx %>%
  filter(data_source_type == "pharmacy" & code_set == "NDC" & is.na(sub_group_pharmacy)) %>%
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
    data_source_type == "pharmacy" & sub_group_pharmacy %in% c("pharm_naltrexone_rx") &
      str_detect(desc, "VIVITROL") ~ "sud_opioid", #only injectable naltrexone is used for treating OUD
    data_source_type == "pharmacy" & sub_group_pharmacy %in% c("pharm_naltrexone_rx") & 
        !str_detect(desc, "VIVITROL") ~ "sud_alcohol",
    data_source_type == "pharmacy" & sub_group_pharmacy %in% c("pharm_armodafinil", "pharm_antiinflammatory",
                                                               "pharm_parkinsons") ~ "mh_other",
    TRUE ~ sub_group_condition
  ))

#Verify that all drugs have been assigned to a sub_group_condition, should return 0
rda_value_sets_new_rx %>% filter(data_source_type == "pharmacy" & is.na(sub_group_condition) & code_set == "NDC") %>% count()


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
# Only need to run the below two chunks if value_set_names substantially change between existing RDA and new one
extracodes <- anti_join(rda_value_sets_existing, rda_value_sets_new, by = "code") %>% 
  filter_out(value_set_name == "apde-added-diagnosis" | data_source_type == "billing")

# Update the value sets of the old existing codes to match with what's in the new RDA
# Update the sud_opioid definition for NDC
extracodes_final <- extracodes %>% 
  mutate(value_set_name = case_when(code_set == "NDC" & value_set_group == "sud" ~ "ndcelement",
                                    code_set == "NDC" & value_set_group == "mh" ~ "mh_ndcelement",
                                    code_set %in% c("ICD10CM", "ICD9CM") & value_set_group == "sud" ~ "sudanyqlfyng_icd_9_10dx",
                                    code_set %in% c("ICD10CM", "ICD9CM") & value_set_group == "mh" ~ "mhdiag9_10",
                                    code_set == "ICD9PCS" & value_set_group == "sud" ~ "sud_wthdrwlmngmnt_icdproccodes", 
                                    TRUE ~ value_set_name),
         sub_group_condition = case_when(sub_group_pharmacy == "pharm_naltrexone_rx" & 
                                           !str_detect(desc, "MORPHINE-NALTREXONE") ~ "sud_alcohol",
                                         sub_group_pharmacy == "pharm_naltrexone_rx" & 
                                           str_detect(desc, "MORPHINE-NALTREXONE") ~ "sud_other_substance",
                                         TRUE ~ sub_group_condition))

# Bind all the data tables 
rda_value_sets_updated <- bind_rows(extracodes_final, rda_value_sets_new_rx, extra_idc_final) %>% 
  distinct(across(-desc), .keep_all = TRUE)

#Confirm that no ICD-CM codes per value_set_name have more than 1 row
rda_value_sets_updated %>% filter(code_set %in% c("ICD9CM", "ICD10CM")) %>% group_by(value_set_name, code) %>%
  mutate(row_count = n()) %>% ungroup() %>% count(row_count)

#Confirm that no NDC codes per value_set_name have more than 1 row
rda_value_sets_updated %>% filter(code_set %in% c("NDC")) %>% group_by(value_set_name, code) %>%
  mutate(row_count = n()) %>% ungroup() %>% count(row_count)


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


