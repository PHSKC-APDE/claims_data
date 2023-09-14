#' @title ref.icdcm_codes table update code
#' 
#' 
#' @description Code to run when updating the ref.icdcm_codes table because of new
#' file availability in icd-cm, ccs, ccw, or other relevant codes. Formerly
#' updated ref.dx_lookup
#' 
#' @details How to update:
#' Step 1 ICD cm codes:
#' - Updated each July for October-Sept fiscal year
#' - ICD 9 codes no longer need updating
#' - ICD 10 codes:
#'   - Archive current version of ICD_9_10_CM_Complete in cross sector data
#'   references/icd-cm folder with current date
#'   - download code description in tabular order from
#'   https://www.cms.gov/medicare/icd-10/2023-icd-10-cm (with appropriate year)
#'   and extract files to cross sector data/general/references/icd-cm/icd-10-cm_cms
#'   - Update and use combine_codes.R file to add new unique values to existing
#'   ICD_9_10_CM_Complete file
#'   - Replace ICD_9_10_CM_Complete in the reference-data folder on a new branch, push, and PR
#' Step 2 external cause of injury info:
#' - Check for updates annually - not updated regularly
#' - https://www.cdc.gov/nchs/injury/injury_matrices.htm#:~:text=The%20external%20cause-of-injury,What%20are%20the%20matrices%3F
#' - ICD 10 CM articles (for more information on external cause of injury section)
#' Step 3 CCW:
#'   - Download updated version of 30 CCW here (updated each February):
#'   https://www2.ccwdata.org/web/guest/condition-categories-chronic 
#'   - Archive old version of ccw17_xx in X-sector/CCW
#'   - Use "Algorithms Change History" at the end to revise ccw17_xx sheet from
#'   reference-data
#'   - PR/merge changes to main
#' Step 4 CCS:
#'   - ICD 9 section should not need updating
#'   - ICD 10:
#'     - Each February, check if there is an equivalent new file to
#'     reference-data/blob/main/claims_data/DXCCSR_v2023-1 here:
#'     https://hcup-us.ahrq.gov/toolssoftware/ccsr/dxccsr.jsp under the
#'     "Downloading Information for the Tool and Documentation" header
#'     - Check there are no new broad description categories
#'     - Check there are no new detail codes to add to the catch-all
#' 	   - Check that all rows have CCS information filled in after 4 passes
#' 	   (add more passes or adjust as necessary)
#' Step 5 RDA-defined mental health and substance use disorder:
#'   - Check to see if there's an updated table on HHSAW (claims.ref_rda_value_set_20xx)
#'   - Check for new drug categories or mental health categories
#'   
#'   
#' Update history:
#' 7/26/18 update: Added NYU ED algorithm
#' 10/11/18 updates: 1) Added plain language for some CCS categories, 2) Corrected ICD10-CM external cause tables
#' 10/15/18 update: Added final CCS categories
#' 5/10/19 update: Added all remaining CCW conditions
#' 7/7/2019 update: Added new ICD-CM codes from Medicare and new Medicaid data
#' 7/9/2019 update: Added new ICD-CM-10 codes from APCD extract 159
#' 5/10/2022 update: Overhauled CCW to account for new conditions and algorithms. General tidying.
#' 5/1/2023 update: Overhaul code to update everything to new versions. Eliminate previous steps 4 and 5.




library(car) # used to recode variables


# SET OPTIONS AND BRING IN PACKAGES ----
options(scipen = 6, digits = 4, warning.length = 8170)
origin <- "1970-01-01"

if (!require("pacman")) {install.packages("pacman")}
pacman::p_load(svDialogs, tidyverse, lubridate, odbc, glue, openxlsx, RecordLinkage, phonics, psych)

## Bring in relevant functions ----
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")


# Step 1: CMS ICD-9-CM and ICD-10-CM codes ----
url <- "https://github.com/PHSKC-APDE/reference-data/blob/main/claims_data/ICD_9_10_CM_Complete.xlsx?raw=true"
icd910cm <- read.xlsx(url, sheet = "Sheet1", colNames = T)

icd9cm <- filter(icd910cm, ver == 9)
icd10cm <- filter(icd910cm, ver == 10)

rm(icd910cm)



# Step 2: CDC ICD-CM 9 and ICD-10-CM external cause of injury definitions ----

## Step 2A: Add in CDC ICD-CM 9 and 10 (proposed) external cause of injury information --
url <- "https://github.com/PHSKC-APDE/reference-data/blob/main/claims_data/cdc_external_injury_matrix_icd_9_19_cm.xlsx?raw=true"
ext_cause_910cm <- read.xlsx(url, sheet = "crosswalk_injurymatrix", colNames = T)
ext_cause_910cm <- ext_cause_910cm %>% mutate(version = as.integer(version))


## Step 2B: Merge external cause info for ICD-9-CM --
ext_cause_9cm <- filter(ext_cause_910cm, version == 9)
icd9cm <- left_join(icd9cm, ext_cause_9cm, by = c("icdcode" = "code", "ver" = "version"))


## Step 2C: Merge external cause info for ICD-10-CM --
ext_cause_10cm <- filter(ext_cause_910cm, version == 10)

## Create truncated versions of ICD-10-CM code --
# Improves merge between full list of ICD codes and external cause list
# Full length is 7, based on exploring specificity of data I feel safe creating 6- and 5-digit versions, but no less
ext_cause_10cm <- ext_cause_10cm %>%
  mutate(code_6 = str_sub(code, 1, 6),
         code_5 = str_sub(code, 1, 5))

## Group intent and mechanism by these truncated versions --
# Keeping only those that are distinct
# First 6 digits
ext10cm_6 <- ext_cause_10cm %>%
  group_by(code_6) %>%
  mutate(cnt = n()) %>%
  ungroup() %>%
  filter(., cnt == 1) %>%
  select(., code_6, intent, mechanism, mechanism_full)

# First 5 digits
ext10cm_5 <- ext_cause_10cm %>%
  group_by(code_5) %>%
  mutate(cnt = n()) %>%
  ungroup() %>%
  filter(., cnt == 1) %>%
  select(., code_5, intent, mechanism, mechanism_full)

# Join distinct truncated dx version back to original table
ext_cause_10cm <- left_join(ext_cause_10cm, ext10cm_6, by = "code_6", suffix = c(".x", ".y")) %>%
  mutate(intent = intent.x, 
         mechanism = mechanism.x,
         mechanism_full = mechanism_full.x,
         code_6 = case_when(!is.na(intent.y) ~ code_6,
                            is.na(intent.y) ~ "")) %>%
  select(., code, code_6, code_5, version, intent, mechanism, mechanism_full)

ext_cause_10cm <- left_join(ext_cause_10cm, ext10cm_5, by = "code_5", suffix = c(".x", ".y")) %>%
  mutate(intent = intent.x, 
         mechanism = mechanism.x,
         mechanism_full = mechanism_full.x,
         code_5 = case_when(!is.na(intent.y) ~ code_5,
                            is.na(intent.y) ~ "")) %>%
  select(., code, code_6, code_5, version, intent, mechanism, mechanism_full)

rm(list = ls(pattern = "^ext10cm_"))

## Merge cause framework with ICD-10-CM code list --
icd10cm <- icd10cm %>%
  mutate(icd_6 = str_sub(icdcode, 1, 6),
         icd_5 = str_sub(icdcode, 1, 5))

# Merge on full ICD digits
icd10cm <- left_join(icd10cm, ext_cause_10cm, by = c("icdcode" = "code", "ver" = "version")) %>%
  mutate(intent_final = intent, mechanism_final = mechanism, mechanism_full_final = mechanism_full) %>% 
  select(., -code_6, -code_5, -intent, -mechanism, -mechanism_full)

# Merge on 6 digits and fill in missing info
icd10cm <- left_join(icd10cm, ext_cause_10cm, by = c("icd_6" = "code_6", "ver" = "version"), suffix = c(".x", ".y")) %>%
  mutate(intent_final = case_when(!is.na(intent_final) ~ intent_final,
                                  !is.na(intent) ~ intent),
         mechanism_final = case_when(!is.na(mechanism_final) ~ mechanism_final,
                                     !is.na(mechanism) ~ mechanism),
         mechanism_full_final = case_when(!is.na(mechanism_full_final) ~ mechanism_full_final,
                                          !is.na(mechanism_full) ~ mechanism_full)) %>%
  select(., -code, -code_5, -intent, -mechanism, -mechanism_full)

# Merge on 5 digits and fill in missing info
icd10cm <- left_join(icd10cm, ext_cause_10cm, by = c("icd_5" = "code_5", "ver" = "version"), suffix = c(".x", ".y")) %>%
  mutate(intent_final = case_when(!is.na(intent_final) ~ intent_final,
                                  !is.na(intent) ~ intent),
         mechanism_final = case_when(!is.na(mechanism_final) ~ mechanism_final,
                                     !is.na(mechanism) ~ mechanism),
         mechanism_full_final = case_when(!is.na(mechanism_full_final) ~ mechanism_full_final,
                                          !is.na(mechanism_full) ~ mechanism_full)) %>%
  select(., -code, -code_6, -intent, -mechanism, -mechanism_full)

## Remove final temp columns and rename as needed
icd10cm <- icd10cm %>%
  rename(intent = intent_final, mechanism = mechanism_final, mechanism_full = mechanism_full_final) %>%
  select(-icd_6, -icd_5)

rm(list = ls(pattern = "^ext_cause_"))
rm(ext10cm_5, ext10cm_6)

## QA for completeness of join for ICD-10-CM external cause of injury intent and mechanism
#Refer to National Health Statistics Reports  Number 136  December 30, 2019, Table B for relevant ICD-10-CM ranges

## V codes (transport accidents); result: PASS
## Missing intent/mechanism for e-bike collision with person or animal
## Missing intent/mechanism for other motorcycle driver collision with person or animal
## Missing intent/mechanism for other motorcycle passenger collision with person or animal
## Missing occasional 5-digit ICD-10-CM code when this is insufficient to specify only 1 intent and/or mechanism
# View(filter(icd10cm, str_detect(icdcode, "^V") & ver == 10 & str_length(icdcode) >= 5) %>% arrange(icdcode))
# all(!is.na(filter(icd10cm, str_detect(icdcode, "^V") & ver == 10 & str_length(icdcode) >= 6)[['dx_description']]))

## W codes (Other external causes of accidental injury); result: PASS
## Missing intent/mechanism for injury between moving and stationary object
# View(filter(icd10cm, str_detect(icdcode, "^W") & ver == 10 & str_length(icdcode) >= 5) %>% arrange(icdcode))
# all(!is.na(filter(icd10cm, str_detect(icdcode, "^W") & ver == 10 & str_length(icdcode) >= 6)[['dx_description']]))

## X codes (Other external causes of accidental injury, self-harm, assault); result: PASS
# View(filter(icd10cm, str_detect(icdcode, "^X") & ver == 10 & str_length(icdcode) >= 5) %>% arrange(icdcode))
# all(!is.na(filter(icd10cm, str_detect(icdcode, "^X") & ver == 10 & str_length(icdcode) >= 6)[['dx_description']]))

## Y codes (legal, war, undetermined intent; result: PASS
# View(filter(icd10cm, str_detect(icdcode, "^Y") & ver == 10 & str_length(icdcode) >= 5) %>% arrange(icdcode))
# all(!is.na(filter(icd10cm, str_detect(icdcode, "^Y") & ver == 10 & str_length(icdcode) >= 6)[['dx_description']]))

## Y codes (legal, war, undetermined intent; result: PASS
# View(filter(icd10cm, str_detect(icdcode, "^T") & ver == 10 & str_length(icdcode) >= 5) %>% arrange(icdcode))
# all(!is.na(filter(icd10cm, str_detect(icdcode, "^T") & ver == 10 & str_length(icdcode) >= 6)[['dx_description']]))


# Step 3: CMS Chronic Condition Warehouse definitions ----
# Bring in CCW lookup
ccw_99_16 <- read.xlsx("https://github.com/PHSKC-APDE/reference-data/blob/main/claims_data/ccw_lookup.xlsx?raw=true", 
                       sheet = "ccw99_16")
ccw_17_xx <- read.xlsx("https://github.com/PHSKC-APDE/reference-data/blob/main/claims_data/ccw_lookup.xlsx?raw=true", 
                       sheet = "ccw17_xx")

ccw <- bind_rows(ccw_99_16, ccw_17_xx) %>% mutate(link = 1)

## Drop vars that are not needed
ccw <- select(ccw, -ccw_code)

## Create CCW condition flags (NA or 1)
ccw <- pivot_wider(ccw, names_from = ccw_abbrev, values_from = link, names_prefix = "ccw_")

## QA - check to make sure no diagnosis codes have more than 1 row
ccw %>%
  group_by(dx) %>%
  mutate(row_count = n()) %>%
  ungroup() %>%
  count(row_count)

## QA - make sure each dx row has at least 1 CCW condition associated with it
ccw_qa <- ccw %>% mutate(chk = rowSums(select(., starts_with("ccw_")), na.rm = T)) %>% count(chk)

if (is.na(min(ccw_qa$chk)) | min(ccw_qa$chk, na.rm = T) == 0) {
  stop("Some DX codes in the CCW table are not associated with any CCW condition")
}

rm(ccw_qa)

## Join to ICD codes --
icd9cm <- left_join(icd9cm, ccw, by = c("icdcode" = "dx", "ver" = "ver"))
icd10cm <- left_join(icd10cm, ccw, by = c("icdcode" = "dx", "ver" = "ver"))

rm(ccw)



# Step 4: Clinical classifications software (CCS) from AHRQ ----

## Step 4A: Read in CCS reference table for ICD-9-CM

ccs_9_raw <- read.xlsx("https://github.com/PHSKC-APDE/reference-data/blob/main/claims_data/ccs_icd9cm.xlsx?raw=true", 
                       sheet = "ccs_icdcm")


##Create broad and detailed CCS categories
#Following document maps CCS ICD-9-CM to CCSR ICD-10-CM for creation of broad categories
#https://kc1.sharepoint.com/:x:/r/teams/DPH-KCCross-SectorData/Shared%20Documents/General/References/ClinicalClassificationsSoftware/CCS_ICD10CM/comparing_high_level_ccs9to10.xlsx?d=w35680f9a2c2f446f8aa9c7cbd38cf2d1&csf=1&web=1&e=jwnDmL

ccs_9_simple <- ccs_9_raw %>%
  mutate(
    
    #CCS broad description using CCSR ICD-10-CM terminology
    ccs_broad_desc = case_when(
      multiccs_lv1 == "1" ~ "Certain infectious and parasitic diseases",
      multiccs_lv1 == "2" ~ "Neoplasms",
      multiccs_lv2 %in% c("3.1", "3.2", "3.3", "3.4", "3.5", "3.6", "3.7", "3.8", "3.9", "3.11") ~
        "Endocrine, nutritional and metabolic diseases",
      multiccs_lv2 == "3.10" | multiccs_lv1 == "4" ~
        "Diseases of the blood and blood-forming organs and certain disorders involving the immune mechanism",
      multiccs_lv1 == "5" ~ "Mental, behavioral and neurodevelopmental disorders",
      multiccs_lv2 %in% c("6.1", "6.2", "6.3", "6.4", "6.5", "6.6", "6.9") ~ "Diseases of the nervous system",
      multiccs_lv2 == "6.7" ~ "Diseases of the eye and adnexa",
      multiccs_lv2 == "6.8" ~ "Diseases of the ear and mastoid process",
      multiccs_lv1 == "7" ~ "Diseases of the circulatory system",
      multiccs_lv1 == "8" ~ "Diseases of the respiratory system",
      multiccs_lv2 %in% c("9.1", "9.3", "9.4", "9.5", "9.6", "9.7", "9.8", "9.9", "9.10", "9.11", "9.12") ~
        "Diseases of the digestive system",
      multiccs_lv2 == "9.2" ~ "Dental diseases",
      multiccs_lv1 == "10" ~ "Diseases of the genitourinary system",
      multiccs_lv1 == "11" ~ "Pregnancy, childbirth and the puerperium",
      multiccs_lv1 == "12" ~ "Diseases of the skin and subcutaneous tissue",
      multiccs_lv1 == "13" ~ "Diseases of the musculoskeletal system and connective tissue",
      multiccs_lv1 == "14" ~ "Congenital malformations, deformations and chromosomal abnormalities",
      multiccs_lv1 == "15" ~ "Certain conditions originating in the perinatal period",
      multiccs_lv1 == "16" ~ "Injury, poisoning and certain other consequences of external causes",
      multiccs_lv2 == "17.1" ~ "Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified",
      multiccs_lv2 == "17.2" ~ "Factors influencing health status and contact with health services",
      ccs == "259" ~ "_UNCLASSIFIED",
      str_detect(ccs, "^26") & multiccs_lv1 == "18" ~ "External causes of morbidity",
      TRUE ~ NA_character_),
    
    #CCS broad code using CCSR ICD-10-CM terminology
    ccs_broad_code = case_when(
      multiccs_lv1 == "1" ~ "INF",
      multiccs_lv1 == "2" ~ "NEO",
      multiccs_lv2 %in% c("3.1", "3.2", "3.3", "3.4", "3.5", "3.6", "3.7", "3.8", "3.9", "3.11") ~ "END",
      multiccs_lv2 == "3.10" | multiccs_lv1 == "4" ~ "BLD",
      multiccs_lv1 == "5" ~ "MBD",
      multiccs_lv2 %in% c("6.1", "6.2", "6.3", "6.4", "6.5", "6.6", "6.9") ~ "NVS",
      multiccs_lv2 == "6.7" ~ "EAR",
      multiccs_lv2 == "6.8" ~ "EYE",
      multiccs_lv1 == "7" ~ "CIR",
      multiccs_lv1 == "8" ~ "RSP",
      multiccs_lv2 %in% c("9.1", "9.3", "9.4", "9.5", "9.6", "9.7", "9.8", "9.9", "9.10", "9.11", "9.12") ~ "DIG",
      multiccs_lv2 == "9.2" ~ "DEN",
      multiccs_lv1 == "10" ~ "GEN",
      multiccs_lv1 == "11" ~ "PRG",
      multiccs_lv1 == "12" ~ "SKN",
      multiccs_lv1 == "13" ~ "MUS",
      multiccs_lv1 == "14" ~ "MAL",
      multiccs_lv1 == "15" ~ "PNL",
      multiccs_lv1 == "16" ~ "INJ",
      multiccs_lv2 == "17.1" ~ "SYM",
      multiccs_lv2 == "17.2" ~ "FAC",
      ccs == "259" ~ "_UNCLASSIFIED",
      str_detect(ccs, "^26") & multiccs_lv1 == "18" ~ "EXT",
      TRUE ~ NA_character_),
    
    #CCS detail description using CCS ICD-9-CM terminology
    ccs_detail_desc = case_when(
      multiccs_lv2 %in% c("2.2", "2.11", "16.10", "17.1", "17.2") ~
        str_trim(str_replace_all(multiccs_lv3_description, "\\[([^\\[\\]]+)\\]", "")),
      ccs == "259" ~ "_UNCLASSIFIED",
      str_detect(ccs, "^26") & multiccs_lv1 == "18" ~ "External causes of morbidity",
      TRUE ~ str_trim(str_replace_all(multiccs_lv2_description, "\\[([^\\[\\]]+)\\]", ""))
    ),
    
    #CCS detail code using CCS ICD-9-CM codes
    ccs_detail_code = case_when(
      multiccs_lv2 %in% c("2.2", "2.11", "16.10", "17.1", "17.2") ~ multiccs_lv3,
      ccs == "259" ~ as.character(ccs),
      str_detect(ccs, "^26") & multiccs_lv1 == "18" ~ as.character(ccs),
      TRUE ~ multiccs_lv2
    ))

##Clean up ccs_detail_desc to use plainer language for certain conditions
#Also add a ccs_catch_all variable to flag cause categories that are too miscellaneous/broad to be useful for reporting leading causes
ccs_9_simple <- ccs_9_simple %>%
  mutate(ccs_detail_desc = case_when(
    ccs_detail_code == "1.2" ~ "Fungal infections",
    ccs_detail_code == "10.1" ~ "Urinary system disease",
    ccs_detail_code == "10.2" ~ "Male reproductive system disease",
    ccs_detail_code == "10.3" ~ "Female reproductive system disease",
    ccs_detail_code == "11.1" ~ "Birth control management",
    ccs_detail_code %in% c("11.2", "11.3", "11.4", "11.5", "11.6") ~ "Pregnancy/childbirth complications",
    ccs_detail_code == "11.7" ~ "Normal pregnancy and/or delivery",
    ccs_detail_code == "12.1" ~ "Skin infections",
    ccs_detail_code == "13.2" ~ "Joint disorders (e.g., arthritis)",
    ccs_detail_code == "13.3" ~ "Spine and back disorders",
    ccs_detail_code == "15.1" ~ "Birth of child",
    ccs_detail_code == "16.2" ~ "Broken bones",
    ccs_detail_code == "16.8" ~ "Minor injuries (e.g., bruise)",
    ccs_detail_code == "246" ~ "Fever of unknown cause",
    ccs_detail_code == "5.11" ~ "Alcohol use disorders",
    ccs_detail_code == "5.12" ~ "Substance use disorders",
    ccs_detail_code == "5.13" ~ "Suicide and self-harm",
    ccs_detail_code == "5.14" ~ "Mental health/SUD screening",
    ccs_detail_code == "6.4" ~ "Seizure disorders",
    ccs_detail_code == "6.5" ~ "Headache",
    ccs_detail_code == "7.2" ~ "Heart disease",
    ccs_detail_code == "8.2" ~ "Chronic obstructive pulmonary disease",
    ccs_detail_code == "8.6" ~ "Failure of the respiratory system",
    ccs_detail_code == "9.10" ~ "Bleeding in the stomach/intestines",
    ccs_detail_code == "9.11" ~ "Non-infectious inflammation of the stomach/intestines",
    ccs_detail_code == "9.12" ~ "Other disorders of the stomach/intestines",
    ccs_detail_code == "9.2" ~ "Dental disease",
    TRUE ~ ccs_detail_desc
  ),
  
  #Create ccs_catch_all variable
  ccs_catch_all = case_when(
    ccs_detail_code %in% c("1.4","11.6","12.2","12.4","13.8","13.9","14.5","15.7","16.12",
                           "2.14","20","258","259","2618","2619","2620","3.11","3.4","4.4",
                           "5.15","5.6","5.7","6.9","8.8","8.9","9.12") ~ 1L,
    TRUE ~ 0L))

##Collapse to required variables only
ccs_9_simple <- select(ccs_9_simple, icdcode, ccs_broad_desc:ccs_catch_all) %>% distinct()


## Step 4B: Read in CCS reference table for ICD-10-CM

ccs_10_raw <- read_csv("https://github.com/PHSKC-APDE/reference-data/blob/main/claims_data/DXCCSR_v2023-1.csv?raw=true", 
                       col_select = c(1,7,8)) %>%
  
  #rename variables
  rename(
    icdcode = `'ICD-10-CM CODE'`,
    ccs_detail_code = `'CCSR CATEGORY 1'`,
    ccs_detail_desc = `'CCSR CATEGORY 1 DESCRIPTION'`
  ) %>%
  
  #remove punctuation
  mutate(
    icdcode = str_remove_all(icdcode, "[:punct:]"),
    ccs_detail_code = str_remove_all(ccs_detail_code, "[:punct:]")) %>%
  
  #create ccs_broad_code
  mutate(ccs_broad_code = str_sub(ccs_detail_code, 1, 3)) %>%
  select(icdcode, ccs_broad_code, ccs_detail_desc, ccs_detail_code)

##Add broad CCS description categories
ccs_10_simple <- ccs_10_raw %>%
  mutate(
    ccs_broad_desc = case_when(
      ccs_broad_code == "BLD" ~ "Diseases of the blood and blood-forming organs and certain disorders involving the immune mechanism",
      ccs_broad_code == "CIR" ~ "Diseases of the circulatory system",
      ccs_broad_code == "DEN" ~ "Dental diseases",
      ccs_broad_code == "DIG" ~ "Diseases of the digestive system",
      ccs_broad_code == "EAR" ~ "Diseases of the ear and mastoid process",
      ccs_broad_code == "END" ~ "Endocrine, nutritional and metabolic diseases",
      ccs_broad_code == "EXT" ~ "External causes of morbidity",
      ccs_broad_code == "EYE" ~ "Diseases of the eye and adnexa",
      ccs_broad_code == "FAC" ~ "Factors influencing health status and contact with health services",
      ccs_broad_code == "GEN" ~ "Diseases of the genitourinary system",
      ccs_broad_code == "INF" ~ "Certain infectious and parasitic diseases",
      ccs_broad_code == "INJ" ~ "Injury, poisoning and certain other consequences of external causes",
      ccs_broad_code == "MAL" ~ "Congenital malformations, deformations and chromosomal abnormalities",
      ccs_broad_code == "MBD" ~ "Mental, behavioral and neurodevelopmental disorders",
      ccs_broad_code == "MUS" ~ "Diseases of the musculoskeletal system and connective tissue",
      ccs_broad_code == "NEO" ~ "Neoplasms",
      ccs_broad_code == "NVS" ~ "Diseases of the nervous system",
      ccs_broad_code == "PNL" ~ "Certain conditions originating in the perinatal period",
      ccs_broad_code == "PRG" ~ "Pregnancy, childbirth and the puerperium",
      ccs_broad_code == "RSP" ~ "Diseases of the respiratory system",
      ccs_broad_code == "SKN" ~ "Diseases of the skin and subcutaneous tissue",
      ccs_broad_code == "SYM" ~ "Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified"
    ))

##Add ccs_catch_all variable
ccs_10_simple <- ccs_10_simple %>%
  mutate(ccs_catch_all = case_when(
    ccs_detail_code %in% c("INF009", "PRG028", "SKN002", "SKN007", "MUS025", "MUS028", "MAL010", "PNL013",
                           "INJ026", "INJ027", "INJ063", "INJ064", "INJ073", "INJ074", "INJ076",
                           "NEO021", "NEO028", "NEO071", "NEO072", "FAC010", "EXT018", "EXT019",
                           "END015", "END016", "BLD010", "NEO069", "MBD013", "NVS006", "RSP006",
                           "RSP007", "RSP016", "DIG025") ~ 1L,
    TRUE ~ 0L)) %>%
  select(icdcode, ccs_broad_desc, ccs_broad_code, ccs_detail_desc, ccs_detail_code, ccs_catch_all)


## Step 4C: Join to ICD-9-CM codes --
icd9cm <- left_join(icd9cm, ccs_9_simple, by = c("icdcode" = "icdcode"))
#y <- filter(icd9cm, is.na(ccs_broad_desc))
#clipr::write_clip(y)

#Fill in missing CCS information if first 3 digits of ICD-9-CM code matches with row below or above
#Intentionally have prioritized row below over row above
icd9cm <- icd9cm %>%
  mutate(
    ccs_broad_desc = case_when(
      !is.na(ccs_broad_desc) ~ ccs_broad_desc,
      str_sub(icdcode,1,3) == str_sub(lead(icdcode, 1, order_by = icdcode),1,3) ~ lead(ccs_broad_desc, 1, order_by = icdcode),
      str_sub(icdcode,1,3) == str_sub(lag(icdcode, 1, order_by = icdcode),1,3) ~ lag(ccs_broad_desc, 1, order_by = icdcode),
      TRUE ~ NA_character_),
    ccs_broad_code = case_when(
      !is.na(ccs_broad_code) ~ ccs_broad_code,
      str_sub(icdcode,1,3) == str_sub(lead(icdcode, 1, order_by = icdcode),1,3) ~ lead(ccs_broad_code, 1, order_by = icdcode),
      str_sub(icdcode,1,3) == str_sub(lag(icdcode, 1, order_by = icdcode),1,3) ~ lag(ccs_broad_code, 1, order_by = icdcode),
      TRUE ~ NA_character_),
    ccs_detail_desc = case_when(
      !is.na(ccs_detail_desc) ~ ccs_detail_desc,
      str_sub(icdcode,1,3) == str_sub(lead(icdcode, 1, order_by = icdcode),1,3) ~ lead(ccs_detail_desc, 1, order_by = icdcode),
      str_sub(icdcode,1,3) == str_sub(lag(icdcode, 1, order_by = icdcode),1,3) ~ lag(ccs_detail_desc, 1, order_by = icdcode),
      TRUE ~ NA_character_),
    ccs_detail_code = case_when(
      !is.na(ccs_detail_code) ~ ccs_detail_code,
      str_sub(icdcode,1,3) == str_sub(lead(icdcode, 1, order_by = icdcode),1,3) ~ lead(ccs_detail_code, 1, order_by = icdcode),
      str_sub(icdcode,1,3) == str_sub(lag(icdcode, 1, order_by = icdcode),1,3) ~ lag(ccs_detail_code, 1, order_by = icdcode),
      TRUE ~ NA_character_),
    ccs_catch_all = case_when(
      !is.na(ccs_catch_all) ~ ccs_catch_all,
      str_sub(icdcode,1,3) == str_sub(lead(icdcode, 1, order_by = icdcode),1,3) ~ lead(ccs_catch_all, 1, order_by = icdcode),
      str_sub(icdcode,1,3) == str_sub(lag(icdcode, 1, order_by = icdcode),1,3) ~ lag(ccs_catch_all, 1, order_by = icdcode),
      TRUE ~ NA_integer_)
  )

#Check to make sure all rows now have CCS information filled in
count(filter(icd9cm, is.na(ccs_broad_desc)))


## Step 4D: Join to ICD-10-CM codes --
icd10cm <- left_join(icd10cm, ccs_10_simple, by = c("icdcode" = "icdcode"))
#y <- filter(icd10cm, is.na(ccs_broad_desc))
#clipr::write_clip(y)

#Use while loop to fill in missing CCS information until all ICD-10-CM codes are categorized
icd10cm_na_ccs_count <- count(filter(icd10cm, is.na(ccs_broad_desc)))$n
loop_pass <- 1

while (icd10cm_na_ccs_count > 0) {
  
  #Info to print for each loop
  print(paste0("Fill in CCS info if first 6,5,4 or 3 digits of ICD-10-CM code matches row above/below; iteration number: ", loop_pass))
  print(paste0("Number of ICD-10-CM codes missing a CCS description: ", icd10cm_na_ccs_count))
  
  #Code to run for each loop
  icd10cm <- icd10cm %>%
    mutate(
      ccs_broad_desc = case_when(
        !is.na(ccs_broad_desc) ~ ccs_broad_desc,
        str_sub(icdcode,1,6) == str_sub(lead(icdcode, 1, order_by = icdcode),1,6) ~ lead(ccs_broad_desc, 1, order_by = icdcode),
        str_sub(icdcode,1,6) == str_sub(lag(icdcode, 1, order_by = icdcode),1,6) ~ lag(ccs_broad_desc, 1, order_by = icdcode),
        str_sub(icdcode,1,5) == str_sub(lead(icdcode, 1, order_by = icdcode),1,5) ~ lead(ccs_broad_desc, 1, order_by = icdcode),
        str_sub(icdcode,1,5) == str_sub(lag(icdcode, 1, order_by = icdcode),1,5) ~ lag(ccs_broad_desc, 1, order_by = icdcode),
        str_sub(icdcode,1,4) == str_sub(lead(icdcode, 1, order_by = icdcode),1,4) ~ lead(ccs_broad_desc, 1, order_by = icdcode),
        str_sub(icdcode,1,4) == str_sub(lag(icdcode, 1, order_by = icdcode),1,4) ~ lag(ccs_broad_desc, 1, order_by = icdcode),
        str_sub(icdcode,1,3) == str_sub(lead(icdcode, 1, order_by = icdcode),1,3) ~ lead(ccs_broad_desc, 1, order_by = icdcode),
        str_sub(icdcode,1,3) == str_sub(lag(icdcode, 1, order_by = icdcode),1,3) ~ lag(ccs_broad_desc, 1, order_by = icdcode),
        TRUE ~ NA_character_),
      ccs_broad_code = case_when(
        !is.na(ccs_broad_code) ~ ccs_broad_code,
        str_sub(icdcode,1,6) == str_sub(lead(icdcode, 1, order_by = icdcode),1,6) ~ lead(ccs_broad_code, 1, order_by = icdcode),
        str_sub(icdcode,1,6) == str_sub(lag(icdcode, 1, order_by = icdcode),1,6) ~ lag(ccs_broad_code, 1, order_by = icdcode),
        str_sub(icdcode,1,5) == str_sub(lead(icdcode, 1, order_by = icdcode),1,5) ~ lead(ccs_broad_code, 1, order_by = icdcode),
        str_sub(icdcode,1,5) == str_sub(lag(icdcode, 1, order_by = icdcode),1,5) ~ lag(ccs_broad_code, 1, order_by = icdcode),
        str_sub(icdcode,1,4) == str_sub(lead(icdcode, 1, order_by = icdcode),1,4) ~ lead(ccs_broad_code, 1, order_by = icdcode),
        str_sub(icdcode,1,4) == str_sub(lag(icdcode, 1, order_by = icdcode),1,4) ~ lag(ccs_broad_code, 1, order_by = icdcode),
        str_sub(icdcode,1,3) == str_sub(lead(icdcode, 1, order_by = icdcode),1,3) ~ lead(ccs_broad_code, 1, order_by = icdcode),
        str_sub(icdcode,1,3) == str_sub(lag(icdcode, 1, order_by = icdcode),1,3) ~ lag(ccs_broad_code, 1, order_by = icdcode),
        TRUE ~ NA_character_),
      ccs_detail_desc = case_when(
        !is.na(ccs_detail_desc) ~ ccs_detail_desc,
        str_sub(icdcode,1,6) == str_sub(lead(icdcode, 1, order_by = icdcode),1,6) ~ lead(ccs_detail_desc, 1, order_by = icdcode),
        str_sub(icdcode,1,6) == str_sub(lag(icdcode, 1, order_by = icdcode),1,6) ~ lag(ccs_detail_desc, 1, order_by = icdcode),
        str_sub(icdcode,1,5) == str_sub(lead(icdcode, 1, order_by = icdcode),1,5) ~ lead(ccs_detail_desc, 1, order_by = icdcode),
        str_sub(icdcode,1,5) == str_sub(lag(icdcode, 1, order_by = icdcode),1,5) ~ lag(ccs_detail_desc, 1, order_by = icdcode),
        str_sub(icdcode,1,4) == str_sub(lead(icdcode, 1, order_by = icdcode),1,4) ~ lead(ccs_detail_desc, 1, order_by = icdcode),
        str_sub(icdcode,1,4) == str_sub(lag(icdcode, 1, order_by = icdcode),1,4) ~ lag(ccs_detail_desc, 1, order_by = icdcode),
        str_sub(icdcode,1,3) == str_sub(lead(icdcode, 1, order_by = icdcode),1,3) ~ lead(ccs_detail_desc, 1, order_by = icdcode),
        str_sub(icdcode,1,3) == str_sub(lag(icdcode, 1, order_by = icdcode),1,3) ~ lag(ccs_detail_desc, 1, order_by = icdcode),
        TRUE ~ NA_character_),
      ccs_detail_code = case_when(
        !is.na(ccs_detail_code) ~ ccs_detail_code,
        str_sub(icdcode,1,6) == str_sub(lead(icdcode, 1, order_by = icdcode),1,6) ~ lead(ccs_detail_code, 1, order_by = icdcode),
        str_sub(icdcode,1,6) == str_sub(lag(icdcode, 1, order_by = icdcode),1,6) ~ lag(ccs_detail_code, 1, order_by = icdcode),
        str_sub(icdcode,1,5) == str_sub(lead(icdcode, 1, order_by = icdcode),1,5) ~ lead(ccs_detail_code, 1, order_by = icdcode),
        str_sub(icdcode,1,5) == str_sub(lag(icdcode, 1, order_by = icdcode),1,5) ~ lag(ccs_detail_code, 1, order_by = icdcode),
        str_sub(icdcode,1,4) == str_sub(lead(icdcode, 1, order_by = icdcode),1,4) ~ lead(ccs_detail_code, 1, order_by = icdcode),
        str_sub(icdcode,1,4) == str_sub(lag(icdcode, 1, order_by = icdcode),1,4) ~ lag(ccs_detail_code, 1, order_by = icdcode),
        str_sub(icdcode,1,3) == str_sub(lead(icdcode, 1, order_by = icdcode),1,3) ~ lead(ccs_detail_code, 1, order_by = icdcode),
        str_sub(icdcode,1,3) == str_sub(lag(icdcode, 1, order_by = icdcode),1,3) ~ lag(ccs_detail_code, 1, order_by = icdcode),
        TRUE ~ NA_character_),
      ccs_catch_all = case_when(
        !is.na(ccs_catch_all) ~ ccs_catch_all,
        str_sub(icdcode,1,6) == str_sub(lead(icdcode, 1, order_by = icdcode),1,6) ~ lead(ccs_catch_all, 1, order_by = icdcode),
        str_sub(icdcode,1,6) == str_sub(lag(icdcode, 1, order_by = icdcode),1,6) ~ lag(ccs_catch_all, 1, order_by = icdcode),
        str_sub(icdcode,1,5) == str_sub(lead(icdcode, 1, order_by = icdcode),1,5) ~ lead(ccs_catch_all, 1, order_by = icdcode),
        str_sub(icdcode,1,5) == str_sub(lag(icdcode, 1, order_by = icdcode),1,5) ~ lag(ccs_catch_all, 1, order_by = icdcode),
        str_sub(icdcode,1,4) == str_sub(lead(icdcode, 1, order_by = icdcode),1,4) ~ lead(ccs_catch_all, 1, order_by = icdcode),
        str_sub(icdcode,1,4) == str_sub(lag(icdcode, 1, order_by = icdcode),1,4) ~ lag(ccs_catch_all, 1, order_by = icdcode),
        str_sub(icdcode,1,3) == str_sub(lead(icdcode, 1, order_by = icdcode),1,3) ~ lead(ccs_catch_all, 1, order_by = icdcode),
        str_sub(icdcode,1,3) == str_sub(lag(icdcode, 1, order_by = icdcode),1,3) ~ lag(ccs_catch_all, 1, order_by = icdcode),
        TRUE ~ NA_integer_)
    )
  
  #Advance indices
  icd10cm_na_ccs_count <- count(filter(icd10cm, is.na(ccs_broad_desc)))$n
  loop_pass <- loop_pass + 1
}

# QA : Confirm all rows now have CCS information filled in by while loop above
count(filter(icd10cm, is.na(ccs_broad_desc)))
#View(filter(icd10cm, is.na(ccs_broad_desc)))

# Clean up
rm(ccs_10_raw, ccs_10_simple, ccs_9_raw, ccs_9_simple, icd10cm_na_ccs_count, loop_pass)


# Step 5: RDA-defined Mental Health and Substance User Disorder-related diagnoses ----

## Connect to HHSAW using ODBC driver
db_hhsaw <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433",
                           database = "hhs_analytics_workspace",
                           uid = keyring::key_list("hhsaw")[["username"]],
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")

## Pull in RDA measure value set from HHSAW
ref.rda_value_set <- dbGetQuery(conn = db_hhsaw,
                                "SELECT *
  FROM [ref].[rda_value_sets_apde]
  where code_set in ('ICD9CM', 'ICD10CM')
  order by code_set, code;")

## Separate by era and collapse to distinct rows at sub_group_condition level
rda_icd9cm <- filter(ref.rda_value_set, icdcm_version == 9) %>%
  distinct(code, sub_group_condition) %>%
  mutate(flag = 1L)

rda_icd10cm <- filter(ref.rda_value_set, icdcm_version == 10) %>%
  distinct(code, sub_group_condition) %>%
  mutate(flag = 1L)

## ICD-9-CM: Pivot data frames to wide to create flags for each condition
rda_icd9cm <- pivot_wider(rda_icd9cm, names_from = sub_group_condition, values_from = flag)

## ICD-9-CM Create summary flags
rda_icd9cm <- rda_icd9cm %>%
  mutate(
    mh_any = coalesce(mh_adhd, mh_adjustment, mh_anxiety, mh_depression, mh_disrupt, mh_mania_bipolar, mh_psychotic),
    sud_any = coalesce(sud_alcohol, sud_cannabis, sud_cocaine, sud_hallucinogen, sud_opioid, sud_sedative,
                       sud_other_stimulant, sud_other_substance),
    bh_any = coalesce(mh_any, sud_any)
  ) %>%
  relocate(code, bh_any, mh_any, sud_any,
           mh_adhd, mh_adjustment, mh_anxiety, mh_depression, mh_disrupt, mh_mania_bipolar, mh_psychotic,
           sud_alcohol, sud_cannabis, sud_cocaine, sud_hallucinogen, sud_opioid, sud_sedative,
           sud_other_stimulant, sud_other_substance)


## ICD-10-CM: Pivot data frames to wide to create flags for each condition
rda_icd10cm <- pivot_wider(rda_icd10cm, names_from = sub_group_condition, values_from = flag)

## ICD-10-CM Create summary flags
rda_icd10cm <- rda_icd10cm %>%
  mutate(
    mh_any = coalesce(mh_adhd, mh_adjustment, mh_anxiety, mh_depression, mh_disrupt, mh_mania_bipolar, mh_psychotic),
    sud_any = coalesce(sud_alcohol, sud_cannabis, sud_cocaine, sud_hallucinogen, sud_inhalant, sud_opioid, sud_sedative,
                       sud_other_stimulant, sud_other_substance),
    bh_any = coalesce(mh_any, sud_any)
  ) %>%
  relocate(code, bh_any, mh_any, sud_any,
           mh_adhd, mh_adjustment, mh_anxiety, mh_depression, mh_disrupt, mh_mania_bipolar, mh_psychotic,
           sud_alcohol, sud_cannabis, sud_cocaine, sud_hallucinogen, sud_inhalant, sud_opioid, sud_sedative,
           sud_other_stimulant, sud_other_substance)

## QA
## Check that bh_any is filled for every code
all(!is.na(rda_icd9cm$bh_any))
all(!is.na(rda_icd10cm$bh_any))

## Join to ICD-9-CM codes
icd9cm <- left_join(icd9cm, rda_icd9cm, by = c("icdcode" = "code"))

## Join to ICD-10-CM codes
icd10cm <- left_join(icd10cm, rda_icd10cm, by = c("icdcode" = "code"))

## Clean up
rm(rda_icd10cm, rda_icd9cm, ref.rda_value_set)



# Step 6: Bind ICD-9-CM and ICD-10-CM information ----
icd910cm <- bind_rows(icd10cm, icd9cm) %>% arrange(ver, icdcode)
rm(icd9cm, icd10cm)

# Normalize variable names with other Medicaid claims data tables
icd910cm <- icd910cm %>%
  rename(icdcm = icdcode, icdcm_version = ver, icdcm_description = dx_description)

# Pull out all CCW names and sort (ensures any new CCW conditions are caught)
ccw_cols <- icd910cm %>% select(starts_with("ccw_")) %>% names() %>% sort()

#Create last_run variable and order variables for final table upload
icd910cm <- icd910cm %>%
  mutate(last_run = Sys.time()) %>%
  select(
    icdcm:icdcm_description,
    ccs_broad_desc:ccs_catch_all,
    all_of(ccw_cols),
    bh_any:sud_other_substance,
    intent:mechanism_full,
    last_run
  ) %>%
  # Remove any duplicates
  distinct()


# QA: Compare distinct ICD-9-CM and ICD-10-CM codes from CHARS, Medicaid, APCD
# to see if there are any that do not join
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")
db_hhsaw <- create_db_connection("hhsaw", interactive = F, prod = T)
db_phclaims <- create_db_connection("phclaims", interactive = F, prod = T)
db_apde <- create_db_connection("APDEStore", interactive = F, prod = T)

mcaid_schema <- "claims"
mcaid_tbl <- "final_mcaid_claim_icdcm_header"
apcd_schema <- "final"
apcd_tbl <- "apcd_claim_icdcm_header"
chars_schema <- "chars"
chars_tbl1 <- "stage_diag"
chars_tbl2 <- "stage_ecode"

mcaid <- DBI::dbGetQuery(db_hhsaw, glue::glue_sql(
  "SELECT DISTINCT icdcm_norm, icdcm_version
   FROM {`mcaid_schema`}.{`mcaid_tbl`}",
  .con = db_hhsaw))
apcd <- DBI::dbGetQuery(db_phclaims, glue::glue_sql(
  "SELECT DISTINCT icdcm_norm, icdcm_version
   FROM {`apcd_schema`}.{`apcd_tbl`}",
  .con = db_phclaims))
chars1 <- DBI::dbGetQuery(db_apde, glue::glue_sql(
  "SELECT DISTINCT DIAG, code_ver
   FROM {`chars_schema`}.{`chars_tbl1`}",
  .con = db_apde))
chars2 <- DBI::dbGetQuery(db_apde, glue::glue_sql(
  "SELECT DISTINCT ECODE, code_ver
   FROM {`chars_schema`}.{`chars_tbl2`}",
  .con = db_apde))

# standardize variable names
chars1 <- rename(chars1, icdcm_norm = DIAG, icdcm_version = code_ver)
chars2 <- rename(chars2, icdcm_norm = ECODE, icdcm_version = code_ver)
chars <- unique(do.call("rbind", list(chars1, chars2))[,c("icdcm_norm", "icdcm_version")])

icd9_codes <- unique(icd910cm[icd910cm$icdcm_version == 9,]$icdcm)
icd10_codes <- unique(icd910cm[icd910cm$icdcm_version == 10,]$icdcm)

# differences for each data source
length(setdiff(mcaid[mcaid$icdcm_version == 9,]$icdcm_norm, icd9_codes))  # 2
length(setdiff(mcaid[mcaid$icdcm_version == 10,]$icdcm_norm, icd10_codes))  # 26
length(setdiff(apcd[apcd$icdcm_version == 9,]$icdcm_norm, icd9_codes))  # 0
length(setdiff(apcd[apcd$icdcm_version == 10,]$icdcm_norm, icd10_codes))  # 390
length(setdiff(chars[chars$icdcm_version == 9,]$icdcm_norm, icd9_codes))  # 236
length(setdiff(chars[chars$icdcm_version == 10,]$icdcm_norm, icd10_codes))  # 351


# Step 7: Upload reference table to SQL Server ----
## Set up server connection ----
server <- dlg_list(c("phclaims", "hhsaw", "both"), title = "Select Server.")$res
if(server == "hhsaw" | server == "both") {
  interactive_auth <- dlg_list(c("TRUE", "FALSE"), title = "Interactive Authentication for HHSAW?")$res
  prod <- dlg_list(c("TRUE", "FALSE"), title = "Production Server for HHSAW?")$res
} else {
  interactive_auth <- T  
  prod <- T
}
to_schema <- "ref"
to_table <- "icdcm_codes"

# If wanting to load table to both servers, do HHSAW first
# Make connection
if (server == "hhsaw" | server == "both"){
  db_claims <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)
  
  # Load data
  dbWriteTable(db_claims, name = DBI::Id(schema = to_schema, table = to_table), 
               value = as.data.frame(icd910cm), 
               overwrite = T)
  
  # Add index
  DBI::dbExecute(db_claims, 
                 glue::glue_sql("CREATE CLUSTERED INDEX [idx_cl_dx_ver_dx] ON {`to_schema`}.{`to_table`} (icdcm, icdcm_version)",
                                .con = db_claims))
}


# Repeat to PHClaims if loading to both servers
if (server == "phclaims" | server == "both") {
  db_claims <- create_db_connection("phclaims", interactive = T, prod = T)
  
  # Load data
  dbWriteTable(db_claims, name = DBI::Id(schema = to_schema, table = to_table), 
               value = as.data.frame(icd910cm), 
               overwrite = T)
  
  # Add index
  DBI::dbExecute(db_claims, 
                 glue::glue_sql("CREATE CLUSTERED INDEX [idx_cl_dx_ver_dx] ON {`to_schema`}.{`to_table`} (icdcm, icdcm_version)",
                                .con = db_claims))
}