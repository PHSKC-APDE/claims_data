# Eli Kern
# APDE, PHSKC
# 2018-4-24

# Code to prepare and upload ICD-CM reference table to SQL Server

#7/26/18 update: Added NYU ED algorithm
#10/11/18 updates: 1) Added plain language for some CCS categories, 2) Corrected ICD10-CM external cause tables
#10/15/18 update: Added final CCS categories
#5/10/19 update: Added all remaining CCW conditions
#7/7/2019 update: Added new ICD-CM codes from Medicare and new Medicaid data
#7/9/2019 update: Added new ICD-CM-10 codes from APCD extract 159
#5/10/2022 update: Overhauled CCW to account for new conditions and algorithms. General tidying.


library(car) # used to recode variables


# SET OPTIONS AND BRING IN PACKAGES ----
options(scipen = 6, digits = 4, warning.length = 8170)
origin <- "1970-01-01"

if (!require("pacman")) {install.packages("pacman")}
pacman::p_load(svDialogs, tidyverse, lubridate, odbc, glue, openxlsx, RecordLinkage, phonics, psych)

## Bring in relevant functions ----
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")


# Step 1: Bring in ICD-9-CM and ICD-10-CM codes ----
url <- "https://github.com/PHSKC-APDE/reference-data/blob/main/claims_data/ICD_9_10_CM_Complete.xlsx?raw=true"
icd910cm <- read.xlsx(url, sheet = "Sheet1", colNames = T)

icd9cm <- filter(icd910cm, ver == 9)
icd10cm <- filter(icd910cm, ver == 10)

rm(icd910cm)



# Step 2: Add in CDC ICD-CM 9 and 10 (proposed) external cause of injury information ----
## Step 2A: Add in CDC ICD-CM 9 and 10 (proposed) external cause of injury information ----
url <- "https://github.com/PHSKC-APDE/reference-data/blob/main/claims_data/cdc_external_injury_matrix_icd_9_19_cm.xlsx?raw=true"
ext_cause_910cm <- read.xlsx(url, sheet = "crosswalk_injurymatrix", colNames = T)
ext_cause_910cm <- ext_cause_910cm %>% mutate(version = as.integer(version))
  

## Step 2B: Merge external cause info for ICD-9-CM ----
ext_cause_9cm <- filter(ext_cause_910cm, version == 9)
icd9cm <- left_join(icd9cm, ext_cause_9cm, by = c("icdcode" = "code", "ver" = "version"))


## Step 2C: Merge external cause info for ICD-10-CM ----
ext_cause_10cm <- filter(ext_cause_910cm, version == 10)

## Create truncated versions of ICD-10-CM code ----
# Improves merge between full list of ICD codes and external cause list
# Full length is 7, based on exploring specificity of data I feel safe creating 6- and 5-digit versions, but no less
ext_cause_10cm <- ext_cause_10cm %>%
  mutate(code_6 = str_sub(code, 1, 6),
         code_5 = str_sub(code, 1, 5))

## Group intent and mechanism by these truncated versions ----
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

## Merge cause framework with ICD-10-CM code list ----
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

#V codes (transport accidents); result: PASS
#Missing intent/mechanism for e-bike collision with person or animal
#Missing intent/mechanism for other motorcycle driver collision with person or animal
#Missing intent/mechanism for other motorcycle passenger collision with person or animal
#Missing occasional 5-digit ICD-10-CM code when this is insufficient to specify only 1 intent and/or mechanism
#View(filter(icd10cm, str_detect(icdcode, "^V") & ver == 10 & str_length(icdcode) >= 5) %>% arrange(icdcode))

#W codes (Other external causes of accidental injury); result: PASS
#Missing intent/mechanism for injury between moving and stationary object
#View(filter(icd10cm, str_detect(icdcode, "^W") & ver == 10 & str_length(icdcode) >= 5) %>% arrange(icdcode))

#X codes (Other external causes of accidental injury, self-harm, assault); result: PASS
#View(filter(icd10cm, str_detect(icdcode, "^X") & ver == 10 & str_length(icdcode) >= 5) %>% arrange(icdcode))

#Y codes (legal, war, undetermined intent; result: PASS
#View(filter(icd10cm, str_detect(icdcode, "^Y") & ver == 10 & str_length(icdcode) >= 5) %>% arrange(icdcode))

#Y codes (legal, war, undetermined intent; result: PASS
#View(filter(icd10cm, str_detect(icdcode, "^T") & ver == 10 & str_length(icdcode) >= 5) %>% arrange(icdcode))


# Step 3: Chronic Condition Warehouse flags ----
# Bring in CCW lookup
ccw_99_16 <- read.xlsx("https://github.com/PHSKC-APDE/reference-data/blob/main/claims_data/ccw_lookup.xlsx?raw=true", 
                       sheet = "ccw99_16")
ccw_17_xx <- read.xlsx("https://github.com/PHSKC-APDE/reference-data/blob/main/claims_data/ccw_lookup.xlsx?raw=true", 
                       sheet = "ccw17_xx")

ccw <- bind_rows(ccw_99_16, ccw_17_xx) %>% mutate(link = 1)

# Create CCW condition 0/1 flags
ccw_condition_vars <- distinct(ccw, ccw_code, ccw_abbrev) %>%
    pivot_wider(names_from = ccw_abbrev, values_from = ccw_code, names_prefix = "ccw_") %>%
    mutate(link = 1) %>% as.data.frame()

# Takes about 2-3 hours on normal computer
ccw <- left_join(ccw, ccw_condition_vars, by = "link") %>%
  select(-link) %>%
  mutate(across(c(starts_with("ccw_"), -"ccw_code", -"ccw_abbrev"), 
                ~ case_when(. == ccw_code ~ as.integer(1),
                            TRUE ~ NA_integer_))) %>%
  select(-ccw_code, -ccw_abbrev) %>%
  #some diagnosis codes are duplicated in CCW lookup, thus copy CCW flag info across rows by code
  group_by(dx) %>%
  mutate(across(starts_with("ccw_"), ~ min(., na.rm = TRUE))) %>%
  ungroup() %>%
  # remove infinity values (consequence of aggregate function on NA values)
  mutate(across(starts_with("ccw_"), ~ ifelse(is.infinite(.), NA_integer_, .))) %>%
  # collapse to one row per code
  distinct(.)


## QA step ----
# Make sure each dx row has at least 1 CCW condition associated with it
ccw_qa <- ccw %>% mutate(chk = rowSums(select(., starts_with("ccw_")), na.rm = T)) %>% count(chk)

if (is.na(min(ccw_qa$chk)) | min(ccw_qa$chk, na.rm = T) == 0) {
  stop("Some DX codes in the CCW table are not associated with any CCW condition")
}

rm(ccw_condition_vars, ccw_qa)


## Join to ICD codes ----
icd9cm <- left_join(icd9cm, ccw, by = c("icdcode" = "dx", "ver" = "ver"))
icd10cm <- left_join(icd10cm, ccw, by = c("icdcode" = "dx", "ver" = "ver"))

rm(ccw)


# Step 4: Clinical classifications software (CCS) from AHRQ HCUP project ----
url <- "https://github.com/PHSKC-APDE/reference-data/blob/main/claims_data/ccs_icd9_10cm.xlsx?raw=true"
ccs <- read.xlsx(url, sheet = "ccs_icdcm", colNames = T)

## Fill in missing plain language text
ccs <- ccs %>%
  mutate(ccs_description_plain_lang = ifelse(is.na(ccs_description_plain_lang), ccs_description, ccs_description_plain_lang),
         multiccs_lv2_plain_lang = ifelse(is.na(multiccs_lv2_plain_lang), multiccs_lv2_description, multiccs_lv2_plain_lang),
         ccs_final_plain_lang = ifelse(is.na(ccs_final_plain_lang), ccs_final_description, ccs_final_plain_lang))


## Join to ICD-9-CM codes ----
icd9cm <- left_join(icd9cm, ccs, by = c("icdcode" = "icdcode", "ver" = "ver"))
# Note there are a small number of ICD-9-CM (mostly header level) that do not match
# Used record linkage approach to find closest matched ICD-9-CM code and copy over CCS level 1 and 2 values (not CCS or CCS level 3)

# Unmatched ICD codes
nomatch <- filter(icd9cm, is.na(multiccs_lv1)) %>%
  mutate(block = str_sub(icdcode, 1, 4)) %>%
  select(., icdcode, block)

# Matched ICD codes
match <- filter(icd9cm, !is.na(multiccs_lv1)) %>%
  mutate(block = str_sub(icdcode, 1, 4)) %>%
  select(., icdcode, block)

# Link based on string comparison, blocking on 1st four digits of ICD-9-CM code (4 chosen through trial and error)
match1 <- compare.linkage(match, nomatch, blockfld = "block", strcmp = "icdcode")

# Code to process RecordLinkage output and create pairs data frame
match1_tmp <- epiWeights(match1)
classify1 <- epiClassify(match1_tmp, threshold.upper = 0.45)
summary(classify1)
pairs1 <- getPairs(classify1, single.rows = TRUE)

# Sort by unmatched ICD-9-CM code, descending by match weight, and matched ICD code and take 1st row grouped by unmatched ICD-9-CM code
# 1st sort variable groups by unmatched ICD code
# 2nd sort variable places highest matched code 1st
# 3rd sort helps when multiple matched codes have same match score - it will send more generic codes (e.g. A200 vs A202) to the top
pairs2 <- pairs1 %>%
  arrange(., icdcode.2, desc(Weight), icdcode.1) %>%
  group_by(icdcode.2) %>%
  slice(., 1) %>%
  ungroup()

# Merge with CCS to get CCS 1 and CCS 2 categories
pairs2 <- pairs2 %>%
  mutate(icdcode_match = icdcode.1, icdcode_nomatch = icdcode.2, ver = 9) %>%
  select(., icdcode_match, icdcode_nomatch, ver)

pairs3 <- left_join(pairs2, ccs, by = c("icdcode_match" = "icdcode", "ver" = "ver")) %>%
  select(., -ccs, -ccs_description, -multiccs_lv3, -multiccs_lv3_description)

# Merge with full ICD-9-CM data and fill in missing info
icd9cm <- left_join(icd9cm, pairs3, by = c("icdcode" = "icdcode_nomatch", "ver" = "ver"))

icd9cm <- icd9cm %>%
  mutate(
    ccs_description_plain_lang = case_when(!is.na(ccs_description_plain_lang.x) ~ ccs_description_plain_lang.x,
                                           is.na(ccs_description_plain_lang.x) ~ ccs_description_plain_lang.y),
    multiccs_lv1 = case_when(!is.na(multiccs_lv1.x) ~ multiccs_lv1.x,
                             is.na(multiccs_lv1.x) ~ multiccs_lv1.y),
    multiccs_lv1_description = case_when(!is.na(multiccs_lv1_description.x) ~ multiccs_lv1_description.x,
                                         is.na(multiccs_lv1_description.x) ~ multiccs_lv1_description.y),
    multiccs_lv2 = case_when(!is.na(multiccs_lv2.x) ~ multiccs_lv2.x,
                             is.na(multiccs_lv2.x) ~ multiccs_lv2.y),
    multiccs_lv2_description = case_when(!is.na(multiccs_lv2_description.x) ~ multiccs_lv2_description.x,
                                         is.na(multiccs_lv2_description.x) ~ multiccs_lv2_description.y),
    multiccs_lv2_plain_lang = case_when(!is.na(multiccs_lv2_plain_lang.x) ~ multiccs_lv2_plain_lang.x,
                                        is.na(multiccs_lv2_plain_lang.x) ~ multiccs_lv2_plain_lang.y),
    ccs_final_code = case_when(!is.na(ccs_final_code.x) ~ ccs_final_code.x,
                               is.na(ccs_final_code.x) ~ ccs_final_code.y),
    ccs_final_description = case_when(!is.na(ccs_final_description.x) ~ ccs_final_description.x,
                                      is.na(ccs_final_description.x) ~ ccs_final_description.y),
    ccs_final_plain_lang = case_when(!is.na(ccs_final_plain_lang.x) ~ ccs_final_plain_lang.x,
                                     is.na(ccs_final_plain_lang.x) ~ ccs_final_plain_lang.y),
    ccs_catch_all = case_when(!is.na(ccs_catch_all.x) ~ ccs_catch_all.x,
                              is.na(ccs_catch_all.x) ~ ccs_catch_all.y)
    ) %>%
  select(-ends_with(".x"), -ends_with(".y"), -icdcode_match)

# clean up
rm(match1, pairs1, pairs2, pairs3, classify1, match, nomatch, match1_tmp)


## Join to ICD-10-CM codes ----
icd10cm <- left_join(icd10cm, ccs, by = c("icdcode" = "icdcode", "ver" = "ver"))
#Note there are a large number of ICD-10-CM (mostly header level) that do not match, too much for manual fixing
#Thus use record linkage approach to find closest matched ICD-10-CM code and copy over CCS level 1 and 2 values (not CCS)

# Unmatched ICD codes
nomatch <- filter(icd10cm, is.na(multiccs_lv1)) %>%
  mutate(block = str_sub(icdcode, 1, 3)) %>%
  select(., icdcode, block)

# Matched ICD codes
match <- filter(icd10cm, !is.na(multiccs_lv1)) %>%
  mutate(block = str_sub(icdcode, 1, 3)) %>%
  select(., icdcode, block)

# Link based on string comparison, blocking on 1st 3 digits of ICD-10-CM code (3 chosen through trial and error)
match1 <- compare.linkage(match, nomatch, blockfld = "block", strcmp = "icdcode")

# Code to process RecordLinkage output and create pairs data frame
match1_tmp <- epiWeights(match1)
classify1 <- epiClassify(match1_tmp, threshold.upper = 0.45)
summary(classify1)
pairs1 <- getPairs(classify1, single.rows = TRUE)

# Sort by unmatched ICD-10-CM code, descending by match weight, and matched ICD code and take 1st row grouped by unmatched ICD-10-CM code
# 1st sort variable groups by unmatched ICD code
# 2nd sort variable places highest matched code 1st
# 3rd sort helps when multiple matched codes have same match score - it will send more generic codes (e.g. A200 vs A202) to the top
pairs2 <- pairs1 %>%
  arrange(., icdcode.2, desc(Weight), icdcode.1) %>%
  group_by(icdcode.2) %>%
  slice(., 1) %>%
  ungroup()

# Merge with CCS to get CCS 1 and CCS 2 categories
pairs2 <- pairs2 %>%
  mutate(icdcode_match = icdcode.1, icdcode_nomatch = icdcode.2, ver = 10) %>%
  select(., icdcode_match, icdcode_nomatch, ver)

pairs3 <- left_join(pairs2, ccs, by = c("icdcode_match" = "icdcode", "ver" = "ver")) %>%
  select(., -ccs, -ccs_description, -multiccs_lv3, -multiccs_lv3_description)

# Merge with full ICD-10-CM data and fill in missing info
icd10cm <- left_join(icd10cm, pairs3, by = c("icdcode" = "icdcode_nomatch", "ver" = "ver"))

icd10cm <- icd10cm %>%
  mutate(
    ccs_description_plain_lang = case_when(!is.na(ccs_description_plain_lang.x) ~ ccs_description_plain_lang.x,
                                           is.na(ccs_description_plain_lang.x) ~ ccs_description_plain_lang.y),
    multiccs_lv1 = case_when(!is.na(multiccs_lv1.x) ~ multiccs_lv1.x,
                             is.na(multiccs_lv1.x) ~ multiccs_lv1.y),
    multiccs_lv1_description = case_when(!is.na(multiccs_lv1_description.x) ~ multiccs_lv1_description.x,
                                         is.na(multiccs_lv1_description.x) ~ multiccs_lv1_description.y),
    multiccs_lv2 = case_when(!is.na(multiccs_lv2.x) ~ multiccs_lv2.x,
                             is.na(multiccs_lv2.x) ~ multiccs_lv2.y),
    multiccs_lv2_description = case_when(!is.na(multiccs_lv2_description.x) ~ multiccs_lv2_description.x,
                                         is.na(multiccs_lv2_description.x) ~ multiccs_lv2_description.y),
    multiccs_lv2_plain_lang = case_when(!is.na(multiccs_lv2_plain_lang.x) ~ multiccs_lv2_plain_lang.x,
                                        is.na(multiccs_lv2_plain_lang.x) ~ multiccs_lv2_plain_lang.y),
    ccs_final_code = case_when(!is.na(ccs_final_code.x) ~ ccs_final_code.x,
                               is.na(ccs_final_code.x) ~ ccs_final_code.y),
    ccs_final_description = case_when(!is.na(ccs_final_description.x) ~ ccs_final_description.x,
                                      is.na(ccs_final_description.x) ~ ccs_final_description.y),
    ccs_final_plain_lang = case_when(!is.na(ccs_final_plain_lang.x) ~ ccs_final_plain_lang.x,
                                     is.na(ccs_final_plain_lang.x) ~ ccs_final_plain_lang.y),
    ccs_catch_all = case_when(!is.na(ccs_catch_all.x) ~ ccs_catch_all.x,
                              is.na(ccs_catch_all.x) ~ ccs_catch_all.y)
  ) %>%
  select(-ends_with(".x"), -ends_with(".y"), -icdcode_match)
  
# clean up
rm(match1, pairs1, pairs2, pairs3, classify1, match, nomatch, match1_tmp)
rm(ccs)



# Step 5: RDA-defined Mental Health and Substance User Disorder-related diagnoses ----
url <- "https://github.com/PHSKC-APDE/reference-data/blob/main/claims_data/mh_sud_dx_lookup_rda.xlsx?raw=true"

mh_rda <- read.xlsx(url, sheet = "mh", colNames = T) %>%
  mutate(mental_dx_rda = 1) %>%
  select(., -dx_description)

sud_rda <- read.xlsx(url, sheet = "sud", colNames = T) %>%
  mutate(sud_dx_rda = 1) %>%
  select(., -dx_description)

# Join to ICD-9-CM codes
icd9cm <- left_join(icd9cm, mh_rda, by = c("icdcode" = "dx", "ver" = "dx_ver"))
icd9cm <- left_join(icd9cm, sud_rda, by = c("icdcode" = "dx", "ver" = "dx_ver"))
# Note: I checked to make sure that all 254 and 246 ICD-9-CM codes merged for MH and SUD, respectively

# Join to ICD-10-CM codes
icd10cm <- left_join(icd10cm, mh_rda, by = c("icdcode" = "dx", "ver" = "dx_ver"))
icd10cm <- left_join(icd10cm, sud_rda, by = c("icdcode" = "dx", "ver" = "dx_ver"))
# Note: I checked to make sure that all 1620 and 872 ICD-10-CM codes merged for MH and SUD, respectively

rm(mh_rda, sud_rda)



# Step 6: Bind ICD-9-CM and ICD-10-CM information ----
icd910cm <- bind_rows(icd9cm, icd10cm)
rm(icd9cm, icd10cm)

# Normalize variable names with other Medicaid claims data tables
icd910cm <- icd910cm %>%
  rename(dx = icdcode, dx_ver = ver)

# Pull out all CCW names and sort (ensures any new CCW conditions are caught)
ccw_cols <- icd910cm %>% select(starts_with("ccw_")) %>% names() %>% sort()

#Order variables for final table upload
icd910cm <- icd910cm %>%
  select(
    dx:dx_description,
    ccs_final_code:ccs_catch_all,
    ccs:ccs_description, ccs_description_plain_lang,
    multiccs_lv1:multiccs_lv2_plain_lang, multiccs_lv3:multiccs_lv3_description,
    all_of(ccw_cols),
    mental_dx_rda:sud_dx_rda,
    injury_icd10cm:mechanism,
    ed_avoid_ca:ed_unclass_nyu
  ) %>%
  # Remove any duplicates
  distinct()


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

# If wanting to load table to both servers, do HHSAW first

# Make connection
db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)

if (server == "hhsaw" | server == "both") {
  to_schema <- "claims"
  to_table <- "ref_dx_lookup"
} else if (server == "phclaims") {
  to_schema <- "ref"
  to_table <- "dx_lookup"
}

# Load data
dbWriteTable(db_claims, name = DBI::Id(schema = to_schema, table = to_table), 
             value = as.data.frame(icd910cm), 
             overwrite = T)

# Add index
DBI::dbExecute(db_claims, 
               glue::glue_sql("CREATE CLUSTERED INDEX [idx_cl_dx_ver_dx] ON {`to_schema`}.{`to_table`} (dx_ver, dx)",
                              .con = db_claims))

# Repeat to PHClaims if loading to both servers
if (server == "both") {
  db_claims <- create_db_connection("phclaims", interactive = T, prod = T)
  to_schema <- "ref"
  to_table <- "dx_lookup"
  
  # Load data
  dbWriteTable(db_claims, name = DBI::Id(schema = to_schema, table = to_table), 
               value = as.data.frame(icd910cm), 
               overwrite = T)
  
  # Add index
  DBI::dbExecute(db_claims, 
                 glue::glue_sql("CREATE CLUSTERED INDEX [idx_cl_dx_ver_dx] ON {`to_schema`}.{`to_table`} (dx_ver, dx)",
                                .con = db_claims))
}
