
####---
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

####---

##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(odbc) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(car) # used to recode variables
library(lubridate) # Used to manipulate dates
library(tidyverse) # Used to manipulate data
library(RecordLinkage) # used to clean up duplicates in the data
library(phonics) # used to extract phonetic version of names
library(psych) # used for summary stats

##### Set date origin #####
origin <- "1970-01-01"

##### Connect to the servers #####
db.claims51 <- dbConnect(odbc(), "PHClaims51")



####---
#---
##### Step 1: Bring in ICD-9-CM and ICD-10-CM codes #####
#---
####---

url <- "https://github.com/PHSKC-APDE/reference-data/blob/master/Claims%20data/ICD_9_10_CM_Complete.xlsx?raw=true"
icd910cm <- read.xlsx(url, sheet = "icd910cm", colNames = T)

icd9cm <- filter(icd910cm, ver == 9)
icd10cm <- filter(icd910cm, ver == 10)

rm(icd910cm)



####---
#---
##### Step 2: Add in CDC ICD-CM 9 and 10 (proposed) external cause of injury information #####
#---
####---

####---
#Step 2A: Add in CDC ICD-CM 9 and 10 (proposed) external cause of injury information
####---

url <- "https://github.com/PHSKC-APDE/reference-data/blob/master/Claims%20data/icd9_10_cm_external_merge_10.11.18.xlsx?raw=true"
ext_cause_910cm <- read.xlsx(url, sheet = "external_matrix", colNames = T)
  
####---
#Step 2B: Merge external cause info for ICD-9-CM
####---

ext_cause_9cm <- filter(ext_cause_910cm, ver == 9)

icd9cm <- left_join(icd9cm, ext_cause_9cm, by = c("icdcode" = "dx", "ver" = "ver")) %>% select(., -len)

####---
#Step 2C: Merge external cause info for ICD-10-CM
####---

ext_cause_10cm <- filter(ext_cause_910cm, ver == 10)

##Create truncted versions of ICD-10-CM code to improve merge between full list of ICD codes and external cause list
#Full length is 7, based on exploring specificity of data I feel safe creating 6- and 5-digit versions, but no less
ext_cause_10cm <- ext_cause_10cm %>%
  mutate(dx_6 = str_sub(dx, 1, 6),
         dx_5 = str_sub(dx, 1, 5)) %>%
  select(., -len)

##Group intent and mechanism by these truncated versions, keeping only those that are distinct

#First 6 digits
ext10cm_6 <- ext_cause_10cm %>%
  group_by(dx_6) %>%
  mutate(cnt = n()) %>%
  ungroup() %>%
  filter(., cnt == 1) %>%
  select(., dx_6, injury_icd10cm, intent, mechanism)

#First 5 digits
ext10cm_5 <- ext_cause_10cm %>%
  group_by(dx_5) %>%
  mutate(cnt = n()) %>%
  ungroup() %>%
  filter(., cnt == 1) %>%
  select(., dx_5, injury_icd10cm, intent, mechanism)

#Join distinct truncated dx version back to original table
ext_cause_10cm <- left_join(ext_cause_10cm, ext10cm_6, by = "dx_6", suffix = c(".x", ".y")) %>%
  mutate(
    intent = intent.x, 
    mechanism = mechanism.x,
    injury_icd10cm = injury_icd10cm.x,
    dx_6 = case_when(
      !is.na(intent.y) ~ dx_6,
      is.na(intent.y) ~ "")
    ) %>%
  select(., dx, dx_6, dx_5, ver, injury_icd10cm, intent, mechanism)

ext_cause_10cm <- left_join(ext_cause_10cm, ext10cm_5, by = "dx_5", suffix = c(".x", ".y")) %>%
  mutate(
    intent = intent.x, 
    mechanism = mechanism.x,
    injury_icd10cm = injury_icd10cm.x,
    dx_5 = case_when(
      !is.na(intent.y) ~ dx_5,
      is.na(intent.y) ~ "")
  ) %>%
  select(., dx, dx_6, dx_5, ver, injury_icd10cm, intent, mechanism)

rm(list = ls(pattern = "^ext10cm_"))

##Merge cause framework with ICD-10-CM code list

icd10cm <- icd10cm %>%
  mutate(icd_6 = str_sub(icdcode, 1, 6),
         icd_5 = str_sub(icdcode, 1, 5))

#Merge on full ICD digits
icd10cm <- left_join(icd10cm, ext_cause_10cm, by = c("icdcode" = "dx", "ver" = "ver")) %>%
  mutate(intent_final = intent, mechanism_final = mechanism, injury_icd10cm_final = injury_icd10cm) %>% 
  select(., -dx_6, -dx_5, -injury_icd10cm, -intent, -mechanism)

#Merge on 6 digits and fill in missing info
icd10cm <- left_join(icd10cm, ext_cause_10cm, by = c("icd_6" = "dx_6", "ver" = "ver"), suffix = c(".x", ".y")) %>%
  mutate(
    intent_final = case_when(
      !is.na(intent_final) ~ intent_final,
      !is.na(intent) ~ intent),
    mechanism_final = case_when(
      !is.na(mechanism_final) ~ mechanism_final,
      !is.na(mechanism) ~ mechanism),
    injury_icd10cm_final = case_when(
      !is.na(injury_icd10cm_final) ~ injury_icd10cm_final,
      !is.na(injury_icd10cm) ~ injury_icd10cm)
  ) %>%
  select(., -dx, -dx_5, -injury_icd10cm, -intent, -mechanism)

#Merge on 5 digits and fill in missing info
icd10cm <- left_join(icd10cm, ext_cause_10cm, by = c("icd_5" = "dx_5", "ver" = "ver"), suffix = c(".x", ".y")) %>%
  mutate(
    intent_final = case_when(
      !is.na(intent_final) ~ intent_final,
      !is.na(intent) ~ intent),
    mechanism_final = case_when(
      !is.na(mechanism_final) ~ mechanism_final,
      !is.na(mechanism) ~ mechanism),
    injury_icd10cm_final = case_when(
      !is.na(injury_icd10cm_final) ~ injury_icd10cm_final,
      !is.na(injury_icd10cm) ~ injury_icd10cm)
  ) %>%
  select(., -dx, -dx_6, -injury_icd10cm, -intent, -mechanism)

##Set all sequelae-related injury diagnosis codes to missing per CDC proposed framework
icd10cm <- icd10cm %>%
  mutate(
    intent = case_when (
      !is.na(intent_final) & str_sub(icdcode,-1,-1) != "S" ~ intent_final),
    mechanism = case_when (
      !is.na(mechanism_final) & str_sub(icdcode,-1,-1) != "S" ~ mechanism_final),
    injury_icd10cm = case_when (
      !is.na(injury_icd10cm_final) & str_sub(icdcode,-1,-1) != "S" ~ injury_icd10cm_final)
  ) %>%
  select(., -icd_6, -icd_5, -injury_icd10cm_final, -intent_final, -mechanism_final)

rm(list = ls(pattern = "^ext_cause_"))



####---
#---
##### Step 3: Chronic Condition Warehouse flags #####
#---
####---

#Bring in CCW lookup
url <- "https://github.com/PHSKC-APDE/reference-data/blob/master/Claims%20data/ccw_lookup.xlsx?raw=true"
ccw <- read.xlsx(url, sheet = "ccw", colNames = T) %>%
  mutate(link = 1)

#Create CCW condition 0/1 flags
ccw_condition_vars <- distinct(select(ccw, ccw_code, ccw_abbrev)) %>%
  spread(., key = ccw_abbrev, ccw_code) %>%
  rename_all(., .funs = list(~paste0("ccw_",.))) %>%
  mutate(link = 1)

ccw <- left_join(ccw, ccw_condition_vars, by = "link") %>%
  select(., -link) %>%
  mutate_at(
    vars(ccw_alzheimer:ccw_stroke_exclude2),
    list(~case_when(
      . == ccw_code ~ as.integer(1),
      TRUE ~ NA_integer_)
    )) %>%

  #some diagnosis codes are duplicated in CCW lookup, thus copy CCW flag info across rows by code
  group_by(dx) %>%
  
  mutate_at(
    vars(ccw_alzheimer:ccw_stroke_exclude2),
    function(x) min(x, na.rm = TRUE)
  ) %>%

  ungroup() %>%
  
  #remove infinity values (consequence of aggregate function on NA values)
  mutate_at(
    vars(ccw_alzheimer:ccw_stroke_exclude2),
    function(x) replace(x, is.infinite(x),NA)
  ) %>%
  
  #collapse to one row per code
  select(., -ccw_code, -ccw_abbrev) %>%
  distinct(.)
rm(ccw_condition_vars)

#Join to ICD-9-CM codes
icd9cm <- left_join(icd9cm, ccw, by = c("icdcode" = "dx", "ver" = "ver"))
#Note: I checked to make sure that all 309 distinct ICD-9-CM codes had a non-missing value in at least 1 of the CCW flags

#Join to ICD-10-CM codes
icd10cm <- left_join(icd10cm, ccw, by = c("icdcode" = "dx", "ver" = "ver"))
#Note: I checked to make sure that all 814 distinct ICD-10-CM codes had a non-missing value in at least 1 of the CCW flags

rm(ccw)



####---
#---
##### Step 4: Avoidable ED visit diagnoses, CA algorithm #####
#---
####---

url <- "https://github.com/PHSKC-APDE/reference-data/blob/master/Claims%20data/CA%20avoidable%20ED%20visits%20ICD%209%20and%2010%20codes%20-%20appendix%20II.xlsx?raw=true"
ed_avoid <- read.xlsx(url, sheet = "Normalized", colNames = T) %>%
  mutate(ed_avoid_ca = 1)

#Join to ICD-9-CM codes
icd9cm <- left_join(icd9cm, ed_avoid, by = c("icdcode" = "icdcode_aed", "ver" = "ver"))
#Note: I checked to make sure that all 141 ICD-9-CM codes merged

#Join to ICD-10-CM codes
icd10cm <- left_join(icd10cm, ed_avoid, by = c("icdcode" = "icdcode_aed", "ver" = "ver"))
#Note: I checked to make sure that all 108 ICD-10-CM codes merged

rm(ed_avoid)


####---
#---
##### Step 5: Avoidable ED visit diagnoses, NYU algorithm #####
#---
####---

url <- "https://github.com/PHSKC-APDE/reference-data/blob/master/Claims%20data/nyu_ed_icd-cm-9_10_merge.xlsx?raw=true"
ed_avoid_nyu <- read.xlsx(url, sheet = "Normalized", colNames = T)

##Join to ICD-9-CM codes
icd9cm <- left_join(icd9cm, ed_avoid_nyu, by = c("icdcode" = "dx_norm", "ver" = "dx_ver"))
#Note there are a small number of ICD-9-CM (mostly header level) that do not match
#Used record linkage approach to find closest matched ICD-9-CM code and copy over NYU values

#Unmatched ICD codes
nomatch <- filter(icd9cm, is.na(ed_mh_nyu)) %>%
  mutate(block = str_sub(icdcode, 1, 4)) %>%
  select(., icdcode, block)

#Matched ICD codes
match <- filter(icd9cm, !is.na(ed_mh_nyu)) %>%
  mutate(block = str_sub(icdcode, 1, 4)) %>%
  select(., icdcode, block)

#Link based on string comparison, blocking on 1st four digits of ICD-9-CM code (4 chosen through trial and error)
match1 <- compare.linkage(match, nomatch, blockfld = "block", strcmp = "icdcode")

#Code to process RecordLinkage output and create pairs data frame
match1_tmp <- epiWeights(match1)
classify1 <- epiClassify(match1_tmp, threshold.upper = 0.45)
summary(classify1)
pairs1 <- getPairs(classify1, single.rows = TRUE)

#Sort by unmatched ICD-9-CM code, descending by match weight, and matched ICD code and take 1st row grouped by unmatched ICD-9-CM code
#1st sort variable groups by unmatched ICD code
#2nd sort variable places highest matched code 1st
#3rd sort helps when multiple matched codes have same match score - it will send more generic codes (e.g. A200 vs A202) to the top
pairs2 <- pairs1 %>%
  arrange(., icdcode.2, desc(Weight), icdcode.1) %>%
  group_by(icdcode.2) %>%
  slice(., 1) %>%
  ungroup()

#Merge with NYU to get NYU ED algorithm flags
pairs2 <- pairs2 %>%
  mutate(icdcode_match = icdcode.1, icdcode_nomatch = icdcode.2, ver = 9) %>%
  select(., icdcode_match, icdcode_nomatch, ver)

pairs3 <- left_join(pairs2, ed_avoid_nyu, by = c("icdcode_match" = "dx_norm", "ver" = "dx_ver")) 

#Merge with full ICD-9-CM data and fill in missing info
icd9cm <- left_join(icd9cm, pairs3, by = c("icdcode" = "icdcode_nomatch", "ver" = "ver"))

icd9cm <- icd9cm %>%
  
  mutate(
    
    ed_needed_unavoid_nyu = case_when(
      !is.na(ed_needed_unavoid_nyu.x) ~ ed_needed_unavoid_nyu.x,
      is.na(ed_needed_unavoid_nyu.x) ~ ed_needed_unavoid_nyu.y
    ),
    
    ed_needed_avoid_nyu = case_when(
      !is.na(ed_needed_avoid_nyu.x) ~ ed_needed_avoid_nyu.x,
      is.na(ed_needed_avoid_nyu.x) ~ ed_needed_avoid_nyu.y
    ),
    
    ed_pc_treatable_nyu = case_when(
      !is.na(ed_pc_treatable_nyu.x) ~ ed_pc_treatable_nyu.x,
      is.na(ed_pc_treatable_nyu.x) ~ ed_pc_treatable_nyu.y
    ),
    
    ed_nonemergent_nyu = case_when(
      !is.na(ed_nonemergent_nyu.x) ~ ed_nonemergent_nyu.x,
      is.na(ed_nonemergent_nyu.x) ~ ed_nonemergent_nyu.y
    ),
    
    ed_mh_nyu = case_when(
      !is.na(ed_mh_nyu.x) ~ ed_mh_nyu.x,
      is.na(ed_mh_nyu.x) ~ ed_mh_nyu.y
    ),
    
    ed_sud_nyu = case_when(
      !is.na(ed_sud_nyu.x) ~ ed_sud_nyu.x,
      is.na(ed_sud_nyu.x) ~ ed_sud_nyu.y
    ),
    
    ed_alc_nyu = case_when(
      !is.na(ed_alc_nyu.x) ~ ed_alc_nyu.x,
      is.na(ed_alc_nyu.x) ~ ed_alc_nyu.y
    ),
    
    ed_injury_nyu = case_when(
      !is.na(ed_injury_nyu.x) ~ ed_injury_nyu.x,
      is.na(ed_injury_nyu.x) ~ ed_injury_nyu.y
    ),
    
    ed_unclass_nyu = case_when(
      !is.na(ed_unclass_nyu.x) ~ ed_unclass_nyu.x,
      is.na(ed_unclass_nyu.x) ~ ed_unclass_nyu.y
    )
  ) %>% 
  select(., -ends_with(".x"), -ends_with(".y"),  -icdcode_match) %>%
  
  #Round NYU percentages to 2 decimal places
  mutate_at(
    vars(c("ed_needed_unavoid_nyu", "ed_needed_avoid_nyu", "ed_pc_treatable_nyu", "ed_nonemergent_nyu", "ed_mh_nyu",
           "ed_sud_nyu", "ed_alc_nyu", "ed_injury_nyu", "ed_unclass_nyu")),
    list(~round(., digits = 2))
  )

#clean up
rm(match1, pairs1, pairs2, pairs3, classify1, match, nomatch, match1_tmp)

##Join to ICD-10-CM codes
icd10cm <- left_join(icd10cm, ed_avoid_nyu, by = c("icdcode" = "dx_norm", "ver" = "dx_ver"))
#Note there are a large number of ICD-10-CM (mostly header level) that do not match, too much for manual fixing
#Thus use record linkage approach to find closest matched ICD-10-CM code and copy over NYU values

#Unmatched ICD codes
nomatch <- filter(icd10cm, is.na(ed_mh_nyu)) %>%
  mutate(block = str_sub(icdcode, 1, 3)) %>%
  select(., icdcode, block)

#Matched ICD codes
match <- filter(icd10cm, !is.na(ed_mh_nyu)) %>%
  mutate(block = str_sub(icdcode, 1, 3)) %>%
  select(., icdcode, block)

#Link based on string comparison, blocking on 1st 3 digits of ICD-10-CM code (3 chosen through trial and error)
match1 <- compare.linkage(match, nomatch, blockfld = "block", strcmp = "icdcode")

#Code to process RecordLinkage output and create pairs data frame
match1_tmp <- epiWeights(match1)
classify1 <- epiClassify(match1_tmp, threshold.upper = 0.45)
summary(classify1)
pairs1 <- getPairs(classify1, single.rows = TRUE)

#Sort by unmatched ICD-10-CM code, descending by match weight, and matched ICD code and take 1st row grouped by unmatched ICD-10-CM code
#1st sort variable groups by unmatched ICD code
#2nd sort variable places highest matched code 1st
#3rd sort helps when multiple matched codes have same match score - it will send more generic codes (e.g. A200 vs A202) to the top
pairs2 <- pairs1 %>%
  arrange(., icdcode.2, desc(Weight), icdcode.1) %>%
  group_by(icdcode.2) %>%
  slice(., 1) %>%
  ungroup()

#Merge with NYU to get NYU ED algorithm flags
pairs2 <- pairs2 %>%
  mutate(icdcode_match = icdcode.1, icdcode_nomatch = icdcode.2, ver = 10) %>%
  select(., icdcode_match, icdcode_nomatch, ver)

pairs3 <- left_join(pairs2, ed_avoid_nyu, by = c("icdcode_match" = "dx_norm", "ver" = "dx_ver")) 

#Merge with full ICD-9-CM data and fill in missing info
#Special case - when there is no match (which means block did not exist, then set all NYU vars to 0 except unclassified - set to 1)
icd10cm <- left_join(icd10cm, pairs3, by = c("icdcode" = "icdcode_nomatch", "ver" = "ver"))

icd10cm <- icd10cm %>%
  mutate(
    
    ed_needed_unavoid_nyu = case_when(
      !is.na(ed_needed_unavoid_nyu.x) ~ ed_needed_unavoid_nyu.x,
      is.na(ed_needed_unavoid_nyu.x) & !is.na(ed_needed_unavoid_nyu.y) ~ ed_needed_unavoid_nyu.y,
      is.na(ed_needed_unavoid_nyu.x) & is.na(ed_needed_unavoid_nyu.y) ~ 0
    ),
    
    ed_needed_avoid_nyu = case_when(
      !is.na(ed_needed_avoid_nyu.x) ~ ed_needed_avoid_nyu.x,
      is.na(ed_needed_avoid_nyu.x) & !is.na(ed_needed_avoid_nyu.y) ~ ed_needed_avoid_nyu.y,
      is.na(ed_needed_avoid_nyu.x) & is.na(ed_needed_avoid_nyu.y) ~ 0
    ),
    
    ed_pc_treatable_nyu = case_when(
      !is.na(ed_pc_treatable_nyu.x) ~ ed_pc_treatable_nyu.x,
      is.na(ed_pc_treatable_nyu.x) & !is.na(ed_pc_treatable_nyu.y) ~ ed_pc_treatable_nyu.y,
      is.na(ed_pc_treatable_nyu.x) & is.na(ed_pc_treatable_nyu.y) ~ 0
    ),
    
    ed_nonemergent_nyu = case_when(
      !is.na(ed_nonemergent_nyu.x) ~ ed_nonemergent_nyu.x,
      is.na(ed_nonemergent_nyu.x) & !is.na(ed_nonemergent_nyu.y) ~ ed_nonemergent_nyu.y,
      is.na(ed_nonemergent_nyu.x) & is.na(ed_nonemergent_nyu.y) ~ 0
    ),
    
    ed_mh_nyu = case_when(
      !is.na(ed_mh_nyu.x) ~ ed_mh_nyu.x,
      is.na(ed_mh_nyu.x) & !is.na(ed_mh_nyu.y) ~ ed_mh_nyu.y,
      is.na(ed_mh_nyu.x) & is.na(ed_mh_nyu.y) ~ 0
    ),
    
    ed_sud_nyu = case_when(
      !is.na(ed_sud_nyu.x) ~ ed_sud_nyu.x,
      is.na(ed_sud_nyu.x) & !is.na(ed_sud_nyu.y) ~ ed_sud_nyu.y,
      is.na(ed_sud_nyu.x) & is.na(ed_sud_nyu.y) ~ 0
    ),
    
    ed_alc_nyu = case_when(
      !is.na(ed_alc_nyu.x) ~ ed_alc_nyu.x,
      is.na(ed_alc_nyu.x) & !is.na(ed_alc_nyu.y) ~ ed_alc_nyu.y,
      is.na(ed_alc_nyu.x) & is.na(ed_alc_nyu.y) ~ 0
    ),
    
    ed_injury_nyu = case_when(
      !is.na(ed_injury_nyu.x) ~ ed_injury_nyu.x,
      is.na(ed_injury_nyu.x) & !is.na(ed_injury_nyu.y) ~ ed_injury_nyu.y,
      is.na(ed_injury_nyu.x) & is.na(ed_injury_nyu.y) ~ 0
    ),
    
    ed_unclass_nyu = case_when(
      !is.na(ed_unclass_nyu.x) ~ ed_unclass_nyu.x,
      is.na(ed_unclass_nyu.x) & !is.na(ed_unclass_nyu.y) ~ ed_unclass_nyu.y,
      is.na(ed_unclass_nyu.x) & is.na(ed_unclass_nyu.y) ~ 1
    )
  ) %>%
  select(., -ends_with(".x"), -ends_with(".y"), -icdcode_match) %>%
  
  #Round NYU percentages to 2 decimal places
  mutate_at(
    vars(c("ed_needed_unavoid_nyu", "ed_needed_avoid_nyu", "ed_pc_treatable_nyu", "ed_nonemergent_nyu", "ed_mh_nyu",
           "ed_sud_nyu", "ed_alc_nyu", "ed_injury_nyu", "ed_unclass_nyu")),
    list(~round(., digits = 2))
  )

#clean up
rm(match1, pairs1, pairs2, pairs3, classify1, match, nomatch, match1_tmp)
rm(ed_avoid_nyu)


####---
#---
##### Step 6: Clinical classifications software (CCS) from AHRQ HCUP project #####
#---
####---

url <- "https://github.com/PHSKC-APDE/reference-data/blob/master/Claims%20data/ccs_icd9_10cm.xlsx?raw=true"
ccs <- read.xlsx(url, sheet = "ccs_icdcm",
                      colNames = T)

## Fill in missing plain language text
ccs <- ccs %>%
  mutate(ccs_description_plain_lang = ifelse(is.na(ccs_description_plain_lang), ccs_description, ccs_description_plain_lang),
         multiccs_lv2_plain_lang = ifelse(is.na(multiccs_lv2_plain_lang), multiccs_lv2_description, multiccs_lv2_plain_lang),
         ccs_final_plain_lang = ifelse(is.na(ccs_final_plain_lang), ccs_final_description, ccs_final_plain_lang))

##Join to ICD-9-CM codes
icd9cm <- left_join(icd9cm, ccs, by = c("icdcode" = "icdcode", "ver" = "ver"))
#Note there are a small number of ICD-9-CM (mostly header level) that do not match
#Used record linkage approach to find closest matched ICD-9-CM code and copy over CCS level 1 and 2 values (not CCS or CCS level 3)

#Unmatched ICD codes
nomatch <- filter(icd9cm, is.na(multiccs_lv1)) %>%
  mutate(block = str_sub(icdcode, 1, 4)) %>%
  select(., icdcode, block)

#Matched ICD codes
match <- filter(icd9cm, !is.na(multiccs_lv1)) %>%
  mutate(block = str_sub(icdcode, 1, 4)) %>%
  select(., icdcode, block)

#Link based on string comparison, blocking on 1st four digits of ICD-9-CM code (4 chosen through trial and error)
match1 <- compare.linkage(match, nomatch, blockfld = "block", strcmp = "icdcode")

#Code to process RecordLinkage output and create pairs data frame
match1_tmp <- epiWeights(match1)
classify1 <- epiClassify(match1_tmp, threshold.upper = 0.45)
summary(classify1)
pairs1 <- getPairs(classify1, single.rows = TRUE)

#Sort by unmatched ICD-9-CM code, descending by match weight, and matched ICD code and take 1st row grouped by unmatched ICD-9-CM code
#1st sort variable groups by unmatched ICD code
#2nd sort variable places highest matched code 1st
#3rd sort helps when multiple matched codes have same match score - it will send more generic codes (e.g. A200 vs A202) to the top
pairs2 <- pairs1 %>%
  arrange(., icdcode.2, desc(Weight), icdcode.1) %>%
  group_by(icdcode.2) %>%
  slice(., 1) %>%
  ungroup()

#Merge with CCS to get CCS 1 and CCS 2 categories
pairs2 <- pairs2 %>%
  mutate(icdcode_match = icdcode.1, icdcode_nomatch = icdcode.2, ver = 9) %>%
  select(., icdcode_match, icdcode_nomatch, ver)

pairs3 <- left_join(pairs2, ccs, by = c("icdcode_match" = "icdcode", "ver" = "ver")) %>%
  select(., -ccs, -ccs_description, -multiccs_lv3, -multiccs_lv3_description)

#Merge with full ICD-9-CM data and fill in missing info
icd9cm <- left_join(icd9cm, pairs3, by = c("icdcode" = "icdcode_nomatch", "ver" = "ver"))

icd9cm <- icd9cm %>%
  mutate(
    ccs_description_plain_lang = case_when(
      !is.na(ccs_description_plain_lang.x) ~ ccs_description_plain_lang.x,
      is.na(ccs_description_plain_lang.x) ~ ccs_description_plain_lang.y
    ),
    multiccs_lv1 = case_when(
      !is.na(multiccs_lv1.x) ~ multiccs_lv1.x,
      is.na(multiccs_lv1.x) ~ multiccs_lv1.y
    ),
    multiccs_lv1_description = case_when(
      !is.na(multiccs_lv1_description.x) ~ multiccs_lv1_description.x,
      is.na(multiccs_lv1_description.x) ~ multiccs_lv1_description.y
    ),
    multiccs_lv2 = case_when(
      !is.na(multiccs_lv2.x) ~ multiccs_lv2.x,
      is.na(multiccs_lv2.x) ~ multiccs_lv2.y
    ),
    multiccs_lv2_description = case_when(
      !is.na(multiccs_lv2_description.x) ~ multiccs_lv2_description.x,
      is.na(multiccs_lv2_description.x) ~ multiccs_lv2_description.y
    ),
    multiccs_lv2_plain_lang = case_when(
      !is.na(multiccs_lv2_plain_lang.x) ~ multiccs_lv2_plain_lang.x,
      is.na(multiccs_lv2_plain_lang.x) ~ multiccs_lv2_plain_lang.y
    ),
    ccs_final_code = case_when(
      !is.na(ccs_final_code.x) ~ ccs_final_code.x,
      is.na(ccs_final_code.x) ~ ccs_final_code.y
    ),
    ccs_final_description = case_when(
      !is.na(ccs_final_description.x) ~ ccs_final_description.x,
      is.na(ccs_final_description.x) ~ ccs_final_description.y
    ),
    ccs_final_plain_lang = case_when(
      !is.na(ccs_final_plain_lang.x) ~ ccs_final_plain_lang.x,
      is.na(ccs_final_plain_lang.x) ~ ccs_final_plain_lang.y
    ),
    ccs_catch_all = case_when(
      !is.na(ccs_catch_all.x) ~ ccs_catch_all.x,
      is.na(ccs_catch_all.x) ~ ccs_catch_all.y
    )
  ) %>%
  select(., -ends_with(".x"), -ends_with(".y"), -icdcode_match)

#clean up
rm(match1, pairs1, pairs2, pairs3, classify1, match, nomatch, match1_tmp)

##Join to ICD-10-CM codes
icd10cm <- left_join(icd10cm, ccs, by = c("icdcode" = "icdcode", "ver" = "ver"))
#Note there are a large number of ICD-10-CM (mostly header level) that do not match, too much for manual fixing
#Thus use record linkage approach to find closest matched ICD-10-CM code and copy over CCS level 1 and 2 values (not CCS)

#Unmatched ICD codes
nomatch <- filter(icd10cm, is.na(multiccs_lv1)) %>%
  mutate(block = str_sub(icdcode, 1, 3)) %>%
  select(., icdcode, block)

#Matched ICD codes
match <- filter(icd10cm, !is.na(multiccs_lv1)) %>%
  mutate(block = str_sub(icdcode, 1, 3)) %>%
  select(., icdcode, block)

#Link based on string comparison, blocking on 1st 3 digits of ICD-10-CM code (3 chosen through trial and error)
match1 <- compare.linkage(match, nomatch, blockfld = "block", strcmp = "icdcode")

#Code to process RecordLinkage output and create pairs data frame
match1_tmp <- epiWeights(match1)
classify1 <- epiClassify(match1_tmp, threshold.upper = 0.45)
summary(classify1)
pairs1 <- getPairs(classify1, single.rows = TRUE)

#Sort by unmatched ICD-10-CM code, descending by match weight, and matched ICD code and take 1st row grouped by unmatched ICD-10-CM code
#1st sort variable groups by unmatched ICD code
#2nd sort variable places highest matched code 1st
#3rd sort helps when multiple matched codes have same match score - it will send more generic codes (e.g. A200 vs A202) to the top
pairs2 <- pairs1 %>%
  arrange(., icdcode.2, desc(Weight), icdcode.1) %>%
  group_by(icdcode.2) %>%
  slice(., 1) %>%
  ungroup()

#Merge with CCS to get CCS 1 and CCS 2 categories
pairs2 <- pairs2 %>%
  mutate(icdcode_match = icdcode.1, icdcode_nomatch = icdcode.2, ver = 10) %>%
  select(., icdcode_match, icdcode_nomatch, ver)

pairs3 <- left_join(pairs2, ccs, by = c("icdcode_match" = "icdcode", "ver" = "ver")) %>%
  select(., -ccs, -ccs_description, -multiccs_lv3, -multiccs_lv3_description)

#Merge with full ICD-10-CM data and fill in missing info
icd10cm <- left_join(icd10cm, pairs3, by = c("icdcode" = "icdcode_nomatch", "ver" = "ver"))

icd10cm <- icd10cm %>%
  mutate(
    ccs_description_plain_lang = case_when(
      !is.na(ccs_description_plain_lang.x) ~ ccs_description_plain_lang.x,
      is.na(ccs_description_plain_lang.x) ~ ccs_description_plain_lang.y
    ),
    multiccs_lv1 = case_when(
      !is.na(multiccs_lv1.x) ~ multiccs_lv1.x,
      is.na(multiccs_lv1.x) ~ multiccs_lv1.y
    ),
    multiccs_lv1_description = case_when(
      !is.na(multiccs_lv1_description.x) ~ multiccs_lv1_description.x,
      is.na(multiccs_lv1_description.x) ~ multiccs_lv1_description.y
    ),
    multiccs_lv2 = case_when(
      !is.na(multiccs_lv2.x) ~ multiccs_lv2.x,
      is.na(multiccs_lv2.x) ~ multiccs_lv2.y
    ),
    multiccs_lv2_description = case_when(
      !is.na(multiccs_lv2_description.x) ~ multiccs_lv2_description.x,
      is.na(multiccs_lv2_description.x) ~ multiccs_lv2_description.y
    ),
    multiccs_lv2_plain_lang = case_when(
      !is.na(multiccs_lv2_plain_lang.x) ~ multiccs_lv2_plain_lang.x,
      is.na(multiccs_lv2_plain_lang.x) ~ multiccs_lv2_plain_lang.y
    ),
    ccs_final_code = case_when(
      !is.na(ccs_final_code.x) ~ ccs_final_code.x,
      is.na(ccs_final_code.x) ~ ccs_final_code.y
    ),
    ccs_final_description = case_when(
      !is.na(ccs_final_description.x) ~ ccs_final_description.x,
      is.na(ccs_final_description.x) ~ ccs_final_description.y
    ),
    ccs_final_plain_lang = case_when(
      !is.na(ccs_final_plain_lang.x) ~ ccs_final_plain_lang.x,
      is.na(ccs_final_plain_lang.x) ~ ccs_final_plain_lang.y
    ),
    ccs_catch_all = case_when(
      !is.na(ccs_catch_all.x) ~ ccs_catch_all.x,
      is.na(ccs_catch_all.x) ~ ccs_catch_all.y
    )
  ) %>%
  select(., -ends_with(".x"), -ends_with(".y"), -icdcode_match)
  
#clean up
rm(match1, pairs1, pairs2, pairs3, classify1, match, nomatch, match1_tmp)
rm(ccs)



####---
#---
##### Step 7: RDA-defined Mental Health and Substance User Disorder-related diagnoses #####
#---
####---

url <- "https://github.com/PHSKC-APDE/reference-data/blob/master/Claims%20data/mh_sud_dx_lookup_rda.xlsx?raw=true"

mh_rda <- read.xlsx(url, sheet = "mh",
                      colNames = T) %>%
  mutate(mental_dx_rda = 1) %>%
  select(., -dx_description)

sud_rda <- read.xlsx(url, sheet = "sud",
                    colNames = T) %>%
  mutate(sud_dx_rda = 1) %>%
  select(., -dx_description)

#Join to ICD-9-CM codes
icd9cm <- left_join(icd9cm, mh_rda, by = c("icdcode" = "dx", "ver" = "dx_ver"))
icd9cm <- left_join(icd9cm, sud_rda, by = c("icdcode" = "dx", "ver" = "dx_ver"))
#Note: I checked to make sure that all 254 and 246 ICD-9-CM codes merged for MH and SUD, respectively

#Join to ICD-10-CM codes
icd10cm <- left_join(icd10cm, mh_rda, by = c("icdcode" = "dx", "ver" = "dx_ver"))
icd10cm <- left_join(icd10cm, sud_rda, by = c("icdcode" = "dx", "ver" = "dx_ver"))
#Note: I checked to make sure that all 1620 and 872 ICD-10-CM codes merged for MH and SUD, respectively

rm(mh_rda, sud_rda)



####---
#---
##### Step 8: Bind ICD-9-CM and ICD-10-CM information #####
#---
####---

icd910cm <- bind_rows(icd9cm, icd10cm)
rm(icd9cm, icd10cm)

#Normalize variable names with other Medicaid claims data tables
icd910cm <- icd910cm %>%
  rename(dx = icdcode, dx_ver = ver)

#Order variables for final table upload
icd910cm <- icd910cm %>%
  select(
    dx:dx_description,
    ccs_final_code:ccs_catch_all,
    ccs:ccs_description, ccs_description_plain_lang,
    multiccs_lv1:multiccs_lv2_plain_lang, multiccs_lv3:multiccs_lv3_description,
    ccw_alzheimer:ccw_stroke_exclude2,
    mental_dx_rda:sud_dx_rda,
    injury_icd10cm:mechanism,
    ed_avoid_ca:ed_unclass_nyu
  ) %>%
  # Remove any duplicates
  distinct()

####---
#---
##### Step 9: Upload reference table to SQL Server #####
#---
####---

# Write your data frame. Note that the package adds a dbo schema so do not include that in the name.
# Also, you can append = T rather than overwrite = T if desired. 
# Overwrite does what you would expect without needing to delete the whole table
tbl_name <- DBI::Id(schema = "ref", table = "dx_lookup_load")
dbWriteTable(db.claims51, name = tbl_name, value = as.data.frame(icd910cm), overwrite = T)
DBI::dbExecute(db.claims51, "CREATE CLUSTERED INDEX [idx_cl_dx_ver_dx] ON phclaims.ref.dx_lookup_load (dx_ver, dx)")