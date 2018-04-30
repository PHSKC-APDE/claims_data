####---
# Eli Kern
# APDE, PHSKC
# 2018-4-24

# Code to prepare and upload ICD-CM reference table to SQL Server
####---

##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(odbc) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(car) # used to recode variables
library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
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

icd910cm <- read.xlsx("//dchs-shares01/dchsdata/DCHSPHClaimsData/References/Diagnostic codes/ICD_9_10_CM_Complete_Eli.xlsx", sheet = "icd910cm",
          colNames = T)

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

ext_cause_910cm <- read.xlsx("//dchs-shares01/dchsdata/DCHSPHClaimsData/References/Injuries/icd9_10_cm_external_elimerge_10.4.17.xlsx", 
                             sheet = "external_matrix", colNames = T)


####---
#Step 2B: Merge external cause info for ICD-9-CM
####---

ext_cause_9cm <- filter(ext_cause_910cm, ver == 9)

icd9cm <- left_join(icd9cm, ext_cause_9cm, by = c("icdcode" = "dx", "ver" = "ver")) %>%
  select(., icdcode, ver, dx_description, intent, mechanism)

####---
#Step 2C: Merge external cause info for ICD-10-CM
####---

ext_cause_10cm <- filter(ext_cause_910cm, ver == 10)

##Create truncted versions of ICD-10-CM code to improve merge between full list of ICD codes and external cause list
#Full length is 7, based on exploring specificity of data I feel safe creating 6- and 5-digit versions, but no less
ext_cause_10cm <- ext_cause_10cm %>%
  
  mutate(
    dx_6 = str_sub(dx, 1, 6),
    dx_5 = str_sub(dx, 1, 5)
  ) %>%
  
  select(., dx, dx_6, dx_5, ver, intent, mechanism)

##Group intent and mechanism by these truncated versions, keeping only those that are distinct

#First 6 digits
ext10cm_6 <- ext_cause_10cm %>%
  group_by(dx_6) %>%
  mutate(cnt = n()) %>%
  ungroup() %>%
  filter(., cnt == 1) %>%
  select(., dx_6, intent, mechanism)

#First 5 digits
ext10cm_5 <- ext_cause_10cm %>%
  group_by(dx_5) %>%
  mutate(cnt = n()) %>%
  ungroup() %>%
  filter(., cnt == 1) %>%
  select(., dx_5, intent, mechanism)

#Join distinct truncated dx version back to original table
ext_cause_10cm <- left_join(ext_cause_10cm, ext10cm_6, by = "dx_6", suffix = c(".x", ".y")) %>%
  mutate(
    intent = intent.x, 
    mechanism = mechanism.x,
    dx_6 = case_when(
      !is.na(intent.y) ~ dx_6,
      is.na(intent.y) ~ ""
      )
    ) %>%
  select(., dx, dx_6, dx_5, ver, intent, mechanism)

ext_cause_10cm <- left_join(ext_cause_10cm, ext10cm_5, by = "dx_5", suffix = c(".x", ".y")) %>%
  mutate(
    intent = intent.x, 
    mechanism = mechanism.x,
    dx_5 = case_when(
      !is.na(intent.y) ~ dx_5,
      is.na(intent.y) ~ ""
    )
  ) %>%
  select(., dx, dx_6, dx_5, ver, intent, mechanism)

rm(list = ls(pattern = "^ext10cm_"))

##Merge cause framework with ICD-10-CM code list

icd10cm <- icd10cm %>%
  mutate(
    icd_6 = str_sub(icdcode, 1, 6),
    icd_5 = str_sub(icdcode, 1, 5)
  )

#Merge on full ICD digits
icd10cm <- left_join(icd10cm, ext_cause_10cm, by = c("icdcode" = "dx", "ver" = "ver")) %>%
  mutate(intent_final = intent, mechanism_final = mechanism) %>%
  select(., icdcode, icd_6, icd_5, ver, dx_description, intent_final, mechanism_final)

#Merge on 6 digits and fill in missing info
icd10cm <- left_join(icd10cm, ext_cause_10cm, by = c("icd_6" = "dx_6", "ver" = "ver"), suffix = c(".x", ".y")) %>%
  mutate(
    intent_final = case_when(
      !is.na(intent_final) ~ intent_final,
      !is.na(intent) ~ intent
    ),
    mechanism_final = case_when(
      !is.na(mechanism_final) ~ mechanism_final,
      !is.na(mechanism) ~ mechanism
    )
  ) %>%
  select(., icdcode, icd_6, icd_5, ver, dx_description, intent_final, mechanism_final)

#Merge on 5 digits and fill in missing info
icd10cm <- left_join(icd10cm, ext_cause_10cm, by = c("icd_5" = "dx_5", "ver" = "ver"), suffix = c(".x", ".y")) %>%
  mutate(
    intent_final = case_when(
      !is.na(intent_final) ~ intent_final,
      !is.na(intent) ~ intent
    ),
    mechanism_final = case_when(
      !is.na(mechanism_final) ~ mechanism_final,
      !is.na(mechanism) ~ mechanism
    )
  ) %>%
  select(., icdcode, icd_6, icd_5, ver, dx_description, intent_final, mechanism_final)

##Set all sequelae-related injury diagnosis codes to missing per CDC proposed framework

icd10cm <- icd10cm %>%
  mutate(
    intent = case_when (
      !is.na(intent_final) & str_sub(icdcode,-1,-1) != "S" ~ intent_final
    ),
    mechanism = case_when (
      !is.na(mechanism_final) & str_sub(icdcode,-1,-1) != "S" ~ mechanism_final
    )
  ) %>%
  select(., icdcode, ver, dx_description, intent, mechanism)

rm(list = ls(pattern = "^ext_cause_"))

####---
#---
##### Step 3: Chronic Condition Warehouse flags #####
#---
####---

#Bring in CCW lookup
ccw <- read.xlsx("//dchs-shares01/dchsdata/DCHSPHClaimsData/References/Chronic Conditions Warehouse/ccw_lookup.xlsx", 
                             sheet = "ccw", colNames = T) %>%
  
  #create ccw_flags
  mutate(
    asthma_ccw = case_when(ccw_code == 6 ~ 1),
    copd_ccw = case_when(ccw_code == 11 ~ 1),
    diabetes_ccw = case_when(ccw_code == 13 ~ 1),
    ischemic_heart_dis_ccw = case_when(ccw_code == 19 ~ 1),
    heart_failure_ccw = case_when(ccw_code == 15 ~ 1),
    hypertension_ccw = case_when(ccw_code == 18 ~ 1),
    chr_kidney_dis_ccw = case_when(ccw_code == 10 ~ 1),
    depression_ccw = case_when(ccw_code == 12 ~ 1)
  ) %>%
  
  #some diagnosis codes are duplicated in CCW lookup, thus copy CCW flag info across rows by code
  group_by(dx) %>%
  
  mutate_at(
    vars(asthma_ccw, copd_ccw, diabetes_ccw, ischemic_heart_dis_ccw, heart_failure_ccw, hypertension_ccw,
         chr_kidney_dis_ccw, depression_ccw),
    function(x) min(x, na.rm = TRUE)
  ) %>%

  ungroup() %>%
  
  #remove infinity values (consequence of aggregate function on NA values)
  mutate_at(
    vars(asthma_ccw, copd_ccw, diabetes_ccw, ischemic_heart_dis_ccw, heart_failure_ccw, hypertension_ccw,
         chr_kidney_dis_ccw, depression_ccw),
    function(x) replace(x, is.infinite(x),NA)
  ) %>%
  
  #collapse to one row per code
  select(., -ccw_code) %>%
  distinct(.)

#Join to ICD-9-CM codes
icd9cm <- left_join(icd9cm, ccw, by = c("icdcode" = "dx", "ver" = "ver"))
#Note: I checked to make sure that all 309 distinct ICD-9-CM codes had a non-missing value in at least 1 of the CCW flags

#Join to ICD-10-CM codes
icd10cm <- left_join(icd10cm, ccw, by = c("icdcode" = "dx", "ver" = "ver"))
#Note: I checked to make sure that all 814 distinct ICD-10-CM codes had a non-missing value in at least 1 of the CCW flags

rm(ccw)

####---
#---
##### Step 4: Avoidable ED visit diagnoses #####
#---
####---

ed_avoid <- read.xlsx("//dchs-shares01/dchsdata/DCHSPHClaimsData/References/ED visits/CA ED algorithm/CA avoidable ED visits ICD 9 and 10 codes - appendix II.xlsx", 
                 sheet = "Normalized", colNames = T) %>%
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
##### Step 5: Clinical classifications software (CCS) from AHRQ HCUP project #####
#---
####---

ccs <- read.xlsx("//dchs-shares01/dchsdata/DCHSPHClaimsData/References/Diagnostic codes/CCS groupings/ccs_icd9_10cm.xlsx", 
                      sheet = "ccs_icdcm", colNames = T) #Join to ICD-9-CM codes

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
    )
  ) %>%
  
  select(., icdcode, ver, dx_description, intent, mechanism, asthma_ccw, copd_ccw, diabetes_ccw, ischemic_heart_dis_ccw,
         heart_failure_ccw, hypertension_ccw, chr_kidney_dis_ccw, depression_ccw, ed_avoid_ca, ccs, ccs_description, 
         multiccs_lv1, multiccs_lv1_description, multiccs_lv2, multiccs_lv2_description, multiccs_lv3, multiccs_lv3_description)

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
    )
  ) %>%
  
  select(., icdcode, ver, dx_description, intent, mechanism, asthma_ccw, copd_ccw, diabetes_ccw, ischemic_heart_dis_ccw,
         heart_failure_ccw, hypertension_ccw, chr_kidney_dis_ccw, depression_ccw, ed_avoid_ca, ccs, ccs_description, 
         multiccs_lv1, multiccs_lv1_description, multiccs_lv2, multiccs_lv2_description, multiccs_lv3, multiccs_lv3_description)
  
#clean up
rm(match1, pairs1, pairs2, pairs3, classify1, match, nomatch, match1_tmp)
rm(ccs)

####---
#---
##### Step 6: Bind ICD-9-CM and ICD-10-CM information #####
#---
####---

icd910cm <- bind_rows(icd9cm, icd10cm)
rm(icd9cm, icd10cm)

####---
#---
##### Step 7: Upload reference table to SQL Server #####
#---
####---

# Remove/delete table if it already exists AND you have changed the data structure (not usually needed)
#dbRemoveTable(db.claims51, name = "ref_diag_lookup")

# Write your data frame. Note that the package adds a dbo schema so don’t include that in the name.
# Also, you can append = T rather than overwrite = T if desired. 
# Overwrite does what you would expect without needing to delete the whole table
dbWriteTable(db.claims51, name = "ref_diag_lookup", value = as.data.frame(icd910cm), overwrite = T)

