# Eli Kern
# APDE, PHSKC
# 2018-7-23

#### Medicaid package orientation, July 2018 ####


##### Set up global parameters and call in libraries #####
options(max.print = 350, tibble.print_max = 50, scipen = 999)
origin <- "1970-01-01" # Date origin
library(odbc) # Connect to SQL server
library(medicaid) # Analyze WA State Medicaid data
library(dplyr) # Work with tidy data
library(rlang) # Work with core language features of R and tidyverse
library(openxlsx) # Read and write data using Microsoft Excel
library(dtupdate) # Update GitHub-sourced packages (e.g. housing, medicaid)

##### Check for available updates to medicaid package ####
github_update()
github_update(auto.install = T) # Install updates if available

##### Connect to SQL Servers #####
db.claims51 <- dbConnect(odbc(), "PHClaims51")


#### Medicaid eligibility function ####
# Takes ~ 55-60 sec for full year
system.time(elig_test <- mcaid_elig_f(server = db.claims51, from_date = "2017-01-01", to_date = "2017-12-31"))
#elig_test <- mcaid_elig_f(server = db.claims51, from_date = "2017-01-01", to_date = "2017-12-31", korean = 1, zip = "98103")


#### Medicaid claims summary function ####
# Takes ~ 110-120 sec for full year
system.time(claim_test <- mcaid_claim_f(server = db.claims51, from_date = "2017-01-01", to_date = "2017-12-31", detailed_claims = T))
#claim_test <- mcaid_claim_f(server = db.claims51, from_date = "2017-01-01", to_date = "2017-12-31", korean = 1, zip = "98103")


#### Coverage group function ####
# Returns standalone data frame or joins result to specified data frame
system.time(elig_test_covgrp <- mcaid_covgrp_f(server = db.claims51, from_date = "2017-01-01", to_date = "2017-12-31"))
system.time(elig_test_covgrp <- mcaid_covgrp_f(server = db.claims51, from_date = "2017-01-01", to_date = "2017-12-31", join = T,
                                               df_join_name = elig_test))


#### Condition person tables function ####
# Returns standalone data frame or joins result to specified data frame
system.time(chr_kidney_dis <- mcaid_condition_f(server = db.claims51, condition = "chr_kidney_dis"))
system.time(depression <- mcaid_condition_f(server = db.claims51, condition = "depression", join_type = "left", df_join_name = claim_test))
system.time(depression <- mcaid_condition_f(server = db.claims51, condition = "depression", join_type = "inner", df_join_name = claim_test))
system.time(asthma <- mcaid_condition_f(server = db.claims51, condition = "asthma", from_date = "2017-01-01", to_date = "2017-01-31"))


#### Tabulate results by fixed and loop by variables ####
system.time(claim_test_cnt<- tabloop_f(df = claim_test, dcount = list_var(id), loop = list_var(age_grp7, female, male, aian, asian, black,
                                                                                 nhpi, white, latino, race_unk, hra), 
                                                                  fixed = list_var(cov_cohort)))


## Tabulate results with automatic filtering and renaming
system.time(claim_test_cnt<- tabloop_f(df = claim_test, dcount = list_var(id), loop = list_var(dual_flag, age_grp7, gender_mx, female, male, gender_unk,
                                                                                      aian, asian, black, nhpi, white, latino, race_unk,
                                                                                      tractce10, zip_new, hra, region, maxlang, english,
                                                                                      spanish, vietnamese, chinese, somali, russian, arabic,
                                                                                      korean, ukrainian, amharic, lang_unk), 
                                      fixed = list_var(cov_cohort), filter = T, rename = T))

  #Generate overall tabulations
  claim_test <- claim_test %>% mutate(overall = "_Overall")
  system.time(claim_test_cnt_overall <- tabloop_f(df = claim_test, dcount = list_var(id), loop = list_var(overall, cov_cohort),
                                                  filter = T, rename = T))

  
## Tabulate results using multiple statistics
system.time(claim_test_cnt <- tabloop_f(df = claim_test, dcount = list_var(id), count = list_var(hra_id),
                                             sum = list_var(ed_cnt, inpatient_cnt), mean = list_var(age), median = list_var(age),
                                             loop = list_var(gender_mx), filter = T, rename = T))


## Tabulate and apply automatic small number suppression
system.time(claim_test_cnt <- tabloop_f(df = claim_test, dcount = list_var(id), count = list_var(hra_id),
                                             sum = list_var(ed_cnt, inpatient_cnt), mean = list_var(age), median = list_var(age),
                                             loop = list_var(gender_mx), filter = T, rename = T, suppress = T, 
                                             suppress_var = list_var(id_dcount)))


## Tabulate number of people with depression using joined data set from mcaid_condition_f function
system.time(claim_test_cnt <- tabloop_f(df = depression, dcount = list_var(id), count = list_var(hra_id),
                                             sum = list_var(ed_cnt, inpatient_cnt, depression_ccw), mean = list_var(age), median = list_var(age),
                                             loop = list_var(gender_mx), filter = T, rename = T, suppress = T, 
                                             suppress_var = list_var(id_dcount)))

## Output results
write.xlsx(claim_test_cnt, file = "S:/Transfer/EliKern/Standards/Claims/claim_test_cnt.xlsx")
