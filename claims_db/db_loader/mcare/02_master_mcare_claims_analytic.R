#### MASTER SCRIPT TO CREATE ANALYTIC TABLES FOR MCARE DATA IN INTHEALTH_EDW
#
# Loads and QAs data with stage in table name as prefix
# Archives current final tables
# Changes stage in table name to final
#
# Eli Kern, PHSKC (APDE)
# Adapted from Eli Kern's APCD analytic script
#
# 2024-05
#

#### SET UP ####

#Set global parameters and call in libraries
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)
pacman::p_load(DBI, glue, tidyverse, lubridate, odbc, configr, RCurl)
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")

#Set expected years of data for QA checks
years_expected_from2014 <- 8 #number of years of data we expect (2014+)
years_expected_from2015 <- 7 #number of years of data we expect (2015+)
years_expected_from2017 <- 5 #number of years of data we expect (2017+)

#Connect to inthealth_edw
interactive_auth <- FALSE
prod <- TRUE
inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

#Set up functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/load_ccw.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/claim_bh.R")


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 1: mcare_elig_demo ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_elig_demo.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_elig_demo.yaml"
inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

### B) Create table
create_table_f(conn = inthealth, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T)

### C) Load tables
system.time(load_stage.mcare_elig_demo_f())

### D) Table-level QA
system.time(mcare_elig_demo_qa <- qa_stage.mcare_elig_demo_qa_f())
rm(config_url)

#Process results
if(all(c(mcare_elig_demo_qa$qa[[1]] == 0
         & mcare_elig_demo_qa$qa[[2]] == 0
         & mcare_elig_demo_qa$qa[[3]] == 0
         & mcare_elig_demo_qa$qa[[4]] == 0))) {
  message("mcare_elig_demo QA result: PASS")
} else {
  stop("mcare_elig_demo QA result: FAIL")
}

### E) Archive current stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.final_mcare_elig_demo TO archive_mcare_elig_demo;",
                              .con = inthealth))

### F) Rename current stg_claims.stage table as stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.stage_mcare_elig_demo TO final_mcare_elig_demo;",
                              .con = inthealth))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 2: mcare_elig_timevar ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_elig_timevar.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_elig_timevar.yaml"
inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

### B) Create table
create_table_f(conn = inthealth, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T)

### C) Load tables
system.time(load_stage.mcare_elig_timevar_f())

### D) Table-level QA
system.time(mcare_elig_timevar_qa <- qa_stage.mcare_elig_timevar_qa_f())
rm(config_url)

#Process results
if(all(c(mcare_elig_timevar_qa$qa[[1]] == 0
         & mcare_elig_timevar_qa$qa[[2]] == 0
         & mcare_elig_timevar_qa$qa[[3]] == 0
         & mcare_elig_timevar_qa$qa[[4]] == 0
         & mcare_elig_timevar_qa$qa[[5]] == 0
         & mcare_elig_timevar_qa$qa[[6]] == 0
         & mcare_elig_timevar_qa$qa[[7]] == 0
         & mcare_elig_timevar_qa$qa[[8]] == 0
         & mcare_elig_timevar_qa$qa[[9]] == 0
         & mcare_elig_timevar_qa$qa[[10]] == 0))) {
  message("mcare_elig_timevar QA result: PASS")
} else {
  stop("mcare_elig_timevar QA result: FAIL")
}

### E) Archive current stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.final_mcare_elig_timevar TO archive_mcare_elig_timevar;",
                              .con = inthealth))

### F) Rename current stg_claims.stage table as stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.stage_mcare_elig_timevar TO final_mcare_elig_timevar;",
                              .con = inthealth))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 3: mcare_claim_line ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_line.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_line.yaml"
inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

### B) Create table
create_table_f(conn = inthealth, 
             config_url = config_url,
             overall = T, ind_yr = F, overwrite = T)

### C) Load tables
system.time(load_stage.mcare_claim_line_f())

### D) Table-level QA
system.time(mcare_claim_line_qa <- qa_stage.mcare_claim_line_qa_f())
rm(config_url)

##Process QA results

#Revenue code check by filetype and year
qa_line_1 <- NULL
for (i in c("hha", "hospice", "inpatient", "outpatient", "snf")) {
  x <- filter(mcare_claim_line_qa, str_detect(qa_type, "revenue code") & filetype_mcare == i) %>%
    nrow()
  if(i == "dme") {
    y <- years_expected_from2015 == x
  } else
    y <- years_expected_from2014 == x
  qa_line_1 <- c(qa_line_1,y)
}
qa_line_1 <- all(qa_line_1)
rm(i,x,y)

#Place of service and type of service codes check by filetype and year
qa_line_2 <- NULL
for (i in c("carrier", "dme")) {
  x <- filter(mcare_claim_line_qa, str_detect(qa_type, "place of service") & filetype_mcare == i) %>%
    nrow()
  if(i == "dme") {
    y <- years_expected_from2015 == x
  } else
    y <- years_expected_from2014 == x
  qa_line_2 <- c(qa_line_2,y)
}
qa_line_2 <- all(qa_line_2)
rm(i,x,y)

#All members included in bene_enrollment table
qa_line_3 <- mcare_claim_line_qa$qa[mcare_claim_line_qa$qa_type=="# members not in bene_enrollment, expect 0"]

#Confirm codes are expected length 
qa_line_4 <- mcare_claim_line_qa$qa[mcare_claim_line_qa$qa_type=="# of claims where length of revenue codes != 4, expect 0"]
qa_line_5 <- mcare_claim_line_qa$qa[mcare_claim_line_qa$qa_type=="# of claims where length of pos codes != 2, expect 0"]
qa_line_6 <- mcare_claim_line_qa$qa[mcare_claim_line_qa$qa_type=="# of claims where length of type of service codes != 1, expect 0"]

#Final QA check
if(all(c(qa_line_1:qa_line_2)) == TRUE & qa_line_3 == 0L & qa_line_4 == 0L & qa_line_5 == 0L & qa_line_6 == 0L) {
  message("mcare_claim_line QA result: PASS")
} else {
  stop("mcare_claim_line QA result: FAIL")
}

### E) Archive current stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.final_mcare_claim_line TO archive_mcare_claim_line;",
                              .con = inthealth))

### F) Rename current stg_claims.stage table as stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.stage_mcare_claim_line TO final_mcare_claim_line;",
                              .con = inthealth))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 4: mcare_claim_icdcm_header ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_icdcm_header.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_icdcm_header.yaml"
inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

### B) Create table
create_table_f(conn = inthealth, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T)

### C) Load tables
system.time(load_stage.mcare_claim_icdcm_header_f())

### D) Table-level QA
system.time(mcare_claim_icdcm_header_qa <- qa_stage.mcare_claim_icdcm_header_qa_f())
rm(config_url)

##Process QA results

#Dx 01 check by filetype and year
qa_icdcm_1 <- NULL
for (i in c("hha", "hospice", "inpatient", "outpatient", "snf", "carrier", "dme")) {
  x <- filter(mcare_claim_icdcm_header_qa, str_detect(qa_type, "dx01") & filetype_mcare == i) %>%
    nrow()
  if(i == "dme") {
    y <- years_expected_from2015 == x
  } else
    y <- years_expected_from2014 == x
  qa_icdcm_1 <- c(qa_icdcm_1,y)
}
qa_icdcm_1 <- all(qa_icdcm_1)
rm(i,x,y)

#Dx admit check by filetype and year
qa_icdcm_2 <- NULL
for (i in c("inpatient", "snf")) {
  x <- filter(mcare_claim_icdcm_header_qa, str_detect(qa_type, "dx_admit") & filetype_mcare == i) %>%
    nrow()
  if(i == "dme") {
    y <- years_expected_from2015 == x
  } else
    y <- years_expected_from2014 == x
  qa_icdcm_2 <- c(qa_icdcm_2,y)
}
qa_icdcm_2 <- all(qa_icdcm_2)
rm(i,x,y)

#Dx ecode 1 check by filetype and year
qa_icdcm_3 <- NULL
for (i in c("inpatient", "snf", "outpatient")) {
  x <- filter(mcare_claim_icdcm_header_qa, str_detect(qa_type, "ecode 1") & filetype_mcare == i) %>%
    nrow()
  if(i == "dme") {
    y <- years_expected_from2015 == x
  } else
    y <- years_expected_from2014 == x
  qa_icdcm_3 <- c(qa_icdcm_3,y)
}
qa_icdcm_3 <- all(qa_icdcm_3)
rm(i,x,y)

#All members included in bene_enrollment table
qa_icdcm_4 <- mcare_claim_icdcm_header_qa$qa[mcare_claim_icdcm_header_qa$qa_type=="# members not in bene_enrollment, expect 0"]

#Final QA check
if(all(c(qa_icdcm_1:qa_icdcm_3)) == TRUE & qa_icdcm_4 == 0L) {
  message("mcare_claim_icdcm_header QA result: PASS")
} else {
  stop("mcare_claim_icdcm_header QA result: FAIL")
}

### E) Archive current stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.final_mcare_claim_icdcm_header TO archive_mcare_claim_icdcm_header;",
                              .con = inthealth))

### F) Rename current stg_claims.stage table as stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.stage_mcare_claim_icdcm_header TO final_mcare_claim_icdcm_header;",
                              .con = inthealth))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 5: mcare_claim_procedure ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_procedure.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_procedure.yaml"
inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

### B) Create table
create_table_f(conn = inthealth, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T)

### C) Load tables
system.time(load_stage.mcare_claim_procedure_f())

### D) Table-level QA
system.time(mcare_claim_procedure_qa <- qa_stage.mcare_claim_procedure_qa_f())
rm(config_url)

##Process QA results

#HCPCS code check by filetype and year
qa_procedure_1 <- NULL
for (i in c("hha", "hospice", "inpatient", "outpatient", "snf", "carrier", "dme")) {
  x <- filter(mcare_claim_procedure_qa, str_detect(qa_type, "hcpcs") & filetype_mcare == i) %>%
    nrow()
  if(i == "dme") {
    y <- years_expected_from2015 == x
  } else
    y <- years_expected_from2014 == x
  qa_procedure_1 <- c(qa_procedure_1,y)
}
qa_procedure_1 <- all(qa_procedure_1)
rm(i,x,y)

#BETOS code check by filetype and year
qa_procedure_2 <- NULL
for (i in c("carrier", "dme")) {
  x <- filter(mcare_claim_procedure_qa, str_detect(qa_type, "betos") & filetype_mcare == i) %>%
    nrow()
  if(i == "dme") {
    y <- years_expected_from2015 == x
  } else
    y <- years_expected_from2014 == x
  qa_procedure_2 <- c(qa_procedure_2,y)
}
qa_procedure_2 <- all(qa_procedure_2)
rm(i,x,y)

#ICD procedure code #1 check by filetype and year
qa_procedure_3 <- NULL
for (i in c("inpatient")) {
  x <- filter(mcare_claim_procedure_qa, str_detect(qa_type, "ICD procedure") & filetype_mcare == i) %>%
    nrow()
  if(i == "dme") {
    y <- years_expected_from2015 == x
  } else
    y <- years_expected_from2014 == x
  qa_procedure_3 <- c(qa_procedure_3,y)
}
qa_procedure_3 <- all(qa_procedure_3)
rm(i,x,y)

#All members included in bene_enrollment table
qa_procedure_4 <- mcare_claim_procedure_qa$qa[mcare_claim_procedure_qa$qa_type=="# members not in bene_enrollment, expect 0"]

#Final QA check
if(all(c(qa_procedure_1:qa_procedure_3)) == TRUE & qa_procedure_4 == 0L) {
  message("mcare_claim_procedure QA result: PASS")
} else {
  stop("mcare_claim_procedure QA result: FAIL")
}

### E) Archive current stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.final_mcare_claim_procedure TO archive_mcare_claim_procedure;",
                              .con = inthealth))

### F) Rename current stg_claims.stage table as stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.stage_mcare_claim_procedure TO final_mcare_claim_procedure;",
                              .con = inthealth))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 6: mcare_claim_provider ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_provider.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_provider.yaml"
inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

### B) Create table
create_table_f(conn = inthealth, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T)

### C) Load tables
system.time(load_stage.mcare_claim_provider_f())

### D) Table-level QA
system.time(mcare_claim_provider_qa <- qa_stage.mcare_claim_provider_qa_f())
rm(config_url)

##Process QA results

#Attending provider check by filetype and year
qa_provider_1 <- NULL
for (i in c("hha", "hospice", "inpatient", "outpatient", "snf")) {
  x <- filter(mcare_claim_provider_qa, str_detect(qa_type, "provider type") & provider_type == "attending" &
                filetype_mcare == i) %>% nrow()
    y <- years_expected_from2014 == x
    qa_provider_1 <- c(qa_provider_1,y)
}
qa_provider_1 <- all(qa_provider_1)
rm(i,x,y)

#Billing provider check by filetype and year
qa_provider_2 <- NULL
for (i in c("hha", "hospice", "inpatient", "outpatient", "snf", "carrier", "dme")) {
  x <- filter(mcare_claim_provider_qa, str_detect(qa_type, "provider type") & provider_type == "billing" &
                filetype_mcare == i) %>% nrow()
  if(i %in% c("carrier", "dme")) {
    y <- years_expected_from2015 == x
  } else {
    y <- years_expected_from2014 == x
  }
  qa_provider_2 <- c(qa_provider_2,y)
}
qa_provider_2 <- all(qa_provider_2)
rm(i,x,y)

#Operating provider check by filetype and year
qa_provider_3 <- NULL
for (i in c("inpatient", "outpatient")) {
  x <- filter(mcare_claim_provider_qa, str_detect(qa_type, "provider type") & provider_type == "operating" &
                filetype_mcare == i) %>% nrow()
  y <- years_expected_from2014 == x
  qa_provider_3 <- c(qa_provider_3,y)
}
qa_provider_3 <- all(qa_provider_3)
rm(i,x,y)

#Other provider check by filetype and year
qa_provider_4 <- NULL
for (i in c("inpatient", "outpatient")) {
  x <- filter(mcare_claim_provider_qa, str_detect(qa_type, "provider type") & provider_type == "other" &
                filetype_mcare == i) %>% nrow()
  y <- years_expected_from2014 == x
  qa_provider_4 <- c(qa_provider_4,y)
}
qa_provider_4 <- all(qa_provider_4)
rm(i,x,y)

#Referring provider check by filetype and year
qa_provider_5 <- NULL
for (i in c("hha", "hospice", "outpatient", "carrier", "dme")) {
  x <- filter(mcare_claim_provider_qa, str_detect(qa_type, "provider type") & provider_type == "referring" &
                filetype_mcare == i) %>% nrow()
  if(i %in% c("outpatient", "dme")) {
    y <- years_expected_from2015 == x
  } else {
    y <- years_expected_from2014 == x
  }
  qa_provider_5 <- c(qa_provider_5,y)
}
qa_provider_5 <- all(qa_provider_5)
rm(i,x,y)

#Rendering provider check by filetype and year
qa_provider_6 <- NULL
for (i in c("snf", "inpatient", "outpatient", "carrier")) {
  x <- filter(mcare_claim_provider_qa, str_detect(qa_type, "provider type") & provider_type == "rendering" &
                filetype_mcare == i) %>% nrow()
  if(i %in% c("inpatient")) {
    y <- years_expected_from2015 == x
  } else {
    y <- years_expected_from2014 == x
  }
  qa_provider_6 <- c(qa_provider_6,y)
}
qa_provider_6 <- all(qa_provider_6)
rm(i,x,y)

#Site of service provider check by filetype and year
qa_provider_7 <- NULL
for (i in c("hha", "hospice", "outpatient", "carrier")) {
  x <- filter(mcare_claim_provider_qa, str_detect(qa_type, "provider type") & provider_type == "site_of_service" &
                filetype_mcare == i) %>% nrow()
  if(i %in% c("outpatient")) {
    y <- years_expected_from2015 == x
  } else if(i %in% c("carrier")) {
    y <- years_expected_from2017 == x
  } else {
    y <- years_expected_from2014 == x
  }
  qa_provider_7 <- c(qa_provider_7,y)
}
qa_provider_7 <- all(qa_provider_7)
rm(i,x,y)

#All members included in bene_enrollment table
qa_provider_8 <- mcare_claim_provider_qa$qa[mcare_claim_provider_qa$qa_type=="# members not in bene_enrollment, expect 0"]

#Final QA check
if(all(c(qa_provider_1:qa_provider_7)) == TRUE & qa_provider_8 == 0L) {
  message("mcare_claim_provider QA result: PASS")
} else {
  stop("mcare_claim_provider QA result: FAIL")
}

### E) Archive current stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.final_mcare_claim_provider TO archive_mcare_claim_provider;",
                              .con = inthealth))

### F) Rename current stg_claims.stage table as stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.stage_mcare_claim_provider TO final_mcare_claim_provider;",
                              .con = inthealth))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 7: mcare_claim_pharm ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_pharm.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_pharm.yaml"
inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

### B) Create table
create_table_f(conn = inthealth, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T)

### C) Load tables
system.time(load_stage.mcare_claim_pharm_f())

### D) Table-level QA
system.time(mcare_claim_pharm_qa <- qa_stage.mcare_claim_pharm_qa_f())
rm(config_url)

##Process QA results

#Facility administered drug check by filetype and year
qa_pharm_1 <- NULL
for (i in c("hospice", "inpatient", "outpatient")) {
  x <- filter(mcare_claim_pharm_qa, str_detect(qa_type, "facility drugs") & filetype_mcare == i) %>% nrow()
  if(i %in% c("outpatient", "inpatient")) {
    y <- years_expected_from2015 == x
  } else {
    y <- years_expected_from2014 == x
  }
  qa_pharm_1 <- c(qa_pharm_1,y)
}
qa_pharm_1 <- all(qa_pharm_1)
rm(i,x,y)

#Pharmacy fills by year
qa_pharm_2 <- NULL
for (i in c("pharmacy")) {
  x <- filter(mcare_claim_pharm_qa, str_detect(qa_type, "pharmacy") & filetype_mcare == i) %>% nrow()
  if(i %in% c("pharmacy")) {
    y <- years_expected_from2014 == x
  }
  qa_pharm_2 <- c(qa_pharm_2,y)
}
qa_pharm_2 <- all(qa_pharm_2)
rm(i,x,y)

#All members included in bene_enrollment table
qa_pharm_3 <- mcare_claim_pharm_qa$qa[mcare_claim_pharm_qa$qa_type=="# members not in bene_enrollment, expect 0"]

#Final QA check
if(all(c(qa_pharm_1:qa_pharm_2)) == TRUE & qa_pharm_3 == 0L) {
  message("mcare_claim_pharm QA result: PASS")
} else {
  stop("mcare_claim_pharm QA result: FAIL")
}

### E) Archive current stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.final_mcare_claim_pharm TO archive_mcare_claim_pharm;",
                              .con = inthealth))

### F) Rename current stg_claims.stage table as stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.stage_mcare_claim_pharm TO final_mcare_claim_pharm;",
                              .con = inthealth))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 8: mcare_claim_pharm_char ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_pharm_char.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_pharm_char.yaml"
inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

### B) Create table
create_table_f(conn = inthealth, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T)

### C) Load tables
system.time(load_stage.mcare_claim_pharm_char_f())

### D) Archive current stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.final_mcare_claim_pharm_char TO archive_mcare_claim_pharm_char;",
                              .con = inthealth))

### E) Rename current stg_claims.stage table as stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.stage_mcare_claim_pharm_char TO final_mcare_claim_pharm_char;",
                              .con = inthealth))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 9: mcare_claim_header ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_header.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_header.yaml"
inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

### B) Create table
create_table_f(conn = inthealth, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T)

### C) Load tables
system.time(load_stage.mcare_claim_header_f())

### D) Table-level QA
system.time(mcare_claim_header_qa <- qa_stage.mcare_claim_header_qa_f())
rm(config_url)

##Process QA results
if(all(c(mcare_claim_header_qa$qa[mcare_claim_header_qa$qa_type=="# of headers"] ==
          mcare_claim_header_qa$qa[mcare_claim_header_qa$qa_type=="# of distinct headers"]
         & mcare_claim_header_qa$qa[mcare_claim_header_qa$qa_type=="# of members not in elig_demo, expect 0"] == 0
         & mcare_claim_header_qa$qa[mcare_claim_header_qa$qa_type=="# of members not in elig_timevar, expect 0"] == 0
         & mcare_claim_header_qa$qa[mcare_claim_header_qa$qa_type=="# of claims with unmatched claim type, expect 0"] == 0
         & mcare_claim_header_qa$qa[mcare_claim_header_qa$qa_type=="# of ipt stays with no discharge date, expect 0"] == 0
         & mcare_claim_header_qa$qa[mcare_claim_header_qa$qa_type=="# of ed_pophealth_id values used for >1 person, expect 0"] == 0
         & mcare_claim_header_qa$qa[mcare_claim_header_qa$qa_type=="# of distinct ed_pophealth_id values"] ==
          mcare_claim_header_qa$qa[mcare_claim_header_qa$qa_type=="max ed_pophealth_id - min + 1"]
         & mcare_claim_header_qa$qa[mcare_claim_header_qa$qa_type=="# of ed_perform rows with no ed_pophealth, expect 0"] == 0
         & mcare_claim_header_qa$qa[mcare_claim_header_qa$qa_type=="# of ed_pophealth visits where the overlap date is greater than 1 day, expect 0"] == 0
         & mcare_claim_header_qa$qa[mcare_claim_header_qa$qa_type=="# of rows where total cost of care does not sum as expected, expect 0"] == 0))) {
  message(paste0("mcare_claim_header QA result: PASS - ", Sys.time()))
} else {
  stop(paste0("mcare_claim_header QA result: FAIL - ", Sys.time()))
}

### E) Archive current stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.final_mcare_claim_header TO archive_mcare_claim_header;",
                              .con = inthealth))

### F) Rename current stg_claims.stage table as stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.stage_mcare_claim_header TO final_mcare_claim_header;",
                              .con = inthealth))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 10: mcare_claim_ccw ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

### A) Create table
create_table_f(
  conn = inthealth,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_ccw.yaml",
  overall = T, ind_yr = F, overwrite = T, server = "hhsaw")

### B) Load tables
system.time(load_ccw(
  server = "hhsaw",
  conn = inthealth,
  source = c("mcare"),
  print_query = FALSE,
  ccw_list_name = "all",
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_ccw.yaml"))

### C) Table-level QA

#all members should be in elig_demo table
mcare_claim_ccw_qa1 <- dbGetQuery(conn = inthealth, glue_sql(
  "select 'stg_claims.stage_mcare_claim_ccw' as 'table', '# members not in elig_demo, expect 0' as qa_type,
    count(distinct a.id_mcare) as qa
    from stg_claims.stage_mcare_claim_ccw as a
    left join stg_claims.final_mcare_elig_demo as b
    on a.id_mcare = b.id_mcare
    where b.id_mcare is null;",
  .con = inthealth))

#count conditions run
mcare_claim_ccw_qa2 <- dbGetQuery(conn = inthealth, glue_sql(
  "select 'stg_claims.stage_mcare_claim_ccw' as 'table', '# conditions, expect 31' as qa_type,
  count(distinct ccw_code) as qa
  from stg_claims.stage_mcare_claim_ccw;",
  .con = inthealth))

#count rows that overlap with prior row or following row, expect 0
mcare_claim_ccw_qa3 <- dbGetQuery(conn = inthealth, glue_sql(
  "
  with temp1 as (
    select id_mcare,
    datediff(day, lag(to_date, 1, null) over(partition by id_mcare, ccw_desc order by from_date), from_date) as prev_row_diff,
    datediff(day, to_date, lead(from_date, 1, null) over(partition by id_mcare, ccw_desc order by from_date)) as next_row_diff
    from stg_claims.stage_mcare_claim_ccw
  )
  select 'stg_claims.stage_mcare_claim_ccw' as 'table', 'overlapping rows, expect 0' as qa_type, count(*) as qa
  from temp1
  where prev_row_diff < 0 or next_row_diff < 0;",
  .con = inthealth))

##Process QA results
if(all(c(mcare_claim_ccw_qa1$qa==0
         & mcare_claim_ccw_qa2$qa==31
         & mcare_claim_ccw_qa3$qa==0))) {
  message(paste0("mcare_claim_ccw QA result: PASS - ", Sys.time()))
} else {
  stop(paste0("mcare_claim_ccw QA result: FAIL - ", Sys.time()))
}

### D) Archive current stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.final_mcare_claim_ccw TO archive_mcare_claim_ccw;",
                              .con = inthealth))

### E) Rename current stg_claims.stage table as stg_claims.final table
DBI::dbExecute(conn = inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.stage_mcare_claim_ccw TO final_mcare_claim_ccw;",
                              .con = inthealth))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 11: mcare_claim_bh ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

### A) Create table
create_table_f(
  conn = inthealth,
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_bh.yaml",
  overall = T, ind_yr = F, overwrite = T, server = "inthealth")

### B) Load tables
system.time(load_bh(
  server = "inthealth",
  conn = inthealth,
  source = "mcare",
  config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_bh.yaml"))
