#### MASTER SCRIPT TO CREATE ANALYTIC CLAIMS TABLES FOR MCARE DATA IN INTHEALTH_EDW
#
# Loads and QAs data with stage in table name as prefix
# Archives current final tables
# Changes stage in table name to final
#
# Eli Kern, PHSKC (APDE)
# Adapted from Eli Kern's APCD analytic script
#
# 2024-05

#Note: Currently only includes code for claims analytic tables, elig tables are run from separate script

#### SET UP ####

#Set global parameters and call in libraries
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)
pacman::p_load(DBI, glue, tidyverse, lubridate, odbc, configr, RCurl)
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")

#Set expected years of data for QA checks
years_expected <- 8 #number of years of data we expect (2014+)
years_expected_dme <- 7 #number of years of data we expect for DME files (2015+)

years_expected_provider_from2014 <- 8 #number of years of data we expect (2014+)
years_expected_provider_from2015 <- 7 #number of years of data we expect (2015+)
years_expected_provider_from2017 <- 5 #number of years of data we expect (2017+)

#Connect to inthealth_edw
dw_inthealth <- create_db_connection("inthealth", interactive = FALSE, prod = TRUE)

#Set up functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/create_table.R")


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 1: mcare_claim_line ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_line.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_line.yaml"

### B) Create table
create_table_f(conn = dw_inthealth, 
             config_url = config_url,
             overall = T, ind_yr = F, overwrite = T)

### C) Load tables
system.time(load_stage.mcare_claim_line_f())

### D) Table-level QA (1 min)
system.time(mcare_claim_line_qa <- qa_stage.mcare_claim_line_qa_f())
rm(config_url)

##Process QA results

#Revenue code check by filetype and year
qa_line_1 <- NULL
for (i in c("hha", "hospice", "inpatient", "outpatient", "snf")) {
  x <- filter(mcare_claim_line_qa, str_detect(qa_type, "revenue code") & filetype_mcare == i) %>%
    nrow()
  if(i == "dme") {
    y <- years_expected_dme == x
  } else
    y <- years_expected == x
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
    y <- years_expected_dme == x
  } else
    y <- years_expected == x
  qa_line_2 <- c(qa_line_2,y)
}
qa_line_2 <- all(qa_line_2)
rm(i,x,y)

#All members included in bene_enrollment table
qa_line_3 <- mcare_claim_line_qa$qa[mcare_claim_line_qa$qa_type=="# members not in bene_enrollment, expect 0"]

#Final QA check
if(all(c(qa_line_1:qa_line_2)) == TRUE & qa_line_3 == 0L) {
  message("mcare_claim_line QA result: PASS")
} else {
  stop("mcare_claim_line QA result: FAIL")
}

### E) Archive current stg_claims.final table
DBI::dbExecute(conn = dw_inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.final_mcare_claim_line TO archive_mcare_claim_line;",
                              .con = dw_inthealth))

### F) Rename current stg_claims.stage table as stg_claims.final table
DBI::dbExecute(conn = dw_inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.stage_mcare_claim_line TO final_mcare_claim_line;",
                              .con = dw_inthealth))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 2: mcare_claim_icdcm_header ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_icdcm_header.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_icdcm_header.yaml"

### B) Create table
create_table_f(conn = dw_inthealth, 
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
    y <- years_expected_dme == x
  } else
    y <- years_expected == x
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
    y <- years_expected_dme == x
  } else
    y <- years_expected == x
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
    y <- years_expected_dme == x
  } else
    y <- years_expected == x
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
DBI::dbExecute(conn = dw_inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.final_mcare_claim_icdcm_header TO archive_mcare_claim_icdcm_header;",
                              .con = dw_inthealth))

### F) Rename current stg_claims.stage table as stg_claims.final table
DBI::dbExecute(conn = dw_inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.stage_mcare_claim_icdcm_header TO final_mcare_claim_icdcm_header;",
                              .con = dw_inthealth))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 3: mcare_claim_procedure ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_procedure.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_procedure.yaml"

### B) Create table
create_table_f(conn = dw_inthealth, 
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
    y <- years_expected_dme == x
  } else
    y <- years_expected == x
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
    y <- years_expected_dme == x
  } else
    y <- years_expected == x
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
    y <- years_expected_dme == x
  } else
    y <- years_expected == x
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
DBI::dbExecute(conn = dw_inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.final_mcare_claim_procedure TO archive_mcare_claim_procedure;",
                              .con = dw_inthealth))

### F) Rename current stg_claims.stage table as stg_claims.final table
DBI::dbExecute(conn = dw_inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.stage_mcare_claim_procedure TO final_mcare_claim_procedure;",
                              .con = dw_inthealth))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 4: mcare_claim_provider ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_provider.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_provider.yaml"

### B) Create table
create_table_f(conn = dw_inthealth, 
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
    y <- years_expected_provider_from2014 == x
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
    y <- years_expected_provider_from2015 == x
  } else {
    y <- years_expected_provider_from2014 == x
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
  y <- years_expected_provider_from2014 == x
  qa_provider_3 <- c(qa_provider_3,y)
}
qa_provider_3 <- all(qa_provider_3)
rm(i,x,y)


#Other provider check by filetype and year
qa_provider_4 <- NULL
for (i in c("inpatient", "outpatient")) {
  x <- filter(mcare_claim_provider_qa, str_detect(qa_type, "provider type") & provider_type == "other" &
                filetype_mcare == i) %>% nrow()
  y <- years_expected_provider_from2014 == x
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
    y <- years_expected_provider_from2015 == x
  } else {
    y <- years_expected_provider_from2014 == x
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
    y <- years_expected_provider_from2015 == x
  } else {
    y <- years_expected_provider_from2014 == x
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
    y <- years_expected_provider_from2015 == x
  } else if(i %in% c("carrier")) {
    y <- years_expected_provider_from2017 == x
  } else {
    y <- years_expected_provider_from2014 == x
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
DBI::dbExecute(conn = dw_inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.final_mcare_claim_provider TO archive_mcare_claim_provider;",
                              .con = dw_inthealth))

### F) Rename current stg_claims.stage table as stg_claims.final table
DBI::dbExecute(conn = dw_inthealth,
               glue::glue_sql("RENAME OBJECT stg_claims.stage_mcare_claim_provider TO final_mcare_claim_provider;",
                              .con = dw_inthealth))



#### PLACEHOLDER FOR PHARM TABLE - NEW TABLE ####


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 6: mcare_claim_header ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_header.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_header.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_claim_header_f())

### D) Table-level QA (x min)
system.time(mcare_claim_header_qa <- qa_stage.mcare_claim_header_qa_f())
rm(config_url)

### E) Run line-level QA script

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcare_claim_header")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcare_claim_header")