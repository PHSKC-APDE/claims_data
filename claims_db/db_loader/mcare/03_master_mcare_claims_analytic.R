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
if(qa_line_1 == TRUE & qa_line_2 == TRUE & qa_line_3 == 0L) {
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

### D) Table-level QA (X min)
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
if(qa_icdcm_1 == TRUE & qa_icdcm_2 == TRUE & qa_icdcm_3 == TRUE & qa_icdcm_4 == 0L) {
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

#### CONTINUE HERE ####

## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 3: mcare_claim_procedure ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_procedure.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_procedure.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_claim_procedure_f())

### D) Table-level QA (14 min)
system.time(mcare_claim_procedure_qa <- qa_stage.mcare_claim_procedure_qa_f())
rm(config_url)

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcare_claim_procedure")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcare_claim_procedure")



## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 4: mcare_claim_provider ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_provider.R")
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.mcare_claim_provider.yaml"

### B) Create table
create_table_f(conn = db_claims, 
               config_url = config_url,
               overall = T, ind_yr = F, overwrite = T, test_mode = F)

### C) Load tables
system.time(load_stage.mcare_claim_provider_f())

### D) Table-level QA (15 min)
system.time(mcare_claim_provider_qa <- qa_stage.mcare_claim_provider_qa_f())
rm(config_url)

### F) Archive current table
alter_schema_f(conn = db_claims, from_schema = "final", to_schema = "archive", table_name = "mcare_claim_provider")

### G) Alter schema on new table
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "final", table_name = "mcare_claim_provider")


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### INDEX ALL TABLES PRIOR TO CREATING CLAIM_HEADER TABLE ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_claim_line on final.mcare_claim_line")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_claim_icdcm_header on final.mcare_claim_icdcm_header")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_claim_procedure on final.mcare_claim_procedure")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_claim_provider on final.mcare_claim_provider")))


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 4: mcare_claim_header ####
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


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### INDEX CLAIM_HEADER TABLE ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_mcare_claim_header on final.mcare_claim_header")))
