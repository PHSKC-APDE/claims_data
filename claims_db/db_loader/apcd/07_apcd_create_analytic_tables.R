#### MASTER CODE TO CREATE ANALYTIC TABLES FOR APCD DATA
#
# Loads and QAs data on stage schema
# Changes schema of existing final tables to archive
# Changes schema of new stage tables to final
# Adds clustered columnstore indexes to new final tables
#
# Eli Kern, PHSKC (APDE)
# Adapted from Alastair Matheson's Medicaid script
# 2019-10

#2022-02: Eli switched to using APDE repo functions where Alastair has moved them over
#2024-03: Eli updated for migration to Azure HHSAW

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, glue)

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/load_table_from_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/add_index.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/alter_schema.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/qa_load_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/load_ccw.R")

## Connect to Synapse
interactive_auth <- FALSE
prod <- TRUE
dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 1: apcd_elig_demo ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

message(paste0("Beginning creation process for apcd_elig_demo - ", Sys.time()))

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_demo.R")

### B) Create table
create_table(conn = dw_inthealth, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_demo.yaml",
               overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")

### C) Load tables
system.time(load_stage.apcd_elig_demo_f())

### D) Table-level QA
system.time(apcd_demo_qa <- qa_stage.apcd_elig_demo_f())

if((apcd_demo_qa$qa[[1]] == apcd_demo_qa$qa[[2]]) & (apcd_demo_qa$qa[[1]] == apcd_demo_qa$qa[[3]])) {
  message(paste0("apcd_elig_demo QA result: PASS - ", Sys.time()))
} else {
  stop(paste0("apcd_elig_demo QA result: FAIL - ", Sys.time()))
}


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 2: apcd_elig_timevar ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

message(paste0("Beginning creation process for apcd_elig_timevar - ", Sys.time()))
dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_timevar.R")

### B) Create table
create_table(conn = dw_inthealth, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_timevar.yaml",
               overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")

### C) Load tables
system.time(load_stage.apcd_elig_timevar_f())

### D) Table-level QA
system.time(apcd_timevar_qa <- qa_stage.apcd_elig_timevar_f())

if(
  (apcd_timevar_qa$qa[apcd_timevar_qa$qa_type=="member count, expect match to raw tables"]==
    apcd_timevar_qa$qa[apcd_timevar_qa$qa_type=="member count, expect match to timevar" & apcd_timevar_qa$table=="stg_claims.apcd_member_month_detail"])
  
  & (apcd_timevar_qa$qa[apcd_timevar_qa$qa_type=="member count, expect match to raw tables"]==
    apcd_timevar_qa$qa[apcd_timevar_qa$qa_type=="member count, expect match to timevar" & apcd_timevar_qa$table=="stg_claims.stage_apcd_elig_demo"])
  
  & (apcd_timevar_qa$qa[apcd_timevar_qa$qa_type=="member count, King 2016, expect match to member_month"]==
     apcd_timevar_qa$qa[apcd_timevar_qa$qa_type=="member count, King 2016, expect match to timevar"])
  
  & apcd_timevar_qa$qa[apcd_timevar_qa$qa_type=="non-WA resident segments with non-null county name, expect 0"]==0
  & apcd_timevar_qa$qa[apcd_timevar_qa$qa_type=="WA resident segments with null county name, expect 0"]==0
  & apcd_timevar_qa$qa[apcd_timevar_qa$qa_type=="count of member elig segments with no coverage, expect 0"]==0
  & apcd_timevar_qa$qa[apcd_timevar_qa$qa_type=="mcaid-mcare duals with dual flag = 0, expect 0"]==0
  
) {
  message(paste0("apcd_elig_timevar QA result: PASS - ", Sys.time()))
} else {
  stop(paste0("apcd_elig_timevar QA result: FAIL - ", Sys.time()))
}


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 3: apcd_elig_plr_DATE ####
# Note: Eventually use claim_elig function to generate these tables
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

message(paste0("Beginning creation process for apcd_elig_plr tables - ", Sys.time()))
dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr.R")

### B) Create table
# 2014
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2014.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")
# 2015
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2015.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")
# 2016
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2016.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")
# 2017
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2017.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")
# 2018
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2018.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")
# 2019
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2019.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")
# 2020
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2020.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")
# 2021
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2021.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")
# 2022
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_elig_plr_2022.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")

### PLACEHOLDER FOR ADDING THE NEXT COMPLETE CALENDAR YEAR TABLE ###


### C) Load tables
system.time(load_stage.apcd_elig_plr_f(from_date = "2014-01-01", to_date = "2014-12-31")) #2014
system.time(load_stage.apcd_elig_plr_f(from_date = "2015-01-01", to_date = "2015-12-31")) #2015
system.time(load_stage.apcd_elig_plr_f(from_date = "2016-01-01", to_date = "2016-12-31")) #2016
system.time(load_stage.apcd_elig_plr_f(from_date = "2017-01-01", to_date = "2017-12-31")) #2017
system.time(load_stage.apcd_elig_plr_f(from_date = "2018-01-01", to_date = "2018-12-31")) #2018
system.time(load_stage.apcd_elig_plr_f(from_date = "2019-01-01", to_date = "2019-12-31")) #2019
system.time(load_stage.apcd_elig_plr_f(from_date = "2020-01-01", to_date = "2020-12-31")) #2020
system.time(load_stage.apcd_elig_plr_f(from_date = "2021-01-01", to_date = "2021-12-31")) #2021
system.time(load_stage.apcd_elig_plr_f(from_date = "2022-01-01", to_date = "2022-12-31")) #2022
##placeholder for adding the next complete calendar year table


### D) Table-level QA
system.time(apcd_plr_2014_qa <- qa_stage.apcd_elig_plr_f(year = "2014"))
system.time(apcd_plr_2015_qa <- qa_stage.apcd_elig_plr_f(year = "2015"))
system.time(apcd_plr_2016_qa <- qa_stage.apcd_elig_plr_f(year = "2016"))
system.time(apcd_plr_2017_qa <- qa_stage.apcd_elig_plr_f(year = "2017"))
system.time(apcd_plr_2018_qa <- qa_stage.apcd_elig_plr_f(year = "2018"))
system.time(apcd_plr_2019_qa <- qa_stage.apcd_elig_plr_f(year = "2019"))
system.time(apcd_plr_2020_qa <- qa_stage.apcd_elig_plr_f(year = "2020"))
system.time(apcd_plr_2021_qa <- qa_stage.apcd_elig_plr_f(year = "2021"))
system.time(apcd_plr_2022_qa <- qa_stage.apcd_elig_plr_f(year = "2022"))
##placeholder for adding the next complete calendar year table

#Process QA results from across all tables
df_list <- list(apcd_plr_2014_qa,
                apcd_plr_2015_qa,
                apcd_plr_2016_qa,
                apcd_plr_2017_qa,
                apcd_plr_2018_qa,
                apcd_plr_2019_qa,
                apcd_plr_2020_qa,
                apcd_plr_2021_qa,
                apcd_plr_2022_qa)
##placeholder for adding the next complete calendar year table
columns <- c("qa_result")
elig_plr_qa_composite_result <- data.frame(matrix(nrow = 0, ncol = length(columns)))

for (i in df_list) {
  elig_plr_qa_composite_result <- rbind(elig_plr_qa_composite_result, (i$qa[i$qa_type=="# members with >1 row, expect 0"]==0
   & i$qa[i$qa_type=="non-WA county for WA resident, expect 0"]==0
   & i$qa[i$qa_type=="non-WA residents, expect 0"]==0
   & i$qa[i$qa_type=="# of members with day counts >365, expect 0"]==0
   & i$qa[i$qa_type=="# of members with percents >100, expect 0"]==0
   & i$qa[i$qa_type=="# of overall Medicaid members out of state, expect 0"]==0
   & i$qa[i$qa_type=="# of overall Medicaid members"]>0))
  
  colnames(elig_plr_qa_composite_result) = columns
  
}

if(all(elig_plr_qa_composite_result$qa_result) == TRUE) {
  message(paste0("apcd_elig_plr QA result: PASS - ", Sys.time()))
} else {
  stop(paste0("apcd_elig_plr QA result: FAIL - ", Sys.time()))
}


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 4: apcd_claim_line ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

message(paste0("Beginning creation process for apcd_claim_line - ", Sys.time()))
dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_line.R")

### B) Create table
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_line.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")

### C) Load tables
system.time(load_stage.apcd_claim_line_f())

### D) Table-level QA
system.time(apcd_line_qa <- qa_stage.apcd_claim_line_f())

##Process QA results
if(all(c(apcd_line_qa$qa[[1]] == 0
         & apcd_line_qa$qa[[2]] == 0))) {
  message(paste0("apcd_claim_line QA result: PASS - ", Sys.time()))
} else {
  stop(paste0("apcd_claim_line QA result: FAIL - ", Sys.time()))
}


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 5: apcd_claim_icdcm_header ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

message(paste0("Beginning creation process for apcd_claim_icdcm_header - ", Sys.time()))
dw_inthealth <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_icdcm_header.R")

### B) Create table
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_icdcm_header.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")

### C) Load tables
system.time(load_stage.apcd_claim_icdcm_header_f())

### D) Table-level QA
system.time(apcd_icdcm_qa <- qa_stage.apcd_claim_icdcm_header_f())

#Process QA results
if(all(c(apcd_icdcm_qa$qa[apcd_icdcm_qa$qa_type=="# members not in elig_demo, expect 0"] == 0
         & apcd_icdcm_qa$qa[apcd_icdcm_qa$qa_type=="# members not in elig_timevar, expect 0"] == 0
         & apcd_icdcm_qa$qa[apcd_icdcm_qa$qa_type=="# of null diagnoses, expect 0"] == 0
         & apcd_icdcm_qa$qa[apcd_icdcm_qa$qa_type=="minimum length of ICD-9-CM, expect 5"] == 5
         & apcd_icdcm_qa$qa[apcd_icdcm_qa$qa_type=="maximum length of ICD-9-CM, expect 5"] == 5
         & apcd_icdcm_qa$qa[apcd_icdcm_qa$qa_type=="minimum length of ICD-10-CM, expect >=3"] >= 3
         & apcd_icdcm_qa$qa[apcd_icdcm_qa$qa_type=="maximum length of ICD-10-CM, expect <=7"] >= 7))) {
  message(paste0("apcd_claim_icdcm_header QA result: PASS - ", Sys.time()))
} else {
  stop(paste0("apcd_claim_icdcm_header QA result: FAIL - ", Sys.time()))
}


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 6: apcd_claim_procedure ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

message(paste0("Beginning creation process for apcd_claim_procedure - ", Sys.time()))

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_procedure.R")

### B) Create table
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_procedure.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")

### C) Load tables
system.time(load_stage.apcd_claim_procedure_f())

### D) Table-level QA
system.time(apcd_procedure_qa <- qa_stage.apcd_claim_procedure_f())

#Process QA results
if(all(c(apcd_procedure_qa$qa[apcd_procedure_qa$qa_type=="# members not in elig_demo, expect 0"] == 0
         & apcd_procedure_qa$qa[apcd_procedure_qa$qa_type=="# members not in elig_timevar, expect 0"] == 0
         & apcd_procedure_qa$qa[apcd_procedure_qa$qa_type=="# of null procedure codes, expect 0"] == 0))) {
  message(paste0("apcd_claim_procedure QA result: PASS - ", Sys.time()))
} else {
  stop(paste0("apcd_claim_procedure QA result: FAIL - ", Sys.time()))
}


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 7: apcd_claim_provider ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

#### CONTINUE HERE ####

message(paste0("Beginning creation process for apcd_claim_provider - ", Sys.time()))

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_provider.R")

### B) Create table
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_provider.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")

### C) Load tables
system.time(load_stage.apcd_claim_provider_f())

### D) Table-level QA
#system.time(apcd_provider_qa <- qa_stage.apcd_claim_provider_f()) - no QA needed as no transformation is done at this stage


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 8: ref.apcd_provider_npi ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

message(paste0("Beginning creation process for ref.apcd_provider_npi - ", Sys.time()))

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.apcd_provider_npi.R")

### B) Create table
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.apcd_provider_npi.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")

### C) Load tables
system.time(load_ref.apcd_provider_npi_f())

### D) Table-level QA
system.time(apcd_provider_npi_qa <- qa_ref.apcd_provider_npi_f())

##Process QA results

### E) Copy table to HHSAW

### F) Index table on HHSAW


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 9: ref.kc_provider_master ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

message(paste0("Beginning creation process for ref.kc_provider_master - ", Sys.time()))

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.kc_provider_master.R")

### B) Create table
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/load_ref.kc_provider_master.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")

### C) Load tables
system.time(load_ref.kc_provider_master_f())

### D) Table-level QA
system.time(kc_provider_master_qa <- qa_ref.kc_provider_master_f())

##Process QA results

### E) Copy table to HHSAW

### F) Index table on HHSAW


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 10: apcd_claim_header ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

message(paste0("Beginning creation process for apcd_claim_header - ", Sys.time()))

### A) Call in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_header.R")

### B) Create table
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_header.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")

### C) Load tables
system.time(load_stage.apcd_claim_header_f())

### D) Table-level QA (X minutes to run!)
system.time(apcd_claim_header_qa <- qa_stage.apcd_claim_header_f())

##Process QA results


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 11: apcd_claim_ccw ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

message(paste0("Beginning creation process for apcd_claim_ccw - ", Sys.time()))

### A) Create table
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_ccw.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")

### B) Load tables
system.time(load_ccw(server = "phclaims", conn = dw_inthealth, source = c("apcd"),
                     config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_ccw.yaml"))

### C) Table-level QA

#all members should be in elig_demo table
apcd_claim_ccw_qa1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
  "select 'stage.apcd_claim_ccw' as 'table', '# members not in elig_demo, expect 0' as qa_type,
    count(distinct a.id_apcd) as qa
    from stage.apcd_claim_ccw as a
    left join final.apcd_elig_demo as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
  .con = dw_inthealth))

#count conditions run
apcd_claim_ccw_qa2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
  "select 'stage.apcd_claim_ccw' as 'table', '# conditions, expect 31' as qa_type,
  count(distinct ccw_code) as qa
  from PHClaims.stage.apcd_claim_ccw;",
  .con = dw_inthealth))

##Process QA results


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### Table 12: apcd_claim_preg_episode ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

message(paste0("Beginning creation process for apcd_claim_preg_episode - ", Sys.time()))

### A) Call in functions
devtools::source_url("BLANK")

### B) Create table
create_table(conn = dw_inthealth, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/stage/tables/load_stage.apcd_claim_preg_episode.yaml",
             overall = T, ind_yr = F, overwrite = T, server = "kcitazrhpasqlprp16.azds.kingcounty.gov")