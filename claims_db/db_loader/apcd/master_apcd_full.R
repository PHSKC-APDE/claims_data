#### MASTER CODE TO RUN A FULL APCD DATA REFRESH
#
# Loads and QAs new raw data to load_raw schema
# Loads and QAs new reference tables to ref schema
# Changes schema of existing stage tables to archive
# Changes schema of new load_raw tables to stage
# Adds clustered columnstore indexes to new stage tables
#
# Eli Kern, PHSKC (APDE)
# Adapted from Alastair Matheson's Medicaid script
#
# 2019-10

#2020-08 modification for extract 249: Added new tables that were created from medical_claim table to facilitate Enclave export
#2021-01 - no modifications needed for extract 277 (same format as extract 249)


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, glue)

db_claims <- dbConnect(odbc(), "PHClaims51")

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/load_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/alter_schema.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/qa_load_sql.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/claim_ccw.R")

## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### STEP 1: Load and QA new raw data to load_raw schema, and reference tables to ref schema ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

#### LOAD_RAW ICDCM ####
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_claim_icdcm_raw_full.yaml",
               overall = T,
               ind_yr = T,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_claim_icdcm_raw_full.R")

system.time(load_load_raw.apcd_icdcm_full_f(etl_date_min = "2014-01-01",
                                            etl_date_max = "2020-06-30",
                                            etl_delivery_date = "2021-01-20", 
                                            etl_note = "Full refresh of APCD data using extract 277"))


#### LOAD_RAW CLAIM_LINE ####
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_claim_line_raw_full.yaml",
               overall = T,
               ind_yr = T,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_claim_line_raw_full.R")

system.time(load_load_raw.apcd_claim_line_full_f(etl_date_min = "2014-01-01",
                                                 etl_date_max = "2020-06-30",
                                                 etl_delivery_date = "2021-01-20", 
                                                 etl_note = "Full refresh of APCD data using extract 277"))


#### LOAD_RAW PROCEDURE ####
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_claim_procedure_raw_full.yaml",
               overall = T,
               ind_yr = T,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_claim_procedure_raw_full.R")

system.time(load_load_raw.apcd_procedure_full_f(etl_date_min = "2014-01-01",
                                                etl_date_max = "2020-06-30",
                                                etl_delivery_date = "2021-01-20", 
                                                etl_note = "Full refresh of APCD data using extract 277"))


#### LOAD_RAW CLAIM_PROVIDER ####
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_claim_provider_raw_full.yaml",
               overall = T,
               ind_yr = T,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_claim_provider_raw_full.R")

system.time(load_load_raw.apcd_claim_provider_full_f(etl_date_min = "2014-01-01",
                                                     etl_date_max = "2020-06-30",
                                                     etl_delivery_date = "2021-01-20", 
                                                     etl_note = "Full refresh of APCD data using extract 277"))


#### LOAD_RAW DENTAL CLAIMS ####
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_dental_claim_full.yaml",
               overall = T,
               ind_yr = T,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_dental_claim_full.R")

system.time(load_load_raw.apcd_dental_claim_full_f(etl_date_min = "2014-01-01",
                                                   etl_date_max = "2020-06-30",
                                                   etl_delivery_date = "2021-01-20", 
                                                   etl_note = "Full refresh of APCD data using extract 277"))


#### LOAD_RAW ELIGIBILITY ####
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_eligibility_full.yaml",
               overall = T,
               ind_yr = T,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_eligibility_full.R")

system.time(load_load_raw.apcd_eligibility_full_f(etl_date_min = "2014-01-01",
                                                  etl_date_max = "2020-06-30",
                                                  etl_delivery_date = "2021-01-20", 
                                                  etl_note = "Full refresh of APCD data using extract 277"))


#### LOAD_RAW MEDICAL_CLAIM_HEADER ####
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_medical_claim_header_full.yaml",
               overall = T,
               ind_yr = T,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_medical_claim_header_full.R")

system.time(load_load_raw.apcd_medical_claim_header_full_f(etl_date_min = "2014-01-01",
                                                           etl_date_max = "2020-06-30",
                                                           etl_delivery_date = "2021-01-20", 
                                                           etl_note = "Full refresh of APCD data using extract 277"))


#### LOAD_RAW MEMBER_MONTH_DETAIL ####
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_member_month_detail_full.yaml",
               overall = T,
               ind_yr = T,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_member_month_detail_full.R")

system.time(load_load_raw.apcd_member_month_detail_full_f(etl_date_min = "2014-01-01",
                                                          etl_date_max = "2020-06-30",
                                                          etl_delivery_date = "2021-01-20", 
                                                          etl_note = "Full refresh of APCD data using extract 277"))


#### LOAD_RAW PHARMACY_CLAIM ####
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_pharmacy_claim_full.yaml",
               overall = T,
               ind_yr = T,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_pharmacy_claim_full.R")

system.time(load_load_raw.apcd_pharmacy_claim_full_f(etl_date_min = "2014-01-01",
                                                     etl_date_max = "2020-06-30",
                                                     etl_delivery_date = "2021-01-20", 
                                                     etl_note = "Full refresh of APCD data using extract 277"))


#### LOAD_RAW PROVIDER ####
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_provider_full.yaml",
               overall = T,
               ind_yr = T,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_provider_full.R")

system.time(load_load_raw.apcd_provider_full_f(etl_date_min = "2014-01-01",
                                               etl_date_max = "2020-06-30",
                                               etl_delivery_date = "2021-01-20", 
                                               etl_note = "Full refresh of APCD data using extract 277"))

#### LOAD_RAW PROVIDER_MASTER ####
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_provider_master_full.yaml",
               overall = T,
               ind_yr = F,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_provider_master_full.R")

system.time(load_load_raw.apcd_provider_master_full_f(etl_date_min = "2014-01-01",
                                                      etl_date_max = "2020-06-30",
                                                      etl_delivery_date = "2021-01-20", 
                                                      etl_note = "Full refresh of APCD data using extract 277"))


#### LOAD_RAW PROVIDER_PRACTICE_ROSTER ####
### Create tables
create_table_f(conn = db_claims, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_provider_practice_roster_full.yaml",
               overall = T,
               ind_yr = F,
               overwrite = T,
               test_mode = F)

### Load tables
# Call in function
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/load_raw/tables/load_load_raw.apcd_provider_practice_roster_full.R")

system.time(load_load_raw.apcd_provider_practice_roster_full_f(etl_date_min = "2014-01-01",
                                                               etl_date_max = "2020-06-30",
                                                               etl_delivery_date = "2021-01-20", 
                                                               etl_note = "Full refresh of APCD data using extract 277"))


#### REF APCD REFERENCE TABLES ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.apcd_reference_tables_full.R")

system.time(load_ref.apcd_reference_tables_full_f())


#### QA ALL TABLES ####
# Eventually incorporate this in to the load function for each table, as Alastair does
qa_result <- odbc::dbGetQuery(db_claims,
                              glue::glue_sql(
                                "select s.Name AS schema_name, t.NAME AS table_name, 
                    	max(p.rows) AS row_count, --I'm taking max here because an index that is not on all rows creates two entries in this summary table
                        max(p.rows)/1000000 as row_count_million,
                    	count(c.COLUMN_NAME) as col_count,
                        cast(round(((sum(a.used_pages) * 8) / 1024.00), 2) as numeric(36, 2)) as used_space_mb, 
                    	    cast(round(((sum(a.used_pages) * 8) / 1024.00 / 1024.00), 2) as numeric(36, 2)) as used_space_gb
                    from sys.tables t
                    inner join sys.indexes i on t.OBJECT_ID = i.object_id
                    inner join sys.partitions p on i.object_id = p.OBJECT_ID and i.index_id = p.index_id
                    inner join sys.allocation_units a on p.partition_id = a.container_id
                    left outer join sys.schemas s on t.schema_id = s.schema_id
                    left join information_schema.columns c on t.name = c.TABLE_NAME and s.name = c.TABLE_SCHEMA
                    where t.NAME NOT LIKE 'dt%' and t.is_ms_shipped = 0 and i.OBJECT_ID > 255
                    	and left(t.name, 4) = 'apcd' and s.name in ('load_raw', 'ref')
                    group by s.Name, t.Name
                    order by schema_name, table_name;",
                                .con = db_claims))

#export
write_csv(qa_result, "//kcitsqlutpdbh51/ImportData/Data/APCD_data_import/qa_result.csv")


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### STEP 2: Change schema of existing stage tables to archive schema ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "apcd_claim_icdcm_raw")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "apcd_claim_line_raw")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "apcd_claim_procedure_raw")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "apcd_claim_provider_raw")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "apcd_dental_claim")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "apcd_eligibility")
#alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "apcd_medical_claim")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "apcd_medical_claim_header")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "apcd_member_month_detail")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "apcd_pharmacy_claim")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "apcd_provider")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "apcd_provider_master")
alter_schema_f(conn = db_claims, from_schema = "stage", to_schema = "archive", table_name = "apcd_provider_practice_roster")


## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### STEP 3: Change schema of new load_raw tables to stage schema ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

alter_schema_f(conn = db_claims, from_schema = "load_raw", to_schema = "stage", table_name = "apcd_claim_icdcm_raw")
alter_schema_f(conn = db_claims, from_schema = "load_raw", to_schema = "stage", table_name = "apcd_claim_line_raw")
alter_schema_f(conn = db_claims, from_schema = "load_raw", to_schema = "stage", table_name = "apcd_claim_procedure_raw")
alter_schema_f(conn = db_claims, from_schema = "load_raw", to_schema = "stage", table_name = "apcd_claim_provider_raw")
alter_schema_f(conn = db_claims, from_schema = "load_raw", to_schema = "stage", table_name = "apcd_dental_claim")
alter_schema_f(conn = db_claims, from_schema = "load_raw", to_schema = "stage", table_name = "apcd_eligibility")
#alter_schema_f(conn = db_claims, from_schema = "load_raw", to_schema = "stage", table_name = "apcd_medical_claim")
alter_schema_f(conn = db_claims, from_schema = "load_raw", to_schema = "stage", table_name = "apcd_medical_claim_header")
alter_schema_f(conn = db_claims, from_schema = "load_raw", to_schema = "stage", table_name = "apcd_member_month_detail")
alter_schema_f(conn = db_claims, from_schema = "load_raw", to_schema = "stage", table_name = "apcd_pharmacy_claim")
alter_schema_f(conn = db_claims, from_schema = "load_raw", to_schema = "stage", table_name = "apcd_provider")
alter_schema_f(conn = db_claims, from_schema = "load_raw", to_schema = "stage", table_name = "apcd_provider_master")
alter_schema_f(conn = db_claims, from_schema = "load_raw", to_schema = "stage", table_name = "apcd_provider_practice_roster")

## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##
#### STEP 4: Create clustered columnstore indexes on each new stage table ####
## -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ##

system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_apcd_claim_icdcm_raw on stage.apcd_claim_icdcm_raw")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_apcd_claim_line_raw on stage.apcd_claim_line_raw")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_apcd_claim_procedure_raw on stage.apcd_claim_procedure_raw")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_apcd_claim_provider_raw on stage.apcd_claim_provider_raw")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_apcd_dental_claim on stage.apcd_dental_claim")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_apcd_eligibility on stage.apcd_eligibility")))
#system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_apcd_medical_claim on stage.apcd_medical_claim")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_apcd_medical_claim_header on stage.apcd_medical_claim_header")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_apcd_member_month_detail on stage.apcd_member_month_detail")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_apcd_pharmacy_claim on stage.apcd_pharmacy_claim")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_apcd_provider on stage.apcd_provider")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_apcd_provider_master on stage.apcd_provider_master")))
system.time(dbSendQuery(conn = db_claims, glue_sql("create clustered columnstore index idx_ccs_stage_apcd_provider_practice_roster on stage.apcd_provider_practice_roster")))