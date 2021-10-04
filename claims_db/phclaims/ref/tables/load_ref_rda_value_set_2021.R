
#### Call in libraries ####
library("dplyr")
library("dbplyr")
library("DBI")
library("odbc")
library("openxlsx")
library(readxl) #Read Excel files

library(writexl) #Export Excel files
library("tidyr")
library("glue")
library("devtools")
library("medicaid")

library("lubridate")
library("janitor")

rm(list=ls()) # Clear objects from Memory
cat("\014") # Clear Console

#### Set up Database Connection ####

dsn <- "PHClaims"
db.connection <- dbConnect(odbc(), dsn)

flie_dir <- "L:/DCHSPHClaimsData/P4P Measure code/RDA specs 2021/"
schema_name <- "xphan"

##### Mental Health Service Penetration Measure 
filename1 <- "MHSP-value-sets-2021-03-26.xlsx"

input <- read.xlsx(paste0(flie_dir, filename1), sheet = 1)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_MH_Proc1_MCG261"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename1), sheet = 2)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_MH_Proc2_MCG4947"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename1), sheet = 3)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_MH_Proc3_MCG3117"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename1), sheet = 4)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_MH_Proc4_MCG4491"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename1), sheet = 5)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_MH_Proc5_MCG4948"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename1), sheet = 6)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_MH_Taxonomy_MCG262"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename1), sheet = 7)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_MI_Diagnosis_7MCGs"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename1), sheet = 8)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_Psychotropic_NDC_5MCGs"), value=input, overwrite=TRUE)



##### Substance Use Disorder Treatment Penetration Measure Definition (AOD)

filename2 <- "SUD-Tx-Penetration-Rate-Value-Sets 2021-03-26.xlsx"

input <- read.xlsx(paste0(flie_dir, filename2), sheet = 1)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_SUD_Dx_ValueSet"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename2), sheet = 2)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_SBIRT_Proc_ValueSet_MCG3169"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename2), sheet = 3)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_Detox_ValueSet"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename2), sheet = 4)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_SUD_OP_Tx_Proc_ValueSet_MG3156"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename2), sheet = 5)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_SUD_OST_ValueSet_MCG3148"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename2), sheet = 6)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_SUD_IP_RES_ValueSet"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename2), sheet = 7)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_SUD_ASMT_ValueSet_MCG3149"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename2), sheet = 8)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_SUD_Taxonomy_ValueSet_MCG3170"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename2), sheet = 9)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_proc_w_prim_SUD_Dx_vs_MCG3324"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename2), sheet = 10)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_proc_w_any_SUD_Dx_vs_MCG4881"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename2), sheet = 11)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_MOUD_ValueSet"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename2), sheet = 12)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_MAUD_ValueSet"), value=input, overwrite=TRUE)

##### Opiate Use Disorder Treatment Penetration Measure Definition (OUD)

filename3 <- "OUD-Tx-Penetration-Rate-Value-Sets-2021-04-01.xlsx"

input <- read.xlsx(paste0(flie_dir, filename3), sheet = 1)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_OUD_Dx_ValueSet"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename3), sheet = 2)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_MOUD_NDC_ValueSet"), value=input, overwrite=TRUE)

input <- read.xlsx(paste0(flie_dir, filename3), sheet = 3)
dbWriteTable(db.connection, name=Id(schema=schema_name, table="tmp_MOUD_Procedure_ValueSet"), value=input, overwrite=TRUE)