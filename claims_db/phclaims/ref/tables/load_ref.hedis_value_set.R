
library("dplyr")
library("dbplyr")
library("DBI")
library("odbc")
library("tidyr")
library("glue")
library("devtools")
#library("medicaid")
library("openxlsx")
library("lubridate")
library("janitor")

dsn <- "PHClaims"
db.connection <- dbConnect(odbc(), dsn)

# # HEDIS Measures 2018
# file.dir <- "L:/DCHSPHClaimsData/References/HEDIS/2018/"
# 
# input <- read.xlsx(paste0(file.dir, "2018 Volume 2 Value Set Directory 07_03_2017.xlsx"), sheet = 2)
# dbWriteTable(db.connection, "tmp_2018 Volume 2 Value Set Directory 07_03_2017-2.xlsx", input, overwrite=TRUE)
# 
# input <- read.xlsx(paste0(file.dir, "2018 Volume 2 Value Set Directory 07_03_2017.xlsx"), sheet = 3)
# dbWriteTable(db.connection, "tmp_2018 Volume 2 Value Set Directory 07_03_2017-3.xlsx", input, overwrite=TRUE)
# 
# input <- read.xlsx(paste0(file.dir, "2018 Volume 2 Value Set Directory 07_03_2017.xlsx"), sheet = 4)
# dbWriteTable(db.connection, "tmp_2018 Volume 2 Value Set Directory 07_03_2017-4.xlsx", input, overwrite=TRUE)
# 
# input <- read.xlsx(paste0(file.dir, "2018 Volume 2 Value Set Directory 07_03_2017.xlsx"), sheet = 5)
# dbWriteTable(db.connection, "tmp_2018 Volume 2 Value Set Directory 07_03_2017-5.xlsx", input, overwrite=TRUE)
# 
# # HEDIS Medications 2018
# file.dir <- "L:/DCHSPHClaimsData/References/HEDIS/2018/"
# 
# input <- read.xlsx(paste0(file.dir, "20180208_HEDIS_NDC_MLD_CompleteDirectory_Workbook_2018.xlsx"), sheet = 2)
# dbWriteTable(db.connection, "tmp_20180208_HEDIS_NDC_MLD_CompleteDirectory_Workbook_2018-2.xlsx", input, overwrite=TRUE)
# 
# input <- read.xlsx(paste0(file.dir, "20180208_HEDIS_NDC_MLD_CompleteDirectory_Workbook_2018.xlsx"), sheet = 3)
# dbWriteTable(db.connection, "tmp_20180208_HEDIS_NDC_MLD_CompleteDirectory_Workbook_2018-3.xlsx", input, overwrite=TRUE)

# HEDIS Measures 2019
file.dir <- "L:/DCHSPHClaimsData/HEDIS/2019/HEDIS_2019_Volume_2_11.06.18/"

input <- read.xlsx(paste0(file.dir, "M. HEDIS 2019 Volume 2 VSD 11_05_2018.xlsx"), sheet = 2)
tbl <- Id(schema="tmp", table="HEDIS_2019_Volume_2_VSD_11_05_2018-2.xlsx")
dbWriteTable(db.connection, name=tbl, value=input, overwrite=TRUE)

input <- read.xlsx(paste0(file.dir, "M. HEDIS 2019 Volume 2 VSD 11_05_2018.xlsx"), sheet = 3)
tbl <- Id(schema="tmp", table="HEDIS_2019_Volume_2_VSD_11_05_2018-3.xlsx")
dbWriteTable(db.connection, name=tbl, value=input, overwrite=TRUE)

input <- read.xlsx(paste0(file.dir, "M. HEDIS 2019 Volume 2 VSD 11_05_2018.xlsx"), sheet = 4)
tbl <- Id(schema="tmp", table="HEDIS_2019_Volume_2_VSD_11_05_2018-4.xlsx")
dbWriteTable(db.connection, name=tbl, value=input, overwrite=TRUE)

input <- read.xlsx(paste0(file.dir, "M. HEDIS 2019 Volume 2 VSD 11_05_2018.xlsx"), sheet = 5)
tbl <- Id(schema="tmp", table="HEDIS_2019_Volume_2_VSD_11_05_2018-5.xlsx")
dbWriteTable(db.connection, name=tbl, value=input, overwrite=TRUE)

library("dplyr")
library("dbplyr")
library("DBI")
library("odbc")
library("tidyr")
library("glue")
library("devtools")
#library("medicaid")
library("openxlsx")
library("lubridate")
library("janitor")

dsn <- "PHClaims"
db.connection <- dbConnect(odbc(), dsn)

# HEDIS Medications 2019
file.dir <- "L:/DCHSPHClaimsData/HEDIS/2019/Medications List Directory/"

input <- read.xlsx(paste0(file.dir, "HEDIS-2019-NDC-MLD-Directory-Complete-Workbook-FINAL-11-1-2018-2.xlsx"), sheet = 2)
tbl <- Id(schema="tmp", table="HEDIS_2019_NDC_MLD_Directory-2.xlsx")
dbWriteTable(db.connection, name=tbl, value=input, overwrite=TRUE)

input <- read.xlsx(paste0(file.dir, "HEDIS-2019-NDC-MLD-Directory-Complete-Workbook-FINAL-11-1-2018-2.xlsx"), sheet = 3)
tbl <- Id(schema="tmp", table="HEDIS_2019_NDC_MLD_Directory-3.xlsx")
dbWriteTable(db.connection, name=tbl, value=input, overwrite=TRUE)

input <- read.xlsx(paste0(file.dir, "HEDIS-2019-NDC-MLD-Directory-Complete-Workbook-FINAL-11-1-2018-2.xlsx"), sheet = 4)
tbl <- Id(schema="tmp", table="HEDIS_2019_NDC_MLD_Directory-4.xlsx")
dbWriteTable(db.connection, name=tbl, value=input, overwrite=TRUE)
