
library("dplyr")
library("dbplyr")
library("DBI")
library("odbc")
library("tidyr")
library("glue")
library("devtools")
library("medicaid")
library("openxlsx")
library("lubridate")
library("janitor")

dsn <- "PHClaims"
db.connection <- dbConnect(odbc(), dsn)

# HEDIS Measures
file.dir <- "L:/DCHSPHClaimsData/References/HEDIS/2018/"

input <- read.xlsx(paste0(file.dir, "2018 Volume 2 Value Set Directory 07_03_2017.xlsx"), sheet = 2)
dbWriteTable(db.connection, "tmp_2018 Volume 2 Value Set Directory 07_03_2017-2.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(file.dir, "2018 Volume 2 Value Set Directory 07_03_2017.xlsx"), sheet = 3)
dbWriteTable(db.connection, "tmp_2018 Volume 2 Value Set Directory 07_03_2017-3.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(file.dir, "2018 Volume 2 Value Set Directory 07_03_2017.xlsx"), sheet = 4)
dbWriteTable(db.connection, "tmp_2018 Volume 2 Value Set Directory 07_03_2017-4.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(file.dir, "2018 Volume 2 Value Set Directory 07_03_2017.xlsx"), sheet = 5)
dbWriteTable(db.connection, "tmp_2018 Volume 2 Value Set Directory 07_03_2017-5.xlsx", input, overwrite=TRUE)

# HEDIS Medications
file.dir <- "L:/DCHSPHClaimsData/References/HEDIS/2018/"

input <- read.xlsx(paste0(file.dir, "20180208_HEDIS_NDC_MLD_CompleteDirectory_Workbook_2018.xlsx"), sheet = 2)
dbWriteTable(db.connection, "tmp_20180208_HEDIS_NDC_MLD_CompleteDirectory_Workbook_2018-2.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(file.dir, "20180208_HEDIS_NDC_MLD_CompleteDirectory_Workbook_2018.xlsx"), sheet = 3)
dbWriteTable(db.connection, "tmp_20180208_HEDIS_NDC_MLD_CompleteDirectory_Workbook_2018-3.xlsx", input, overwrite=TRUE)
