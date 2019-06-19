
library("chron")
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

# RAC Codes
file.dir <- "L:/DCHSPHClaimsData/Analyses/Philip/99_Documentation/RDA/"

input <- read.xlsx(paste0(file.dir, "Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS.xlsx"), sheet = 1)
tbl <- Id(schema="tmp", table="Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS-1")
dbWriteTable(db.connection, name=tbl, value=input, overwrite=TRUE)

input <- read.xlsx(paste0(file.dir, "Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS.xlsx"), sheet = 2)
tbl <- Id(schema="tmp", table="Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS-2")
dbWriteTable(db.connection, name=tbl, value=input, overwrite=TRUE)

input <- read.xlsx(paste0(file.dir, "Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS.xlsx"), sheet = 3)
tbl <- Id(schema="tmp", table="Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS-3")
dbWriteTable(db.connection, name=tbl, value=input, overwrite=TRUE)

input <- read.xlsx(paste0(file.dir, "Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS.xlsx"), sheet = 4)
tbl <- Id(schema="tmp", table="Medicaid-RAC-Codes-for-Inclusion-Criteria-and-Grouping DSHS-4")
dbWriteTable(db.connection, name=tbl, value=input, overwrite=TRUE)