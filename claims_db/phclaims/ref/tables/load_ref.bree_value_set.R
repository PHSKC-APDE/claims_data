
#library("chron")
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

file.dir <- "C:/Users/xxx/github/claims_data/claims_db/phclaims/ref/tables_data/"

input <- read.xlsx(paste0(file.dir, "Bree-Opioid-NDC-2017-include.xlsx"), sheet = 1)
tbl <- Id(schema="tmp", table="Bree_Opioid_NDC_2017_include")
dbWriteTable(db.connection, name=tbl, value=input, overwrite=TRUE)

input <- read.xlsx(paste0(file.dir, "Bree-Opioid-NDC-2017-exclude.xlsx"), sheet = 1)
tbl <- Id(schema="tmp", table="Bree_Opioid_NDC_2017_exclude")
dbWriteTable(db.connection, name=tbl, value=input, overwrite=TRUE)
