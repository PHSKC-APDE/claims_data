
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

#dsn <- "Analytics"
dsn <- "PHClaims"
db.connection <- dbConnect(odbc(), dsn)

file.dir <- "C:/Users/psylling/github/claims_data/claims_db/phclaims/ref/tables_data/"

input <- read.xlsx(paste0(file.dir, "ref.ahrq_value_set.xlsx"), sheet = 1)
tbl <- Id(schema="tmp", table="ref.ahrq_value_set.xlsx")
dbWriteTable(db.connection, name=tbl, value=input, overwrite=TRUE)
