
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

##### PROVIDER TAXONOMY CODES
file.dir <- "L:/DCHSPHClaimsData/References/Medicare/"

input <- read.xlsx(paste0(file.dir, "CROSSWALK_MEDICARE_PROVIDER_SUPPLIER_to_HEALTHCARE_PROVIDER_TAXONOMY_CLEANED.xlsx"), sheet = 2)
dbWriteTable(db.connection, "CROSSWALK_MEDICARE_PROVIDER_SUPPLIER_to_HEALTHCARE_PROVIDER_TAXONOMY_CLEANED.xlsx", input, overwrite=TRUE)
