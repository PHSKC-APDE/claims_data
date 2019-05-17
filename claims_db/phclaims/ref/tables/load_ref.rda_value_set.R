
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

dsn <- "Analytics"
#dsn <- "PHClaims"
db.connection <- dbConnect(odbc(), dsn)

##### MENTAL HEALTH MEASURES
mh.file.dir <- "L:/DCHSPHClaimsData/References/RDA_measures/MHSP-metric-specification-20180430/"

input <- read.xlsx(paste0(mh.file.dir, "MH-Dx-value-set-ICD9-10.xlsx"), sheet = 1)
dbWriteTable(db.connection, "tmp_MH-Dx-value-set-ICD9-10.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(mh.file.dir, "MH-procedure-value-set_20180918.xlsx"), sheet = 1)
dbWriteTable(db.connection, "tmp_MH-procedure-value-set_20180918.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(mh.file.dir, "MH-procedure-with-Dx-value-set_20180920.xlsx"), sheet = 1)
dbWriteTable(db.connection, "tmp_MH-procedure-with-Dx-value-set_20180920.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(mh.file.dir, "MH-Rx-value-set-20180430.xlsx"), sheet = 1)
dbWriteTable(db.connection, "tmp_MH-Rx-value-set-20180430.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(mh.file.dir, "MH-taxonomy-value-set.xlsx"), sheet = 1)
dbWriteTable(db.connection, "tmp_MH-taxonomy-value-set.xlsx", input, overwrite=TRUE)

##### SUD MEASURES
sud.file.dir <- "L:/DCHSPHClaimsData/References/RDA_measures/Old SUD Tx Pen measure/"

input <- read.xlsx(paste0(sud.file.dir, "SUD-Tx-Pen-Value-Set-1.xlsx"), sheet = 1)
dbWriteTable(db.connection, "tmp_SUD-Tx-Pen-Value-Set-1.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(sud.file.dir, "SUD-Tx-Pen-Value-Set-2.xlsx"), sheet = 1)
dbWriteTable(db.connection, "tmp_SUD-Tx-Pen-Value-Set-2-1.xlsx", input, overwrite=TRUE)
input <- read.xlsx(paste0(sud.file.dir, "SUD-Tx-Pen-Value-Set-2.xlsx"), sheet = 2)
dbWriteTable(db.connection, "tmp_SUD-Tx-Pen-Value-Set-2-2.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(sud.file.dir, "SUD-Tx-Pen-Value-Set-3.xlsx"), sheet = 1)
dbWriteTable(db.connection, "tmp_SUD-Tx-Pen-Value-Set-3-1.xlsx", input, overwrite=TRUE)
input <- read.xlsx(paste0(sud.file.dir, "SUD-Tx-Pen-Value-Set-3.xlsx"), sheet = 2)
dbWriteTable(db.connection, "tmp_SUD-Tx-Pen-Value-Set-3-2.xlsx", input, overwrite=TRUE)
input <- read.xlsx(paste0(sud.file.dir, "SUD-Tx-Pen-Value-Set-3.xlsx"), sheet = 3)
dbWriteTable(db.connection, "tmp_SUD-Tx-Pen-Value-Set-3-3.xlsx", input, overwrite=TRUE)
input <- read.xlsx(paste0(sud.file.dir, "SUD-Tx-Pen-Value-Set-3.xlsx"), sheet = 4)
dbWriteTable(db.connection, "tmp_SUD-Tx-Pen-Value-Set-3-4.xlsx", input, overwrite=TRUE)
input <- read.xlsx(paste0(sud.file.dir, "SUD-Tx-Pen-Value-Set-3.xlsx"), sheet = 5)
dbWriteTable(db.connection, "tmp_SUD-Tx-Pen-Value-Set-3-5.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(sud.file.dir, "SUD-Tx-Pen-Value-Set-3_20180928.xlsx"), sheet = 1)
input <- mutate(input, FROM_DATE=as.Date(FROM_DATE-2, origin="1900-01-01"), TO_DATE=as.Date(TO_DATE-2, origin="1900-01-01"))
dbWriteTable(db.connection, "tmp_SUD-Tx-Pen-Value-Set-3-1_20180928.xlsx", input, overwrite=TRUE)
input <- read.xlsx(paste0(sud.file.dir, "SUD-Tx-Pen-Value-Set-3_20180928.xlsx"), sheet = 2)
input <- mutate(input, FROM_DATE=as.Date(FROM_DATE-2, origin="1900-01-01"), TO_DATE=as.Date(TO_DATE-2, origin="1900-01-01"))
dbWriteTable(db.connection, "tmp_SUD-Tx-Pen-Value-Set-3-2_20180928.xlsx", input, overwrite=TRUE)
input <- read.xlsx(paste0(sud.file.dir, "SUD-Tx-Pen-Value-Set-3_20180928.xlsx"), sheet = 3)
input <- mutate(input, FROM_DATE=as.Date(FROM_DATE-2, origin="1900-01-01"), TO_DATE=as.Date(TO_DATE-2, origin="1900-01-01"))
dbWriteTable(db.connection, "tmp_SUD-Tx-Pen-Value-Set-3-3_20180928.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(sud.file.dir, "SUD-Tx-Pen-Value-Set-4.xlsx"), sheet = 1)
dbWriteTable(db.connection, "tmp_SUD-Tx-Pen-Value-Set-4.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(sud.file.dir, "SUD-Tx-Pen-Value-Set-5.xlsx"), sheet = 1)
dbWriteTable(db.connection, "tmp_SUD-Tx-Pen-Value-Set-5.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(sud.file.dir, "SUD-Tx-Pen-Value-Set-6.xlsx"), sheet = 1)
dbWriteTable(db.connection, "tmp_SUD-Tx-Pen-Value-Set-6.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(sud.file.dir, "SUD-Tx-Pen-Value-Set-7.xlsx"), sheet = 1)
dbWriteTable(db.connection, "tmp_SUD-Tx-Pen-Value-Set-7.xlsx", input, overwrite=TRUE)

##### OUD MEASURES
oud.file.dir <- "L:/DCHSPHClaimsData/References/RDA_measures/"

input <- read.xlsx(paste0(oud.file.dir, "OUD-Tx-Pen-Value-Set-1.xlsx"), sheet = 1)
dbWriteTable(db.connection, "tmp_OUD-Tx-Pen-Value-Set-1.xlsx", input, overwrite=TRUE)

input <- read.xlsx(paste0(oud.file.dir, "OUD-Tx-Pen-Value-Set-2.xlsx"), sheet = 1)
dbWriteTable(db.connection, "tmp_OUD-Tx-Pen-Value-Set-2-1.xlsx", input, overwrite=TRUE)
input <- read.xlsx(paste0(oud.file.dir, "OUD-Tx-Pen-Value-Set-2.xlsx"), sheet = 2)
dbWriteTable(db.connection, "tmp_OUD-Tx-Pen-Value-Set-2-2.xlsx", input, overwrite=TRUE)
input <- read.xlsx(paste0(oud.file.dir, "OUD-Tx-Pen-Value-Set-2.xlsx"), sheet = 3)
dbWriteTable(db.connection, "tmp_OUD-Tx-Pen-Value-Set-2-3.xlsx", input, overwrite=TRUE)

##### SUD INITIATION-ENGAGEMENT
file.dir <- "L:/DCHSPHClaimsData/References/RDA_measures/"

input <- read.xlsx(paste0(file.dir, "SUD-Treatment-Initiation-and-Engagement.xlsx"), sheet = 3)
dbWriteTable(db.connection, "SUD-Treatment-Initiation-and-Engagement.xlsx", input, overwrite=TRUE)
