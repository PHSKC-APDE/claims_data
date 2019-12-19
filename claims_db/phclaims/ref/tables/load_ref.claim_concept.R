
library("chron")
library("dplyr")
library("dbplyr")
library("DBI")
library("odbc")
library("tidyr")
library("glue")
library("devtools")
library("openxlsx")
library("lubridate")
library("janitor")
library("stringr")

# Connect to PHClaims
db.connection <- dbConnect(odbc(), "PHClaims")

sql <- glue::glue_sql("
IF OBJECT_ID('[ref].[claim_concept]') IS NOT NULL
DROP TABLE [ref].[claim_concept];
CREATE TABLE [ref].[claim_concept]
([concept_id] SMALLINT NOT NULL
,[concept_column_name] VARCHAR(255)
,[concept_name] VARCHAR(255)
,[desc] VARCHAR(1000)
,[reference] VARCHAR(255)
,CONSTRAINT [pk_claim_concept_concept_id] PRIMARY KEY CLUSTERED ([concept_id]));
", .con = conn)
odbc::dbGetQuery(conn = db.connection, sql)

sql <- glue::glue_sql("
TRUNCATE TABLE [ref].[claim_concept];
", .con = conn)
odbc::dbGetQuery(conn = db.connection, sql)

file.dir <- "C:/Users/psylling/github/claims_data/claims_db/phclaims/ref/tables_data/"

input <- read.xlsx(paste0(file.dir, "ref.claim_concept.xlsx"), sheet = 1)
input <- input %>% arrange(concept_id)

tbl <- Id(schema="ref", table="claim_concept")
dbWriteTable(db.connection, name=tbl, value=input, append=TRUE)
