
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
IF OBJECT_ID('[ref].[other_value_set]', 'U') IS NOT NULL
DROP TABLE [ref].[other_value_set];
CREATE TABLE [ref].[other_value_set]
([value_set_group] VARCHAR(200) NOT NULL
,[value_set_name] VARCHAR(200) NOT NULL
,[code_set] VARCHAR(50) NOT NULL
,[code] VARCHAR(20) NOT NULL
,[desc_1] VARCHAR(200) NULL
,CONSTRAINT [pk_other_value_set] PRIMARY KEY CLUSTERED([value_set_name], [code_set], [code]));
", .con = conn)
odbc::dbGetQuery(conn = db.connection, sql)

sql <- glue::glue_sql("
TRUNCATE TABLE [ref].[other_value_set];
", .con = conn)
odbc::dbGetQuery(conn = db.connection, sql)

file.dir <- "C:/Users/psylling/github/claims_data/claims_db/phclaims/ref/tables_data/"

input <- read.xlsx(paste0(file.dir, "ref.other_value_set.xlsx"), sheet = 1)
input <- filter(input, code != "A-to-set-column-as-chr")
input <- input %>% mutate(code = ifelse(code_set=="UBREV", str_pad(code, width=4, side="left", pad="0"), code))
input <- input %>% arrange(value_set_name, code_set, code)

tbl <- Id(schema="ref", table="other_value_set")
dbWriteTable(db.connection, name=tbl, value=input, append=TRUE)
