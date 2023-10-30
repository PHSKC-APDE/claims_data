#devtools::install_github("PHSKC-APDE/claims_data")
options(max.print = 350, tibble.print_max = 50, scipen = 999)
origin <- "1970-01-01" # Date origin
Sys.setenv(TZ="America/Los_Angeles") # Set Time Zone
library(tidyverse) # Manipulate data
library(data.table) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(RecordLinkage)
library(claims)
library(keyring)
library(openxlsx2)

#Enter credentials for HHSAW
#key_set("hhsaw", username = "eli.kern@kingcounty.gov")
key_list()

#Establish connection to HHSAW
db_hhsaw <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433",
                           database = "hhs_analytics_workspace",
                           uid = keyring::key_list("hhsaw")[["username"]],
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")

#File path for export
export_path <- "//phcifs.ph.lcl/SFTP_DATA/APDEDataExchange/UW_Dugan_Team/Staging/"


####Step 1: Bring in metadata from SQL Server ####

#Table and column metadata
col_meta <- dbGetQuery(
  conn = db_hhsaw, "
select table_schema, table_name, column_name, ordinal_position,
case
	when data_type = 'varchar' then data_type + '(' + cast(CHARACTER_MAXIMUM_LENGTH as varchar(255)) + ')'
	when data_type = 'numeric' then data_type + '(' + cast(NUMERIC_PRECISION as varchar(255)) + ',' + cast(NUMERIC_SCALE as varchar(255)) + ')'
	else data_type
end as data_type
from INFORMATION_SCHEMA.COLUMNS
where table_schema in ('claims', 'ref') and
table_name in ('tmp_ek_mcaid_elig_timevar', 'tmp_ek_mcaid_elig_demo', 'tmp_ek_mcaid_claim_procedure',
	'tmp_ek_mcaid_claim_pharm', 'tmp_ek_mcaid_claim_line', 'tmp_ek_mcaid_claim_icdcm_header',
	'tmp_ek_mcaid_claim_header', 'tmp_ek_mcaid_claim_ccw', 'tmp_ek_mcaid_claim_bh',
	'ref_date', 'icdcm_codes', 'ref_geo_kc_zip', 'ref_kc_claim_type', 'ref_mcaid_rac_code', 'ref_mco')
order by table_schema, table_name, ordinal_position;")

#Remove tmp_ek_ prefix from table names
col_meta <- col_meta %>% mutate(table_name = str_replace_all(table_name, "tmp_ek_", ""))

#Table row counts
row_meta <- dbGetQuery(
  conn = db_hhsaw, "
select s.Name AS table_schema, t.NAME AS table_name,
    max(p.rows) AS row_count_sql --I'm taking max here because an index that is not on all rows creates two entries in this summary table
from sys.tables t
inner join sys.indexes i on t.OBJECT_ID = i.object_id
inner join sys.partitions p on i.object_id = p.OBJECT_ID and i.index_id = p.index_id
left outer join sys.schemas s ON t.schema_id = s.schema_id
where s.Name in ('claims', 'ref') and
	t.Name in ('tmp_ek_mcaid_elig_timevar', 'tmp_ek_mcaid_elig_demo', 'tmp_ek_mcaid_claim_procedure',
	'tmp_ek_mcaid_claim_pharm', 'tmp_ek_mcaid_claim_line', 'tmp_ek_mcaid_claim_icdcm_header',
	'tmp_ek_mcaid_claim_header', 'tmp_ek_mcaid_claim_ccw', 'tmp_ek_mcaid_claim_bh',
	'ref_date', 'icdcm_codes', 'ref_geo_kc_zip', 'ref_kc_claim_type', 'ref_mcaid_rac_code', 'ref_mco')
group by s.Name, t.Name; ")

#Remove tmp_ek_ prefix from table names
row_meta <- row_meta %>% mutate(table_name = str_replace_all(table_name, "tmp_ek_", ""))


####Step 2: Bring in log file saved from Visual Studio SSIS package ####

#Read in raw text file
log_raw <- read_table("//phcifs.ph.lcl/SFTP_DATA/APDEDataExchange/UW_Dugan_Team/Staging/log_2023-01-20.txt", 
                  col_names = FALSE, skip = 1)

#Subset to rows with row counts and needed columns
log <- log_raw %>%
  filter(str_detect(X9, "rows")) %>%
  rename(table_name = X6, row_count_exported = X8) %>%
  mutate(
    table_name = str_trim(str_replace_all(table_name, "\"", ""), side = c("both")),
    row_count_exported = as.integer(row_count_exported)) %>%
  select(table_name, row_count_exported)

#Join SSIS exported row counts to SQL table row counts
row_count_qa <- left_join(row_meta, log, by = "table_name")

#Test for row count match
row_count_qa <- row_count_qa %>% mutate(row_count_match = case_when(row_count_sql == row_count_exported ~ 1, TRUE ~ 0))
count(row_count_qa, row_count_match)


####Step 3: Export metadata ####
data <- list(col_meta, row_count_qa)
sheet <- list("table_column_formats", "table_row_counts")
filename <- paste0(export_path, "p1_tables_metadata_2023-10-26.xlsx")
write_xlsx(data, file = filename, sheetName = sheet)