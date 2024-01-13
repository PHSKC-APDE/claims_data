#devtools::install_github("PHSKC-APDE/claims_data")
options(max.print = 350, tibble.print_max = 50, scipen = 999)
origin <- "1970-01-01" # Date origin
Sys.setenv(TZ="America/Los_Angeles") # Set Time Zone
pacman::p_load(tidyverse, odbc, openxlsx2, rlang, glue, keyring)

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

#Remove tmp_ek_ prefix from table names and add ref prefix to icdcm_codes
col_meta <- col_meta %>% mutate(table_name = str_replace_all(table_name, "tmp_ek_", ""))
col_meta <- col_meta %>% mutate(table_name = str_replace_all(table_name, "icdcm_codes", "ref_icdcm_codes"))

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

#Remove tmp_ek_ prefix from table names and add ref prefix to icdcm_codes
row_meta <- row_meta %>% mutate(table_name = str_replace_all(table_name, "tmp_ek_", ""))
row_meta <- row_meta %>% mutate(table_name = str_replace_all(table_name, "icdcm_codes", "ref_icdcm_codes"))


####Step 3: Export metadata ####
data <- list(col_meta, row_meta)
sheet <- list("table_column_formats", "table_row_counts")
filename <- paste0(export_path, "p1_tables_metadata_", Sys.Date(), ".xlsx")
write_xlsx(data, file = filename, sheetName = sheet)