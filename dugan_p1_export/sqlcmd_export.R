#Script to export claims data tables from HHSAW to CIFS folder for ASTHO-funded tobacco project
#Run this from a VM to prevent VPN/network disconnects
#Will take a few/several hours to run

#devtools::install_github("PHSKC-APDE/claims_data")
options(max.print = 350, tibble.print_max = 50, scipen = 999)
origin <- "1970-01-01" # Date origin
Sys.setenv(TZ="America/Los_Angeles") # Set Time Zone
pacman::p_load(tidyverse, odbc, openxlsx2, rlang, glue, keyring)

#Enter credentials for HHSAW
#key_set("hhsaw", username = "eli.kern@kingcounty.gov"

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

#### Step 1: Set lists of tables ####
claims_schema_tables <- list(
  "tmp_ek_mcaid_elig_timevar",
  "tmp_ek_mcaid_elig_demo",
  "tmp_ek_mcaid_claim_procedure",
  "tmp_ek_mcaid_claim_pharm",
  "tmp_ek_mcaid_claim_line",
  "tmp_ek_mcaid_claim_icdcm_header",
  "tmp_ek_mcaid_claim_header",
  "tmp_ek_mcaid_claim_ccw",
  "tmp_ek_mcaid_claim_bh",
  "ref_date",
  "ref_geo_kc_zip",
  "ref_kc_claim_type",
  "ref_mcaid_rac_code",
  "ref_mco")

ref_schema_tables <- list("icdcm_codes")


#### Step 2: Set fixed parameters for sqlcmd utility export ####
#Note that bcp is faster (although sqlcmd also removes trailing white space and converts control characters to single spaces)
  #but column headings cannot be exported with bcp
server <-  "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433"
database <- "hhs_analytics_workspace"
user <- keyring::key_list("hhsaw")[["username"]]
pass <- keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]])


#### Step 3: Export all claims schema tables ####
# Add a [1] after claims_schema_tables in lapply statement to test on 1 table

system.time(lapply(claims_schema_tables, function(x) {
  
  #Set schema and table
  schema <- "claims"
  table <- x
  
  #Set up string containing column headings for binding to output within sqlcmd command
  table_columns <- dbGetQuery(conn = db_hhsaw, statement = 
  glue("select column_name, ordinal_position from information_schema.columns
  where table_schema = '{schema}' and table_name = '{table}'
  order by ordinal_position;")) 
  
  table_column_string <- paste("'",as.character(table_columns$column_name),"'",collapse=", ",sep="")
  
  #Set up sqlcmd utility arguments
  #https://learn.microsoft.com/en-us/sql/tools/sqlcmd/sqlcmd-utility?view=sql-server-ver16&tabs=odbc%2Cwindows&pivots=cs1-bash
  sqlcmd_args <- c(glue('-S "{server}" -d {database} ',
                        '-G -U {user} -P {pass} -C -N -M ',
                        '-s, -W -k2 -h -1 ',
                        '-Q "set nocount on; select {table_column_string}; select * from {schema}.{table};" ',
                        '-o "{export_path}{table}.csv"'))
  
  # Run sqlcmd command
  system.time(system2(command = "sqlcmd", args = c(sqlcmd_args), stdout = TRUE, stderr = TRUE))
}))


#### Step 4: Export all ref schema tables ####
# Add a [1] after ref_schema_tables in lapply statement to test on 1 table

system.time(lapply(ref_schema_tables, function(x) {
  
  #Set schema and table
  schema <- "ref"
  table <- x
  
  #Set up string containing column headings for binding to output within sqlcmd command
  table_columns <- dbGetQuery(conn = db_hhsaw, statement = 
                                glue("select column_name, ordinal_position from information_schema.columns
  where table_schema = '{schema}' and table_name = '{table}'
  order by ordinal_position;")) 
  
  table_column_string <- paste("'",as.character(table_columns$column_name),"'",collapse=", ",sep="")
  
  #Set up sqlcmd utility arguments
  #https://learn.microsoft.com/en-us/sql/tools/sqlcmd/sqlcmd-utility?view=sql-server-ver16&tabs=odbc%2Cwindows&pivots=cs1-bash
  sqlcmd_args <- c(glue('-S "{server}" -d {database} ',
                        '-G -U {user} -P {pass} -C -N -M ',
                        '-s, -W -k2 -h -1 ',
                        '-Q "set nocount on; select {table_column_string}; select * from {schema}.{table};" ',
                        '-o "{export_path}ref_{table}.csv"'))
  
  # Run sqlcmd command
  system.time(system2(command = "sqlcmd", args = c(sqlcmd_args), stdout = TRUE, stderr = TRUE))
}))


#### Step 5: Should I count rows in exported files? ####