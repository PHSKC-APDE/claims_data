#### CODE TO SET CURRENT DB CONNECTION
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12


### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_partial.R

### Function elements
# server = whether we are working in HHSAW or PHClaims or other

create_db_connection <- function(server = "phclaims") {
  
  library(odbc) # Read to and write from SQL
  
  if (server == "hhsaw") {
    conn <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = "tcp:kcitazrhpasqldev20.database.windows.net,1433",
                           database = "hhs_analytics_workspace",
                           uid = keyring::key_list("hhsaw_dev")[["username"]],
                           pwd = keyring::key_get("hhsaw_dev", keyring::key_list("hhsaw_dev")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")
  }
  else if (server == "inthealth") {
    conn <- DBI::dbConnect(odbc::odbc(),
                                   driver = "ODBC Driver 17 for SQL Server",
                                   server = "tcp:kcitazrhpasqldev20.database.windows.net,1433",
                                   database = "inthealth_edw",
                                   uid = keyring::key_list("hhsaw_dev")[["username"]],
                                   pwd = keyring::key_get("hhsaw_dev", keyring::key_list("hhsaw_dev")[["username"]]),
                                   Encrypt = "yes",
                                   TrustServerCertificate = "yes",
                                   Authentication = "ActiveDirectoryPassword")
  }
  else {
    conn <- DBI::dbConnect(odbc::odbc(), "PHClaims51")
  }
  return(conn)
}