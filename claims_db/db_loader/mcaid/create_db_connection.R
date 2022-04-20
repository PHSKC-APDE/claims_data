#### CODE TO SET CURRENT DB CONNECTION
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12


### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_partial.R

### Function elements
# server = whether we are working in HHSAW or PHClaims or other

create_db_connection <- function(server = c("phclaims", "hhsaw", "inthealth"),
                                 prod = T,
                                 interactive = F) {
  
  server <- match.arg(server)
  
  if (server == "hhsaw") {
    db_name <- "hhs_analytics_workspace"
  } else if (server == "inthealth") {
    db_name <- "inthealth_edw"
  }

  if (prod == T & server %in% c("hhsaw", "inthealth")) {
    server_name <- "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433"
  } else {
    server_name <- "tcp:kcitazrhpasqldev20.database.windows.net,1433"
  }
  
  if (server == "phclaims") {
    conn <- DBI::dbConnect(odbc::odbc(), "PHClaims51")
  } else if (interactive == F) {
    conn <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = server_name,
                           database = db_name,
                           uid = keyring::key_list("hhsaw_dev")[["username"]],
                           pwd = keyring::key_get("hhsaw_dev", keyring::key_list("hhsaw_dev")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")
  } else if (interactive == T) {
    conn <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = server_name,
                           database = db_name,
                           uid = keyring::key_list("hhsaw_dev")[["username"]],
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryInteractive")
  }
  
  return(conn)
}