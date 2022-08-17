#### CODE TO DELETE DATA FROM IDH
#
# Kai Fukutaki, PHSKC (APDE)
#
# 2022-06
#
# The goal is to delete old records (right now from claims data) that are part
# of expiring data usage agreements.
#
# Before use, ensure that you have updated the table_name_file and delete_year
# variables to the ones relevant to your deletion. The table_name_file expects
# columns for server, schema, table, and date_column (date column is which
# column of the table to use for finding rows to delete). You may also need to
# add an option for a delete condition, if there are different date columns than
# before.
#
# The table that KCIT pulls from is stage.mcaid_elig
#

library(data.table)
library(glue)
library(odbc) # Read to and write from SQL
library(svDialogs)

devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/mcaid/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/db_loader/scripts_general/add_index.R")


#### CONSTANTS AND SETUP ####
server <- dlg_list(c("phclaims", "hhsaw", "inthealth"), title = "Select Server.")$res
if(server != "phclaims") {
  interactive_auth <- dlg_list(c("TRUE", "FALSE"), title = "Interactive Authentication?")$res
  prod <- dlg_list(c("TRUE", "FALSE"), title = "Production Server?")$res
} else {
  interactive_auth <- T
  prod <- T
}
table_name_file <- "C:/Users/kfukutaki/OneDrive - King County/Documents/Code/20220602_deletion_tests/mcaid_tables.csv"
delete_year <- 2012

# Table with information on each mcaid schema, table, and date_col in dev inthealth_edw for given server
table_names <- fread(table_name_file)
table_names <- table_names[table_names$db_server==server,]


index_info_df <- data.frame(table=c(table_name),
                            index_name=c(index_info$index_name),
                            index_type=c(index_info$index_type))

# Loop through each table, rename, copy all but delete condition into new table
for (row in 1:nrow(table_names)){
  table_name <- table_names[row,]$table
  schema <- table_names[row,]$schema
  date_col <- table_names[row,]$date_column
  
  if (is.na(date_col)){
    message(glue("No known way to handle this NA date column! Skipping to next table."))
    next
  } else if (date_col %in% c("CLNDR_YEAR_MNTH", "FROM_SRVC_DATE", "first_service_date")) {
    delete_condition <- DBI::SQL(glue("{tolower(date_col)} LIKE '{delete_year}%'"))
  } else {
    message(glue("No known way to handle this date column: {date_col}! Skipping to next table."))
    next
  }
  
  ### Rename old tables with "_to_delete" suffix, copy data from them into new tables
  new_table_name <- glue("{table_name}_to_delete")
  db_claims <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  # Get index_name and index_type
  # Use this approach to get any needed column names: https://stackoverflow.com/a/26094007/6609686
  index_info <- DBI::dbGetQuery(db_claims, glue::glue_sql(
    "SELECT t.Name AS table_name,
            i.Name AS index_name, 
            CASE WHEN i.type_desc = 'CLUSTERED' THEN 'cl'
              WHEN i.type_desc = 'CLUSTERED COLUMNSTORE' THEN 'ccs'
              END AS index_type,
            Ic.key_ordinal AS column_ordinal,
            c.name AS column_name,
            ty.name AS column_type
    FROM sys.indexes i
    INNER JOIN 
      sys.tables t ON t.object_id = i.object_id
    INNER JOIN 
      sys.index_columns ic 
      ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    INNER JOIN 
      sys.columns c 
      ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    INNER JOIN 
      sys.types ty 
      ON c.system_type_id = ty.system_type_id
    WHERE 
    t.name = {table_name} 
    ORDER BY t.Name, i.name, ic.key_ordinal",
    .con = db_claims
  ))
  
  # Drop index from table
  if (nrow(index_info > 0) & !is.na(index_info$index_name[1])) {
    DBI::dbExecute(db_claims, glue::glue_sql(
      "DROP INDEX {`index_info$index_name[1]`} ON {`schema`}.{`table_name`}",
      .con = db_claims))
  }
  
  # Keep track of number of rows
  old_rows <- dbGetQuery(db_claims, glue::glue_sql(
    "select count (*) as cnt from {`schema`}.{`table_name`}", .con = db_claims))
  
  
  # Rename table to "_to_delete"
  if (server != "inthealth"){
    condensed_tablename <- DBI::SQL(glue("{schema}.{table_name}"))
    DBI::dbExecute(db_claims, glue::glue_sql(
      "EXEC sp_rename '{condensed_tablename}', {`new_table_name`}",
      .con = db_claims
    ))
  } else{
    DBI::dbExecute(db_claims, glue::glue_sql(
      "RENAME OBJECT {`schema`}.{`table_name`} TO {`new_table_name`}",
      .con = db_claims
    ))
  }
  
  # Check all rows transferred
  new_rows <- dbGetQuery(db_claims, glue::glue_sql(
    "select count (*) as cnt from {`schema`}.{`new_table_name`}", .con = db_claims))
  
  if (old_rows != new_rows) {
    stop("Not all rows copied over for ", schema, ".", table_name)
  }
  
  # Copy all data except deletion year into new table
  DBI::dbExecute(db_claims, glue::glue_sql(
    "
    SELECT * INTO {`schema`}.{`table_name`}
    FROM {`schema`}.{`new_table_name`} 
    WHERE NOT {delete_condition}",
    .con = db_claims
  ))
  
  # Add index to new table
  if (nrow(index_info > 0) & !is.na(index_info$index_name[1])) {
    if (index_info$index_type[1] == "cl") {
      index_cols <- index_info$column_name
    } else {
      index_cols <- NULL
    }
    
    add_index(conn = db_claims, server = server, 
              to_schema = schema, to_table = table_name,
              index_name = index_info$index_name[1],
              index_type = index_info$index_type[1],
              index_vars = index_cols,
              drop_index = F)
  }
}




