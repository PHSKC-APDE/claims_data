#### FUNCTION TO ALTER SCHEMA OF A SQL TABLE
# Eli Kern
# Created:        2019-10-18
# Last modified:  2019-10-18

### PARAMETERS
# conn = name of the connection to the SQL database
# from_schema = name of current schema for table
# to_schema = desired new schema for table
# table_name = name of table

alter_schema_f <- function(
  conn = NULL,
  from_schema = NULL,
  to_schema = NULL,
  table_name = NULL
  ) {
  
  #### ERROR CHECKS ####
  if (is.null(conn)) {
    print("No DB connection specificed, trying PHClaims51")
    conn <- odbc::dbConnect(odbc(), "PHClaims51")
  }
  
  if (dbExistsTable(conn, DBI::Id(schema = from_schema, table = table_name)) == F) {
    stop("From table does not exist")
  }
  
  schema_check <- odbc::dbGetQuery(conn,
               glue::glue_sql(
               "if not exists (
               select schema_name
               from information_schema.schemata
               where schema_name = {to_schema})
               select 'To schema does not exist' as 'message'",
               .con = conn))
  if (length(schema_check$message) == 1) {
    stop("To schema does not exist")
  }
  

  #### DROP TABLE IN TO SCHEMA IF IT ALREADY EXISTS ####
  if (dbExistsTable(conn, DBI::Id(schema = to_schema, table = table_name)) == T) {
    odbc::dbGetQuery(conn,
               glue::glue_sql(
               "drop table {`to_schema`}.{`table_name`}",
               .con = conn))
  }
  
  
  #### ALTER SCHEMA ####
  odbc::dbGetQuery(conn,
               glue::glue_sql(
               "alter schema {`to_schema`} transfer {`from_schema`}.{`table_name`}",
               .con = conn))
  
  #### Finish with a message ####
  print(glue::glue("Table {table_name} has been switched from {from_schema} to {to_schema} schema."))
}