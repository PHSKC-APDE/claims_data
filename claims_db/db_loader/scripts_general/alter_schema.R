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
  table_name = NULL,
  rename_index = F,
  index_name = NULL, 
  odbc_name = "PHClaims51"
  ) {
  
  #### ERROR CHECKS ####
  if (is.null(conn)) {
    print(paste0("No DB connection specificed, trying ", odbc_name))
    conn <- odbc::dbConnect(odbc(), odbc_name)
  }
  
  if (dbExistsTable(conn, DBI::Id(schema = from_schema, table = table_name)) == F) {
    stop(glue::glue("From table ({from_schema}.{table_name}) does not exist"))
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
  
  if (from_schema == to_schema) {
    stop("From and to schema cannot be the same")
  }
  
  if (rename_index == T & is.null(index_name)) {
    stop("Provide a new index name")
  }


  #### RETAIN NAME OF EXISTING INDICES IF NEEDED ####
  if (rename_index == T) {
    # This code pulls out the index name
    old_index_names <- dbGetQuery(conn = conn, 
    glue::glue_sql("SELECT DISTINCT a.index_name
    FROM
    (SELECT ind.name AS index_name
    FROM
    (SELECT object_id, name, type_desc FROM sys.indexes
    WHERE type_desc LIKE '%CLUSTERED%') ind
    INNER JOIN
    (SELECT name, schema_id, object_id FROM sys.tables
    WHERE name = {table_name}) t
    ON ind.object_id = t.object_id
    INNER JOIN
    (SELECT name, schema_id FROM sys.schemas
    WHERE name = {from_schema}) s
    ON t.schema_id = s.schema_id) a", 
                   .con = conn))
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
  
  
  #### RENAME INDICES ####
  if (rename_index == T) {
    if (length(old_index_names$index_name) > 0) {
      # If there are multiple indices, check the number of names are the same
      if (length(old_index_names$index_name) > length(index_name)) {
        warning(paste0("There were ", length(old_index_names$index_name), " existing indices but only ",
                       length(index_name), "new names provided. Only the first ",
                       length(index_name), " index/indices will be renamed."))
      } else if (length(old_index_names$index_name) < length(index_name)) {
        warning(paste0("There were ", length(old_index_names$index_name), " existing indices but ",
                       length(index_name), "new names provided. Only the first ",
                       length(index_name), " new names will be used."))
      }
      
      lapply(seq_along(index_name), function(x) {
        DBI::dbExecute(db_claims,
                       glue::glue_sql("EXEC sp_rename N'{`to_schema`}.{`table_name`}.{`old_index_names$index_name[x]`}', 
                   N'{`index_name[x]`}', N'INDEX';",
                                      .con = db_claims))
      })
    } else {
      warning("No existing index was found to rename")
    }
  }

  
  #### Finish with a message ####
  message(glue::glue("Table {table_name} has been switched from {from_schema} to {to_schema} schema."))
}