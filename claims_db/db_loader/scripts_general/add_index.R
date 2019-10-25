#### FUNCTION TO ADD AN INDEX TO A SQL TABLE BASEC ON A YAML FILE
# Alastair Matheson
# Created:        2019-10-20
# Last modified:  2019-10-20

# Currently supports clustered columstore and clustered index types
# Type desired is deduced from the config file and is not specified

### PARAMETERS
# conn = name of the connection to the SQL database
# table_config = name of config object that has index details
# Usually based on a YAML file that has been read in.
# Looks for a combination of index_type, index_name and index fields.
# Also need index_name but the other two fields will depend on type of index desired.
# Config must also include schema/to_schema and table/to_table fields.
# drop_index = remove any existing clustered or clustered columnstore indices

add_index_f <- function(conn, table_config, drop_index = T) {
  
  ### Error check
  if (is.null(table_config$index_name)) {
    stop("Index name must be specified in table_config file")
  }
  
  ### Deduce the index type to load
  # See if the index_type variable exists
  if (!is.null(table_config$index_type)) {
    if (table_config$index_type == 'ccs') {
      index_type <- 'ccs'
    } else if (table_config$index_type != 'cl') {
      stop("Unknown index type specified")
    }
  } else {
    if (is.null(table_config$index)) {
      stop("Neither index_type nor variables to index on are present. Cannot proceed.")
    } else {
      message("Variables to index on are present. A clustered index will be used.")
      index_type <- 'cl'
    }
  }

  
  ### Pull out schema and table names
  if (!is.null(table_config$to_schema)) {
    schema <- table_config$to_schema
  } else if (!is.null(table_config$schema)) {
    schema <- table_config$schema
  } else {
    stop("schema field not found in config")
  }
  
  if (!is.null(table_config$to_table)) {
    table_name <- table_config$to_table
  } else if (!is.null(table_config$table)) {
    table_name <- table_config$table
  } else {
    stop("table field not found in config")
  }
  
  
  # Remove existing index if desired (and an index exists)
  if (drop_index == T) {
    # This code pulls out the index name
    existing_index <- dbGetQuery(conn, 
    glue::glue_sql("SELECT DISTINCT a.existing_index
                   FROM
                    (SELECT ind.name AS existing_index
                    FROM
                     (SELECT object_id, name, type_desc FROM sys.indexes
                       WHERE type_desc LIKE 'CLUSTERED%') ind
                    JOIN
                     (SELECT name, schema_id, object_id FROM sys.tables
                       WHERE name = {table_name}) t
                    ON ind.object_id = t.object_id
                  INNER JOIN
                  (SELECT name, schema_id FROM sys.schemas
                    WHERE name = {schema}) s
                  ON t.schema_id = s.schema_id) a", 
                   .con = conn))[[1]]
    
    if (length(existing_index) != 0) {
      message("Removing existing clustered/clustered columnstore index")
      dbGetQuery(conn,
                 glue::glue_sql("DROP INDEX {`existing_index`} ON 
                                  {`schema`}.{`table_name`}", .con = conn))
    }
  }
  
  
  message(glue::glue("Adding index ({table_config$index_name}) to {schema}.{table_name}"))
  
  
  if (index_type == 'ccs') {
    # Clustered columnstore index
    dbGetQuery(conn,
               glue::glue_sql("CREATE CLUSTERED COLUMNSTORE INDEX {`table_config$index_name`} ON 
                              {`schema`}.{`table_name`}",
                              .con = conn))
  } else {
    # Clustered index
    dbGetQuery(conn,
               glue::glue_sql("CREATE CLUSTERED INDEX {`table_config$index_name`} ON 
                              {`schema`}.{`table_name`}({`index_vars`*})",
                              index_vars = table_config$index,
                              .con = conn))
  }
  
}