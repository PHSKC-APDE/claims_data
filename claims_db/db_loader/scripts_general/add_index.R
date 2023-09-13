#### FUNCTION TO ADD AN INDEX TO A SQL TABLE BASEC ON A YAML FILE
# Alastair Matheson
# Created:        2019-10-20
# Last modified:  2019-10-20

# Currently supports clustered columstore and clustered index types
# Type desired is deduced from the config file and is not specified

### PARAMETERS
# conn = name of the connection to the SQL database
# server = name of server being used (if using newer YAML format)
# table_config = name of config object that has index details
# Usually based on a YAML file that has been read in.
# Looks for a combination of index_type, index_name and index fields.
# Also need index_name but the other two fields will depend on type of index desired.
# Config must also include to_schema/to_schema and table/to_table fields.
# drop_index = remove any existing clustered or clustered columnstore indices

add_index_f <- function(conn, 
                        server = NULL,
                        table_config, 
                        drop_index = T, 
                        test_mode = F) {
  
  #### ERROR CHECK ####
  if (is.null(table_config$index_name)) {
    stop("Index name must be specified in table_config file")
  }
  
  
  #### SET UP SERVER ####
  if (is.null(server)) {
    server <- NA
  } else if (server %in% c("phclaims", "hhsaw")) {
    server <- server
  } else if (!server %in% c("phclaims", "hhsaw")) {
    stop("Server must be NULL, 'phclaims', or 'hhsaw'")
  }
  
  
  #### DEDUCE INDEX TYPE ####
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

  
  #### PULL OUT to_schema AND TABLE NAMES ####
  if (!is.na(server)) {
    to_schema <- table_config[[server]][["to_schema"]]
    to_table <- table_config[[server]][["to_table"]]
  } else {
    # Set up to work with both new and old way of using YAML files
    if (!is.null(table_config$to_schema)) {
      to_schema <- table_config$to_schema
    } else {
      to_schema <- table_config$schema
    }
    
    if (!is.null(table_config$to_table)) {
      to_table <- table_config$to_table
    } else {
      to_table <- table_config$table
    }
  }
  
  if (test_mode == T) {
    message("FUNCTION WILL BE RUN IN TEST MODE, INDEXING TABLE IN TMP to_schema")
    to_table <- glue::glue("{to_schema}_{to_table}")
    to_schema <- "tmp"
  }
  

  
  #### REMOVE EXISTING INDEX ID DESIRED ####
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
                       WHERE name = {to_table}) t
                    ON ind.object_id = t.object_id
                  INNER JOIN
                  (SELECT name, schema_id FROM sys.schemas
                    WHERE name = {to_schema}) s
                  ON t.schema_id = s.schema_id) a", 
                   .con = conn))[[1]]
    
    if (length(existing_index) != 0) {
      message("Removing existing clustered/clustered columnstore index")
      dbGetQuery(conn,
                 glue::glue_sql("DROP INDEX {`existing_index`} ON 
                                  {`to_schema`}.{`to_table`}", .con = conn))
    }
  }
  
  
  #### ADD INDEX ####
  message(glue::glue("Adding index ({table_config$index_name}) to {to_schema}.{to_table}"))
  
  
  if (index_type == 'ccs') {
    # Clustered columnstore index
    dbGetQuery(conn,
               glue::glue_sql("CREATE CLUSTERED COLUMNSTORE INDEX {`table_config$index_name`} ON 
                              {`to_schema`}.{`to_table`}",
                              .con = conn))
  } else {
    # Clustered index
    dbGetQuery(conn,
               glue::glue_sql("CREATE CLUSTERED INDEX {`table_config$index_name`} ON 
                              {`to_schema`}.{`to_table`}({`index_vars`*})",
                              index_vars = table_config$index,
                              .con = conn))
  }
  
}