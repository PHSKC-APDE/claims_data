## Code to create ref.ed_dental_hcup
## A lookup table for ICD-9-CM/ICD-10-CM codes relevant for define non-traumatic dental conditions in the ED
## Eli Kern
## 2020-02

##### Set up global parameters and call in libraries #####
options(max.print = 350, tibble.print_max = 50, scipen = 999)
origin <- "1970-01-01" # Date origin
library(odbc) # Connect to SQL server
library(tidyverse) # Work with tidy data
library(openxlsx) # Read and write data using Microsoft Excel

##### Connect to SQL Servers #####
conn <- dbConnect(odbc(), "PHClaims51")

#### Get YAML config file #####
yaml_path <- "C:/Users/kerneli/King County/King County Cross-Sector Data - Documents/References/Geographic definitions/create_ref.geo_kc_zip.yaml"

table_config <- yaml::yaml.load_file(yaml_path)
file_path <- table_config[["file_path"]][[1]]
schema <- table_config[["schema"]][[1]]
to_table <- table_config[["to_table"]][[1]]
vars <- table_config$vars


##### Create table shell #####

# Set up table name
tbl_name <- DBI::Id(schema = schema, table = to_table)

# Remove table if it exists
try(dbRemoveTable(conn, tbl_name), silent = T)

# Create table
DBI::dbCreateTable(conn, tbl_name, fields = table_config$vars)


##### Load data to SQL table #####

#Read in data file
#file <- read.xlsx(file_path)
file <- read_csv(file_path)

#Write data to SQL
dbWriteTable(conn, name = tbl_name, value = as.data.frame(file), append = T)
