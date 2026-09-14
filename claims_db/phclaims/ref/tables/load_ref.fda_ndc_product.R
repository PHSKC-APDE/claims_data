## Code to create ref.fda_ndc_product
## A lookup table for medications in claims data, from the FDA NDC
## Eli Kern
## 2020-07

##### Set up global parameters and call in libraries #####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)
library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, RCurl, configr, glue)

##### Connect to SQL Servers #####
conn <- dbConnect(odbc(), "PHClaims51")

#### Get YAML config file #####
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/main/claims_db/phclaims/ref/tables/create_ref.fda_ndc_product.yaml"
table_config <- yaml::yaml.load_file(config_url)
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
file <- read_csv(file_path, col_types = "cccccccccccccccccccc")

#Replace one non-ASCII character with blank
file <- file %>% mutate(LABELERNAME = str_replace_all(LABELERNAME, "�", ""))

#Use Sys.time to add last_run column
file <- file %>% mutate(last_run = Sys.time())

#Write data to SQL
dbWriteTable(conn, name = tbl_name, value = as.data.frame(file), overwrite = T)











