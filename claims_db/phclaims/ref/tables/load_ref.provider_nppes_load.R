##Code to create and load data to ref.provider_nppes_load
##Lookup table for provider NPIs and other information
##Reference: https://download.cms.gov/nppes/NPI_Files.html
##Eli Kern (PHSKC-APDE)
##2020-09


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)
library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, RCurl, configr, glue, data.table)

##### Connect to SQL Servers #####
conn <- dbConnect(odbc(), "PHClaims51")


#### Get YAML config file #####
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/load_ref.provider_nppes_load.yaml"
table_config <- yaml::yaml.load_file(config_url)
file_path <- table_config$overall[["file_path"]][[1]]
schema <- table_config[["schema"]][[1]]
to_table <- table_config[["table"]][[1]]
vars <- table_config$vars
col_names_sql <- names(unlist(vars, recursive = TRUE, use.names = TRUE))


##### Create table shell #####

# Set up table name
tbl_name <- DBI::Id(schema = schema, table = to_table)

# Remove table if it exists
try(dbRemoveTable(conn, tbl_name), silent = T)

# Create table
DBI::dbCreateTable(conn, tbl_name, fields = table_config$vars)


#### Import and clean data ####
#data <- read_csv(file_path, n_max = 10000, col_types = cols(.default = "c"), trim_ws = T)
data <- read_csv(file_path, col_types = cols(.default = "c"), trim_ws = T)

#Remove commas
data <- data %>% mutate_all(~gsub(",","",.))

#Use Sys.time to add last_run column
data <- data %>% mutate(last_run = Sys.time())

#Set column names using YAML file
data.table::setnames(data, names(data), col_names_sql)


#### Load data to SQL table shell ####
dbWriteTable(conn, name = tbl_name, value = as.data.frame(data), append = T)