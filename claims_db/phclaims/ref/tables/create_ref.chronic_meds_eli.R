## Code to create ref.chronic_meds_eli
## A lookup table for chronic condition medications in claims data, based on Aetna chronic med lists from 2013, 2018 and 2020
## Eli Kern
## 2020-07

##### Set up global parameters and call in libraries #####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)
library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, RCurl, configr, glue, openxlsx)

##### Connect to SQL Servers #####
conn <- dbConnect(odbc(), "PHClaims51")

#### Step 1: Pull in chronic medication list created by Eli, adapted from Aetna 2013, 2018, and 2020 documentation ####
file_path1 <- "C:/Users/kerneli/King County/King County Cross-Sector Data - Documents/General/References/Pharmacy/chronic_meds_eli.xlsx"
med_list <- read.xlsx(file_path1, sheet = "med_list")

#Prep for fuzzy join
med_list <- med_list %>%
  mutate(match_name = case_when(
    str_detect(drug_name, "%") == T ~ str_replace_all(drug_name, "%", ""),
    TRUE ~ paste0("^", drug_name)))


#### Step 2: Bring in distinct drug names from mcaid claims ####
mcaid_meds <- dbGetQuery(conn = conn, 
"select distinct lower(ndc_desc) as ndc_desc from [PHClaims].[stage].[mcaid_claim] where ndc is not null;")


#### Step 3: Fuzzy join of mcaid drug names with chronic medication list ####
chronic_meds_crosswalk <- mcaid_meds %>%
  fuzzyjoin::regex_left_join(med_list, by = c(ndc_desc = "match_name"))


#QA Make sure there aren't any drugs in mcaid claims that join to more than one entry in chronic meds list
test <- chronic_meds_crosswalk %>%
  group_by(ndc_desc) %>%
  mutate(count = n())
View(filter(test, count >1))
rm(test)





#### Get YAML config file #####
config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/ref/tables/create_ref.fda_ndc_product.yaml"
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











