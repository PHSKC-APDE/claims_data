##Code to create ref.provider_nppes_wa_load
##Lookup table for provider NPIs and other information
##Reference: https://download.cms.gov/nppes/NPI_Files.html
##Eli Kern (PHSKC-APDE)
##2020-10


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)
library(pacman)
pacman::p_load(tidyverse, lubridate, odbc, RCurl, configr, glue, data.table)

##### Connect to SQL Servers #####
conn <- dbConnect(odbc(), "PHClaims51")


##### STEP 1: Subset national NPPES table to only providers with practice location in WA state #####

# Remove table if it exists
tbl_name <- DBI::Id(schema = "ref", table = "provider_nppes_wa_load")
try(dbRemoveTable(conn, tbl_name), silent = T)

# Create new table
dbSendQuery(conn = conn, 
            "select *
            into phclaims.ref.provider_nppes_wa_load
            from phclaims.ref.provider_nppes_load
            where address_practice_state = 'WA' or address_practice_state = 'WASHINGTON';")
