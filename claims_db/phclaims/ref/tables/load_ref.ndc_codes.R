#' @title NDC Reference Table
#' 
#' @description Upload drug code reference information to HHSAW
#' 
#' @details Download files from NDC website (FDA), then convert the Excel files
#' to CSVs. These are saved in Medicaid CIFS folder. We then upload these as
#' reference tables with version-date info.
#' 
#' 2024-06-19 Eli update: Revamped script to include unfinished, compound, and additional missing drugs for tobacco cessation
#' 
#' TODO:
#' - Add automatic pulling of data from website on regular basis
#' 

## Clear memory and load packages ----
rm(list=ls())
pacman::p_load(data.table, lubridate, stringr, readxl, readr, iotools, odbc, sqldf, tidyverse)
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")

## Prevent scientific notation except for huge numbers ----
options("scipen"=999) # turn off scientific notation

## Connect to the servers ----
db_hhsaw <- create_db_connection("hhsaw", interactive = F, prod = T)

## Set date for NDC folder name ----
ndc_download_date <- "20240619"

## Read data ----
product <- fread(file.path("//dphcifs/apde-cdip/Mcaid-Mcare/mcaid_raw/ndc_reference_tables/", ndc_download_date,
                           "/product.csv"), encoding="Latin-1")
package <- fread(file.path("//dphcifs/apde-cdip/Mcaid-Mcare/mcaid_raw/ndc_reference_tables/", ndc_download_date,
                           "/package.csv"), encoding="Latin-1")
unfinished_product <- fread(file.path("//dphcifs/apde-cdip/Mcaid-Mcare/mcaid_raw/ndc_reference_tables/", ndc_download_date,
                                      "/unfinished_product.csv"), encoding="Latin-1")
unfinished_package <- fread(file.path("//dphcifs/apde-cdip/Mcaid-Mcare/mcaid_raw/ndc_reference_tables/", ndc_download_date,
                                      "/unfinished_package.csv"), encoding="Latin-1")
# excluded_product <- fread(file.path("//dphcifs/apde-cdip/Mcaid-Mcare/mcaid_raw/ndc_reference_tables/", ndc_download_date,
#                                       "/Products_excluded.csv"), encoding="Latin-1")
# excluded_package <- fread(file.path("//dphcifs/apde-cdip/Mcaid-Mcare/mcaid_raw/ndc_reference_tables/", ndc_download_date,
#                                       "/Packages_excluded.csv"), encoding="Latin-1")
compounders <- fread(file.path("//dphcifs/apde-cdip/Mcaid-Mcare/mcaid_raw/ndc_reference_tables/", ndc_download_date,
                           "/compounders_ndc_directory.csv"), encoding="Latin-1")


## Select desired columns ----
package <- package[,c("PRODUCTID", "PRODUCTNDC", "NDCPACKAGECODE",
                      "STARTMARKETINGDATE", "ENDMARKETINGDATE")]
product <- product[,c("PRODUCTID", "PRODUCTNDC", "PROPRIETARYNAME",
                      "PROPRIETARYNAMESUFFIX", "NONPROPRIETARYNAME",
                      "DOSAGEFORMNAME", "ACTIVE_NUMERATOR_STRENGTH",
                      "ACTIVE_INGRED_UNIT")]
unfinished_package <- unfinished_package[,c("PRODUCTID", "PRODUCTNDC", "NDCPACKAGECODE",
                      "STARTMARKETINGDATE", "ENDMARKETINGDATE")]
unfinished_product <- unfinished_product[,c("PRODUCTID", "PRODUCTNDC", "NONPROPRIETARYNAME",
                      "DOSAGEFORMNAME", "ACTIVE_NUMERATOR_STRENGTH",
                      "ACTIVE_INGRED_UNIT")]
compounders <- compounders[,c("PRODUCTNDC", "NDCPACKAGECODE", "PROPRIETARYNAME",
                              "PROPRIETARYNAMESUFFIX", "NONPROPRIETARYNAME",
                          "DOSAGEFORMNAME", "ACTIVEINGREDIENTSINFO")]

# excluded_package <- excluded_package[,c("PRODUCTID", "PRODUCTNDC", "NDCPACKAGECODE",
#                                             "STARTMARKETINGDATE", "ENDMARKETINGDATE")]
# excluded_product <- excluded_product[,c("PRODUCTID", "PRODUCTNDC", "PROPRIETARYNAME",
#                                             "PROPRIETARYNAMESUFFIX", "NONPROPRIETARYNAME",
#                                             "DOSAGEFORMNAME", "ACTIVE_NUMERATOR_STRENGTH",
#                                             "ACTIVE_INGRED_UNIT")]

## Wrangle ----
# Join on product ndc
ndcs <- merge(package, product, by=c("PRODUCTID", "PRODUCTNDC"), all.x=T, all.y=F)
unfinished <- merge(unfinished_package, unfinished_product, by=c("PRODUCTID", "PRODUCTNDC"), all.x=T, all.y=F) %>%
  mutate(PROPRIETARYNAME = NA_character_,
         PROPRIETARYNAMESUFFIX = NA_character_,
         STARTMARKETINGDATE = as.integer(gsub("-", "", as.character(as.Date(STARTMARKETINGDATE, format = "%d-%b-%y")))),
         ENDMARKETINGDATE = as.integer(gsub("-", "", as.character(as.Date(ENDMARKETINGDATE, format = "%d-%b-%y"))))) %>%
  relocate(PROPRIETARYNAME:PROPRIETARYNAMESUFFIX, .after = ENDMARKETINGDATE)
compounders <- compounders %>%
  mutate(PRODUCTID = NA_character_, STARTMARKETINGDATE = NA_integer_, ENDMARKETINGDATE = NA_integer_,
         ACTIVE_INGRED_UNIT = NA_character_) %>%
  rename(ACTIVE_NUMERATOR_STRENGTH = ACTIVEINGREDIENTSINFO) %>%
  relocate(PRODUCTID, .before = everything()) %>%
  relocate(STARTMARKETINGDATE:ENDMARKETINGDATE, .after = NDCPACKAGECODE)

rm(product, package, unfinished_package, unfinished_product)

## Bind all files ----
allcodes <- bind_rows(ndcs, unfinished, compounders)

# Split according to rules for converting NDCs from 10 to 11 digits
allcodes$is442 <- grepl("[[:alnum:]]{4}-[[:alnum:]]{4}-[[:alnum:]]{2}", allcodes$NDCPACKAGECODE)
allcodes$is532 <- grepl("[[:alnum:]]{5}-[[:alnum:]]{3}-[[:alnum:]]{2}", allcodes$NDCPACKAGECODE)
allcodes$is541 <- grepl("[[:alnum:]]{5}-[[:alnum:]]{4}-[[:alnum:]]{1}", allcodes$NDCPACKAGECODE)

allcodes[, c("strsplit1", "strsplit2", "strsplit3") := tstrsplit(NDCPACKAGECODE, "-", fixed=TRUE)]
allcodes[,ndc := fcase(is442, paste0("0",  strsplit1,  strsplit2,  strsplit3), 
                       is532, paste0(strsplit1, "0",  strsplit2,  strsplit3), 
                       is541, paste0(strsplit1,  strsplit2, "0", strsplit3), 
                       default = NA_character_)]
allcodes[, c("is442","is532","is541","strsplit1","strsplit2","strsplit3"):=NULL]

# Add version-date and reorder columns
allcodes$last_run <- Sys.time()
setcolorder(allcodes, c("PRODUCTID", "ndc", "PRODUCTNDC", "NDCPACKAGECODE",
                        "STARTMARKETINGDATE", "ENDMARKETINGDATE", "NONPROPRIETARYNAME",
                        "PROPRIETARYNAME", "PROPRIETARYNAMESUFFIX", "DOSAGEFORMNAME",
                        "ACTIVE_NUMERATOR_STRENGTH", "ACTIVE_INGRED_UNIT"))

# TODO: Investigate these ones, read file as Excel file to preserve leading zeroes on NDC codes
missing <- read_excel("//dphcifs/apde-cdip/Mcaid-Mcare/mcaid_raw/ndc_reference_tables/missingndc.xlsx") %>%
  mutate(PRODUCTID = NA_character_) %>%
  relocate(PRODUCTID, .before = everything())
allcodes <- rbindlist(list(allcodes, missing), use.names=T, fill=T)

## Upload ----
DBI::dbWriteTable(conn = db_hhsaw, 
                  name = DBI::Id(schema = "ref", table = "ndc_codes"),
                  value = allcodes,
                  overwrite = T)

## Optional outline of code if upload ever gets too big and needs to be chunked ----
## Create Master loop MBSF_AB data ----
# set up parameters for loading data to SQL in chunks 
# max.row.num <- nrow(allcodes) # number of rows in the original R dataset
# chunk.size <- 10000 # number of rows uploaded per batch
# number.chunks <-  ceiling(max.row.num/chunk.size) # number of chunks to be uploaded
# starting.row <- 1 # the starting row number for each chunk to be uploaded. Will begin with 1 for the first chunk
# ending.row <- chunk.size  # the final row number for each chunk to be uploaded. Will begin with the overall chunk size for the first chunk
# 
# # Create loop for appending new data
# for(i in 1:number.chunks){
#   # counter so we know it isn't broken
#   print(paste0(yr, ": Loading chunk ", i, " of ", number.chunks, ": rows ", starting.row, "-", ending.row))  
#   
#   # subset the data (i.e., create a data 'chunk')
#   temp.dt<-allcodes[starting.row:ending.row,] 
#   
#   # load the data chunk into SQL
#   dbWriteTable(conn = db_hhsaw, name = mbsfab, value = temp.dt, row.names = FALSE, header = FALSE, append = TRUE) # load data to SQL
#   
#   # set the starting and ending rows for the next chunk to be uploaded
#   starting.row <- starting.row + chunk.size
#   ifelse(ending.row + chunk.size < max.row.num, 
#          ending.row <- ending.row + chunk.size,
#          ending.row <- max.row.num)
# } # close the for loop that appends the chunks
