#' @title NDC Reference Table
#' 
#' @description Upload drug code reference information to HHSAW
#' 
#' @details Download files from NDC website (FDA), then convert the Excel files
#' to CSVs. These are saved in Medicaid CIFS folder. We then upload these as
#' reference tables with version-date info.
#' 
#' TODO:
#' - Add automatic pulling of data from website on regular basis
#' 

## Clear memory and load packages ----
rm(list=ls())
pacman::p_load(data.table, lubridate, stringr, readxl, readr, iotools, odbc, sqldf)
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")

## Prevent scientific notation except for huge numbers ----
options("scipen"=999) # turn off scientific notation

## Connect to the servers ----
db_hhsaw <- create_db_connection("hhsaw", interactive = F, prod = T)

## Read data ----
product <- fread("X:/Mcaid-Mcare/mcaid_raw/ndc_reference_tables/20240403/product.csv", encoding="Latin-1")
package <- fread("X:/Mcaid-Mcare/mcaid_raw/ndc_reference_tables/20240403/package.csv", encoding="Latin-1")

package <- package[,c("PRODUCTID", "PRODUCTNDC", "NDCPACKAGECODE",
                      "STARTMARKETINGDATE", "ENDMARKETINGDATE")]
product <- product[,c("PRODUCTID", "PRODUCTNDC", "PROPRIETARYNAME",
                      "PROPRIETARYNAMESUFFIX", "NONPROPRIETARYNAME",
                      "DOSAGEFORMNAME", "ACTIVE_NUMERATOR_STRENGTH",
                      "ACTIVE_INGRED_UNIT")]

## Wrangle ----
# Join on product ndc
allcodes <- merge(package, product, by=c("PRODUCTID", "PRODUCTNDC"), all.x=T, all.y=F)
rm(product, package)

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

# TODO: Investigate these ones
missing <- fread("X:/Mcaid-Mcare/mcaid_raw/ndc_reference_tables/missingndc.csv", encoding="Latin-1")
missing$ndc <- as.character(missing$ndc)
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
