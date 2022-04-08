#### COUNT LINES IN APCD EXTRACTED TABLE CHUNKS
#
# Eli Kern, PHSKC (APDE)
#
# 2019-10
#
# Notes:
# 1) I compared countLines with using wc.exe in Rtools, with the latter being slightly faster
# 2) To be able to use wc.exe from Rtools, must add C:\Rtools\bin to the Path variable in environmental variables
#
# Run time: 8.5 hours, 156 table chunks, 10/14/19


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)
library(tidyverse)
library(openxlsx)
read_dir <- "//kcitsqlutpdbh51/ImportData/Data/APCD_data_import"
#write_path <- "C:/Users/kerneli/King County/King County Cross-Sector Data - General/ETL/"
write_path <- "//kcitsqlutpdbh51/ImportData/Data/APCD_data_import/"

#Create list of tables
table_list <- list("claim_icdcm_raw", "claim_line_raw", "claim_procedure_raw", "claim_provider_raw", "dental_claim", "eligibility", "medical_claim_header",
                   "member_month_detail", "pharmacy_claim", "provider", "provider_master", "provider_practice_roster")

#Loop over table chunks within each table folder and count lines
system.time(apcd_row_count <- lapply(table_list, function(x) {
  
  #Loop over table chunk list
  file_list <- as.list(list.files(path = file.path(read_dir, paste0(x, "_export")), pattern = "*.csv", full.names = T))
  
  #Count lines and save as data frame
  inner_df <- lapply(file_list, function(x) {
    print(x)
    args <- paste0("-l ", x)
    result <- system2(command = "wc", args = args, stdout = TRUE)
    result_parse <- str_split(result, " ", simplify = T)
    df <- data.frame(table_chunk = result_parse[2], line_count = result_parse[1], stringsAsFactors = F)
  }) %>%
    bind_rows()  
}) %>%
  bind_rows())

#Export results
today <- Sys.Date()
filename <- paste0(write_path, "apcd_table_chunk_row_count_", today, ".xlsx")
write.xlsx(apcd_row_count, file = filename, sheet = "results")