
# Load new IC-10-CM tables from cms.gov/medicare/icd-10 and combine all new rows
# with existing ICD_9_10_CM_Complete.xlsx file

# NOTE: This code will need to be updated each year to load the new file -
# in general, it should only require reading in the new year file and the old
# all years file to combine

pacman::p_load(data.table, DBI, dplyr, glue, lubridate, readxl, reshape2, stringr, tidyverse, xlsx)

root_dir <- "C:/Users/kfukutaki/OneDrive - King County/Shared Documents/General/References/ICD-CM/ICD-10-CM_CMS"

data2019 <- fread(file = glue::glue("{root_dir}/icd10cm_codes_addenda_2019/icd10cm_order_2019.txt"),
                  sep = "",
                  header = F,
)
data2019 <- data.table(icdcode = substr(data2019$V1, 7, 12), dx_description = substr(data2019$V1, 17, 77))

data2020 <- fread(file = glue::glue("{root_dir}/2020 Code Descriptions/icd10cm_order_2020.txt"),
                  sep = "",
                  header = F,
)
data2020 <- data.table(icdcode = substr(data2020$V1, 7, 12), dx_description = substr(data2020$V1, 17, 77))

data2021 <- fread(file = glue::glue("{root_dir}/2021-code-descriptions-tabular-order/icd10cm_order_2021.txt"),
                  sep = "",
                  header = F,
)
data2021 <- data.table(icdcode = substr(data2021$V1, 7, 12), dx_description = substr(data2021$V1, 17, 77))

data2022 <- fread(file = glue::glue("{root_dir}/2022 Code Descriptions/icd10cm_order_2022.txt"),
                  sep = "",
                  header = F,
)
data2022 <- data.table(icdcode = substr(data2022$V1, 7, 12), dx_description = substr(data2022$V1, 17, 77))

data2023 <- fread(file = glue::glue("{root_dir}/2023 Code Descriptions in Tabular Order/icd10cm_order_2023.txt"),
                  sep = "",
                  header = F,
)
data2023 <- data.table(icdcode = substr(data2023$V1, 7, 12), dx_description = substr(data2023$V1, 17, 77))


new_data <- bind_rows(data2019, data2020, data2021, data2022, data2023)
new_data <- new_data[!duplicated(new_data), ]
new_data <- new_data %>% 
  mutate(across(where(is.character), str_trim))
new_data[, 'ver'] <- 10

# bring in existing table from <2019
#old_data <- read_excel("C:/Users/kfukutaki/OneDrive - King County/Documents/Code/reference-data/claims_data/ICD_9_10_CM_Complete.xlsx")
old_data <- read_excel("C:/Users/kfukutaki/OneDrive - King County/Documents/Code/Data/ICD_9_10_CM_Complete_20230515.xlsx")
old_data <- as.data.table(old_data)


all_data <- bind_rows(old_data, new_data)
all_data <- distinct(all_data, icdcode, ver, .keep_all = TRUE)

write.xlsx(all_data,
           file="C:/Users/kfukutaki/OneDrive - King County/Documents/Code/reference-data/claims_data/ICD_9_10_CM_Complete.xlsx",
           row.names = F)
