# Eli Kern
# APDE, PHSKC
# 2019-1-28

#### Uncompress WA-APCD GZIP files from Analytic Enclave ####

library(R.utils)
filelist <- list.files("//dphcifs/apde-cdip/apcd/apcd_data_import", 
                       pattern = "\\.gz$",
                       recursive = T,
                       full.names = T)
for(file in filelist) {
  gunzip(file, remove = F)
}