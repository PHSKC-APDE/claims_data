# Susan Hernandez
# APDE, PHSKC
# 2023-6-23
#file location //dphcifs/apde-cdip/apcd/apcd_data_import

#### Uncompress individual file- WA-APCD GZIP files from Analytic Enclave ####

library(R.utils).
filelist <- "//dphcifs/apde-cdip/apcd/apcd_data_import/medical_claim_header_export/medical_claim_header_009.csv.gz"
                      
for(file in filelist) {
  gunzip(file, remove = F)
}

