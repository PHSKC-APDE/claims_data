# Eli Kern
# APDE, PHSKC
# 2023-01

#### Move GZIP files and format file from Staging to FromKingCounty folder on CIFS ####

library(R.utils)
filelist <- list.files("//phcifs.ph.lcl/SFTP_DATA/APDEDataExchange/UW_Dugan_Team/Staging", 
                       pattern = "\\.gz$|\\.xlsx$",
                       recursive = T,
                       full.names = T)

system.time(for(file in filelist) {
  destination <- gsub("Staging", "FromKingCounty", file)
  copyFile(file, destination, overwrite = T, verbose = F)
})