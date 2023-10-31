# Eli Kern
# APDE, PHSKC
# 2023-01

#### Compress P1 data CSV files as GZIP files for Dugan project ####

library(R.utils)
filelist <- list.files("//phcifs.ph.lcl/SFTP_DATA/APDEDataExchange/UW_Dugan_Team/Staging", 
                       pattern = "\\.csv$",
                       recursive = T,
                       full.names = T)
system.time(for(file in filelist) {
  print(file)
  system.time(gzip(file, remove = T, overwrite = T))
})