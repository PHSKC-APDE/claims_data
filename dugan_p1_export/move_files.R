# Eli Kern
# APDE, PHSKC
# 2023-01

#### Move GZIP files, format file and WAHBE CSV file from Staging to FromKingCounty folder on CIFS ####

library(R.utils)
filelist <- list.files("//phcifs.ph.lcl/SFTP_DATA/APDEDataExchange/UW_Dugan_Team/Staging", 
                       pattern = "\\.gz$|\\.xlsx$|\\.csv$",
                       recursive = T, ##This means all files will be copied, thus don't keep anything here you don't want copied
                       full.names = T)

system.time(for(file in filelist) {
  print(file)
  destination <- gsub("Staging", "FromKingCounty", file)
  copyFile(file, destination, overwrite = T, verbose = F)
})