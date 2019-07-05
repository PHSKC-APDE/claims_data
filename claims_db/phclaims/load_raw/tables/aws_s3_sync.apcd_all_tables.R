# Eli Kern
# APDE, PHSKC
# 2019-1-28

#### Save WA-APCD data from Amazon S3 bucket to local secure drive ####

##### Set up global parameters and call in libraries #####
options(max.print = 350, tibble.print_max = 50, scipen = 999)
origin <- "1970-01-01" # Date origin
write_path <- "\\\\phdata01/epe_data/APCD/Data_export/" ##Folder to save Amazon S3 files to
s3_folder <- "\"s3://waae-kc-ext/apcd_export/\"" ##Name of S3 folder containing data and format files

#### Save/sync files from Amazon S3 bucket to drive ####

#List files in S3 folder
system2(command = "aws", args = c("s3", "ls", s3_folder))

#Sync local folder to S3 bucket folder (this will download S3 files that have different size or modified date)
system2(command = "aws", args = c("s3", "sync", s3_folder, write_path))


