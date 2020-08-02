# Eli Kern
# APDE, PHSKC
# 2019-1-28

#### Save WA-APCD data from Amazon S3 bucket to local secure drive ####

#2019-10-1 update: Changed local file location from J drive to machine running SQL Server

##### Set up global parameters and call in libraries #####
options(max.print = 350, tibble.print_max = 50, scipen = 999)
origin <- "1970-01-01" # Date origin
write_path <- "\\\\kcitsqlutpdbh51/ImportData/Data/APCD_data_import/" ##Folder to save Amazon S3 files to
s3_folder <- "\"s3://waae-kc-ext/apcd_export/\"" ##Name of S3 folder containing data and format files

#### Save/sync files from Amazon S3 bucket to drive ####
#Copy and paste credentials from C:\Users\kerneli\.aws

Sys.setenv("AWS_ACCESS_KEY_ID" = "FILL-IN-BLANK",
           "AWS_SECRET_ACCESS_KEY" = "FILL-IN-BLANK",
           "AWS_DEFAULT_REGION" = "us-west-2")

#List files in S3 folder
system2(command = "aws", args = c("s3", "ls", s3_folder))

#Sync local folder to S3 bucket folder (this will download S3 files that have different size or modified date)
system2(command = "aws", args = c("s3", "sync", s3_folder, write_path))

