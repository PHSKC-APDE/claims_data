# Eli Kern
# APDE, PHSKC
# 2019-1-28

#### Save WA-APCD data from Amazon S3 bucket to local secure drive ####

#2019-10-1 update: Changed local file location from J drive to machine running SQL Server
#2023-09-20 update: Change local file path to CIFS folder given that server 51 is being retired

##### Set up global parameters and call in libraries #####
options(max.print = 350, tibble.print_max = 50, scipen = 999)
library(tidyverse)
origin <- "1970-01-01" # Date origin
write_path <- "//dphcifs/apde-cdip/apcd/apcd_data_import/" ##Folder to save Amazon S3 files to
s3_folder <- "\"s3://waae-kc-ext/apcd_export/\"" ##Name of S3 folder containing data and format files

#### Save/sync files from Amazon S3 bucket to drive ####
#Import credentials from C:\Users\[USERNAME]\.aws
#credentials <- read.csv("C:/Users/kerneli/.aws/credentials") #Eli's KC laptop
#credentials <- read.csv("C:/Users/kerneli.PH/.aws/credentials") #Eli's account on KCITENGPRRSTUD00.kc.kingcounty.lcl
credentials <- read.csv("C:/Users/shernandez/.aws/credentials") #Susan's account on KCITENGPRRSTUD00.kc.kingcounty.lcl
credentials <- separate(credentials, col = X.default., sep = " = ", into = c("var_name", "value"))

Sys.setenv("AWS_ACCESS_KEY_ID" =  credentials$value[1],
           "AWS_SECRET_ACCESS_KEY" = credentials$value[2],
           "AWS_DEFAULT_REGION" = "us-west-2")

#List files in S3 folder
system2(command = "aws", args = c("s3", "ls", s3_folder))

#Sync local folder to S3 bucket folder (this will download S3 files that have different size or modified date)
system2(command = "aws", args = c("s3", "sync", s3_folder, write_path))