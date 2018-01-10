###############################################################################
# Eli Kern
# 2018-1-9

# Optimize speed of exporting data to SQL server

###############################################################################


##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(RODBC) # Used to connect to SQL server
library(openxlsx) # Used to import/export Excel files
library(car) # used to recode variables
library(stringr) # Used to manipulate string data
library(lubridate) # Used to manipulate dates
library(dplyr) # Used to manipulate data
library(RecordLinkage) # used to clean up duplicates in the data
library(phonics) # used to extract phonetic version of names

##### Set date origin #####
origin <- "1970-01-01"

##### Connect to the servers #####
#db.claims50 <- odbcConnect("PHClaims50")
db.claims51 <- odbcConnect("PHClaims51")
#db.apde <- odbcConnect("PH_APDEStore50")
#db.apde51 <- odbcConnect("PH_APDEStore51")

##### Bring in Medicaid eligibility data for DOB processing #####
#Note to bring in test subset of Medicaid data, insert "top 50000" between SELECT and z.MEDICAID_RECIPIENT_ID

ptm01 <- proc.time() # Times how long this query takes (~400 secs)
elig_dob <- sqlQuery(
  db.claims51,
  " select *
    FROM [PHClaims].[dbo].[elig_dob]",
  stringsAsFactors = FALSE
)
proc.time() - ptm01

#### Grab column types from SQL server table
tmp <- sqlColumns(db.claims51, "elig_dob") ## this function grabs a bunch of info about the columns) 
varTypes <- as.character(tmp$TYPE_NAME) 
names(varTypes) <- as.character(tmp$COLUMN_NAME)

#Try exporting elig_dob subset with different methods, measure proc time

temp <- slice(elig_dob,1:10000)

#Write full table to secure drive for testing
write.table(elig_dob,"\\\\dchs-shares01\\dchsdata\\DCHSPHClaimsData\\Data\\temp.txt",quote=FALSE,sep=",",row.names=FALSE,col.names=FALSE,append=FALSE)

#Scenario 1
ptm02 <- proc.time() # Times how long this query takes - 25.83 sec
#sqlDrop(db.claims51, "dbo.temp") # Commented out because not always necessary
sqlSave(
  db.claims51,
  temp,
  tablename = "dbo.temp",
  rownames = FALSE,
  varTypes = varTypes
)
proc.time() - ptm02

#Scenario 2
ptm03 <- proc.time() # Times how long this query takes - 24.81 sec
sqlDrop(db.claims51, "dbo.temp") # Commented out because not always necessary
sqlSave(
  db.claims51,
  temp,
  tablename = "dbo.temp",
  rownames = FALSE,
  fast = TRUE,
  varTypes = c(
    dobnew = "Date",
    ssnnew = "Varchar(255)"
  )
)
proc.time() - ptm03

#Scenario 3
#sqlDrop(db.claims51, "dbo.temp") # Commented out because not always necessary
ptm04 <- proc.time()
write.table(temp,"\\\\dchs-shares01\\dchsdata\\DCHSPHClaimsData\\Data\\temp.txt",quote=FALSE,sep=",",row.names=FALSE,col.names=FALSE,append=FALSE)
sqlQuery(db.claims51,
  "BULK INSERT [PHClaims].[dbo].[temp]
                FROM '\\\\dchs-shares01\\dchsdata\\DCHSPHClaimsData\\Data\\temp.txt'
                WITH
                (
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '\\n'
                )"
  )
proc.time() - ptm04

## SQL server doesn't have access to the file that I saved on the M drive - access is denied


