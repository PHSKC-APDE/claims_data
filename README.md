# King County Medicaid eligibility and claims data
This README describes the structure and content of Medicaid eligibility and claims data that King County government routinely receives from the WA State Health Care Authority (HCA), as well as an R package King County has developed to facilitate analysis and dissemination of these data.

## Available data tables on SQL Server
Currently King County receives quarterly Medicaid eligibility and claims data files (i.e. ProviderOne data) from HCA and loads these into SQL. Moving forward under a new Master Data Sharing Agreement (DSA), King County will begin to receive monthly files which consist of a rolling 12-month refresh of eligibility and claims data. These monthly files will be loaded to SQL Server through an update process – old records will be replaced with new records where duplicates exist, and new records without old duplicates will simply be appended.

King County analysts transform raw eligibility and claims data to create an array of analytic-ready tables that can be used to flexibly compute people and event-based statistics over time, such as the count of Emergency Department visits by Medicaid member race/ethnicity.

For more information on data tables available on King County's SQL Servers, users can review the [purpose and structure of each table](https://kc1.sharepoint.com/:x:/r/teams/KingCountyCross-SectorData/Shared%20Documents/References/Medicaid/Medicaid%20data%20table%20structure.xlsx?d=w1fd93a1ee35a40a7a9e3e6331c61ddbe&csf=1&e=XiFffk), as well as a [data dictionary](https://kc1.sharepoint.com/:x:/r/teams/KingCountyCross-SectorData/Shared%20Documents/References/Medicaid/King%20County%20ProviderOne%20Data%20Dictionary_ForOneDrive.xlsx?d=w79ec29aa4c1346a1874bd3fc6c6591a7&csf=1&e=rsS8LI) that describes each individual data element.

An ever-growing group of King County analysts meet every three weeks to discuss their shared experiences using Medicaid claims data. Users can view [ongoing agenda items and point people for specific topics](https://kc1-my.sharepoint.com/:x:/r/personal/eli_kern_kingcounty_gov/Documents/Shared%20with%20Everyone/PH-DCHS%20Healthcare%20Data%20Meetings.xlsx?d=w632b8ab629f34250ab2dbe4bdf52405e&csf=1&e=GWeyLm).

## Claims R package for rapid data analysis
King County analysts developed the *claims* R package to facilitate querying and analyzing the aforementioned analytic-ready eligibility and claims data tables.

Instructions for installing the *claims* package:
- Make sure devtools is installed (`install.packages("devtools")`).
- Type `devtools::install_github("PHSKC-APDE/claims_data")`

Instructions for updating the *claims* package:
- Simply reinstall the package by typing `devtools::install_github("PHSKC-APDE/claims_data")`

Current functionality of the *claims* package (v 0.1.3):
- Request an eligibility and demographics-based Medicaid member cohort
- Request a claims summary (e.g. ED visits, avoidable ED, behavioral health hospital stays) for a member cohort
- Request coverage group information (e.g. persons with disabilities) and automatically join to a specified data frame
- Request chronic health condition (e.g. asthma) information and automatically join to a specified data frame
- Tabulate counts by fixed and looped by variables (i.e. data aggregation), with automatic suppression and other features
- Calculate the top N causes of ED visits and hospitalizations

Training resources:
- R users can view a [training video](https://kc1-my.sharepoint.com/:v:/r/personal/eli_kern_kingcounty_gov/Documents/Shared%20with%20Everyone/Medicaid%20R%20Package%20Training_2018.mp4?csf=1&e=3OydL9) for how to use the *claims* package.
- Users can also view the [R script used in the training video](https://github.com/PHSKC-APDE/Medicaid/blob/master/Medicaid%20package%20orientation.R).

## ETL Folder Access and Credentials Requirements for Claims and Housing Projects
Folder Access:
- \\\\kcitsqlutpdbh51\importdata\data - zip/csv files
  - KC_Claim
  - KC_Elig
- \\\\dchs-shares01\DCHSDATA\DCHSPHClaimsData - Geocoding/clean address data
- \\\\kcitetldepim001\informatica\address - Clean address data
- \\\\phdata01\DROF_DATA\DOH DATA\Housing - Public housing data
  - Organized_data
  - KCHA
  - SHA
  - Geocoding
  
SQL Database Access:
- KCITSQLUTPDBH51
  - KCIT SQL Server (local)
  - Windows Authentication
  - ODBC - User DSN - SQL Server - /w Windows NT authentication - PHClaims51
- KCITSQLPRPDBM50
  - KCIT SQL Server (local)
  - Windows Authentication
  - ODBC - User DSN - SQL Server - /w Windows NT authentication - PHClaims50
 - KCITSQLUTPDBH51
   - KCIT SQL Server (local)
   - Windows Authentication
   - ODBC - User DSN - SQL Server - /w Windows NT authentication - PH_APDEStore51
 - KCITSQLPRPDBM50
   - KCIT SQL Server (local)
   - Windows Authentication
   - ODBC - User DSN - SQL Server - /w Windows NT authentication - PH_APDE_Store50
 - kcitazrhpasqldev20.database.windows.net
   - Azure SQL Server (cloud)
   - Azure Active Directory - Universal with MFA
 - kcitazrhpasqlprp16.azds.kingcounty.gov
   - Azure SQL Server (cloud)
   - Azure Active Directory - Universal with MFA

R Keyrings:
- hca_sftp - Access to HCA SFTP file portal
- here - Sign up for HERE freemium API (https://developer.here.com/) -  App ID and API Key
- hhsaw_dev - Access to Azure SQL Servers, will require to update as you update your KC and PH domain passwords
 
