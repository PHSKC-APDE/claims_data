# King County Medicaid eligibility and claims data
This README describes the structure and content of Medicaid eligibility and claims data that King County government routinely receives from the WA State Health Care Authority (HCA), as well as an R package King County has developed to facilitate analysis and dissemination of these data.

## Available data tables on SQL Server
Currently King County receives quarterly Medicaid eligibility and claims data files (i.e. ProviderOne data) from HCA and loads these into SQL. Moving forward under a new Master Data Sharing Agreement (DSA), King County will begin to receive monthly files which consist of a rolling 12-month refresh of eligibility and claims data. These monthly files will be loaded to SQL Server through an update process – old records will be replaced with new records where duplicates exist, and new records without old duplicates will simply be appended.

King County analysts transform the raw eligibility and claims data to create an array of analytic-ready tables that can be used to flexibly compute people and event-based statistics over time, such as the count of Emergency Department visits by Medicaid member race/ethnicity.

For more information on data tables available on King County's SQL Servers, users can review the [purpose and structure of each table](https://kc1-my.sharepoint.com/:x:/r/personal/eli_kern_kingcounty_gov/Documents/Shared%20with%20Everyone/Medicaid%20data%20table%20structure.xlsx?d=w13d589b863b647269b03d645618ba7b2&csf=1&e=7i3atF), as well as a [data dictionary](https://kc1-my.sharepoint.com/:x:/r/personal/eli_kern_kingcounty_gov/Documents/Shared%20with%20Everyone/King%20County%20ProviderOne%20Data%20Dictionary_ForOneDrive.xlsx?d=wef8139919d58457c89f8b20c87aaf096&csf=1&e=f6nDgg) that describes each individual data element.

An ever-growing group of King County analysts meet every three weeks to discuss their shared experiences using Medicaid claims data. Users can view [ongoing agenda items and point people for specific topics](https://kc1-my.sharepoint.com/:x:/r/personal/eli_kern_kingcounty_gov/Documents/Shared%20with%20Everyone/PH-DCHS%20Healthcare%20Data%20Meetings.xlsx?d=w632b8ab629f34250ab2dbe4bdf52405e&csf=1&e=GWeyLm).

## Medicaid R package for rapid data analysis
King County analysts developed the *medicaid* R package to facilitate querying and analyzing the aforementioned analytic-ready eligibility and claims data tables.

Instructions for installing the *medicaid* package:
- Make sure devtools is installed (install.packages("devtools")).
- Type devtools::install_github("PHSKC-APDE/Medicaid")

Instructions for updating the *medicaid* package:
- Simply reinstall the package by typing devtools::install_github("PHSKC-APDE/Medicaid")

Current functionality of the *medicaid* package (v 0.1.3):
- Request an eligibility and demographics-based Medicaid member cohort
- Request a claims summary (e.g. ED visits, avoidable ED, behavioral health hospital stays) for a member cohort
- Request coverage group information (e.g. persons with disabilities) and automatically join to a specified data frame
- Request chronic health condition (e.g. asthma) information and automatically join to a specified data frame
- Tabulate counts by fixed and looped by variables (i.e. data aggregation), with automatic suppression and other features
- Calculate the top N causes of ED visits and hospitalizations

Training resources:
- R users can view a [training video](https://kc1-my.sharepoint.com/:v:/r/personal/eli_kern_kingcounty_gov/Documents/Shared%20with%20Everyone/Medicaid%20R%20Package%20Training_2018.mp4?csf=1&e=3OydL9) for how to use the *medicaid* package.
- Users can also view the [R script used in the training video](https://github.com/PHSKC-APDE/Medicaid/blob/master/Medicaid%20package%20orientation.R).
