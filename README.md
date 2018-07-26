# Medicaid
This R package is for querying and analyzing Washington State Medicaid eligibility and claims data.

Some of the code is specific to WA State Medicaid claims data. 
However, many of the functions and concepts can be applied to other claims data sets, including Medicare data.

- See eligibility cleanup folder for code that processes and stores Medicaid eligibility data on SQL server
- See claims cleanup folder for code that processes and stores Medicaid claims data on SQL server
- See analysis folder for useful functions that are not yet included in the medicaid R package
- See reference documents folder for documentation helpful for understanding WA State Medicaid data
- The R folder contains the functions used in the medicaid package.
- The man folder contains the help files for the medicaid package.

A data dictionary for the Medicaid data tables stored on King County SQL Server can be accessed [on OneDrive](https://kc1-my.sharepoint.com/:x:/g/personal/eli_kern_kingcounty_gov/EZE5ge9YnXxFifiyDIeq8JYBDbiRHIK_t_9-ERAhd13zhQ?e=5PZPiH).

# Intructions for installing the Medicaid package
1) Make sure devtools is installed (install.packages("devtools")).
2) Type devtools::install_github("PHSKC-APDE/Medicaid")

# Current functionality of the Medicaid package (v 0.1.0)
- Request an eligibility and demographics-based Medicaid member cohort
- Request a claims summary (e.g. ED visits, avoidable ED, behavioral health hospital stays) for a member cohort
- Request coverage group information (e.g. persons with disabilities) and automatically join to a specified data frame
- Request chronic health condition (e.g. asthma) information and automatically join to a specified data frame
- Tabulate counts by fixed and looped by variables (i.e. data aggregation), with automatic suppression and other features
