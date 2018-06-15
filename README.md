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

# Intructions for installing the medicaid package
1) Clone this repository.
2) Make sure devtools is installed (install.packages("devtools")).
3) Type devtools::install_github("PHSKC-APDE/Medicaid")
