# Author: Danny Colombara
# 
# Date: August 14, 2019
#
# R version: 3.5.3
#
# Purpose: Create SQL Medicare QA tables (just a shell that will be filled with QA process data)
#          Follow Medicaid tables as a template 
#

## Set up environment ----
    pacman::p_load(odbc, glue)

## Connect to SQL server ----
    db_claims <- dbConnect(odbc(), "PHClaims51") 

## Create [PHClaims].[metadata].[qa_mcaid] ----
    setDT(DBI::dbGetQuery(db_claims, "SELECT TOP 0 * INTO metadata.qa_mcare FROM metadata.qa_mcaid"))
    
## Create [PHClaims].[metadata].[qa_mcaid_values] ----
    setDT(DBI::dbGetQuery(db_claims, "SELECT TOP 0 * INTO metadata.qa_mcare_values FROM metadata.qa_mcaid_values"))  
    
## The end! 