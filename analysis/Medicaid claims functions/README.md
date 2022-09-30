
# Medicaid eligibility and claims stored procedures

## Purpose
This folder contains stored procedures that return extracts of Medicaid eligibility and/or claims data from SQL Server. The *medicaid* R package, described [here](https://github.com/PHSKC-APDE/Medicaid#medicaid) used to call these stored procedures to return such extracts to R. These have now been deprecated and removed from PHClaims server, and they were never on the HHSAW side.

The sp_mcaidcohort_sql was replaced by the claims_elig.R script.

The sp_claims stored procedures have not yet been rebuilt in the claims R package, but this can always be added to the claims package in the future if desired. These stored procedures simply returned claims summary tables at the person level (running on top of what is now claims_elig).

## Eligibility data stored procedures
- sp_mcaidcohort_r_step1.sql generates temp tables covering demographic and coverage topics for a requested Medicaid member cohort
- sp_mcaidcohort_r_step2.sql joins the above temp tables and returns a final select query to R
- sp_mcaidcohort_sql.sql does the same thing as step 1 and 2 above, but creates a temp table instead of returning a select query

## Claims data stored procedures
- sp_mcaid_claims_simple_r.sql returns a subset of claim summary variables to R, joined to eligibility data from sp_mcaidcohort_sql
- sp_mcaid_claims_detail_r.sql returns a more detailed set of claim summary variables to R, joined to eligibility data from sp_mcaidcohort_sql
