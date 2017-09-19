*Initial exploration of Medicaid data
*Eli Kern
*6/6/2017

clear
set more off, perm

odbc load id = "MEDICAID_RECIPIENT_ID" in 1/100, table("NewEligibility") dsn(PHClaims50)

exec(`"SELECT ID, "Last Name", "Job Title" FROM Employees WHERE ID <= 5"') 
