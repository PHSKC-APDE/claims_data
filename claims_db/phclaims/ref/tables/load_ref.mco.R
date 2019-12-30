#### CODE TO LOAD REF.MCO
# Alastair Matheson (PHSKC, APDE)
#
# 2019-12-17
#
# Assuming working from a project where working directory is the claims repo

#### Set up connection
db_claims <- DBI::dbConnect(odbc::odbc(), "PHClaims51")

#### Pull in csv file
mco <- data.table::fread(file.path(getwd(), "claims_db/phclaims/ref/tables_data",
                                   "ref.mco.csv"))

#### Load to SQL
DBI::dbWriteTable(conn = db_claims, 
                  name = DBI::Id(schema = "ref", table = "mco"),
                  value = mco,
                  overwrite = T)
