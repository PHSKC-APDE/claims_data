##Code to create and load data to ref.geo_county_code_wa
##Lookup table for mapping WA county number to FIPS county codes
##Eli Kern (PHSKC-APDE)
##2019-10
## Updated by Alastair Matheson, 2022-05


# Set up global parameter and call in libraries ----
if (!require("pacman")) {install.packages("pacman")}
pacman::p_load(tidyverse, odbc)

# Bring in data file
county_codes <- rads.data::spatial_county_codes
county_codes <- county_codes %>% 
  mutate(geo_county_code_fips = stringr::str_pad(geo_county_code_fips, width = 3, pad = "0"),
         across(c("geo_county_fips_long", "geo_county_code_order", "geo_county_code_gnis",
                  "geo_county_code_tiger", "geo_county_code_aff"), ~ as.character(.)))


# Load to PHClaims ----
db_claims <- DBI::dbConnect(odbc::odbc(), "PHClaims51")

DBI::dbWriteTable(db_claims,
                  name = DBI::Id(schema = "ref", table = "geo_county_code_wa"),
                  value = county_codes,
                  overwrite = T, append = F)


# Load to HHSAW ----
db_hhsaw <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433",
                           database = "hhs_analytics_workspace",
                           uid = keyring::key_list("hhsaw")[["username"]],
                           pwd = keyring::key_get("hhsaw", keyring::key_list("hhsaw")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")

DBI::dbWriteTable(db_hhsaw,
                  name = DBI::Id(schema = "claims", table = "ref_geo_county_code_wa"),
                  value = county_codes,
                  overwrite = T, append = F)
