#### CODE TO GEOCODE ADDRESSES (MEDICAID AND HOUSING)
# Alastair Matheson, PHSKC (APDE)
#
# 2019-09
#
# This only geocodes addresses newly added to the ref.address_clean table


#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 350, scipen = 999, warning.length = 8170)

library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(sf) # Read shape files

geocode_path <- "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Geocoding"
s_shapes <- "//phshare01/epe_share/WORK/REQUESTS/Maps/Shapefiles/"
g_shapes <- "//gisdw/kclib/Plibrary2/"

# Add connection to the SQL db
db_claims <- dbConnect(odbc(), "PHClaims51")


#### PULL IN DATA ####
# Join ref.address_clean to ref.address_geocode to find addresses not geocoded
adds_to_code <- dbGetQuery(
  db_claims,
  "SELECT DISTINCT a.*, b.geocoded
    FROM
  (SELECT geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean
  FROM ref.address_clean) a
  LEFT JOIN
  (SELECT geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
  1 as geocoded
  FROM ref.address_geocode) b
  ON 
  (a.geo_add1_clean = b.geo_add1_clean OR (a.geo_add1_clean IS NULL AND b.geo_add1_clean IS NULL)) AND
  (a.geo_city_clean = b.geo_city_clean OR (a.geo_city_clean IS NULL AND b.geo_city_clean IS NULL)) AND
  (a.geo_state_clean = b.geo_state_clean OR (a.geo_state_clean IS NULL AND b.geo_state_clean IS NULL)) AND
  (a.geo_zip_clean = b.geo_zip_clean OR (a.geo_zip_clean IS NULL AND b.geo_zip_clean IS NULL))
  WHERE b.geocoded IS NULL")


# Combine addresses into single field to reduce erroneous matches
adds_to_code <- adds_to_code %>%
  mutate(geo_add_single = glue("{geo_add1_clean}, {geo_city_clean}, {geo_zip_clean}", .na = ""))

### Write out for processing in ArcGIS
data.table::fwrite(adds_to_code,
                   file.path(geocode_path, paste0("distinct_addresses_", Sys.Date(), ".csv")))



### Import data
# First pull in list of shape files in folder
geocode_files <- list.files(path = geocode_path, pattern = "(d|D)istinct_addresses_geocoded_[0-9|-]*.shp$")

# Pull in shape file
adds_coded_esri <- read_sf(file.path(geocode_path, max(geocode_files)))

# Convert to WSG84 projected coordinate system to obtain x/y
adds_coded_esri <- st_transform(adds_coded_esri, 3857)

adds_coded_esri <- adds_coded_esri %>%
  rename(geo_add1_clean = geo_add1_c,
         geo_city_clean = geo_city_c,
         geo_state_clean = geo_state_,
         geo_zip_clean = geo_zip_cl) %>%
  mutate(geo_x = st_coordinates(adds_coded_esri)[,1],
         geo_y = st_coordinates(adds_coded_esri)[,2]) %>%
  mutate_at(vars(geo_x, geo_y), list( ~ ifelse(is.na(.), 0, .))) %>%
  select(geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
         Loc_name, Status, geo_x, geo_y, Addr_type, Match_addr) %>%
  st_drop_geometry()

# Recreate coordinates
adds_coded_esri <- st_as_sf(adds_coded_esri, coords = c("geo_x", "geo_y"), 
                               crs = 3857, remove = F)

# Convert to WSG84 geographic coordinate system to obtain lat/lon
adds_coded_esri <- st_transform(adds_coded_esri, 4326)

adds_coded_esri <- adds_coded_esri %>%
  mutate(geo_lon = st_coordinates(adds_coded_esri)[,1],
         geo_lat = st_coordinates(adds_coded_esri)[,2]) %>%
  st_drop_geometry()


### Find addresses that need additional geocoding
adds_coded_unmatch <- adds_coded_esri %>%
  filter(Status == "U" | Loc_name == "zip_5_digit_gc" | is.na(Loc_name)) %>%
  mutate(geo_add_single = paste(geo_add1_clean, geo_city_clean, geo_state_clean,
                                geo_zip_clean, "USA", sep = ", "))


### Run through the HERE geocoder
# Get an API here: https://developer.here.com
if ("rstudioapi" %in% installed.packages()[,"Package"]) {
  app_id <- rstudioapi::askForPassword(prompt = 'Please enter HERE app id: ')
  app_code <- rstudioapi::askForPassword(prompt = 'Please enter HERE app code: ')
} else {
  app_id <- readline(prompt = "Please enter HERE app id: ")
  app_code <- readline(prompt = "Please enter HERE app code: ")
}

here_url <- "http://geocoder.api.here.com/6.2/geocode.json"


geocode_here_f <- function(address, here_app_code = app_code, here_app_id = app_id) {
  
  if (!is.character(address)) {
    stop("'address' must be a character string")
  } else {
    add_text <- address
  }
  
  # Query HERE servers (make sure API key is stored)
  geo_query <- httr::GET(here_url, 
                         query = list(app_id = here_app_id,
                                      app_code = here_app_code,
                                      searchtext = add_text))
  
  # Convert results to a list
  geo_reply <- httr::content(geo_query)
  
  # Set up response for when answer is not specific enough
  answer <- data.frame(lat = NA,
                       lon = NA,
                       formatted_address = NA,
                       address_type = NA)
  
  # Check for a result
  if (length(geo_reply$Response$View) > 0) {
    
    # Convert to a data frame
    geo_reply <- as.data.frame(geo_reply$Response$View)
    
    answer <- answer %>%
      mutate(
        lat = geo_reply$Result.Location.NavigationPosition.Latitude,
        lon = geo_reply$Result.Location.DisplayPosition.Longitude,
        formatted_address = geo_reply$Result.Location.Address.Label,
        address_type = geo_reply$Result.MatchLevel
      )
  }
  return(answer)
}

# Initialise a dataframe to hold the results
adds_here <- data.frame()
# Find out where to start in the address list (if the script was interrupted before):
startindex <- 1

# Start the geocoding process - address by address - can do 250k per month
for (i in seq(startindex, nrow(adds_coded_unmatch))) {
  print(paste("Working on index", i, "of", nrow(adds_coded_unmatch)))
  # query the geocoder - this will pause here if we are over the limit.
  result <- geocode_here_f(address = adds_coded_unmatch$geo_add_single[i])
  result$index <- i
  result$input_add <- adds_coded_unmatch$geo_add_single[i]
  result$geo_check_here <- 1
  # append the answer to the results file
  adds_here <- rbind(adds_here, result)
}

# Look at match results
adds_here %>% group_by(address_type) %>% summarise(count = n())

# Combine HERE results back to unmatched data
adds_coded_here <- left_join(adds_coded_unmatch, adds_here,
                                  by = c("geo_add_single" = "input_add")) %>%
  select(geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
         lat, lon, formatted_address, address_type, geo_check_here) %>%
  mutate_at(vars(lat, lon), list( ~ replace_na(., 0)))

# Convert to WSG84 projected coordinate system to obtain x/y
adds_coded_here <- st_as_sf(adds_coded_here, coords = c("lon", "lat"), 
                                 crs = 4326, remove = F)
adds_coded_here <- st_transform(adds_coded_here, 3857)
adds_coded_here <- adds_coded_here %>%
  mutate(geo_x = st_coordinates(adds_coded_here)[,1],
         geo_y = st_coordinates(adds_coded_here)[,2]) %>%
  st_drop_geometry() %>%
  rename(geo_lat = lat, geo_lon = lon) %>%
  distinct()

### Combine back to initial data
adds_coded <- left_join(adds_coded_esri, adds_coded_here,
                             by = c("geo_add1_clean", "geo_city_clean",
                                    "geo_state_clean", "geo_zip_clean"))

# Look at how the HERE geocodes improved things
adds_coded %>% group_by(Loc_name, address_type) %>% summarise(count = n())


# Collapse to useful columns and select matching from each source as appropriate
adds_coded <- adds_coded %>%
  # Add metadata indicating where the geocode comes from and if ZIP centroid
  mutate(
    formatted_address = as.character(formatted_address),
    address_type = as.character(address_type),
    geo_check_esri = 1,
    geo_check_here = ifelse(is.na(geo_check_here), 0, geo_check_here),
    geo_geocode_source = case_when(
      !is.na(geo_lat.x) & 
        Loc_name %in% c("address_point_", "pin_address_on", "st_address_us", 
                        "trans_network_", "Pierce_gcs", "Snohomish_gcs",
                        "Kitsap_gcs") ~ "esri",
      !is.na(geo_lat.y) & address_type %in% c("houseNumber", "street") ~ "here",
      !is.na(geo_lat.x) & Loc_name == "zip_5_digit_gc" ~ "esri",
      !is.na(geo_lat.y) & address_type %in% c("postalCode", "district") ~ "here",
      TRUE ~ NA_character_),
    geo_zip_centroid = ifelse((geo_geocode_source == "esri" & Loc_name == "zip_5_digit_gc") |
                                (geo_geocode_source == "here" & address_type %in% c("postalCode", "district")),
                              1, 0),
    geo_street_centroid = ifelse(geo_geocode_source == "here" & address_type == "street", 1, 0),
    # Move address and coordindate data into a single field
    geo_add_geocoded = ifelse(geo_geocode_source == "esri", 
                              toupper(Match_addr), 
                              toupper(formatted_address)),
    geo_zip_geocoded = case_when(
      geo_geocode_source == "esri" ~ str_sub(Match_addr,
                                             str_locate(Match_addr, "[:digit:]{5}$")[,1],
                                             str_locate(Match_addr, "[:digit:]{5}$")[,2]),
      geo_geocode_source == "here" & str_detect(formatted_address, "^[:digit:]{5},") ~ 
        str_sub(formatted_address, 
                str_locate(formatted_address, "^[:digit:]{5},")[,1],
                str_locate(formatted_address, "^[:digit:]{5},")[,2] - 1),
      geo_geocode_source == "here" & str_detect(formatted_address, " [:digit:]{5},") ~ 
        str_sub(formatted_address, 
                str_locate(formatted_address, " [:digit:]{5},")[,1] + 1,
                str_locate(formatted_address, " [:digit:]{5},")[,2] - 1)),
    geo_add_type = case_when(
      geo_geocode_source == "esri" ~ Loc_name,
      geo_geocode_source == "here" ~ address_type
    ),
    geo_lon = ifelse(geo_geocode_source == "esri", geo_lon.x, geo_lon.y),
    geo_lat = ifelse(geo_geocode_source == "esri", geo_lat.x, geo_lat.y),
    geo_x = ifelse(geo_geocode_source == "esri", geo_x.x, geo_x.y),
    geo_y = ifelse(geo_geocode_source == "esri", geo_y.x, geo_y.y)
  ) %>%
  select(geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean, 
         geo_add_geocoded, geo_zip_geocoded, geo_add_type,
         geo_check_esri, geo_check_here, geo_geocode_source, 
         geo_zip_centroid, geo_street_centroid,
         geo_lon, geo_lat, geo_x, geo_y)


### Remove any addresses that could not be geocoded to an acceptable level
# (mostly HERE geocodes at the city level)
adds_coded <- adds_coded %>% filter(!is.na(geo_geocode_source))


#### JOIN TO SPATIAL FILES OF INTEREST ####
### Set up as spatial file
adds_coded <- st_as_sf(adds_coded, coords = c("geo_lon", "geo_lat"), 
                            crs = 4326, remove = F)


### Bring in shape files for relevant geographies
block <- st_read(file.path(s_shapes, "Blocks/2010/WA state wide/block10.shp"))
puma <- st_read(file.path(s_shapes, "PUMAs/WA_2013_puma10/tl_2013_53_puma10.shp"))
zcta <- st_read(file.path(s_shapes, "ZCTA/tl_2010_53_zcta510.shp"))
hra <- st_read(file.path(s_shapes, "HRA-HealthReportingAreas/HRA_2010Block_Clip.shp"))
region <- st_read(file.path(s_shapes, "Regions/KC_HRA_Rgn.shp"))
school <- st_read(file.path(s_shapes, "Schools/School Districts/2010 Census/Unified/KC_school_districts_water_trim_northshore_extended.shp"))
kcc_dist <- st_read(file.path(g_shapes, "district/shapes/polygon/kccdst.shp"))
wa_dist <- st_read(file.path(g_shapes, "district/shapes/polygon/legdst.shp"))
scc_dist <- st_read(file.path(s_shapes, "Council Districts/SCCdistrict.shp"))

### Convert all shapefiles to EPSG 4326
block <- st_transform(block, 4326)
puma <- st_transform(puma, 4326)
zcta <- st_transform(zcta, 4326)
hra <- st_transform(hra, 4326)
region <- st_transform(region, 4326)
school <- st_transform(school, 4326)
kcc_dist <- st_transform(kcc_dist, 4326)
wa_dist <- st_transform(wa_dist, 4326)
scc_dist <- st_transform(scc_dist, 4326)


# Block (also contains state and county FIPS)
adds_coded_joined <- st_join(adds_coded, block) %>%
  select(geo_add1_clean:geo_y, STATEFP10, COUNTYFP10, TRACTCE10, BLOCKCE10, GEOID10, 
         geometry) %>%
  rename(geo_statefp10 = STATEFP10, geo_countyfp10 = COUNTYFP10, 
         geo_tractce10 = TRACTCE10, geo_blockce10 = BLOCKCE10, geo_block_geoid10 = GEOID10)
# PUMAs
adds_coded_joined <- st_join(adds_coded_joined, puma) %>%
  select(geo_add1_clean:geo_block_geoid10, PUMACE10, GEOID10, NAMELSAD10,
         geometry) %>%
  rename(geo_pumace10 = PUMACE10, geo_puma_geoid10 = GEOID10, geo_puma_name = NAMELSAD10)
# ZCTA
adds_coded_joined <- st_join(adds_coded_joined, zcta) %>%
  select(geo_add1_clean:geo_puma_name, ZCTA5CE10, GEOID10,
         geometry) %>%
  rename(geo_zcta5ce10 = ZCTA5CE10, geo_zcta_geoid10 = GEOID10)
# HRA
adds_coded_joined <- st_join(adds_coded_joined, hra) %>%
  select(geo_add1_clean:geo_zcta_geoid10, VID, HRA2010v2_,
         geometry) %>%
  rename(geo_hra_id = VID, geo_hra = HRA2010v2_)
# Region
adds_coded_joined <- st_join(adds_coded_joined, region) %>%
  select(geo_add1_clean:geo_hra, RgnVID, Rgn2012,
         geometry) %>%
  rename(geo_region_id = RgnVID, geo_region = Rgn2012)
# School districts
adds_coded_joined <- st_join(adds_coded_joined, school) %>%
  select(geo_add1_clean:geo_region, GEOID10, NAME10,
         geometry) %>%
  rename(geo_school_geoid10 = GEOID10, geo_school = NAME10)
# King County Council districts
adds_coded_joined <- st_join(adds_coded_joined, kcc_dist) %>%
  select(geo_add1_clean:geo_school, kccdst, geometry) %>%
  rename(geo_kcc_dist = kccdst)
# WA legislative districts
adds_coded_joined <- st_join(adds_coded_joined, wa_dist) %>%
  select(geo_add1_clean:geo_kcc_dist, LEGDST,
         geometry) %>%
  rename(geo_wa_legdist = LEGDST)
# Seattle City County districts
adds_coded_joined <- st_join(adds_coded_joined, scc_dist) %>%
  select(geo_add1_clean:geo_wa_legdist, SCCDST,
         geometry) %>%
  rename(geo_scc_dist = SCCDST)


### Convert factors to character
adds_coded_load <- adds_coded_joined %>%
  mutate_at(vars(geo_statefp10, geo_countyfp10, geo_tractce10, geo_blockce10,
                 geo_block_geoid10, geo_pumace10, geo_puma_geoid10, geo_puma_name,
                 geo_zcta5ce10, geo_zcta_geoid10, geo_hra, geo_region, 
                 geo_school_geoid10, geo_school),
            list( ~ as.character(.))) %>%
  mutate_at(vars(geo_kcc_dist, geo_wa_legdist, geo_scc_dist),
            list( ~ as.integer(.))) %>%
  st_drop_geometry() %>%
  mutate(last_run = Sys.time())


#### LOAD TO SQL ####
# Check how many rows are already in the stage table
stage_rows_before <- as.numeric(dbGetQuery(db_claims, "SELECT COUNT (*) FROM stage.address_geocode"))

# Write data
dbWriteTable(db_claims,
             name = DBI::Id(schema = "stage", table = "address_geocode"), 
             value = as.data.frame(adds_coded_load), 
             append = T, overwrite = F)


#### BASIC QA ####
### Compare row counts now
row_load_ref_geo <- nrow(adds_coded_load)
stage_rows_after <- as.numeric(dbGetQuery(db_claims, "SELECT COUNT (*) FROM stage.address_geocode"))

if (stage_rows_before + row_load_ref_geo == stage_rows_after == F) {
  warning("Number of rows added to stage.address_geocode now expected value")
}