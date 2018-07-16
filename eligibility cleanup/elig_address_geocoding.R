###############################################################################
# Code to create geocoded data for all addresses in the Medicaid eligibility data
# 
# Alastair Matheson (PHSKC-APDE)
# 2016-08-08
#
# Current steps:
#
# Cleaning (not this code file):
# 1) Run full addresses through Informatica address tool
# 2) Join cleaned adds back to original address list
# 3) Load to SQL (mcaid_elig_address_clean)
# NB. Will eventually add in manual cleanup process before Informatica step
#
# Geocoding (this code file):
# 1) Keep distinct clean street addresses in a separate file
# 2) Geocode clean adds with KC ESRI geolocator
# 3) Geocode remaining addresses via Google/Opencage
# 4) Join the geocoded data together and check coordinates/projections align
# 5) Spatial joins with various geographies of interest
# 6) Load to SQL (mcaid_elig_address)
#
###############################################################################

##### Set up global parameter and call in libraries #####
options(max.print = 700, scipen = 0)

library(odbc) # used to connect to SQL server
library(tidyverse) # used to manipulate data
library(openxlsx) # used to export data to Excel
library(ggmap) # used to geocode remaining addresses
# Note: may need to install latest version of ggmap from Github:
# devtools::install_github("dkahle/ggmap")
# This will allow the use of a Google Maps API key
library(opencage) # used to geocode addresses (alternative)
library(rgdal) # Used to convert coordinates between ESRI and Google output
library(sf) # newer package for working with spatial data


geo_path <- "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Data processing protocols/Geocoding/"
db.claims51 <- dbConnect(odbc(), "PHClaims51")
s_shapes <- "//phshare01/epe_share/WORK/REQUESTS/Maps/Shapefiles/"
g_shapes <- "//gisdw/kclib/Plibrary2/"

# set bounds for searches
bounds_goog <- "& bounds=47,-122.7|48,-121"
bounds_opencage <- c(-123, 46.8, -120.5, 48.5)


#### Bring in all the relevant address data ####
esri_geo <- data.table::fread(paste0(geo_path, "ESRI_geocoded_2018-06-20.csv"))

# Bring in shape files for relevant geographies
block <- st_read(file.path(s_shapes, "Blocks/2010/WA state wide/block10.shp"))
puma <- st_read(file.path(s_shapes, "PUMAs/WA_2013_puma10/tl_2013_53_puma10.shp"))
zcta <- st_read(file.path(s_shapes, "ZCTA/tl_2010_53_zcta510.shp"))
hra <- st_read(file.path(s_shapes, "HRA-HealthReportingAreas/HRA_2010Block_Clip.shp"))
region <- st_read(file.path(s_shapes, "Regions/KC_HRA_Rgn.shp"))
school <- st_read(file.path(s_shapes, "Schools/School Districts/2010 Census/Unified/KC_school_districts_water_trim_northshore_extended.shp"))
kcc_dist <- st_read(file.path(g_shapes, "district/shapes/polygon/kccdst.shp"))
wa_dist <- st_read(file.path(g_shapes, "district/shapes/polygon/legdst.shp"))
scc_dist <- st_read(file.path(s_shapes, "Council Districts/SCCdistrict.shp"))



#### Separate into separate files for geocoding ####
esri_unmat <- esri_geo %>%
  filter(Loc_name == "zip_5_digit_gc" |  Status == "U") %>%
  select(FID, add1, city, state, zip_1) %>%
  mutate(address = paste(add1, city, state, zip_1, "USA", sep = ", "))


#### Set up to run via Google Maps ####
# Using function found here: https://www.shanelynn.ie/massive-geocoding-with-r-and-google-maps/
#define a function that will process googles server responses for us.
geocode_goog <- function(address) {
  #use the gecode function to query google servers
  geo_reply <- geocode(address, output = 'all', messaging = TRUE, override_limit = TRUE)
  print(geo_reply$status)
  #now extract the bits that we need from the returned list
  answer <- data.frame(lat = NA, long = NA, accuracy = NA, 
                       formatted_address = NA, address_type = NA, status = NA)
  answer$status <- geo_reply$status
  
  #if we are over the query limit - want to pause for 2 hours
  while(geo_reply$status == "OVER_QUERY_LIMIT") {
    print(paste0("OVER QUERY LIMIT - Pausing for 2 hours at: ", 
                 as.character(Sys.time())))
    Sys.sleep(60*60*2)
    geo_reply <- geocode(address, output='all', messaging=TRUE, override_limit=TRUE)
    answer$status <- geo_reply$status
  }
  
  #return NAs if we didn't get a match:
  if (geo_reply$status != "OK"){
    return(answer)
  }   
  # else, extract what we need from the Google server reply into a dataframe:
  answer$lat <- geo_reply$results[[1]]$geometry$location$lat
  answer$long <- geo_reply$results[[1]]$geometry$location$lng   
  if (length(geo_reply$results[[1]]$types) > 0){
    answer$accuracy <- geo_reply$results[[1]]$types[[1]]
  }
  answer$address_type <- paste(geo_reply$results[[1]]$types, collapse=',')
  answer$formatted_address <- geo_reply$results[[1]]$formatted_address
  
  return(answer)
}

# Initialise a dataframe to hold the results
goog_geocoded <- data.frame()
# Find out where to start in the address list (if the script was interrupted before):
startindex <- 1
# If a temp file exists - load it up and count the rows!
tempfilename <- paste0(geo_path, "Google_geocoded_2018-06-20.rds")
if (file.exists(tempfilename)){
  print("Found temp file - resuming from index:")
  goog_geocoded <- readRDS(tempfilename)
  startindex <- nrow(goog_geocoded)
  print(startindex)
}

# Start the geocoding process - address by address. geocode() function takes care of query speed limit.
for (i in seq(startindex, nrow(esri_unmat))){
  print(paste("Working on index", i, "of", nrow(esri_unmat)))
  #query the google geocoder - this will pause here if we are over the limit.
  result <- geocode_goog(esri_unmat$address[i]) 
  result$index <- i
  FID <- esri_unmat$FID[i]
  result <- cbind(result, FID)
  #append the answer to the results file.
  goog_geocoded <- rbind(goog_geocoded, result)
  #save temporary results as we are going along
  saveRDS(goog_geocoded, tempfilename)
}



#### Trying using Opencage instead ####
# Store API key temporarily
# Sign up for a key here: https://opencagedata.com (2,500 limit per day)
OPENCAGE_KEY <- rstudioapi::askForPassword(prompt = 'Please enter API key: ')


# Make a function to geocode and return results
geocode_cage <- function(address) {
  # Query open cage servers (make sure API key is stored)
  geo_reply <- opencage_forward(address, key = OPENCAGE_KEY, 
                                # Keep annotations to get Mercator coords
                                no_annotations = F,
                                # ensure the search is not stored on their servers
                                no_record = T,
                                bounds = bounds_opencage,
                                countrycode = "US")
  
  #Note how many attempts are left
  print(paste0(geo_reply$rate_info$remaining, " tries remaining"))
  
  # If we are over the query limit - wait until the reset
  while(geo_reply$rate_info$remaining < 1 & 
        Sys.time() < geo_reply$rate_info$reset) {
    print(paste0("No queries remaining - resume at: ", 
                 geo_reply$rate_info$reset))
    # Putting in several hours for now since the calc fails
    #Sys.sleep(abs(geo_reply$rate_info$reset - Sys.time()))
    Sys.sleep(60*60*24)
  }
  
  # Set up response for when answer is not specific enough
  answer <- data.frame(lat = NA,
                       long = NA,
                       x = NA_real_,
                       y = NA_real_,
                       formatted_address = NA,
                       address_type = NA,
                       confidence = NA)
  
  # Temporarily store results as a df to filter and sort
  answer_tmp <- as.data.frame(geo_reply$results)
  answer_tmp <- answer_tmp %>%
    filter(components._type == "building") %>%
    # Take the most confident (i.e., smallest bounding box)
    arrange(desc(confidence)) %>%
    slice(1)
  
  # Return NAs if we didn't get a match:
  if (nrow(answer_tmp) == 0) {
    return(answer)
  }   
  # Else, extract what we need into a dataframe:
  answer <- answer %>%
    mutate(
      lat = answer_tmp$geometry.lat,
      long = answer_tmp$geometry.lng,
      x = answer_tmp$annotations.Mercator.x,
      y = answer_tmp$annotations.Mercator.y,
      formatted_address = answer_tmp$formatted,
      address_type = answer_tmp$components._type,
      confidence = answer_tmp$confidence
    )
  
  return(answer)
}


# Initialise a dataframe to hold the results
cage_geocoded <- data.frame()
# Find out where to start in the address list (if the script was interrupted before):
startindex <- 1
# If a temp file exists - load it up and count the rows!
tempfile_cage <- paste0(geo_path, "Opencage_geocoded_2018-06-20.rds")
if (file.exists(tempfile_cage)){
  print("Found temp file - resuming from index:")
  cage_geocoded <- readRDS(tempfile_cage)
  startindex <- nrow(cage_geocoded)
  print(startindex)
}

# Start the geocoding process - address by address. geocode() function takes care of query speed limit.
for (i in seq(startindex, nrow(esri_unmat))){
  print(paste("Working on index", i, "of", nrow(esri_unmat)))
  #query the google geocoder - this will pause here if we are over the limit.
  result <- geocode_cage(esri_unmat$address[i]) 
  result$index <- i
  FID <- esri_unmat$FID[i]
  result <- cbind(result, FID)
  #append the answer to the results file.
  cage_geocoded <- rbind(cage_geocoded, result)
  #save temporary results as we are going along
  saveRDS(cage_geocoded, tempfile_cage)
}

# Remove any duplicates that snuck in due to retarting the process
cage_geocoded <- cage_geocoded %>% distinct()


#### Join results back to ESRI data ####
# If Opencage geocoding is complete, read in file here
cage_geocoded <- readRDS(paste0(geo_path, "Opencage_geocoded_2018-06-20.rds"))
add_geocoded <- left_join(esri_geo, cage_geocoded, by = "FID")

### Convert to sf object type and transform CRS
add_geocoded_sf <- st_as_sf(add_geocoded, crs = 2285, coords = c("X", "Y"))
add_geocoded_sf <- st_transform(add_geocoded_sf, 3857)


### Keep data from supplemental geocoding if present
add_geocoded_sf <- add_geocoded_sf %>%
  #### TEMP MEASURE UNTIL FIX UP PROCESSING OF ADDRESS CLEANING
  rename(add1_new = add1, city_new = city, state_new = state,
         zip_new = zip_1) %>%
  #### END TEMP MEASURE
  mutate(
    # Add metadata indicating where the geocode comes from and if ZIP centroid
    check_esri = 1,
    check_opencage = ifelse(is.na(index), 0, 1),
    geocode_source = ifelse(is.na(lat), "esri", "opencage"),
    zip_centroid = ifelse(geocode_source == "esri" & 
                            Loc_name == "zip_5_digit_gc", 1, 0),
    ### Move address and coordindate data into a single field
    add_geocoded = ifelse(geocode_source == "opencage",
                          formatted_address, Match_addr),
    # Extract the ZIP from opencage address
    zip_opencage = ifelse(
      is.na(formatted_address), NA, 
            str_sub(formatted_address,
                    str_locate(formatted_address, "[:digit:]{5},[:space:]")[,1],
                    str_locate(formatted_address, "[:digit:]{5},[:space:]")[,2]-2)),
    # Keep the geocoded ZIP
    zip_geocoded = ifelse(geocode_source == "opencage" & !is.na(zip_opencage),
                          zip_opencage, ZIP),
    add_type = ifelse(geocode_source == "opencage",
                      address_type, Addr_type),
    x = case_when(
      geocode_source == "opencage" ~ as.numeric(x),
      geocode_source == "esri" & Status != "U" ~ st_coordinates(.)[,1]
      ),
    y = case_when(
      geocode_source == "opencage" ~ as.numeric(y),
      geocode_source == "esri" & Status != "U" ~ st_coordinates(.)[,2]
      )
    ) %>%
  select(add1_new, city_new, state_new, zip_new,
         add_geocoded, zip_geocoded, add_type, x, y,
         check_esri, check_opencage, geocode_source, zip_centroid) %>%
  # Remove any missing coordinates (these will gradually be manualled corrected)
  filter(!is.na(x))


# Replace geometry with combined values (seem to need to clear then recreate)
st_geometry(add_geocoded_sf) <- NULL
add_geocoded_sf <- st_as_sf(add_geocoded_sf, crs = 3857, 
                            coords = c("x", "y"), 
                            remove = F)

# Convert to lat/lon (EPSG:4326) and extract columns
add_geocoded_sf <- st_transform(add_geocoded_sf, 4326)
add_geocoded_sf <- add_geocoded_sf %>%
  mutate(
    lon = st_coordinates(.)[,1],
    lat = st_coordinates(.)[,2]
  )


#### Make spatial joins with geography shape files ####
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


### Join to each shapefile
# Could maybe use a list but need to process results for each join differently
# Also some shapefiles have the same column names

# Block (also contains state and county FIPS)
add_geocoded_sf <- st_join(add_geocoded_sf, block) %>%
  select(add1_new:lat, STATEFP10, COUNTYFP10, TRACTCE10, BLOCKCE10, GEOID10, 
         geometry) %>%
  rename(statefp10 = STATEFP10, countyfp10 = COUNTYFP10, 
         tractce10 = TRACTCE10, blockce10 = BLOCKCE10, blockgeoid10 = GEOID10)
# PUMAs
add_geocoded_sf <- st_join(add_geocoded_sf, puma) %>%
  select(add1_new:block_geoid10, PUMACE10, GEOID10, NAMELSAD10,
         geometry) %>%
  rename(pumace10 = PUMACE10, puma_geoid10 = GEOID10, puma_name = NAMELSAD10)
# ZCTA
add_geocoded_sf <- st_join(add_geocoded_sf, zcta) %>%
  select(add1_new:puma_name, ZCTA5CE10, GEOID10,
         geometry) %>%
  rename(zcta5ce10 = ZCTA5CE10, zcta_geoid10 = GEOID10)
# HRA
add_geocoded_sf <- st_join(add_geocoded_sf, hra) %>%
  select(add1_new:zcta_geoid10, VID, HRA2010v2_,
         geometry) %>%
  rename(hra_id = VID, hra = HRA2010v2_)
# Region
add_geocoded_sf <- st_join(add_geocoded_sf, region) %>%
  select(add1_new:hra, RgnVID, Rgn2012,
         geometry) %>%
  rename(region_id = RgnVID, region = Rgn2012)
# School districts
add_geocoded_sf <- st_join(add_geocoded_sf, school) %>%
  select(add1_new:region, GEOID10, NAME10,
         geometry) %>%
  rename(school_geoid10 = GEOID10, school = NAME10)
# King County Council districts
add_geocoded_sf <- st_join(add_geocoded_sf, kcc_dist) %>%
  select(add1_new:school, kccdst, geometry) %>%
  rename(kcc_dist = kccdst)
# WA legislative districts
add_geocoded_sf <- st_join(add_geocoded_sf, wa_dist) %>%
  select(add1_new:kcc_dist, LEGDST,
         geometry) %>%
  rename(wa_legdist = LEGDST)
# Seattle City County districts
add_geocoded_sf <- st_join(add_geocoded_sf, scc_dist) %>%
  select(add1_new:wa_legdist, SCCDST,
         geometry) %>%
  rename(scc_dist = SCCDST)


#### LOAD TO SQL ####
### Convert back to a data frame
add_geocoded <- st_set_geometry(add_geocoded_sf, NULL)

### Load
# Need to specify schema
tbl <- DBI::Id(schema = "dbo", table = "ref_address_geocoded")
# May need to delete table first if data structure and columns have changed
dbRemoveTable(db.claims51, name = tbl)
dbWriteTable(db.claims51, name = tbl, 
             value = as.data.frame(add_geocoded), overwrite = T)


