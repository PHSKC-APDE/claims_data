#### CODE TO GEOCODE ADDRESSES (MEDICAID AND HOUSING)
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05
#
# A full refresh will completely recreate the stage.address_geocode 
#   based on the existing shape files.
# Generally, the user will want to use the existing ref.address_geocode table.


stage_address_geocode_f <- function(full_refresh = F) {
  #### SET UP PATHS ####
  # Make options in function eventually
  geocode_path <- "//dchs-shares01/DCHSDATA/DCHSPHClaimsData/Geocoding"
  s_shapes <- "//phshare01/epe_share/WORK/REQUESTS/Maps/Shapefiles/"
  g_shapes <- "//gisdw/kclib/Plibrary2/"
  
  #### PULL IN CLEANED ADDRESSES AND EXISTING GEOCODES ####
  address_clean <- dbGetQuery(db_claims,
                              "SELECT DISTINCT geo_add1_clean, geo_city_clean, 
                            geo_state_clean, geo_zip_clean
                            FROM ref.address_clean")
  
  if (full_refresh == F) {
    address_geocode <- dbGetQuery(db_claims,
                                  "SELECT DISTINCT geo_add1_clean, geo_city_clean, 
                                  geo_state_clean, geo_zip_clean,
                                  1 as 'geocoded'
                                  FROM ref.address_geocode")
  } else {
    #### BRING IN PREVIOUSLY GEOCODED DATA (ESRI) ####
    ### 2018-06-20
    # Note that the 2018-06-20 geocdes were on the original/raw addresses, not cleaned.
    # Renaming to clean for the purposes of appending data. Not a big deal as cleaned
    # addresses that aren't matched will be geocoded anyway.
    geocode_2018_06_20 <- read_sf(file.path(geocode_path, 
                                            "Distinct_addresses_geocoded_2018-06-20.shp"))
    geocode_2018_06_20 <- geocode_2018_06_20 %>%
      rename(geo_add1_clean = add1,
             geo_city_clean = city,
             geo_state_clean = state,
             geo_zip_clean = zip_1) %>%
      mutate(geo_x = st_coordinates(geocode_2018_06_20)[,1],
             geo_y = st_coordinates(geocode_2018_06_20)[,2]) %>%
      # Remove oddly coded coordinates that crash R
      mutate_at(vars(geo_x, geo_y), funs(ifelse(. < -100000000, 0, .))) %>%
      select(geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
             Loc_name, Status, geo_x, geo_y, Addr_type, Match_addr) %>%
      st_drop_geometry()
    
    # Recreate coordinates
    geocode_2018_06_20 <- st_as_sf(geocode_2018_06_20, coords = c("geo_x", "geo_y"), 
                                   crs = 3857, remove = F)
    
    # Convert to WSG84 geographic coordinate system to obtain lat/lon
    geocode_2018_06_20 <- st_transform(geocode_2018_06_20, 4326)
    
    geocode_2018_06_20 <- geocode_2018_06_20 %>%
      mutate(geo_lon = st_coordinates(geocode_2018_06_20)[,1],
             geo_lat = st_coordinates(geocode_2018_06_20)[,2])

    
    ### 2019-04-30
    geocode_2019_04_30 <- read_sf(file.path(geocode_path, 
                                            "Distinct_addresses_geocoded_2019-04-30.shp"))
    
    # Convert to WSG84 projected coordinate system to obtain x/y
    geocode_2019_04_30 <- st_transform(geocode_2019_04_30, 3857)
    
    geocode_2019_04_30 <- geocode_2019_04_30 %>%
      rename(geo_add1_clean = geo_add1_c,
             geo_city_clean = geo_city_c,
             geo_state_clean = geo_state_,
             geo_zip_clean = geo_zip_cl) %>%
      mutate(geo_x = st_coordinates(geocode_2019_04_30)[,1],
             geo_y = st_coordinates(geocode_2019_04_30)[,2]) %>%
      mutate_at(vars(geo_x, geo_y), funs(ifelse(is.na(.), 0, .))) %>%
      select(geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
             Loc_name, Status, geo_x, geo_y, Addr_type, Match_addr) %>%
      st_drop_geometry()
    
    # Recreate coordinates
    geocode_2019_04_30 <- st_as_sf(geocode_2019_04_30, coords = c("geo_x", "geo_y"), 
                                   crs = 3857, remove = F)
    
    # Convert to WSG84 geographic coordinate system to obtain lat/lon
    geocode_2019_04_30 <- st_transform(geocode_2019_04_30, 4326)
    geocode_2019_04_30 <- geocode_2019_04_30 %>%
      mutate(geo_lon = st_coordinates(geocode_2019_04_30)[,1],
             geo_lat = st_coordinates(geocode_2019_04_30)[,2])
    
    
    ### Combine data
    address_geocode_esri <- rbind(geocode_2018_06_20, geocode_2019_04_30)
    
    ### Set to dataframe and remove any duplicates
    # The CRS conversions above lead to slightly different geo_y/geo_lon values
    #    for the same address. Take the first one.
    address_geocode_esri <- address_geocode_esri %>% st_drop_geometry() %>%
      mutate(geo_zip_clean = as.character(geo_zip_clean)) %>%
      group_by(geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean) %>%
      slice(1) %>% ungroup() %>%
      distinct()
    
    ### Find addresses that need additional geocoding
    address_geocode_unmatch <- address_geocode_esri %>%
      filter(Status == "U" | Loc_name == "zip_5_digit_gc" | is.na(Loc_name)) %>%
      mutate(address_concat = paste(geo_add1_clean, geo_city_clean, geo_state_clean,
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
    for (i in seq(startindex, nrow(address_geocode_unmatch))) {
      print(paste("Working on index", i, "of", nrow(address_geocode_unmatch)))
      # query the geocoder - this will pause here if we are over the limit.
      result <- geocode_here_f(address = address_geocode_unmatch$address_concat[i])
      result$index <- i
      result$input_add <- address_geocode_unmatch$address_concat[i]
      result$geo_check_here <- 1
      # append the answer to the results file
      adds_here <- rbind(adds_here, result)
    }
    
    # Look at match results
    adds_here %>% group_by(address_type) %>% summarise(count = n())
    
    # Combine HERE results back to unmatched data
    address_geocode_here <- left_join(address_geocode_unmatch, adds_here,
                                      by = c("address_concat" = "input_add")) %>%
      select(geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean,
             lat, lon, formatted_address, address_type, geo_check_here) %>%
      mutate_at(vars(lat, lon), funs(replace_na(., 0)))
   
    # Convert to WSG84 projected coordinate system to obtain x/y
    address_geocode_here <- st_as_sf(address_geocode_here, coords = c("lon", "lat"), 
                                   crs = 4326, remove = F)
    address_geocode_here <- st_transform(address_geocode_here, 3857) %>%
      mutate(geo_x = st_coordinates(address_geocode_here)[,1],
             geo_y = st_coordinates(address_geocode_here)[,2]) %>%
      st_drop_geometry() %>%
      rename(geo_lat = lat, geo_lon = lon) %>%
      distinct()
    
    
    ### Combine back to initial data
    address_geocode <- left_join(address_geocode_esri, address_geocode_here,
                                 by = c("geo_add1_clean", "geo_city_clean",
                                        "geo_state_clean", "geo_zip_clean"))
    
    # Collapse to useful columns and select matching from each source as appropriate
    address_geocode <- address_geocode %>%
      # Add metadata indicating where the geocode comes from and if ZIP centroid
      mutate(
        formatted_address = as.character(formatted_address),
        address_type = as.character(address_type),
        geo_check_esri = 1,
        geo_check_here = ifelse(is.na(geo_check_here), 0, geo_check_here),
        geo_geocode_source = ifelse(!is.na(geo_lat.y) & (address_type == "houseNumber" | is.na(Loc_name)),
                                "here", "esri"),
        geo_zip_centroid = ifelse((geo_geocode_source == "esri" & Loc_name == "zip_5_digit_gc") |
                                (geo_geocode_source == "here" & address_type %in% c("postalCode", "district")),
                              1, 0),
        geo_street_centroid = ifelse(geo_geocode_source == "here" & address_type == "street", 1, 0),
        # Move address and coordindate data into a single field
        geo_add_geocoded = ifelse(geo_geocode_source == "esri", 
                              toupper(Match_addr), 
                              toupper(formatted_address)),
        geo_zip_geocoded = ifelse(geo_geocode_source == "esri",
                              str_sub(Match_addr,
                                      str_locate(Match_addr, "[:digit:]{5}$")[,1],
                                      str_locate(Match_addr, "[:digit:]{5}$")[,2]), 
                              str_sub(formatted_address,
                                      str_locate(formatted_address, " [:digit:]{5},")[,1],
                                      str_locate(formatted_address, " [:digit:]{5},")[,2]-1)),
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
    
    
    ### Remove previoous files to free up space
    rm(geocode_2018_06_20, geocode_2019_04_30)
    rm(address_geocode_esri, address_geocode_here, address_geocode_unmatch)
    rm(adds_here)
    
  }

  
  #### JOIN TO SPATIAL FILES OF INTEREST ####
  ### Set up as spatial file
  address_geocode <- address_geocode %>% filter(!is.na(geo_lon))
  address_geocode <- st_as_sf(address_geocode, coords = c("geo_lon", "geo_lat"), 
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
  address_geocode_joined <- st_join(address_geocode, block) %>%
    select(geo_add1_clean:geo_y, STATEFP10, COUNTYFP10, TRACTCE10, BLOCKCE10, GEOID10, 
           geometry) %>%
    rename(geo_statefp10 = STATEFP10, geo_countyfp10 = COUNTYFP10, 
           geo_tractce10 = TRACTCE10, geo_blockce10 = BLOCKCE10, geo_block_geoid10 = GEOID10)
  # PUMAs
  address_geocode_joined <- st_join(address_geocode_joined, puma) %>%
    select(geo_add1_clean:geo_block_geoid10, PUMACE10, GEOID10, NAMELSAD10,
           geometry) %>%
    rename(geo_pumace10 = PUMACE10, geo_puma_geoid10 = GEOID10, geo_puma_name = NAMELSAD10)
  # ZCTA
  address_geocode_joined <- st_join(address_geocode_joined, zcta) %>%
    select(geo_add1_clean:geo_puma_name, ZCTA5CE10, GEOID10,
           geometry) %>%
    rename(geo_zcta5ce10 = ZCTA5CE10, geo_zcta_geoid10 = GEOID10)
  # HRA
  address_geocode_joined <- st_join(address_geocode_joined, hra) %>%
    select(geo_add1_clean:geo_zcta_geoid10, VID, HRA2010v2_,
           geometry) %>%
    rename(geo_hra_id = VID, geo_hra = HRA2010v2_)
  # Region
  address_geocode_joined <- st_join(address_geocode_joined, region) %>%
    select(geo_add1_clean:geo_hra, RgnVID, Rgn2012,
           geometry) %>%
    rename(geo_region_id = RgnVID, geo_region = Rgn2012)
  # School districts
  address_geocode_joined <- st_join(address_geocode_joined, school) %>%
    select(geo_add1_clean:geo_region, GEOID10, NAME10,
           geometry) %>%
    rename(geo_school_geoid10 = GEOID10, geo_school = NAME10)
  # King County Council districts
  address_geocode_joined <- st_join(address_geocode_joined, kcc_dist) %>%
    select(geo_add1_clean:geo_school, kccdst, geometry) %>%
    rename(geo_kcc_dist = kccdst)
  # WA legislative districts
  address_geocode_joined <- st_join(address_geocode_joined, wa_dist) %>%
    select(geo_add1_clean:geo_kcc_dist, LEGDST,
           geometry) %>%
    rename(geo_wa_legdist = LEGDST)
  # Seattle City County districts
  address_geocode_joined <- st_join(address_geocode_joined, scc_dist) %>%
    select(geo_add1_clean:geo_wa_legdist, SCCDST,
           geometry) %>%
    rename(geo_scc_dist = SCCDST)
  
  
  ### Convert factors to character
  address_geocode_load <- address_geocode_joined %>%
    mutate_at(vars(geo_statefp10, geo_countyfp10, geo_tractce10, geo_blockce10,
                   geo_block_geoid10, geo_pumace10, geo_puma_geoid10, geo_puma_name,
                   geo_zcta5ce10, geo_zcta_geoid10, geo_hra, geo_region, 
                   geo_school_geoid10, geo_school),
              funs(as.character(.))) %>%
    mutate_at(vars(geo_kcc_dist, geo_wa_legdist, geo_scc_dist),
              funs(as.integer(.))) %>%
    st_drop_geometry() %>%
    mutate(last_run = Sys.time())
  
  
  #### LOAD TO SQL ####
  # Set up table name
  tbl_id <- DBI::Id(schema = "stage", table = "address_geocode")
  
  # Write data
  dbWriteTable(db_claims, tbl_id, 
               value = as.data.frame(address_geocode_load), overwrite = T)
  
  
  #### RETURN ROW COUNT FOR QA ####
  row_load_ref_geo <- nrow(address_geocode_load)
  
  return(row_load_ref_geo)
  
}

