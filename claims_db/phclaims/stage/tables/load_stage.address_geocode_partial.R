#### CODE TO GEOCODE ADDRESSES (MEDICAID AND HOUSING)
# Alastair Matheson, PHSKC (APDE)
#
# 2019-09, updated 2020-03
#
# A full refresh will completely recreate the stage.address_geocode 
#   based on the existing shape files.
# Generally, the user will want to use the existing ref.address_geocode table.

### Run from master_mcaid_partial script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/mcaid/master_mcaid_partial.R


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

stage_address_geocode_f <- function(conn = NULL,
                                    server = c("hhsaw", "phclaims"),
                                    config = NULL,
                                    get_config = F,
                                    full_refresh = F) {

  # Set up variables specific to the server
  server <- match.arg(server)
  
  if (get_config == T){
    if (stringr::str_detect(config, "^http")) {
      config <- yaml::yaml.load(getURL(config))
    } else{
      stop("A URL must be specified in config if using get_config = T")
    }
  }
  
  from_schema <- config[[server]][["from_schema"]]
  from_table <- config[[server]][["from_table"]]
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  stage_schema <- config[[server]][["stage_schema"]]
  stage_table <- ifelse(is.null(config[[server]][["stage_table"]]), '',
                      config[[server]][["stage_table"]])
  ref_schema <- config[[server]][["ref_schema"]]
  ref_table <- config[[server]][["ref_table"]]
  
  
  
  #### PULL IN DATA ####
  if (full_refresh == F) {
    # Join ref.address_clean to ref.address_geocode to find addresses not geocoded
    adds_to_code <- dbGetQuery(
      conn,
      glue::glue_sql("SELECT DISTINCT a.*, b.geocoded
                     FROM
                     (SELECT geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean, geo_hash_geocode
                       FROM {`ref_schema`}.address_clean WHERE geo_geocode_skip = 0) a
                     LEFT JOIN
                     (SELECT geo_hash_geocode, 1 as geocoded
                       FROM {`ref_schema`}.address_geocode) b
                     ON 
                     a.geo_hash_geocode = b.geo_hash_geocode
                     WHERE b.geocoded IS NULL",
                     .con = conn))
  } else {
    adds_to_code <- dbGetQuery(conn,
                               glue::glue_sql("SELECT DISTINCT geo_add1_clean, geo_city_clean, 
                                              geo_state_clean, geo_zip_clean, geo_hash_geocode
                                              FROM {`ref_schema`}.address_clean",
                                              .con = conn))
  }
  
  
  if (nrow(adds_to_code) > 0) {
    # Combine addresses into single field to reduce erroneous matches
    adds_to_code <- adds_to_code %>%
      mutate(geo_add_single = paste(geo_add1_clean, geo_city_clean, geo_zip_clean, sep = ", "))
    
    
    #### RUN THROUGH ESRI GEOCODER ####
    ### Source geocoding function
    # Temporrily the repo is private so need auth
    eval(parse(text = httr::content(httr::GET(
      url = "https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/kc_geocode.R",
      httr::authenticate(Sys.getenv("GITHUB_TOKEN"), "")), "text")))
    # devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/kc_geocode.R")
    
    
    ### Run the addresses through the geocoder, taking the best result only
    adds_coded_esri <- bind_rows(lapply(adds_to_code$geo_add_single, kc_geocode, 
                                        street = NULL, city = NULL, zip = NULL, max_return = 10,
                                        best_result = T))
    
    
    
    ### Convert CRS and set up fields of interest
    adds_coded <- left_join(adds_to_code, adds_coded_esri, by = c("geo_add_single" = "input_addr")) %>%
      # Keep track of rows we want to drop IF the HERE geocoder also fails
      # These are recoded to be 0 so the conversion to SF works, but we don't
      # want to actually keep those coordinates
      mutate(drop = ifelse(is.na(lat), 1L, 0L)) %>%
      mutate_at(vars(lat, lon), list( ~ replace_na(., 0))) %>%
      st_as_sf(coords = c("lon", "lat"),
               crs = "+proj=lcc +lat_1=47.5 +lat_2=48.73333333333333 +lat_0=47 +lon_0=-120.8333333333333
           +x_0=500000.0000000001 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=us-ft +no_defs",
               remove = T)
    
    adds_coded <- st_transform(adds_coded, 3857)
    
    adds_coded <- adds_coded %>%
      mutate(geo_x = st_coordinates(adds_coded)[,1],
             geo_y = st_coordinates(adds_coded)[,2]) %>%
      mutate_at(vars(geo_x, geo_y), list( ~ ifelse(is.na(.), 0, .))) %>%
      select(geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean, geo_hash_geocode,
             locName, score, geo_x, geo_y, matchAddr, addressType, drop)
    
    # Convert to WSG84 geographic coordinate system to obtain lat/lon
    adds_coded <- st_transform(adds_coded, 4326)
    
    adds_coded <- adds_coded %>%
      mutate(geo_lon = st_coordinates(adds_coded)[,1],
             geo_lat = st_coordinates(adds_coded)[,2]) %>%
      st_drop_geometry()
    
    
    
    #### RUN THROUGH HERE GEOCODER ####
    ### Find addresses that need additional geocoding
    adds_coded_unmatch <- adds_coded %>%
      filter(locName == "zip_5_digit_gc" | is.na(locName)) %>%
      mutate(geo_add_single = paste(geo_add1_clean, geo_city_clean, geo_state_clean,
                                    geo_zip_clean, "USA", sep = ", ")) %>%
      select(geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean, geo_hash_geocode, geo_add_single)
    
    
    if (nrow(adds_coded_unmatch) > 0) {
      ### Run through the HERE geocoder
      # Get an API here: https://developer.here.com
      # Store using keyring
      if (is.na(keyring::key_list("here")[1, 2]) |
          is.na(keyring::key_get("here", keyring::key_list("here")[1, 2]))) {
        app_id <- readline(prompt = "Please enter HERE app id: ")
        app_code <- readline(prompt = "Please enter HERE app code: ")
      } else {
        app_id <- keyring::key_list("here")[1, 2]
        app_code <- keyring::key_get("here", keyring::key_list("here")[1, 2])
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
        select(geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean, geo_hash_geocode,
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
      
    }
    
    
    #### BRING ESRI AND HERE DATA TOGETHER ####
    if (nrow(adds_coded_unmatch) > 0) {
      # Collapse to useful columns and select matching from each source as appropriate
      adds_coded <- left_join(adds_coded, adds_coded_here, 
                              by = c("geo_add1_clean", "geo_city_clean", "geo_state_clean",
                                     "geo_zip_clean", "geo_hash_geocode")) 
      
      # Look at how the HERE geocodes improved things
      print(adds_coded %>% group_by(locName, address_type) %>% summarise(count = n()))
      
      
      # Add metadata indicating where the geocode comes from and if ZIP centroid
      adds_coded <- adds_coded %>%
        mutate(
          formatted_address = as.character(formatted_address),
          address_type = as.character(address_type),
          geo_check_esri = 1,
          geo_check_here = ifelse(is.na(geo_check_here), 0, geo_check_here),
          geo_geocode_source = case_when(
            !is.na(geo_lat.x) & 
              locName %in% c("address_point_", "pin_address_on", "st_address_us", 
                             "trans_network_", 
                             "king_address_point",
                             "Kitsap_gcs", "ktsp_roadcl", "ktsp_siteaddr_pin",
                             "Pierce_gcs", "pir_address_point", "pir_roads",
                             "Snohomish_gcs", "sno_site_address", 
                             "sno_streets_centerline") ~ "esri",
            !is.na(geo_lat.y) & address_type %in% c("houseNumber", "street") ~ "here",
            !is.na(geo_lat.x) & locName == "zip_5_digit_gc" ~ "esri",
            !is.na(geo_lat.y) & address_type %in% c("postalCode") ~ "here",
            TRUE ~ NA_character_))
      
      # Print out any loc_names not accounted for
      adds_coded %>% filter(is.na(geo_geocode_source)) %>% distinct(locName, address_type)
      
      
      adds_coded <- adds_coded %>%
        mutate(geo_zip_centroid = ifelse((geo_geocode_source == "esri" & locName == "zip_5_digit_gc") |
                                           (geo_geocode_source == "here" & address_type %in% c("postalCode")),
                                         1, 0),
               geo_street_centroid = ifelse(geo_geocode_source == "here" & address_type == "street", 1, 0),
               # Move address and coordindate data into a single field
               geo_add_geocoded = ifelse(geo_geocode_source == "esri", 
                                         toupper(matchAddr), 
                                         toupper(formatted_address)),
               geo_zip_geocoded = case_when(
                 geo_geocode_source == "esri" ~ str_sub(matchAddr,
                                                        str_locate(matchAddr, "[:digit:]{5}$")[,1],
                                                        str_locate(matchAddr, "[:digit:]{5}$")[,2]),
                 geo_geocode_source == "here" & str_detect(formatted_address, "^[:digit:]{5},") ~ 
                   str_sub(formatted_address, 
                           str_locate(formatted_address, "^[:digit:]{5},")[,1],
                           str_locate(formatted_address, "^[:digit:]{5},")[,2] - 1),
                 geo_geocode_source == "here" & str_detect(formatted_address, " [:digit:]{5},") ~ 
                   str_sub(formatted_address, 
                           str_locate(formatted_address, " [:digit:]{5},")[,1] + 1,
                           str_locate(formatted_address, " [:digit:]{5},")[,2] - 1)),
               geo_add_type = case_when(
                 geo_geocode_source == "esri" ~ locName,
                 geo_geocode_source == "here" ~ address_type
               ),
               geo_lon = ifelse(geo_geocode_source == "esri", geo_lon.x, geo_lon.y),
               geo_lat = ifelse(geo_geocode_source == "esri", geo_lat.x, geo_lat.y),
               geo_x = ifelse(geo_geocode_source == "esri", geo_x.x, geo_x.y),
               geo_y = ifelse(geo_geocode_source == "esri", geo_y.x, geo_y.y)
        ) %>%
        select(geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean, 
               geo_hash_geocode, geo_add_geocoded, geo_zip_geocoded, geo_add_type,
               geo_check_esri, geo_check_here, geo_geocode_source, 
               geo_zip_centroid, geo_street_centroid,
               geo_lon, geo_lat, geo_x, geo_y, drop) %>%
        mutate(geo_zip_clean = as.character(geo_zip_clean))
    } else {
      # If the HERE geocoder was not needed, run this
      adds_coded <- adds_coded_esri %>% 
        mutate(geo_add_geocoded = toupper(Match_addr),
               geo_zip_geocoded = str_sub(Match_addr, 
                                          str_locate(Match_addr, "[:digit:]{5}$")[,1],
                                          str_locate(Match_addr, "[:digit:]{5}$")[,2]),
               geo_add_type = Loc_name,
               geo_check_esri = 1L,
               geo_check_here = 0L,
               geo_geocode_source = "esri",
               geo_zip_centroid = 0L,
               geo_street_centroid = 0L) %>%
        select(geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean, 
               geo_hash_geocode, geo_add_geocoded, geo_zip_geocoded, geo_add_type,
               geo_check_esri, geo_check_here, geo_geocode_source, 
               geo_zip_centroid, geo_street_centroid,
               geo_lon, geo_lat, geo_x, geo_y, drop)
    }
    
    
    
    ### Identify addresses that could not be geocoded to an acceptable level
    # Will flag them in ref.address_clean as addresses to skip future geocoding attempts
    adds_geocode_skip <- adds_coded %>% 
      filter(is.na(geo_geocode_source) | (drop == 1 & geo_geocode_source == "esri")) %>% 
      select(geo_hash_geocode)
    
    if (nrow(adds_geocode_skip) > 0) {
      # Set up SQL to update values in stage table
      update_sql <- glue::glue_data_sql(adds_geocode_skip, 
                                        "UPDATE {`stage_schema`}.{DBI::SQL(stage_table)}address_clean 
                                    SET geo_geocode_skip = 1 
                                    WHERE (geo_add1_clean = {geo_add1_clean} AND geo_city_clean = {geo_city_clean} AND
                                    geo_state_clean = {geo_state_clean} AND geo_zip_clean = {geo_zip_clean})",
                                        .con = conn)
      # Need to account for NULL values properly
      update_sql <- str_replace_all(update_sql, "= NULL", "Is NULL")
      # Run code
      DBI::dbExecute(conn, glue::glue_collapse(update_sql, sep = "; "))
      
      # Check that more addresses were flagged for skipping
      stage_geocode_skip <- as.integer(DBI::dbGetQuery(
        conn, 
        glue::glue_sql("SELECT SUM(geo_geocode_skip) AS skip_cnt FROM {`stage_schema`}.{DBI::SQL(stage_table)}address_clean",
                       .con = conn)))
      ref_geocode_skip <- as.integer(DBI::dbGetQuery(
        conn, glue::glue_sql("SELECT SUM(geo_geocode_skip) AS skip_cnt FROM {`ref_schema`}.address_clean",
                             .con = conn)))
      
      if (stage_geocode_skip >= ref_geocode_skip) {
        # Update in ref table
        update_sql <- glue::glue_data_sql(adds_geocode_skip, 
                                          "UPDATE {`ref_schema`}.address_clean 
                                    SET geo_geocode_skip = 1 
                                    WHERE (geo_add1_clean = {geo_add1_clean} AND geo_city_clean = {geo_city_clean} AND
                                    geo_state_clean = {geo_state_clean} AND geo_zip_clean = {geo_zip_clean})",
                                          .con = conn)
        DBI::dbExecute(conn, glue::glue_collapse(update_sql, sep = "; "))
        
        # Check counts
        ref_geocode_skip_new <- as.integer(DBI::dbGetQuery(
          conn, glue::glue_sql("SELECT SUM(geo_geocode_skip) AS skip_cnt FROM {`ref_schema`}.address_clean",
                               .con = conn)))
        
        if (stage_geocode_skip == ref_geocode_skip_new) {
          message("Succesfully updated ref.address_clean with geocode_skip flags")
        }
      } else {
        message("Number of rows in set to skip geocode in stage (", stage_geocode_skip,
                ") is less than the current ref table (", ref_geocode_skip, ")")
      }
    }
    
    
    ### Remove any addresses that could not be geocoded to an acceptable level
    # (mostly HERE geocodes at the city level)
    adds_coded <- adds_coded %>% 
      filter(!(is.na(geo_geocode_source) | (drop == 1 & geo_geocode_source == "esri"))) %>%
      select(-drop)
    
    
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
    
    ### Convert factors to character etc.
    adds_coded_load <- adds_coded_joined %>%
      mutate_at(vars(geo_zip_clean, 
                     geo_statefp10, geo_countyfp10, geo_tractce10, geo_blockce10,
                     geo_block_geoid10, geo_pumace10, geo_puma_geoid10, geo_puma_name,
                     geo_zcta5ce10, geo_zcta_geoid10, geo_hra, geo_region, 
                     geo_school_geoid10, geo_school),
                list( ~ as.character(.))) %>%
      mutate(geo_scc_dist = str_replace(geo_scc_dist, "SCC", "")) %>%
      mutate_at(vars(geo_kcc_dist, geo_wa_legdist, geo_scc_dist),
                list( ~ as.integer(.))) %>%
      st_drop_geometry() %>%
      mutate(last_run = Sys.time())
    
    #### RENAME FIELDS ####
    # Not currently doing this. Think through more.
    # If doing, need to finish renames here and change YAML file
    # adds_coded_load <- adds_coded_joined %>%
    #   rename(geo_statefp10 = geo_state_code, geo_countyfp10 = geo_county_code,
    #          geo_tractce10 = geo_tract_code, geo_blockce10 = geo_block_code,
    #          geo_block_geoid10 = geo_block_fullcode)
    
    
    #### LOAD TO SQL ####
    if (full_refresh == F) {
      # Check how many rows are already in the stage table
      stage_rows_before <- as.numeric(dbGetQuery(
        conn, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}",
                             .con = conn)))
      stage_rows_before_distinct <- as.numeric(dbGetQuery(
        conn, 
        glue::glue_sql("SELECT COUNT (*) 
                     FROM
                     (SELECT DISTINCT geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean, 
                       geo_hash_geocode, geo_add_geocoded, geo_zip_geocoded, geo_add_type, geo_check_esri, 
                       geo_check_here, geo_geocode_source, geo_zip_centroid, geo_street_centroid, 
                       geo_lon, geo_lat, geo_x, geo_y, geo_statefp10, geo_countyfp10, 
                       geo_tractce10, geo_blockce10, geo_block_geoid10, geo_pumace10, 
                       geo_puma_geoid10, geo_puma_name, geo_zcta5ce10, geo_zcta_geoid10, 
                       geo_hra_id, geo_hra, geo_region_id, geo_region, geo_school_geoid10, 
                       geo_school, geo_kcc_dist, geo_wa_legdist, geo_scc_dist
                       FROM {`to_schema`}.{`to_table`}) a",
                       .con = conn)))
    } else if (full_refresh == T) {
      # Create new table if it doesn't exist
      try(create_table_f(conn = conn, config_url = "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/create_stage.address_geocode.yaml"))
    }
    
    
    
    # Write data
    dbWriteTable(conn,
                 name = DBI::Id(schema = to_schema, table = to_table), 
                 value = as.data.frame(adds_coded_load), 
                 append = T, overwrite = F)
    
    
    #### BASIC QA ####
    if (full_refresh == F) {
      ### Compare row counts now
      row_load_ref_geo <- nrow(adds_coded_load)
      stage_rows_after <- as.numeric(dbGetQuery(
        conn, glue::glue_sql("SELECT COUNT (*) FROM {`to_schema`}.{`to_table`}", .con = conn)))
      stage_rows_after_distinct <- as.numeric(dbGetQuery(
        conn, 
        glue::glue_sql("SELECT COUNT (*) 
                     FROM
                     (SELECT DISTINCT geo_add1_clean, geo_city_clean, geo_state_clean, geo_zip_clean, 
                       geo_hash_geocode, geo_add_geocoded, geo_zip_geocoded, geo_add_type, geo_check_esri, 
                       geo_check_here, geo_geocode_source, geo_zip_centroid, geo_street_centroid, 
                       geo_lon, geo_lat, geo_x, geo_y, geo_statefp10, geo_countyfp10, 
                       geo_tractce10, geo_blockce10, geo_block_geoid10, geo_pumace10, 
                       geo_puma_geoid10, geo_puma_name, geo_zcta5ce10, geo_zcta_geoid10, 
                       geo_hra_id, geo_hra, geo_region_id, geo_region, geo_school_geoid10,   
                       geo_school, geo_kcc_dist, geo_wa_legdist, geo_scc_dist
                       FROM {`to_schema`}.{`to_table`}) a",
                       .con = conn)))
      
      if ((stage_rows_before + row_load_ref_geo == stage_rows_after) == F) {
        warning("Number of rows added to ref.stage_address_geocode not expected value")
        stage_geocode_qa_fail <- 1
      } else {
        stage_geocode_qa_fail <- 0
      }
      
      return(stage_geocode_qa_fail)
    }
    
  } else {
    message("No new addresses were found to geocode")
    return(1L)
  }
  
  
  
   
}