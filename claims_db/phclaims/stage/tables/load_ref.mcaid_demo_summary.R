# Load packages
pacman::p_load(apde.etl, lubridate,  rads)


#' @title load_ref.mcaid_demo_summary
#' 
#' @description 
#' Table includes annual counts of Medicaid members by demographics
#' King County residents with full benefits are included
#' Measures include: age group, gender, race, ZIP code, KC Council District
#' Suppression is applied to small counts <= 10

#' 
#' @details TBD
#' 
#' Note: # Carolina Johnson
#' 1/22/2026
#' Purpose: Code for Medicaid demographic roll up table for DCHS (via ref schema)
#' Nithia C.- QA
#' 

## Set up constants and dbs ----

load_ref_mcaid_demo_summary <- function(conn = NULL,
                                            server = c("hhsaw", "phclaims"),
                                            config = NULL,
                                            get_config = F) {
  
  # Set up variables specific to the server
  server <- match.arg(server)
  
  if (get_config == T){
    if (stringr::str_detect(config, "^http")) {
      config <- yaml::yaml.load(getURL(config))
    } else{
      stop("A URL must be specified in config if using get_config = T")
    }
  }
  ref_schema <- config[[server]][["ref_schema"]]
  elig_schema <- config[[server]][["elig_schema"]]
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  demo_table <- config[[server]][["demo_table"]]
  month_table <- config[[server]][["month_table"]]
  geocode_table <- config[[server]][["geocode_table"]]
  
  qa_schema <- config[[server]][["qa_schema"]]
  qa_table <- ifelse(is.null(config[[server]][["qa_table"]]), '',
                     config[[server]][["qa_table"]])
  

  message("Running queries to build ", to_schema, ".", to_table)
  
  last_run <- DBI::dbGetQuery(conn, "SELECT GETDATE()")[1,1]
  
  message("Step 1: Create table with most common geocode per person per year")
#### Step 1: Create table with most common geocode per person per year ####
# Prep query
sql_query_1 <- glue::glue_sql(
  "set nocount on; --necessary to allow multi-stage commands to be sent to SQL Server from R
  DROP TABLE IF EXISTS ##final_yearly_geocode;
  
  select distinct id_mcaid, year, geo_hash_geocode
  into ##final_yearly_geocode
  FROM
  (
  select id_mcaid, year, geo_hash_geocode, ROW_NUMBER() OVER(PARTITION BY id_mcaid, year ORDER BY geo_freq DESC) AS RowNum
  from 
  (select id_mcaid, year, geo_hash_geocode, count(geo_hash_geocode) as geo_freq
  from {`elig_schema`}.{`month_table`} m 
  group by id_mcaid, year, geo_hash_geocode	
  ) x ) y
  where RowNum = 1;",
  .con = conn)

# Send query
# Run time: < 1 min
system.time(DBI::dbSendQuery(conn = conn, statement = sql_query_1))

# QA check 1: Confirm one geo_hash_geocode per person per year
qa1_check <- DBI::dbGetQuery(conn = conn, statement = "select id_mcaid, year
                        from (
                        select id_mcaid, year, count(distinct geo_hash_geocode) as geo_count
                        from ##final_yearly_geocode
                        group by id_mcaid, year) as a
                        where geo_count > 1;")
qa1_result <- nrow(qa1_check) == 0

if (qa1_result) {
  DBI::dbExecute(conn = conn,
                 glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Confirmed one geo_hash_geocode per person per year', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There was only one geo_hash_geocode per person per year')",
                                .con = conn))
} else {
  DBI::dbExecute(conn = conn,
                 glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'There are {nrow(qa1_check)} duplicated geo_hash_geocodes', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There was more than one geo_hash_geocode per person per year')",
                                .con = conn))

  stop("FAIL: ", schema, ".", to_table, " failed to load. QA1 - Multiple geo_hash_geocodes per person per year.")
}


  
  #### Step 2: Create person-year table with all relevant measures
message("Step 2: Create person-year table with all relevant measures")
  # Prep SQL query
  sql_query_2 <- glue::glue_sql(
    "set nocount on; --necessary to allow multi-stage commands to be sent to SQL Server from R
    
    DROP TABLE IF EXISTS ##clients;
    
    select distinct m.id_mcaid, m.year, dob,
    case
    when gender_recent = 'Unknown' then gender_me else gender_recent 
    end as gender,
    case 
    when DATEDIFF(month, dob,  DATEFROMPARTS(m.year, 7, 1))/12  < 18 then '0-17' 
    when DATEDIFF(month, dob,  DATEFROMPARTS(m.year, 7, 1))/12 between 18 and 24 then '18-24' 
    when DATEDIFF(month, dob,  DATEFROMPARTS(m.year, 7, 1))/12 between 25 and 34 then '25-34'
    when DATEDIFF(month, dob,  DATEFROMPARTS(m.year, 7, 1))/12 between 35 and 44 then '35-44'
    when DATEDIFF(month, dob,  DATEFROMPARTS(m.year, 7, 1))/12 between 45 and 54 then '45-54'
    when DATEDIFF(month, dob,  DATEFROMPARTS(m.year, 7, 1))/12 between 55 and 64 then '55-64'
    when DATEDIFF(month, dob,  DATEFROMPARTS(m.year, 7, 1))/12 > 64 then '65+' 
    end as age_group,
    race_aian, race_asian, race_black, race_latino, race_nhpi, race_white, race_unk, geo_id20_kccdist as kccdist, geo_zip_clean as zip
    into ##clients
    from {`elig_schema`}.{`month_table`} m
    left join ##final_yearly_geocode g on g.id_mcaid = m.id_mcaid and g.year = m.[year]
    left join {`elig_schema`}.{`demo_table`} d on m.id_mcaid = d.id_mcaid
    left join {`ref_schema`}.{`geocode_table`} a on g.geo_hash_geocode = a.geo_hash_geocode
    where m.full_benefit = 1 and geo_kc = 1 and m.year < year(CURRENT_DATE)
    --adding the last condition to not generate records for people not born yet in a given year
    and dob <= datefromparts(m.year, 12, 31);",
    .con = conn)
  
  # Send SQL query
  # Run time: < 1 min
  system.time(DBI::dbSendQuery(conn = conn, statement = sql_query_2))


# QA check #2a: Confirm rows for years with complete data only are present
latest_mcaid_year <- DBI::dbGetQuery(conn = conn, statement =
                                       glue::glue_sql("select max(year) as latest_mcaid_year from {`elig_schema`}.{`month_table`} where RIGHT(CAST(year_month AS VARCHAR(6)), 2) = '12';", .con = conn))$latest_mcaid_year

latest_data_year <- DBI::dbGetQuery(conn = conn, statement = 
                                 "select max(year) as latest_data_year from ##clients;")$latest_data_year

qa2a_result <- latest_mcaid_year == latest_data_year

if (qa2a_result) {
  DBI::dbExecute(conn = conn,
                 glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Confirmed rows for years with complete data only are present', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There are only rows for years with complete data present')",
                                .con = conn))
} else {
  DBI::dbExecute(conn = conn,
                 glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Rows for years with incomplete data present', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'There are rows for years with incomplete data present')",
                                .con = conn))
  
  stop("FAIL: ", schema, ".", to_table, " failed to load. QA2A - There are rows for years with incomplete data present.")
}

  # QA check #2b: Confirm annual counts of Medicaid members in ##clients match direct query of claims.final_mcaid_elig_month
  qa2_t1 <- DBI::dbGetQuery(conn = conn, statement = "select year, count(distinct id_mcaid) as total_pop from ##clients group by year order by year;")
  
  # Prep for SQL syntax
  latest_mcaid_year <- DBI::SQL(latest_mcaid_year)
  qa2_t2_query <- glue::glue_sql("select year, count(distinct m.id_mcaid) as total_pop from {`elig_schema`}.{`month_table`} m
                       left join {`elig_schema`}.{`demo_table`} d
                       on m.id_mcaid = d.id_mcaid
                       where full_benefit = 1 and geo_kc = 1 and dob <= datefromparts(m.year, 12, 31) and m.year <= {`latest_mcaid_year`}
                       group by year order by year;", .con = conn)
  qa2_t2 <- DBI::dbGetQuery(conn = conn, statement = qa2_t2_query)
  
  qa2b_result <- identical(qa2_t1, qa2_t2)

  
if (qa2b_result) {
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Confirmed annual counts of Medicaid members in ##clients sub-table match direct query of {DBI::SQL(elig_schema)}.{DBI::SQL(month_table)}', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'Annual counts match')",
                                  .con = conn))
  } else {
    DBI::dbExecute(conn = conn,
                   glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Annual counts of Medicaid members in ##clients sub-table does NOT match direct query of {DBI::SQL(elig_schema)}.{DBI::SQL(month_table)}', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'Annual counts do NOT match')",
                                  .con = conn))
    
    stop("FAIL: ", schema, ".", to_table, " failed to load. QA2B - There are rows for years with incomplete data present.")
}
  
  
  
  #### Step 3: Create long table allowing person counts by independent category group
  message("Step 3: Create long table allowing person counts by independent category group")
  # Prep SQL query
  sql_query_3 <- glue::glue_sql(
    "set nocount on; --necessary to allow multi-stage commands to be sent to SQL Server from R
      
      DROP TABLE IF EXISTS ##client_long;
      
  select id_mcaid, year, 
  case when measure like 'race_%' then 'race_aic' else measure end as measure, 
  case when measure like 'race_%' then replace(measure, 'race_', '') else value end as value
  into ##client_long
  from (
  select id_mcaid, year, measure, value
  from (
  select id_mcaid, year, cast(gender as varchar) as gender, cast(age_group as varchar) as age_group, 
  cast(race_aian as varchar) as race_aian, cast(race_asian as varchar) as race_asian, cast(race_black as varchar) as race_black, cast(race_latino as varchar) as race_latino, 
  cast(race_nhpi as varchar) as race_nhpi, cast(race_white as varchar) as race_white , cast( race_unk as varchar) as race_unk, cast(kccdist as varchar) as kccdist, cast(zip as varchar) as zip
  from ##clients) cl 
  UNPIVOT (value for measure in (gender, age_group, race_aian, race_asian, race_black, race_latino, race_nhpi, race_white, race_unk, kccdist, zip)) as unpvt) as long
  where value <> '0';",
    .con = conn)
  
  # Send SQL query to server
  # Run time: < 1 min
  system.time(DBI::dbSendQuery(conn = conn, statement = sql_query_3))


# QA check 3: Confirm expansion occurred as expected - 1 row per person per measure where measure is not race
qa3_check_a <- DBI::dbGetQuery(conn = conn, statement = "select id_mcaid, year, measure
                        from (
                        select id_mcaid, year, measure, count(value) as freq
                        from ##client_long
                        where measure <> 'race_aic'
                        group by id_mcaid, year, measure) as a
                        where freq > 1;")

qa3_check_b <- DBI::dbGetQuery(conn = conn, statement = "select id_mcaid, year, measure
                        from (
                        select id_mcaid, year, measure, value, count(value) as freq
                        from ##client_long
                        where measure = 'race_aic'
                        group by id_mcaid, year, measure, value) as a
                        where freq > 1;")

qa3_result <- nrow(qa3_check_a) == 0 & nrow(qa3_check_b) == 0

if (qa3_result) {
  DBI::dbExecute(conn = conn,
                 glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Confirmed expansion occurred as expected - 1 row per person per measure where measure is not race', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'Expected expansion occurred')",
                                .con = conn))
} else {
  DBI::dbExecute(conn = conn,
                 glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Expansion did NOT occurr as expected - 1 row per person per measure where measure is not race', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'Expected expansion did NOT occurr')",
                                .con = conn))
  
  stop("FAIL: ", schema, ".", to_table, " failed to load. QA3 - Expected expansion did NOT occurr.")
}




  
  #### Step 4: Create final table ####
message("Step 4: Prepare final table")
  # Prep SQL query
  sql_query_4 <- glue::glue_sql(
    "set nocount on; --necessary to allow multi-stage commands to be sent to SQL Server from R
    
    select year, measure, lower(value) as value, 
    -- implementing small number suppression - nullifying all counts 1-10 inclusive
    case when pop between 1 and 10 then NULL else pop end as pop, total_pop, GETDATE() as last_run
    from (
    select cl.year, cl.measure, value, count(distinct id_mcaid)  as pop, total_pop
    from ##client_long cl
    left join 
    -- calculating total population per year and measure (total pop is constant except for kccdist, which has NULL values in the geocode)
    (select year, measure, count(distinct id_mcaid) as total_pop 
    from ##client_long
    group by year, measure) yt on cl.year = yt.year and cl.measure = yt.measure
    group by cl.year, cl.measure, value, total_pop) c 
    order by measure, value, year;",
    .con = conn)
  
  # Send SQL query to server
  # Run time: < 1 min
  counts <- DBI::dbGetQuery(conn = conn, statement = sql_query_4)


# QA check 4: Validate calculated annual population total_pop against claims.final_mcaid_elig_month (kccdist totals are different due to missingness in the Medicaid data)
qa4_t1 <- counts %>% filter(measure != "kccdist") %>% distinct(year, total_pop)
qa4_result <- identical(qa4_t1, qa2_t2)

if (qa4_result) {
  DBI::dbExecute(conn = conn,
                 glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Validated calculated annual population total_pop against {DBI::SQL(elig_schema)}.{DBI::SQL(month_table)} (kccdist totals are different due to missingness in the Medicaid data)', 
                   'PASS', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'Calculated annual population total_pop validated')",
                                .con = conn))
} else {
  DBI::dbExecute(conn = conn,
                 glue::glue_sql("INSERT INTO {`qa_schema`}.{DBI::SQL(qa_table)}qa_mcaid
                   (last_run, table_name, qa_item, qa_result, qa_date, note) 
                   VALUES ({format(last_run, usetz = FALSE)}, 
                   '{DBI::SQL(to_schema)}.{DBI::SQL(to_table)}',
                   'Could NOT validate calculated annual population total_pop against {DBI::SQL(elig_schema)}.{DBI::SQL(month_table)} (kccdist totals are different due to missingness in the Medicaid data)', 
                   'FAIL', 
                   {format(Sys.time(), usetz = FALSE)}, 
                   'Calculated annual population total_pop could NOT be validated')",
                                .con = conn))
  
  stop("FAIL: ", schema, ".", to_table, " failed to load. QA4 - Calculated annual population total_pop could NOT be validated.")
}

  
  #### Step 5: Write table ####
  message("Step 5: Write table")
  DBI::dbWriteTable(conn, name = DBI::Id(schema = "ref", table = "mcaid_demo_summary"), 
               value = as.data.frame(counts), 
               overwrite = T)
  
  DBI::dbExecute(conn = conn,
                 glue::glue_sql("UPDATE {`to_schema`}.{`to_table`} SET pop = 0 WHERE pop IS NULL;
                                DROP TABLE IF EXISTS ##final_yearly_geocode;
                                DROP TABLE IF EXISTS ##clients;
                                DROP TABLE IF EXISTS ##client_long;",
                                .con = conn))

  message("Completed")
}
