## Code to create stage.SOURCE_claim_ccw table
## Person-level CCW condition status by time period
## Eli Kern (PHSKC-APDE)
## 2019-5-8 
## Run time: XX min

## This is for Type 1 CCW definitions - that require 1 claim in lookback period to meet condition criteria

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 30, scipen = 999)
library(tidyverse) # Used to manipulate tidy data
library(lubridate) # Used to manipulate dates
library(odbc) # Used to connect to SQL server
origin <- "1970-01-01"
db.claims51 <- dbConnect(odbc(), "PHClaims51")
config_url <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/phclaims/stage/tables/load_stage.apcd_claim_ccw.yaml"
top_rows <- "top 50000" #Use this parameter for script testing - set to "top 5000" for example

# ### ### ### ### ### ### ###
#### Step 1: Load parameters from config file #### 
# ### ### ### ### ### ### ###

table_config <- yaml::yaml.load(RCurl::getURL(config_url))
conditions <- table_config[str_detect(names(table_config), "cond_")]
schema <- table_config[str_detect(names(table_config), "schema")][[1]]
from_table_claim_header <- table_config[str_detect(names(table_config), "from_table_claim_header")][[1]]
from_table_icdcm <- table_config[str_detect(names(table_config), "from_table_icdcm")][[1]]
to_table <- table_config[str_detect(names(table_config), "to_table")][[1]]
source_data <- table_config[str_detect(names(table_config), "source_data")][[1]]

## Temporary code: set parameters for testing
ccw_code <- table_config$cond_arthritis$ccw_code
ccw_desc <- table_config$cond_arthritis$ccw_desc
ccw_abbrev <- table_config$cond_arthritis$ccw_abbrev
lookback_months <- table_config$cond_arthritis$lookback_months
dx_fields <- table_config$cond_arthritis$dx_fields
dx_exclude1 <- table_config$cond_arthritis$dx_exclude1
dx_exclude2 <- table_config$cond_arthritis$dx_exclude2
dx_exclude1_fields <- table_config$cond_arthritis$dx_exclude1_fields
dx_exclude2_fields <- table_config$cond_arthritis$dx_exclude2_fields
claim_type1 <- paste(as.character(table_config$cond_arthritis$claim_type1), collapse=",")
claim_type2 <- paste(as.character(table_config$cond_arthritis$claim_type2), collapse=",")
condition_type <- table_config$cond_arthritis$condition_type

#For looping later on
lapply(conditions, function(x){
  ccw_code <- x$ccw_code
  ccw_desc <- x$ccw_desc
  ccw_abbrev <- x$ccw_abbrev
  lookback_months <- x$lookback_months
  dx_fields <- x$dx_fields
  dx_exclude1 <- x$dx_exclude1
  dx_exclude2 <- x$dx_exclude2
  dx_exclude1_fields <- x$dx_exclude1_fields
  dx_exclude2_fields <- x$dx_exclude2_fields
  claim_type1 <- paste(as.character(x$claim_type1), collapse=",")
  claim_type2 <- paste(as.character(x$claim_type2), collapse=",")
  condition_type <- x$condition_type
})


# ### ### ### ### ### ### ###
#### Step 2: Create branching code segments for type 1 versus type 2 conditions #### 
# ### ### ### ### ### ### ###

## Construct where statement for claim count requirements
if(condition_type == 1){
  claim_count_condition <- "where (b.condition_1_cnt >= 1)"
}
if(condition_type == 2){
  claim_count_condition <- 
    "where (b.condition_1_cnt >= 1) or (b.condition_2_cnt >=2 and abs(datediff(day, b.condition_2_min_date, b.condition_2_max_date)) >=1)"
}

## Construct where statement for diagnosis field numbers
if(dx_fields == "1-2"){
  dx_fields_condition <- " where icdcm_number in ('01','02')"
}
if(dx_fields == "1"){
  dx_fields_condition <- " where icdcm_number = '01'" 
}
if(dx_fields == "any"){
  dx_fields_condition <- ""
}

## Construct diagnosis field number code for exclusion code
if(!is.null(dx_exclude1_fields)){
  if(dx_exclude1_fields == "1-2"){
    dx_exclude1_fields_condition <- " and diag.icdcm_number in ('01','02')"
  }
  if(dx_exclude1_fields == "1"){
    dx_exclude1_fields_condition <- " and diag.icdcm_number = '01'" 
  }
  if(dx_exclude1_fields == "any"){
    dx_exclude1_fields_condition <- ""
  }
}
if(!is.null(dx_exclude2_fields)){
  if(dx_exclude2_fields == "1-2"){
    dx_exclude2_fields_condition <- " and diag.icdcm_number in ('01','02')"
  }
  if(dx_exclude2_fields == "1"){
    dx_exclude2_fields_condition <- " and diag.icdcm_number = '01'"
  }
  if(dx_exclude2_fields == "any"){
    dx_exclude2_fields_condition <- ""
  }
}

##Construct diagnosis-based exclusion code
if(is.null(dx_exclude1) & is.null(dx_exclude2)){
  dx_exclude_condition <- ""
}
if(!is.null(dx_exclude1) & is.null(dx_exclude2)){
  dx_exclude_condition <- paste0(
    "--left join diagnoses to claim-level exclude flag if specified
    left join(
      select diag.claim_header_id, max(ref.ccw_", dx_exclude1, ") as exclude1

    --pull out claim and diagnosis fields
    from (
      select ", top_rows, " id_", source_data, ", claim_header_id, icdcm_norm, icdcm_version, icdcm_number
      from PHClaims.", from_table_icdcm, 
    ") diag
    
    --join to diagnosis reference table, subset to those with CCW exclusion flag
    inner join (
      select ", top_rows, " dx, dx_ver, ccw_", dx_exclude1, "
      from PHClaims.ref.dx_lookup
      where ccw_", dx_exclude1, " = 1
    ) ref
  
    on (diag.icdcm_norm = ref.dx) and (diag.icdcm_version = ref.dx_ver)
    where (ref.ccw_", dx_exclude1, " = 1", dx_exclude1_fields_condition, ")
    group by diag.claim_header_id
    ) as exclude
    on diag_lookup.claim_header_id = exclude.claim_header_id
    where exclude.exclude1 is null")
}
if(!is.null(dx_exclude1) & !is.null(dx_exclude2)){
  dx_exclude_condition <- paste0(
    "--left join diagnoses to claim-level exclude flag if specified
    left join(
      select diag.claim_header_id, max(ref.ccw_", dx_exclude1, ") as exclude1, max(ref.ccw_", dx_exclude2, ") as exclude2

    --pull out claim and diagnosis fields
    from (
      select ", top_rows, " id_", source_data, ", claim_header_id, icdcm_norm, icdcm_version, icdcm_number
      from PHClaims.", from_table_icdcm, 
    ") diag
    
    --join to diagnosis reference table, subset to those with CCW exclusion flag
    inner join (
      select ", top_rows, " dx, dx_ver, ccw_", dx_exclude1, ", ccw_", dx_exclude2, "
      from PHClaims.ref.dx_lookup
      where ccw_", dx_exclude1, " = 1 or ccw_", dx_exclude2, " = 1
    ) ref
  
    on (diag.icdcm_norm = ref.dx) and (diag.icdcm_version = ref.dx_ver)
    where (ref.ccw_", dx_exclude1, " = 1", dx_exclude1_fields_condition, ") or (ref.ccw_", dx_exclude2, " = 1", dx_exclude2_fields_condition, ")
    group by diag.claim_header_id
    ) as exclude
    on diag_lookup.claim_header_id = exclude.claim_header_id
    where exclude.exclude1 is null and exclude.exclude2 is null")
}


# ### ### ### ### ### ### ###
#### Step 3: create temp table to hold condition-specific claims and dates #### 
# ### ### ### ### ### ### ###

ptm01 <- proc.time() # Times how long this query takes
# Build SQL query
sql1 <- paste0(
  
  "--#drop temp table if it exists
  if object_id('tempdb..##header') IS NOT NULL drop table ##header;
  
  --apply CCW claim type criteria to define conditions 1 and 2
  select header.id_", source_data, ", header.claim_header_id, header.claim_type_id, header.first_service_dt, diag_lookup.ccw_", ccw_abbrev, ", 
  case when header.claim_type_id in (select * from PHClaims.dbo.Split('", claim_type1, "', ',')) then 1 else 0 end as 'condition1',
  case when header.claim_type_id in (select * from PHClaims.dbo.Split('", claim_type2, "', ',')) then 1 else 0 end as 'condition2',
  case when header.claim_type_id in (select * from PHClaims.dbo.Split('", claim_type1, "', ','))
    then header.first_service_dt else null end as 'condition_1_from_date',
  case when header.claim_type_id in (select * from PHClaims.dbo.Split('", claim_type2, "', ',')) 
    then header.first_service_dt else null end as 'condition_2_from_date'

  into ##header
  
  --pull out claim type and service dates
  from (
    select id_", source_data, ", claim_header_id, claim_type_id, first_service_dt
    from PHClaims.", from_table_claim_header, 
  ") header
  
  --right join to claims containing a diagnosis in the CCW condition definition
  right join (
    select diag.id_", source_data, ", diag.claim_header_id, ref.ccw_", ccw_abbrev, "
    
    --pull out claim and diagnosis fields
    from (
      select ", top_rows, " id_", source_data, ", claim_header_id, icdcm_norm, icdcm_version
      from PHClaims.", from_table_icdcm, dx_fields_condition, 
    ") diag

  --join to diagnosis reference table, subset to those with CCW condition
    inner join (
      select ", top_rows, " dx, dx_ver, ccw_", ccw_abbrev, "
      from PHClaims.ref.dx_lookup
      where ccw_", ccw_abbrev, " = 1
    ) ref
    
    on (diag.icdcm_norm = ref.dx) and (diag.icdcm_version = ref.dx_ver)
  ) as diag_lookup
  
  on header.claim_header_id = diag_lookup.claim_header_id
  ", dx_exclude_condition, ";")

#Run SQL query
sql_result <- dbSendQuery(db.claims51,sql1)
dbClearResult(sql_result)


# ### ### ### ### ### ### ###
#### Step 4: create temp table to hold ID and rolling time period matrix #### 
# ### ### ### ### ### ### ###

#Build SQL query
sql2 <- paste0(

  "if object_id('tempdb..##rolling_tmp') IS NOT NULL drop table ##rolling_tmp;
  
  --join rolling time table to person ids
  select id.id_", source_data, ", rolling.start_window, rolling.end_window
  
  into ##rolling_tmp
  
  from (
    select distinct id_", source_data, ", 'link' = 1 from ##header
  ) as id
  
  right join (
    select cast(start_window as date) as 'start_window', cast(end_window as date) as 'end_window',
    'link' = 1
    from PHClaims.ref.rolling_time_", lookback_months, "mo_2012_2020
  ) as rolling
  
  on id.link = rolling.link
  order by id.id_", source_data, ", rolling.start_window;"
)

#Run SQL query
sql_result <- dbSendQuery(db.claims51,sql2)
dbClearResult(sql_result)


# ### ### ### ### ### ### ###
#### Step 5: identify condition status over time and collapse to contiguous time periods  #### 
# ### ### ### ### ### ### ###

#Build SQL query
sql3 <- paste0(
  
  "--#drop temp table if it exists
  if object_id('tempdb..##", ccw_abbrev, "') IS NOT NULL drop table ##", ccw_abbrev, ";
  
  --collapse to single row per ID and contiguous time period
  select distinct d.id_", source_data, ", min(d.start_window) as 'from_date', max(d.end_window) as 'to_date', ", ccw_code, " as 'ccw_code',
    '", ccw_abbrev, "' as 'ccw_desc'
  
  into ##", ccw_abbrev, "\n",
  
  "from (
  	--set up groups where there is contiguous time
    select c.id_", source_data, ", c.start_window, c.end_window, c.discont, c.temp_row,
  
    sum(case when c.discont is null then 0 else 1 end) over
      (order by c.id_", source_data, ", c.temp_row rows between unbounded preceding and current row) as 'grp'
  
    from (
      --pull out ID and time periods that contain appropriate claim counts
      select b.id_", source_data, ", b.start_window, b.end_window, b.condition_1_cnt, b.condition_2_min_date, b.condition_2_max_date,
    
      --create a flag for a discontinuity in a person's disease status
      case
        when datediff(month, lag(b.start_window) over (partition by b.id_", source_data, " order by b.id_", source_data, ", b.start_window), b.start_window) <= 1 then null
        when b.start_window < lag(b.end_window) over (partition by b.id_", source_data, " order by b.id_", source_data, ", b.start_window) then null
        when row_number() over (partition by b.id_", source_data, " order by b.id_", source_data, ", b.start_window) = 1 then null
        else row_number() over (partition by b.id_", source_data, " order by b.id_", source_data, ", b.start_window)
      end as 'discont',
  
    row_number() over (partition by b.id_", source_data, " order by b.id_", source_data, ", b.start_window) as 'temp_row'
  
    from (
  --sum condition1 and condition2 claims by ID and period, take min and max service date for each condition2 claim by ID and period
      select a.id_", source_data, ", a.start_window, a.end_window, sum(a.condition1) as 'condition_1_cnt', sum(a.condition2) as 'condition_2_cnt',
        min(a.condition_2_from_date) as 'condition_2_min_date', max(a.condition_2_from_date) as 'condition_2_max_date'
    
      from (
      --pull ID, time period and claim information, subset to ID x time period rows containing a relevant claim
      select matrix.id_", source_data, ", matrix.start_window, matrix.end_window, cond.first_service_dt, cond.condition1,
        cond.condition2, condition_2_from_date
      
      --pull in ID x time period matrix
      from (
        select id_", source_data, ", start_window, end_window
        from ##rolling_tmp
      ) as matrix
      
      --join to condition temp table
      left join (
        select id_", source_data, ", first_service_dt, condition1, condition2, condition_2_from_date
        from ##header
      ) as cond
      
      on matrix.id_", source_data, " = cond.id_", source_data, "
      where cond.first_service_dt between matrix.start_window and matrix.end_window
    ) as a
    group by a.id_", source_data, ", a.start_window, a.end_window
  ) as b", "\n",
  claim_count_condition, "\n",
  ") as c
) as d
group by d.id_", source_data, ", d.grp
order by d.id_", source_data, ", from_date;"
)

#Run SQL query
sql_result <- dbSendQuery(db.claims51,sql3)
dbClearResult(sql_result)

#Run time of all steps
proc.time() - ptm01


# ### ### ### ### ### ### ###
#### Step 6: Union all condition tables into final stage table  #### 
# ### ### ### ### ### ### ###
