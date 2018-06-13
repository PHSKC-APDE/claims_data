#### About this script #########################
# Eli Kern
# 2018-6-11
# APDE, PHSKC
# Code to create stored procedure that generates table to hold chronic condition diagnosis status (CCW) for 
  # collapsed time periods -> dbo.mcaid_claim_CONDITION_person
# This is for Type 2 CCW definitions - that require 1 or 2 claims in lookback period to meet condition criteria, depending on claim type
# Expects 4 parameters - condition name, number of years in lookback period, claim types to be included (1 for 1-claim check, 1 for 2-claim check)
# Version 1.0

#Time for entire script to run: ~35 seconds for asthma


##### Set up global parameter and call in libraries #####
options(max.print = 350, tibble.print_max = 30, scipen = 999)

library(tidyverse) # Used to manipulate tidy data
library(lubridate) # Used to manipulate dates
library(odbc) # Used to connect to SQL server
library(RCurl) # Used for pulling files from web (i.e. GitHub)

##### Set date origin #####
origin <- "1970-01-01"

##### Connect to the servers #####
db.claims51 <- dbConnect(odbc(), "PHClaims51")


##### Step 0: set parameters for person-condition table #####

condition <- "chr_kidney_dis"
lookback <- "24mo"
claim_type1 <- "31,33,12,23"
claim_type2 <- "1,3,26,27,28,34"

#Use this parameter for script testing - set to "top 5000" for example
top_rows <- ""

ptm01 <- proc.time() # Times how long this query takes

##### step 1: create temp table to hold condition-specific claims and dates #####

# Build SQL query
sql <- paste0(
  
  "--#drop temp table if it exists
  if object_id('tempdb..##condition_tmp') IS NOT NULL 
  drop table ##condition_tmp
  
  --apply CCW claim type criteria to define conditions 1 and 2
  select header.id, header.tcn, header.clm_type_code, header.from_date, diag.", condition, "_ccw, 
  
	  case when header.clm_type_code in (select * from PHClaims.dbo.Split('", claim_type1, "', ',')) then 1 else 0 end as 'condition1',
	  case when header.clm_type_code in (select * from PHClaims.dbo.Split('", claim_type2, "', ',')) then 1 else 0 end as 'condition2',
	  case when header.clm_type_code in (select * from PHClaims.dbo.Split('", claim_type1, "', ','))  
      then header.from_date else null end as 'condition_1_from_date',
	  case when header.clm_type_code in (select * from PHClaims.dbo.Split('", claim_type2, "', ',')) 
      then header.from_date else null end as 'condition_2_from_date'
  
  into ##condition_tmp
  
  --pull out claim type and service dates
  from (
    select id, tcn, clm_type_code, from_date
    from PHClaims.dbo.mcaid_claim_header
  ) header
  
  --right join to claims containing a diagnosis in the CCW condition definition
  right join (
    select diag.id, diag.tcn, ref.", condition, "_ccw
    
    --pull out claim and diagnosis fields
    from (
      select ", top_rows, " id, tcn, dx_norm, dx_ver
      from PHClaims.dbo.mcaid_claim_dx
    ) diag

  --join to diagnosis reference table, subset to those with CCW condition
    inner join (
      select ", top_rows, " dx, dx_ver, ", condition, "_ccw
      from PHClaims.dbo.ref_dx_lookup
      where ", condition, "_ccw = 1
    ) ref
    
    on (diag.dx_norm = ref.dx) and (diag.dx_ver = ref.dx_ver)
  ) as diag
  
  on header.tcn = diag.tcn"
  
)

#Run SQL query
dbSendQuery(db.claims51,sql)


##### step 2: create temp table to hold ID and rolling time period matrix #####

#Build SQL query
sql2 <- paste0(

  "if object_id('tempdb..##rolling_tmp') IS NOT NULL 
  drop table ##rolling_tmp
  
  --join rolling time table to person ids
  select id, start_window, end_window
  
  into ##rolling_tmp
  
  from (
    select distinct id, 'link' = 1 from ##condition_tmp
  ) as id
  
  right join (
    select cast(start_window as date) as 'start_window', cast(end_window as date) as 'end_window',
    'link' = 1
    from PHClaims.dbo.ref_rolling_time_", lookback, "_2012_2020
  ) as rolling
  
  on id.link = rolling.link
  order by id.id, rolling.start_window"
)

#Run SQL query
dbSendQuery(db.claims51,sql2)

##### step 3: identify condition status over time and collapse to contiguous time periods #####

#Build SQL query
sql3 <- paste0(
  
  "if object_id('PHClaims.dbo.mcaid_claim_", condition, "_person_load', 'U') IS NOT NULL 
  drop table PHClaims.dbo.mcaid_claim_", condition, "_person_load;
  
  --collapse to single row per ID and contiguous time period
  select distinct d.id, min(d.start_window) as 'from_date', max(d.end_window) as 'to_date', '", condition, "_ccw' = 1
  
  into PHClaims.dbo.mcaid_claim_", condition, "_person_load
  
  from (
  --set up groups where there is contiguous time
  select c.id, c.start_window, c.end_window, c.discont, c.temp_row,
  
  sum(case when c.discont is null then 0 else 1 end) over
  (order by c.id, c.temp_row rows between unbounded preceding and current row) as 'grp'
  
  from (
  --pull out ID and time periods that contain 1 condition1 claim or 2 condition2 claims at least 1 day apart
  select b.id, b.start_window, b.end_window, b.condition_1_cnt, b.condition_2_cnt, b.condition_2_min_date, b.condition_2_max_date,
  datediff(day, b.condition_2_min_date, b.condition_2_max_date) as 'condition_2_datediff',
  
  --create a flag for a discontinuity in a person's disease status
  case
  when datediff(month, lag(b.start_window) over (partition by b.id order by b.id, b.start_window), b.start_window) <= 1 then null
  when b.start_window < lag(b.end_window) over (partition by b.id order by b.id, b.start_window) then null
  when row_number() over (partition by b.id order by b.id, b.start_window) = 1 then null
  else row_number() over (partition by b.id order by b.id, b.start_window)
  end as 'discont',
  
  row_number() over (partition by b.id order by b.id, b.start_window) as 'temp_row'
  
  from (
  --count condition1 and condition2 claims by ID and time period, and calculate minimum and maximum service date for each condition2 claim by ID and time period
  select a.id, a.start_window, a.end_window,
  sum(a.condition1) as 'condition_1_cnt', sum(a.condition2) as 'condition_2_cnt',
  min(a.condition_2_from_date) as 'condition_2_min_date', max(a.condition_2_from_date) as 'condition_2_max_date'
  
  from (
  --pull ID, time period and condition claim information, subset to ID x time period rows containing an condition claim
  select matrix.id, matrix.start_window, matrix.end_window, cond.from_date, cond.condition1, cond.condition2, condition_2_from_date
  
  --pull in ID x time period matrix
  from (
  select id, start_window, end_window
  from ##rolling_tmp
  ) as matrix
  
  --join to condition temp table
  left join (
  select id, from_date, condition1, condition2, condition_2_from_date
  from ##condition_tmp
  ) as cond
  
  on matrix.id = cond.id
  where cond.from_date between matrix.start_window and matrix.end_window
  ) as a
  group by a.id, a.start_window, a.end_window
  ) as b
  where (b.condition_1_cnt >= 1) or (b.condition_2_cnt >=2 and abs(datediff(day, b.condition_2_min_date, b.condition_2_max_date)) >=1)
  ) as c
  ) as d
  group by d.id, d.grp
  order by d.id, from_date"

)

#Run SQL query
dbSendQuery(db.claims51,sql3)

#Run time of all steps
proc.time() - ptm01
