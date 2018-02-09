##Initial load elig_dob table from SQL server 51, PH Claims ##


##### Create age variable using any reference date #####

#Set up local macros
refdate <- 20161231
agevar <- "age2016" ##will need to learn how to write function in R to use this, equivalent to local macro in Stata

elig_dob <- elig_dob %>%
  mutate(
    age2016 = as.integer(interval(dobnew,ymd(refdate))/years(1)),
    id = str_to_upper(id)
  )

##### Bring in elig_overall table #####
#Join to dob table in order to count # of individuals by age bands who were enrolled in 2016
ptm01 <- proc.time() # Times how long this query takes (~400 secs)
elig_overall <- sqlQuery(
  db.claims51,
  " select *
  FROM [PHClaims].[dbo].elig_overall",
  stringsAsFactors = FALSE
)
proc.time() - ptm01

##### Calculate covered days and months for any date range, need to add 1 day to include end date ##### 

#Set up local macros 
start <- 20160101
end <- 20161231
dayvar <- "cov2016_dy" ##need to use in function
movar <- "cov2016_mth" ##need to use in function
dayvar_tot <- "cov2016_dy_tot" ##need to use in function
movar_tot <- "cov2016_mth_tot" ##need to use in function

elig_overall <- elig_overall %>%
  
  mutate(
    
    #Interval
    #int_temp = lubridate::intersect(interval(ymd(20120101),ymd(20121231)),interval(ymd(startdate),ymd(enddate))),
    
    #Days
    cov2016_dy = (day(as.period(lubridate::intersect(interval(ymd(start),ymd(end)),interval(ymd(startdate),ymd(enddate))),"days"))) + 1,
    
    #Months
    cov2016_mth = round(cov2016_dy/30,digits=0)
  )

#Replace NA with 0 for covered days/months
elig_overall$cov2016_dy <- car::recode(elig_overall$cov2016_dy,"NA=0")
elig_overall$cov2016_mth <- car::recode(elig_overall$cov2016_mth,"NA=0")


##### Total coverage days and months per calendar year #####
elig_overall <- elig_overall %>%
  group_by(MEDICAID_RECIPIENT_ID) %>%
  
  mutate(
    
    #Days  
    cov2016_dy_tot = sum(cov2016_dy),
    
    #Months  
    cov2016_mth_tot = sum(cov2016_mth)
  ) %>%
  ungroup()