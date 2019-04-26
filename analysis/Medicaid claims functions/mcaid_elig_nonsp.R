##Testing a version of mcaid_elig_f that does not use a stored procedure

##### Set up global parameters and call in libraries #####
options(max.print = 350, tibble.print_max = 50, scipen = 999)
origin <- "1970-01-01" # Date origin
library(odbc) # Connect to SQL server
library(medicaid) # Analyze WA State Medicaid data
library(dplyr) # Work with tidy data
library(rlang) # Work with core language features of R and tidyverse
library(openxlsx) # Read and write data using Microsoft Excel

##### Connect to SQL Servers #####
db.claims51 <- dbConnect(odbc(), "PHClaims51")


#### Run function ####
system.time(elig_test <- mcaid_elig_f(db.claims51, from_date = "2017-01-01", to_date = "2017-12-31"))

#### Create function ####
mcaid_elig_f <- function(server, from_date = Sys.Date() - months(12), to_date = Sys.Date() - months(6), covmin = 0, ccov_min = 1,
                         covgap_max = "null", dualmax = 100, agemin = 0, agemax = 200, female = "null", male = "null", 
                         aian = "null", asian = "null", black = "null", nhpi = "null", white = "null", latino = "null",
                         zip = "null", region = "null", english = "null", spanish = "null", vietnamese = "null",
                         chinese = "null", somali = "null", russian = "null", arabic = "null", korean = "null",
                         ukrainian = "null", amharic = "null", maxlang = "null", id = "null") {
  
  ### Error checks ###
  if(missing(server)) {
    stop("please provide a SQL server where data resides")
  }
  
  if(from_date > to_date & !missing(from_date) & !missing(to_date)) {
    stop("from_date date must be <= to_date date")
  }
  
  if(missing(from_date) & missing(to_date)) {
    print("Default from_date and to_date dates used - 12 and 6 months prior to today's date, respectively")
  }
  
  if((missing(from_date) & !missing(to_date)) | (!missing(from_date) & missing(to_date))) {
    stop("If from_date date provided, to_date date must also be provided. And vice versa.")
  }
  
  if(!is.numeric(covmin) | covmin < 0 | covmin > 100){
    stop("Coverage requirement must be numeric between 0 and 100")
  }
  
  if(!is.numeric(dualmax) | dualmax < 0 | dualmax > 100){
    stop("Dual eligibility must be numeric between 0 and 100")
  }
  
  if(!is.numeric(ccov_min) | ccov_min < 1){
    stop("Minimum continuous coverage days must be a positive integer greater than 0")
  }
  
  if((!is.numeric(covgap_max) | covgap_max < 0) & !is.character(covgap_max)){
    stop("Maximum continuous coverage gap must be a positive integer")
  }
  
  if(!is.numeric(agemin) | !is.numeric(agemax)) {
    stop("Age min and max must be provided as numerics")
  }
  
  if(agemin > agemax & !missing(agemin) & !missing(agemax)) {
    stop("Minimum age must be <= maximum age")
  }
  
  
  if(!(aian %in% c("null",0, 1)) | !(asian %in% c("null",0, 1)) | !(black %in% c("null",0, 1)) |
     !(nhpi %in% c("null",0, 1)) | !(white %in% c("null",0, 1)) | !(latino %in% c("null",0, 1)) |
     !(female %in% c("null",0, 1)) | !(male %in% c("null",0, 1)) | !(english %in% c("null",0, 1)) |
     !(spanish %in% c("null",0, 1)) | !(vietnamese %in% c("null",0, 1)) | !(chinese %in% c("null",0, 1)) |
     !(somali %in% c("null",0, 1)) | !(russian %in% c("null",0, 1)) | !(arabic %in% c("null",0, 1)) |
     !(korean %in% c("null",0, 1)) | !(ukrainian %in% c("null",0, 1)) | !(amharic %in% c("null",0, 1))) {
    stop("Race, sex and language parameters must be left missing or set to 'null', 0 or 1")
  }
  
  if(!is.character(zip) | !is.character(region) | !is.character(maxlang) | !is.character(id)) {
    stop("Geographic, 'maxlang' and 'id' parameters must be input as comma-separated characters with no spaces between items")
  }
  
  ### Create user message ###
  cat(paste(
    "You have selected a Medicaid member cohort with the following characteristics:\n",
    "Coverage begin date: ", from_date, "(inclusive)\n",
    "Coverage end date: ", to_date, " (inclusive)\n",
    "Coverage requirement: ", covmin, " percent or more of requested date range\n",
    "Minimum continuous coverage requirement: ", ccov_min, " days during requested date range\n",
    "Maximum continuous coverage gap: ", covgap_max, " days during requested date range\n",
    "Medicare-Medicaid dual eligibility: ", dualmax, " percent or less of requested date range\n",
    "Minimum age: ", agemin, " years and older\n",
    "Maximum age: ", agemax, " years and younger\n",    
    "Female alone or in combination, ever: ", female, "\n",
    "Male alone or in combination, ever: ", male, "\n",  
    "AI/AN alone or in combination, ever: ", aian, "\n",
    "Asian alone or in combination, ever: ", asian, "\n",   
    "Black alone or in combination, ever: ", black, "\n",
    "NH/PI alone or in combination, ever: ", nhpi, "\n",
    "White alone or in combination, ever: ", white, "\n",
    "Latino alone or in combination, ever: ", latino, "\n",
    "ZIP codes: ", zip, "\n",
    "HRA-based regions: ", region, "\n",
    "English language alone or in combination, ever: ", english, "\n",  
    "Spanish language alone or in combination, ever: ", spanish, "\n",
    "Vietnamese language alone or in combination, ever: ", vietnamese, "\n",   
    "Chinese language alone or in combination, ever: ", chinese, "\n",
    "Somali language alone or in combination, ever: ", somali, "\n",
    "Russian language alone or in combination, ever: ", russian, "\n",
    "Arabic language alone or in combination, ever: ", arabic, "\n",
    "Korean language alone or in combination, ever: ", korean, "\n",
    "Ukrainian language alone or in combination, ever: ", ukrainian, "\n",
    "Amharic language alone or in combination, ever: ", amharic, "\n",
    "Languages: ", maxlang, "\n",
    "Requested Medicaid IDs: ", id, "\n",
    sep = ""))
  
  ### Process parameters for SQL query ###
  
  #Time parameters
  from_date_t <- paste0("'", from_date, "'")
  to_date_t <- paste0("'", to_date, "'")
  duration <- as.numeric(as.Date(to_date) - as.Date(from_date)) + 1
  
  #Gender
  ifelse(missing(female), 
         female <- "",
         female <- paste0("and x.female =", female))
  ifelse(missing(male), 
         male <- "",
         male <- paste0("and x.male =", male))
  
  #Race
  ifelse(missing(aian), 
         aian <- "",
         aian <- paste0("and x.aian =", aian))
  ifelse(missing(asian), 
         asian <- "",
         asian <- paste0("and x.asian =", asian))
  ifelse(missing(black), 
         black <- "",
         black <- paste0("and x.black =", black))
  ifelse(missing(nhpi), 
         nhpi <- "",
         nhpi <- paste0("and x.nhpi =", nhpi))
  ifelse(missing(white), 
         white <- "",
         white <- paste0("and x.white =", white))
  ifelse(missing(latino), 
         latino <- "",
         latino <- paste0("and x.latino =", latino))
  
  #Language binary flags
  ifelse(missing(english), 
         english <- "",
         english <- paste0("and x.english =", english))
  ifelse(missing(spanish), 
         spanish <- "",
         spanish <- paste0("and x.spanish =", spanish))
  ifelse(missing(vietnamese), 
         vietnamese <- "",
         vietnamese <- paste0("and x.vietnamese =", vietnamese))
  ifelse(missing(chinese), 
         chinese <- "",
         chinese <- paste0("and x.chinese =", chinese)) 
  ifelse(missing(somali), 
         somali <- "",
         somali <- paste0("and x.somali =", somali)) 
  ifelse(missing(russian), 
         russian <- "",
         russian <- paste0("and x.russian =", russian)) 
  ifelse(missing(arabic), 
         arabic <- "",
         arabic <- paste0("and x.arabic =", arabic)) 
  ifelse(missing(korean), 
         korean <- "",
         korean <- paste0("and x.korean =", korean))  
  ifelse(missing(ukrainian), 
         ukrainian <- "",
         ukrainian <- paste0("and x.ukrainian =", ukrainian))   
  ifelse(missing(amharic), 
         amharic <- "",
         amharic <- paste0("and x.amharic =", amharic))
  
  #Most frequently reported language
  ifelse(missing(maxlang),
         maxlang <- "",
         maxlang <- paste0("and x.maxlang in (select * from PHClaims.dbo.Split(\'", maxlang, "\', ','))"))
  
  #Geography
  ifelse(missing(zip),
         zip <- "",
         zip <- paste0("and zip.zip_new in (select * from PHClaims.dbo.Split(\'", zip, "\', ','))"))
  
  ifelse(missing(region),
         region <- "",
         region <- paste0("and reg.region in (select * from PHClaims.dbo.Split(\'", region, "\', ','))"))
  
  #Coverage-related parameters
  ifelse(missing(covgap_max), 
         covgap_max <- "",
         covgap_max <- paste0("and a.covgap_max <=", covgap_max))
  
  #List of IDs
  ifelse(missing(id),
         id <- "",
         id <- paste0("and a.id in (select * from PHClaims.dbo.Split(\'", id, "\', ','))"))
  

  ### Build SQL queries one by one ###
  
  #STEP 1: Temp table for IDs in requested time period
  sql1 <- paste0(
    "if object_id('tempdb..##id') IS NOT NULL drop table ##id
    select distinct id
    into ##id
    from PHClaims.dbo.mcaid_elig_overall
    where from_date <=", to_date_t, "and to_date >=", from_date_t
  )
  
  #STEP 2: Temp table for demo info
  sql2 <- paste0(
    "if object_id('tempdb..##demo') IS NOT NULL drop table ##demo
     select x.id, x.dobnew, x.age, 
     case
       when x.age >= 0 and x.age < 5 then '0-4'
       when x.age >= 5 and x.age < 12 then '5-11'
       when x.age >= 12 and x.age < 18 then '12-17'
       when x.age >= 18 and x.age < 25 then '18-24'
       when x.age >= 25 and x.age < 45 then '25-44'
       when x.age >= 45 and x.age < 65 then '45-64'
       when x.age >= 65 then '65 and over'
     end as 'age_grp7',
     x.gender_mx, x.male, x.female, x.male_t, x.female_t, x.gender_unk, x.race_eth_mx, x.race_mx, x.aian, x.asian,
     x.black, x.nhpi, x.white, x.latino, x.aian_t, x.asian_t, x.black_t, x.nhpi_t, x.white_t,
     x.latino_t, x.race_unk, x.maxlang, x.english, x.spanish, x.vietnamese, x.chinese, x.somali, x.russian,
     x.arabic, x.korean, x.ukrainian, x.amharic, x. english_t, x.spanish_t, x.vietnamese_t,
     x.chinese_t, x.somali_t, x.russian_t, x.arabic_t, x.korean_t, x.ukrainian_t, x.amharic_t, x.lang_unk
     
     into ##demo
     
     from( 	
       select distinct id, 
       --age vars
       dobnew, 		
       case
         when floor((datediff(day, dobnew,", to_date_t, ") + 1) / 365.25) >=0 then floor((datediff(day, dobnew,", to_date_t, ") + 1) / 365.25)
         when floor((datediff(day, dobnew,", to_date_t, ") + 1) / 365.25) = -1 then 0
       end as 'age',
       --gender vars
       gender_mx, male, female, male_t, female_t, gender_unk,
       --race vars
       race_eth_mx, race_mx, aian, asian, black, nhpi, white, latino, aian_t, asian_t, black_t, nhpi_t, white_t, latino_t, race_unk,
       --language vars
       maxlang, english, spanish, vietnamese, chinese, somali, russian, arabic, korean, ukrainian, amharic,
       english_t, spanish_t, vietnamese_t, chinese_t, somali_t, russian_t, arabic_t, korean_t, ukrainian_t,
       amharic_t, lang_unk
       from PHClaims.dbo.mcaid_elig_demoever
       where exists (select id from ##id where id = PHClaims.dbo.mcaid_elig_demoever.id)
     ) as x
    
    --age subsets
	  where x.age >=", agemin, "and x.age <=", agemax,
    female, male, aian, asian, black, nhpi, white, latino, english, spanish, vietnamese, chinese, somali, russian,
    arabic, korean, ukrainian, amharic, maxlang
  )

  #STEP 3: Temp table for geo info
  sql3 <- paste0(
    "if object_id('tempdb..##geo') IS NOT NULL drop table ##geo
    select distinct zip.id, tract.tractce10, zip.zip_new, hra.hra_id, reg.hra, reg.region_id, reg.region
    into ##geo
    
    --zip codes
    from (
    select y.id, y.zip_new
    from (
    select x.id, x.zip_new, x.zip_dur, row_number() over (partition by x.id order by x.zip_dur desc, x.zip_new) as 'zipr'
    from (
    select a.id, a.zip_new, sum(a.covd) + 1 as 'zip_dur'
    from (
    select id, zip_new,
    
    /**if coverage period fully contains date range then person time is just date range */
    iif(from_date <= ", from_date_t, " and to_date >= ", to_date_t, ", datediff(day, ", from_date_t, ", ", to_date_t, ") + 1, 
    
    /**if coverage period begins before date range start and ends within date range */
    iif(from_date <= ", from_date_t, " and to_date < ", to_date_t, " and to_date >= ", from_date_t, ", datediff(day, ", from_date_t, ", to_date) + 1,
    
    /**if coverage period begins within date range and ends after date range end */
    iif(from_date > ", from_date_t, " and to_date >= ", to_date_t, " and from_date <= ", to_date_t, ", datediff(day, from_date, ", to_date_t, ") + 1,
    
    /**if coverage period begins after date range start and ends before date range end */
    iif(from_date > ", from_date_t, " and to_date < ", to_date_t, ", datediff(day, from_date, to_date) + 1,
    
    null)))) as 'covd'
    
    from PHClaims.dbo.mcaid_elig_address
    where exists (select id from ##id where id = PHClaims.dbo.mcaid_elig_address.id)
    ) as a
    group by a.id, a.zip_new
    ) as x
    ) as y
    where y.zipr = 1
    ) as zip
    
    --HRAs
    inner join (
    select y.id, y.hra_id
    from (
    select x.id, x.hra_id, x.hra_dur, row_number() over (partition by x.id order by x.hra_dur desc, x.hra_id) as 'hrar'
    from (
    select a.id, a.hra_id, sum(a.covd) + 1 as 'hra_dur'
    from (
    select id, hra_id,
    
    /**if coverage period fully contains date range then person time is just date range */
    iif(from_date <= ", from_date_t, " and to_date >= ", to_date_t, ", datediff(day, ", from_date_t, ", ", to_date_t, ") + 1, 
    
    /**if coverage period begins before date range start and ends within date range */
    iif(from_date <= ", from_date_t, " and to_date < ", to_date_t, " and to_date >= ", from_date_t, ", datediff(day, ", from_date_t, ", to_date) + 1,
    
    /**if coverage period begins within date range and ends after date range end */
    iif(from_date > ", from_date_t, " and to_date >= ", to_date_t, " and from_date <= ", to_date_t, ", datediff(day, from_date, ", to_date_t, ") + 1,
    
    /**if coverage period begins after date range start and ends before date range end */
    iif(from_date > ", from_date_t, " and to_date < ", to_date_t, ", datediff(day, from_date, to_date) + 1,
    
    null)))) as 'covd'
    
    from PHClaims.dbo.mcaid_elig_address
    where exists (select id from ##id where id = PHClaims.dbo.mcaid_elig_address.id)
    ) as a
    group by a.id, a.hra_id
    ) as x
    ) as y
    where y.hrar = 1
    ) as hra
    on zip.id = hra.id
    
    --Tracts
    inner join (
    select y.id, y.tractce10
    from (
    select x.id, x.tractce10, x.tract_dur, row_number() over (partition by x.id order by x.tract_dur desc, x.tractce10) as 'tractr'
    from (
    select a.id, a.tractce10, sum(a.covd) + 1 as 'tract_dur'
    from (
    select id, tractce10,
    
    /**if coverage period fully contains date range then person time is just date range */
    iif(from_date <= ", from_date_t, " and to_date >= ", to_date_t, ", datediff(day, ", from_date_t, ", ", to_date_t, ") + 1, 
    
    /**if coverage period begins before date range start and ends within date range */
    iif(from_date <= ", from_date_t, " and to_date < ", to_date_t, " and to_date >= ", from_date_t, ", datediff(day, ", from_date_t, ", to_date) + 1,
    
    /**if coverage period begins within date range and ends after date range end */
    iif(from_date > ", from_date_t, " and to_date >= ", to_date_t, " and from_date <= ", to_date_t, ", datediff(day, from_date, ", to_date_t, ") + 1,
    
    /**if coverage period begins after date range start and ends before date range end */
    iif(from_date > ", from_date_t, " and to_date < ", to_date_t, ", datediff(day, from_date, to_date) + 1,
    
    null)))) as 'covd'
    
    from PHClaims.dbo.mcaid_elig_address
    where exists (select id from ##id where id = PHClaims.dbo.mcaid_elig_address.id)
    ) as a
    group by a.id, a.tractce10
    ) as x
    ) as y
    where y.tractr = 1
    ) as tract
    on zip.id = tract.id
    
    --select HRA-based region based on selected HRA
    left join (
    select hra_id, hra, region_id, region
    from PHClaims.ref.region_hra_1017
    ) as reg
    on hra.hra_id = reg.hra_id
    
    where 1 = 1", zip, region
  )
  
  #STEP 4: Temp table for coverage info
  sql4 <- paste0(
    "if object_id('tempdb..##cov') IS NOT NULL drop table ##cov
    select a.id, a.covd, a.covper, a.ccovd_max, a.covgap_max
    into ##cov
    from (
    select z.id, z.covd, z.covper, z.ccovd_max,
    case
    when z.pregap_max >= z.postgap_max then z.pregap_max
    else z.postgap_max
    end as 'covgap_max'
    
    from (
    select y.id, sum(y.covd) as 'covd', cast(sum((y.covd * 1.0)) / (", duration, " * 1.0) * 100.0 as decimal(4,1)) as 'covper',
    max(y.covd) as 'ccovd_max', max(y.pregap) as 'pregap_max', max(y.postgap) as 'postgap_max'
    
    from (
    select distinct x.id, x.from_date, x.to_date,
    
    --calculate coverage days during specified time period
    /**if coverage period fully contains date range then person time is just date range */
    iif(x.from_date <= ", from_date_t, " and x.to_date >= ", to_date_t, ", datediff(day, ", from_date_t, ", ", to_date_t, ") + 1, 
    
    /**if coverage period begins before date range start and ends within date range */
    iif(x.from_date <= ", from_date_t, " and x.to_date < ", to_date_t, " and x.to_date >= ", from_date_t, ", datediff(day, ", from_date_t, ", x.to_date) + 1,
    
    /**if coverage period begins within date range and ends after date range end */
    iif(x.from_date > ", from_date_t, " and x.to_date >= ", to_date_t, " and x.from_date <= ", to_date_t, ", datediff(day, x.from_date, ", to_date_t, ") + 1,
    
    /**if coverage period begins after date range start and ends before date range end */
    iif(x.from_date > ", from_date_t, " and x.to_date < ", to_date_t, ", datediff(day, x.from_date, x.to_date) + 1,
    
    null)))) as 'covd',
    
    --calculate coverage gaps during specified time period
    case
    when x.from_date <= ", from_date_t, " then 0
    when lag(x.to_date,1) over (partition by x.id order by x.to_date) is null then datediff(day, ", from_date_t, ", x.from_date) - 1
    else datediff(day, lag(x.to_date,1) over (partition by x.id order by x.to_date), x.from_date) - 1
    end as 'pregap',
    
    case
    when x.to_date >= ", to_date_t, " then 0
    when lead(x.to_date,1) over (partition by x.id order by x.to_date) is null then datediff(day, x.to_date, ", to_date_t, ") - 1
    else datediff(day, x.to_date, lead(x.from_date,1) over (partition by x.id order by x.from_date)) - 1
    end as 'postgap'
    
    from PHClaims.dbo.mcaid_elig_overall as x
    where x.from_date <= ", to_date_t, " and x.to_date >= ", from_date_t, "
    ) as y
    group by y.id
    ) as z
    ) as a
    where a.covper >=", covmin, "and a.ccovd_max >=", ccov_min, covgap_max, id
  )
  
  #STEP 5: Temp table for dual flag
  sql5 <- paste0(
    "if object_id('tempdb..##dual') IS NOT NULL drop table ##dual
    select z.id, z.duald, z.dualper, case when z.duald >= 1 then 1 else 0 end as 'dual_flag'
    into ##dual
    from (
    	select y.id, sum(y.duald) as 'duald', 
    	cast(sum((y.duald * 1.0)) / (", duration, " * 1.0) * 100.0 as decimal(4,1)) as 'dualper'
    
    		from (
    			select distinct x.id, x.dual, x.from_date, x.to_date,
    
    			/**if coverage period fully contains date range then person time is just date range */
    			iif(x.from_date <= ", from_date_t, " and x.to_date >= ", to_date_t, " and x.dual = 'Y', datediff(day, ", from_date_t, ", ", to_date_t, ") + 1, 
    	
    			/**if coverage period begins before date range start and ends within date range */
    			iif(x.from_date <= ", from_date_t, " and x.to_date < ", to_date_t, " and x.to_date >= ", from_date_t, " and x.dual = 'Y', datediff(day, ", from_date_t, ", x.to_date) + 1,
    
    			/**if coverage period begins within date range and ends after date range end */
    			iif(x.from_date > ", from_date_t, " and x.to_date >= ", to_date_t, " and x.from_date <= ", to_date_t, " and x.dual = 'Y', datediff(day, x.from_date, ", to_date_t, ") + 1,
    
    			/**if coverage period begins after date range start and ends before date range end */
    			iif(x.from_date > ", from_date_t, " and x.to_date < ", to_date_t, " and x.dual = 'Y', datediff(day, x.from_date, x.to_date) + 1,
    
    			0)))) as 'duald'
    			from PHClaims.dbo.mcaid_elig_covgrp as x
    			where x.from_date <= ", to_date_t, " and x.to_date >= ", from_date_t, "
    		) as y
    		group by y.id
    	) as z
    	where z.dualper <=", dualmax
  )
  
  #STEP 6: Join all tables
  sql6 <- paste0(
    "select cov.id, 
	  case
      when cov.covgap_max <= 30 and dual.dual_flag = 0 then 'small gap, nondual'
      when cov.covgap_max > 30 and dual.dual_flag = 0 then 'large gap, nondual'
      when cov.covgap_max <= 30 and dual.dual_flag = 1 then 'small gap, dual'
      when cov.covgap_max > 30 and dual.dual_flag = 1 then 'large gap, dual'
    end as 'cov_cohort',
    
    cov.covd, cov.covper, cov.ccovd_max, cov.covgap_max, dual.duald, dual.dualper, dual.dual_flag, demo.dobnew, demo.age, demo.age_grp7, demo.gender_mx, demo.male, demo.female, 
    demo.male_t, demo.female_t, demo.gender_unk, demo.race_eth_mx, demo.race_mx, demo.aian, demo.asian, demo.black, demo.nhpi, demo.white, demo.latino, demo.aian_t, demo.asian_t, demo.black_t, 
    demo.nhpi_t, demo.white_t, demo.latino_t, demo.race_unk, cast(geo.tractce10 as varchar(200)) as 'tractce10', cast(geo.zip_new as varchar(200)) as 'zip_new', cast(geo.hra_id as varchar(200)) as 'hra_id',
    geo.hra, cast(geo.region_id as varchar(200)) as 'region_id', geo.region, demo.maxlang, demo.english, demo.spanish, demo.vietnamese, demo.chinese, demo.somali, 
    demo.russian, demo.arabic, demo.korean, demo.ukrainian, demo.amharic, demo.english_t, demo.spanish_t, demo.vietnamese_t, demo.chinese_t, demo.somali_t, demo.russian_t,
    demo.arabic_t, demo.korean_t, demo.ukrainian_t, demo.amharic_t, demo.lang_unk
    
    --1st table - coverage
    from (
    select id, covd, covper, ccovd_max, covgap_max from ##cov
    )as cov
    
    --2nd table - dual eligibility duration
    inner join (
    select id, duald, dualper, dual_flag from ##dual
    ) as dual
    on cov.id = dual.id
    
    --3rd table - sub-county areas
    inner join (
    select id, tractce10, zip_new, hra_id, hra, region_id, region from ##geo
    ) as geo
    --join on ID
    on cov.id = geo.id
    
    --4th table - age, gender, race, and language
    inner join (
    select id, dobnew, age, age_grp7, gender_mx, male, female, 
    male_t, female_t, gender_unk, race_eth_mx, race_mx, aian, asian, black, nhpi, white, latino, aian_t, asian_t, black_t, 
    nhpi_t, white_t, latino_t, race_unk, maxlang, english, spanish, vietnamese, chinese, somali, 
    russian, arabic, korean, ukrainian, amharic, english_t, spanish_t, vietnamese_t, chinese_t, somali_t, russian_t,
    arabic_t, korean_t, ukrainian_t, amharic_t, lang_unk
    from ##demo
    ) as demo
    --join on ID
    on cov.id = demo.id"
  )
  
  ### Execute batched SQL query ###
  sqlbatch_f(server, list(sql1, sql2, sql3, sql4, sql5, sql6))
}
