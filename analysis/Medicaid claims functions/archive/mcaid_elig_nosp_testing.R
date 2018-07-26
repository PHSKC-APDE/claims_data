mcaid_elig_nosp_f <- function(server, from_date = Sys.Date() - months(12), to_date = Sys.Date() - months(6), covmin = 0, ccov_min = 1,
                         covgap_max = NULL, dualmax = 100, agemin = 0, agemax = 200, id = "null") {
  
  #Error checks
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
  
  if(!is.numeric(agemin) | !is.numeric(agemax)) {
    stop("Age min and max must be provided as numerics")
  }
  
  if(agemin > agemax & !missing(agemin) & !missing(agemax)) {
    stop("Minimum age must be <= maximum age")
  }
  
  #Run parameters message
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
    "Requested Medicaid IDs: ", id, "\n",
    sep = ""))
  
  #Build parameters for SQL query
  
  duration <- as.numeric(as.Date(to_date) - as.Date(from_date)) + 1
  
  from_date_t <- paste("\'", from_date, "\'", sep = "")
  to_date_t <- paste("\'", to_date, "\'", sep = "")
  
  ifelse(is.null(covgap_max),
         covgap_max_t <- "is not null",
         covgap_max_t <- paste0("<= ", covgap_max))
  
  ifelse(missing(id), 
         id_t <- "",
         id_t <- paste0("and a.id in (select * from PHClaims.dbo.Split(", "\'", id, "\', ','))"))
  
  #Build SQL queries
  
  #ID temp table
  sql1 <- paste0(
    "if object_id('tempdb..##id') IS NOT NULL drop table ##id
    select distinct id
    into ##id
    from PHClaims.dbo.mcaid_elig_overall
    where from_date <= ", to_date_t, " and to_date >= ", from_date_t)
  
  #Demo temp table
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
      x.arabic, x.korean, x.ukrainian, x.amharic, x.english_t, x.spanish_t, x.vietnamese_t,
      x.chinese_t, x.somali_t, x.russian_t, x.arabic_t, x.korean_t, x.ukrainian_t, x.amharic_t, x.lang_unk
    
    into ##demo
    
    from( 	
      select distinct id, 
      --age vars
      dobnew, 		
      case
      when floor((datediff(day, dobnew, ", to_date_t, ") + 1) / 365.25) >=0 then floor((datediff(day, dobnew, ", to_date_t, ") + 1) / 365.25)
      when floor((datediff(day, dobnew, ", to_date_t, ") + 1) / 365.25) = -1 then 0
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
    where x.age >= ", agemin, " and x.age <= ", agemax)
  
  #Geo temp table
  sql3 <- paste0(
    "if object_id('tempdb..##geo') IS NOT NULL drop table ##geo
    select distinct zip.id, zip.zip_new, reg.kcreg_zip
    into ##geo
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
    
    --select ZIP-based region based on selected ZIP code
    left join (
    select zip, kcreg_zip
    from PHClaims.dbo.ref_region_zip_1017
    ) as reg
    on zip.zip_new = reg.zip")
  
  #Coverage temp table
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
    where a.covper >= ", covmin, " and a.ccovd_max >= ", ccov_min, " and a.covgap_max ", covgap_max_t, id_t)
  
  #Dual flag temp table
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
	  where z.dualper <= ", dualmax)
  
  #Join all tables and return result
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
    demo.nhpi_t, demo.white_t, demo.latino_t, demo.race_unk, geo.zip_new, geo.kcreg_zip, demo.maxlang, demo.english, demo.spanish, demo.vietnamese, demo.chinese, demo.somali, 
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
    select id, zip_new, kcreg_zip from ##geo
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
    on cov.id = demo.id")
  

  #Execute batched SQL query
  sqlbatch_f(server, list(sql1, sql2, sql3, sql4, sql5, sql6))
  
}