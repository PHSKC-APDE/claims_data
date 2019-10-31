#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_ELIG_PLR
# Eli Kern, PHSKC (APDE)
#
# 2019-10

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_elig_plr_f <- function(from_date = NULL, to_date = NULL) {
  
  ### Require extract_end_date
  if (is.null(from_date) | is.null(to_date)) {
    stop("Enter the from and to date for this PLR table: \"YYYY-MM-DD\"")
  }
  
  ### Process year for table name
  table_name_year <- stringr::str_sub(from_date,1,4)
  table_name_year <- paste0("apcd_elig_plr_", table_name_year)
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "
    --------------------------
    --STEP 1: Calculate coverage days and gaps in date range
    --------------------------
    
    if object_id('tempdb..#cov1') is not null drop table #cov1;
    select distinct id_apcd, from_date, to_date,
    ---------
    --MEDICAL coverage days
    ---------
    --calculate total medical coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and med_covgrp != 0 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and med_covgrp != 0 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and med_covgrp != 0 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and med_covgrp != 0 then datediff(day, from_date, to_date) + 1
      else 0
     end as med_total_covd,
    
    --calculate Medicaid medical coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and med_medicaid = 1 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and med_medicaid = 1 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and med_medicaid = 1 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and med_medicaid = 1 then datediff(day, from_date, to_date) + 1
      else 0
     end as med_medicaid_covd,
     
    --calculate Medicare medical coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and med_medicare = 1 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and med_medicare = 1 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and med_medicare = 1 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and med_medicare = 1 then datediff(day, from_date, to_date) + 1
      else 0
     end as med_medicare_covd,
     
    --calculate Commercial medical coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and med_commercial = 1 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and med_commercial = 1 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and med_commercial = 1 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and med_commercial = 1 then datediff(day, from_date, to_date) + 1
      else 0
     end as med_commercial_covd,
     
    ---------
    --PHARMACY coverage days
    ---------
    --calculate total pharmacy coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and pharm_covgrp != 0 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and pharm_covgrp != 0 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and pharm_covgrp != 0 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and pharm_covgrp != 0 then datediff(day, from_date, to_date) + 1
      else 0
     end as pharm_total_covd,
    
    --calculate Medicaid pharmacy coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and pharm_medicaid = 1 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and pharm_medicaid = 1 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and pharm_medicaid = 1 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and pharm_medicaid = 1 then datediff(day, from_date, to_date) + 1
      else 0
     end as pharm_medicaid_covd,
     
    --calculate Medicare pharmacy coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and pharm_medicare = 1 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and pharm_medicare = 1 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and pharm_medicare = 1 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and pharm_medicare = 1 then datediff(day, from_date, to_date) + 1
      else 0
     end as pharm_medicare_covd,
     
    --calculate Commercial pharmacy coverage days during date range
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and pharm_commercial = 1 then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and pharm_commercial = 1 then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and pharm_commercial = 1 then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and pharm_commercial = 1 then datediff(day, from_date, to_date) + 1
      else 0
     end as pharm_commercial_covd,
     
    ---------
    --Medicaid-Medicare DUAL (medical or pharm) coverage days
    ---------
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and ((med_medicaid = 1 or pharm_medicaid = 1) and (med_medicare = 1 or pharm_medicare = 1))
        then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and 
        ((med_medicaid = 1 or pharm_medicaid = 1) and (med_medicare = 1 or pharm_medicare = 1)) then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and 
        ((med_medicaid = 1 or pharm_medicaid = 1) and (med_medicare = 1 or pharm_medicare = 1)) then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and ((med_medicaid = 1 or pharm_medicaid = 1) and (med_medicare = 1 or pharm_medicare = 1))
        then datediff(day, from_date, to_date) + 1
      else 0
     end as dual_covd,
    
     ---------
    --Medicaid Full Benefit (medical or pharmacy) coverage days
    ---------
    case
      --coverage period fully contains date range
      when from_date <= {from_date} and to_date >= {to_date} and ((med_medicaid = 1 or pharm_medicaid = 1) and (full_benefit = 1))
        then datediff(day, {from_date}, {to_date}) + 1
      --coverage period begins before and ends within date range
      when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} and 
        ((med_medicaid = 1 or pharm_medicaid = 1) and (full_benefit = 1)) then datediff(day, {from_date}, to_date) + 1
      --coverage period begins within and ends after date range
      when from_date > {from_date}  and to_date >= {to_date} and from_date <= {to_date} and 
        ((med_medicaid = 1 or pharm_medicaid = 1) and (full_benefit = 1)) then datediff(day, from_date, {to_date}) + 1
      --coverage period begins and ends within date range
      when from_date > {from_date} and to_date < {to_date} and ((med_medicaid = 1 or pharm_medicaid = 1) and (full_benefit = 1))
        then datediff(day, from_date, to_date) + 1
      else 0
     end as full_benefit_covd,
    
    ---------
    --MEDICAL coverage gaps
    ---------
    --calculate total coverage gaps during date range
    case
    	--coverage period begins before date range
      when from_date <= {from_date} and med_covgrp != 0 then 0
      --for first row of coverage
    	when lag(to_date,1) over (partition by id_apcd order by to_date) is null and med_covgrp != 0 then datediff(day, {from_date}, from_date)
    	--otherwise take difference between current row and previous row
      when med_covgrp != 0 then datediff(day, lag(to_date,1) over (partition by id_apcd order by to_date), from_date) - 1
    end as med_total_pregap,
    
    case
    	--coverage period begins before date range
      when to_date >= {to_date} and med_covgrp != 0 then 0
      --for first row of coverage
    	when lead(to_date,1) over (partition by id_apcd order by to_date) is null and med_covgrp != 0 then datediff(day, to_date, {to_date})
    	--otherwise take difference between current row and previous row
      when med_covgrp != 0 then datediff(day, to_date, lead(from_date,1) over (partition by id_apcd order by from_date)) - 1
    end as med_total_postgap,
    
    --calculate Medicaid coverage gaps during date range
    case
    	--coverage period begins before date range
      when from_date <= {from_date} and med_medicaid = 1 then 0
      --for first row of coverage
    	when lag(to_date,1) over (partition by id_apcd, med_medicaid order by to_date) is null and med_medicaid = 1 then datediff(day, {from_date}, from_date)
    	--otherwise take difference between current row and previous row
      when med_medicaid = 1 then datediff(day, lag(to_date,1) over (partition by id_apcd, med_medicaid order by to_date), from_date) - 1
    end as med_medicaid_pregap,
    
    case
    	--coverage period begins before date range
      when to_date >= {to_date} and med_medicaid = 1 then 0
      --for first row of coverage
    	when lead(to_date,1) over (partition by id_apcd, med_medicaid order by to_date) is null and med_medicaid = 1 then datediff(day, to_date, {to_date})
    	--otherwise take difference between current row and previous row
      when med_medicaid = 1 then datediff(day, to_date, lead(from_date,1) over (partition by id_apcd, med_medicaid order by from_date)) - 1
    end as med_medicaid_postgap,
    
    --calculate Medicare coverage gaps during date range
    case
    	--coverage period begins before date range
      when from_date <= {from_date} and med_medicare = 1 then 0
      --for first row of coverage
    	when lag(to_date,1) over (partition by id_apcd, med_medicare order by to_date) is null and med_medicare = 1 then datediff(day, {from_date}, from_date)
    	--otherwise take difference between current row and previous row
      when med_medicare = 1 then datediff(day, lag(to_date,1) over (partition by id_apcd, med_medicare order by to_date), from_date) - 1
    end as med_medicare_pregap,
    
    case
    	--coverage period begins before date range
      when to_date >= {to_date} and med_medicare = 1 then 0
      --for first row of coverage
    	when lead(to_date,1) over (partition by id_apcd, med_medicare order by to_date) is null and med_medicare = 1 then datediff(day, to_date, {to_date})
    	--otherwise take difference between current row and previous row
      when med_medicare = 1 then datediff(day, to_date, lead(from_date,1) over (partition by id_apcd, med_medicare order by from_date)) - 1
    end as med_medicare_postgap,
    
    --calculate Commercial coverage gaps during date range
    case
    	--coverage period begins before date range
      when from_date <= {from_date} and med_commercial = 1 then 0
      --for first row of coverage
    	when lag(to_date,1) over (partition by id_apcd, med_commercial order by to_date) is null and med_commercial = 1 then datediff(day, {from_date}, from_date)
    	--otherwise take difference between current row and previous row
      when med_commercial = 1 then datediff(day, lag(to_date,1) over (partition by id_apcd, med_commercial order by to_date), from_date)  - 1
    end as med_commercial_pregap,
    
    case
    	--coverage period begins before date range
      when to_date >= {to_date} and med_commercial = 1 then 0
      --for first row of coverage
    	when lead(to_date,1) over (partition by id_apcd, med_commercial order by to_date) is null and med_commercial = 1 then datediff(day, to_date, {to_date})
    	--otherwise take difference between current row and previous row
      when med_commercial = 1 then datediff(day, to_date, lead(from_date,1) over (partition by id_apcd, med_commercial order by from_date)) - 1
    end as med_commercial_postgap,
    
    ---------
    --PHARMACY coverage gaps
    ---------
    --calculate total coverage gaps during date range
    case
    	--coverage period begins before date range
      when from_date <= {from_date} and pharm_covgrp != 0 then 0
      --for first row of coverage
    	when lag(to_date,1) over (partition by id_apcd order by to_date) is null and pharm_covgrp != 0 then datediff(day, {from_date}, from_date)
    	--otherwise take difference between current row and previous row
      when pharm_covgrp != 0 then datediff(day, lag(to_date,1) over (partition by id_apcd order by to_date), from_date) - 1
    end as pharm_total_pregap,
    
    case
    	--coverage period begins before date range
      when to_date >= {to_date} and pharm_covgrp != 0 then 0
      --for first row of coverage
    	when lead(to_date,1) over (partition by id_apcd order by to_date) is null and pharm_covgrp != 0 then datediff(day, to_date, {to_date})
    	--otherwise take difference between current row and previous row
      when pharm_covgrp != 0 then datediff(day, to_date, lead(from_date,1) over (partition by id_apcd order by from_date)) - 1
    end as pharm_total_postgap,
    
    --calculate Medicaid coverage gaps during date range
    case
    	--coverage period begins before date range
      when from_date <= {from_date} and pharm_medicaid = 1 then 0
      --for first row of coverage
    	when lag(to_date,1) over (partition by id_apcd, pharm_medicaid order by to_date) is null and pharm_medicaid = 1 then datediff(day, {from_date}, from_date)
    	--otherwise take difference between current row and previous row
      when pharm_medicaid = 1 then datediff(day, lag(to_date,1) over (partition by id_apcd, pharm_medicaid order by to_date), from_date) - 1
    end as pharm_medicaid_pregap,
    
    case
    	--coverage period begins before date range
      when to_date >= {to_date} and pharm_medicaid = 1 then 0
      --for first row of coverage
    	when lead(to_date,1) over (partition by id_apcd, pharm_medicaid order by to_date) is null and pharm_medicaid = 1 then datediff(day, to_date, {to_date})
    	--otherwise take difference between current row and previous row
      when pharm_medicaid = 1 then datediff(day, to_date, lead(from_date,1) over (partition by id_apcd, pharm_medicaid order by from_date)) - 1
    end as pharm_medicaid_postgap,
    
    --calculate Medicare coverage gaps during date range
    case
    	--coverage period begins before date range
      when from_date <= {from_date} and pharm_medicare = 1 then 0
      --for first row of coverage
    	when lag(to_date,1) over (partition by id_apcd, pharm_medicare order by to_date) is null and pharm_medicare = 1 then datediff(day, {from_date}, from_date)
    	--otherwise take difference between current row and previous row
      when pharm_medicare = 1 then datediff(day, lag(to_date,1) over (partition by id_apcd, pharm_medicare order by to_date), from_date) - 1
    end as pharm_medicare_pregap,
    
    case
    	--coverage period begins before date range
      when to_date >= {to_date} and pharm_medicare = 1 then 0
      --for first row of coverage
    	when lead(to_date,1) over (partition by id_apcd, pharm_medicare order by to_date) is null and pharm_medicare = 1 then datediff(day, to_date, {to_date})
    	--otherwise take difference between current row and previous row
      when pharm_medicare = 1 then datediff(day, to_date, lead(from_date,1) over (partition by id_apcd, pharm_medicare order by from_date)) - 1
    end as pharm_medicare_postgap,
    
    --calculate Commercial coverage gaps during date range
    case
    	--coverage period begins before date range
      when from_date <= {from_date} and pharm_commercial = 1 then 0
      --for first row of coverage
    	when lag(to_date,1) over (partition by id_apcd, pharm_commercial order by to_date) is null and pharm_commercial = 1 then datediff(day, {from_date}, from_date)
    	--otherwise take difference between current row and previous row
      when pharm_commercial = 1 then datediff(day, lag(to_date,1) over (partition by id_apcd, pharm_commercial order by to_date), from_date) - 1
    end as pharm_commercial_pregap,
    
    case
    	--coverage period begins before date range
      when to_date >= {to_date} and pharm_commercial = 1 then 0
      --for first row of coverage
    	when lead(to_date,1) over (partition by id_apcd, pharm_commercial order by to_date) is null and pharm_commercial = 1 then datediff(day, to_date, {to_date})
    	--otherwise take difference between current row and previous row
      when pharm_commercial = 1 then datediff(day, to_date, lead(from_date,1) over (partition by id_apcd, pharm_commercial order by from_date)) - 1
    end as pharm_commercial_postgap
    
    into #cov1
    from phclaims.final.apcd_elig_timevar
    where from_date <= {to_date} and to_date >= {from_date};
     
    
    --------------------------
    --STEP 2: Identify longest continuous coverage period by coverage type
    --------------------------
    
    if object_id('tempdb..#cov2') is not null drop table #cov2;
    select id_apcd, from_date, to_date, med_total_covd, dual_covd, full_benefit_covd, med_medicaid_covd, med_medicare_covd, med_commercial_covd, med_total_pregap, med_total_postgap, med_medicaid_pregap, med_medicaid_postgap, 
      med_medicare_pregap, med_medicare_postgap, med_commercial_pregap, med_commercial_postgap, pharm_total_covd, pharm_medicaid_covd, pharm_medicare_covd, pharm_commercial_covd, pharm_total_pregap, 
      pharm_total_postgap, pharm_medicaid_pregap, pharm_medicaid_postgap, pharm_medicare_pregap, pharm_medicare_postgap, pharm_commercial_pregap, pharm_commercial_postgap,
    
    ---------
    --MEDICAL longest coverage period
    ---------
    case
      when lag(med_total_covd,1) over (partition by id_apcd order by from_date) is null then med_total_covd
      when lag(med_total_covd,1) over (partition by id_apcd order by from_date) = 0 then med_total_covd
      when lag(med_total_covd,1) over (partition by id_apcd order by from_date) > 0 and med_total_covd != 0
        then sum(med_total_covd) over (partition by id_apcd order by from_date rows between unbounded preceding and current row)
      else 0
    end as med_total_ccovd_max,
    
    case
      when lag(med_medicaid_covd,1) over (partition by id_apcd order by from_date) is null then med_medicaid_covd
      when lag(med_medicaid_covd,1) over (partition by id_apcd order by from_date) = 0 then med_medicaid_covd
      when lag(med_medicaid_covd,1) over (partition by id_apcd order by from_date) > 0 and med_medicaid_covd != 0
        then sum(med_medicaid_covd) over (partition by id_apcd order by from_date rows between unbounded preceding and current row)
      else 0
    end as med_medicaid_ccovd_max,
    
    case
      when lag(med_medicare_covd,1) over (partition by id_apcd order by from_date) is null then med_medicare_covd
      when lag(med_medicare_covd,1) over (partition by id_apcd order by from_date) = 0 then med_medicare_covd
      when lag(med_medicare_covd,1) over (partition by id_apcd order by from_date) > 0 and med_medicare_covd != 0
        then sum(med_medicare_covd) over (partition by id_apcd order by from_date rows between unbounded preceding and current row)
      else 0
    end as med_medicare_ccovd_max,
    
    case
      when lag(med_commercial_covd,1) over (partition by id_apcd order by from_date) is null then med_commercial_covd
      when lag(med_commercial_covd,1) over (partition by id_apcd order by from_date) = 0 then med_commercial_covd
      when lag(med_commercial_covd,1) over (partition by id_apcd order by from_date) > 0 and med_commercial_covd != 0
        then sum(med_commercial_covd) over (partition by id_apcd order by from_date rows between unbounded preceding and current row)
      else 0
    end as med_commercial_ccovd_max,
    
    ---------
    --PHARMACY longest coverage period
    ---------
    case
      when lag(pharm_total_covd,1) over (partition by id_apcd order by from_date) is null then pharm_total_covd
      when lag(pharm_total_covd,1) over (partition by id_apcd order by from_date) = 0 then pharm_total_covd
      when lag(pharm_total_covd,1) over (partition by id_apcd order by from_date) > 0 and pharm_total_covd != 0
        then sum(pharm_total_covd) over (partition by id_apcd order by from_date rows between unbounded preceding and current row)
      else 0
    end as pharm_total_ccovd_max,
    
    case
      when lag(pharm_medicaid_covd,1) over (partition by id_apcd order by from_date) is null then pharm_medicaid_covd
      when lag(pharm_medicaid_covd,1) over (partition by id_apcd order by from_date) = 0 then pharm_medicaid_covd
      when lag(pharm_medicaid_covd,1) over (partition by id_apcd order by from_date) > 0 and pharm_medicaid_covd != 0
        then sum(pharm_medicaid_covd) over (partition by id_apcd order by from_date rows between unbounded preceding and current row)
      else 0
    end as pharm_medicaid_ccovd_max,
    
    case
      when lag(pharm_medicare_covd,1) over (partition by id_apcd order by from_date) is null then pharm_medicare_covd
      when lag(pharm_medicare_covd,1) over (partition by id_apcd order by from_date) = 0 then pharm_medicare_covd
      when lag(pharm_medicare_covd,1) over (partition by id_apcd order by from_date) > 0 and pharm_medicare_covd != 0
        then sum(pharm_medicare_covd) over (partition by id_apcd order by from_date rows between unbounded preceding and current row)
      else 0
    end as pharm_medicare_ccovd_max,
    
    case
      when lag(pharm_commercial_covd,1) over (partition by id_apcd order by from_date) is null then pharm_commercial_covd
      when lag(pharm_commercial_covd,1) over (partition by id_apcd order by from_date) = 0 then pharm_commercial_covd
      when lag(pharm_commercial_covd,1) over (partition by id_apcd order by from_date) > 0 and pharm_commercial_covd != 0
        then sum(pharm_commercial_covd) over (partition by id_apcd order by from_date rows between unbounded preceding and current row)
      else 0
    end as pharm_commercial_ccovd_max
    
    into #cov2
    from #cov1;
    
    
    --------------------------
    --STEP 4: Summarize coverage and RAC-based information to person level
    --------------------------
    if object_id('tempdb..#cov3') is not null drop table #cov3;
      ---------
      --MEDICAL variables
      ---------
      select id_apcd as id, sum(med_total_covd) as med_total_covd, 
        cast(sum((med_total_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as med_total_covper, 
        sum(dual_covd) as dual_covd,
        cast(sum((dual_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as dual_covper,
        case when sum(dual_covd) > 0 then 1 else 0 end as dual_flag,
    	sum(full_benefit_covd) as full_benefit_covd,
        cast(sum((full_benefit_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as full_benefit_covper,
        sum(med_medicaid_covd) as med_medicaid_covd, sum(med_medicare_covd) as med_medicare_covd, sum(med_commercial_covd) as med_commercial_covd,
        cast(sum((med_medicaid_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as med_medicaid_covper,
        cast(sum((med_medicare_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as med_medicare_covper,
        cast(sum((med_commercial_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as med_commercial_covper,
        max(med_total_ccovd_max) as med_total_ccovd_max, max(med_medicaid_ccovd_max) as med_medicaid_ccovd_max, max(med_medicare_ccovd_max) as med_medicare_ccovd_max, max(med_commercial_ccovd_max) as med_commercial_ccovd_max, 
      ---------
      --MEDICAL longest coverage gap
      ---------
        case
          when max(med_total_pregap) >= max(med_total_postgap) then max(med_total_pregap)
          else max(med_total_postgap)
        end as med_total_covgap_max,
        case
          when max(med_medicaid_pregap) >= max(med_medicaid_postgap) then max(med_medicaid_pregap)
          else max(med_medicaid_postgap)
        end as med_medicaid_covgap_max,
        case
          when max(med_medicare_pregap) >= max(med_medicare_postgap) then max(med_medicare_pregap)
          else max(med_medicare_postgap)
        end as med_medicare_covgap_max,
        case
          when max(med_commercial_pregap) >= max(med_commercial_postgap) then max(med_commercial_pregap)
          else max(med_commercial_postgap)
        end as med_commercial_covgap_max,
    
      ---------
      --PHARMACY variables
      ---------
        sum(pharm_total_covd) as pharm_total_covd, 
        cast(sum((pharm_total_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as pharm_total_covper,
        sum(pharm_medicaid_covd) as pharm_medicaid_covd, sum(pharm_medicare_covd) as pharm_medicare_covd, sum(pharm_commercial_covd) as pharm_commercial_covd,
        cast(sum((pharm_medicaid_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as pharm_medicaid_covper,
        cast(sum((pharm_medicare_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as pharm_medicare_covper,
        cast(sum((pharm_commercial_covd * 1.0)) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as pharm_commercial_covper,
        max(pharm_total_ccovd_max) as pharm_total_ccovd_max, max(pharm_medicaid_ccovd_max) as pharm_medicaid_ccovd_max, max(pharm_medicare_ccovd_max) as pharm_medicare_ccovd_max, max(pharm_commercial_ccovd_max) as pharm_commercial_ccovd_max,
      ---------
      --PHARMACY longest coverage gap
      ---------
        case
          when max(pharm_total_pregap) >= max(pharm_total_postgap) then max(pharm_total_pregap)
          else max(pharm_total_postgap)
        end as pharm_total_covgap_max,
        case
          when max(pharm_medicaid_pregap) >= max(pharm_medicaid_postgap) then max(pharm_medicaid_pregap)
          else max(pharm_medicaid_postgap)
        end as pharm_medicaid_covgap_max,
        case
          when max(pharm_medicare_pregap) >= max(pharm_medicare_postgap) then max(pharm_medicare_pregap)
          else max(pharm_medicare_postgap)
        end as pharm_medicare_covgap_max,
        case
          when max(pharm_commercial_pregap) >= max(pharm_commercial_postgap) then max(pharm_commercial_pregap)
          else max(pharm_commercial_postgap)
        end as pharm_commercial_covgap_max
    
      into #cov3
      from #cov2
      group by id_apcd;
    
    
    --------------------------
    --STEP 5: Summarize geographic information for member residence
    --------------------------
    if object_id('tempdb..#geo') is not null drop table #geo;
    ---------
    --Assign each member to a single ZIP code for requested date range
    ---------
    select c.id, c.geo_zip, d.geo_county, e.geo_ach
    into #geo
    from (
      select b.id, b.geo_zip, b.zip_dur, row_number() over (partition by b.id order by b.zip_dur desc, b.geo_zip) as zipr
    	from (
    		select a.id, a.geo_zip, sum(a.covd) + 1 as zip_dur
    		from (
      		select id_apcd as id, geo_zip,
        		case
        			/**if coverage period fully contains date range then person time is just date range */
        		  when from_date <= {from_date} and to_date >= {to_date} then
        		    datediff(day, {from_date}, {to_date}) + 1
        			/**if coverage period begins before date range start and ends within date range */
        			when from_date <= {from_date} and to_date < {to_date} and to_date >= {from_date} then 
        			 datediff(day, {from_date}, to_date) + 1
        			/**if coverage period begins within date range and ends after date range end */
        			when from_date > {from_date} and to_date >= {to_date} and from_date <= {to_date} then 
        			 datediff(day, from_date, {to_date}) + 1
        			/**if coverage period begins after date range start and ends before date range end */
        			when from_date > {from_date} and to_date < {to_date} then datediff(day, from_date, to_date) + 1
        			else null
        		end as covd
          from phclaims.final.apcd_elig_timevar
          where from_date <= {to_date} and to_date >= {from_date}
    			) as a
    			group by a.id, a.geo_zip
        ) as b
      ) as c
    left join (select zip_code, zip_group_desc as geo_county from phclaims.ref.apcd_zip_group where zip_group_type_desc = 'County') as d
    on c.geo_zip = d.zip_code
    left join (select zip_code, zip_group_desc as geo_ach from phclaims.ref.apcd_zip_group where left(zip_group_type_desc, 3) = 'Acc') as e
    on c.geo_zip = e.zip_code
    where c.zipr = 1;
    
    
    --------------------------
    --STEP 6: For each member's selected ACH, calculate duration (days) and percentage of time spent in ACH
    --------------------------
    if object_id('tempdb..#ach') is not null drop table #ach;
    ---------
    --Assign each member to a single ZIP code for requested date range
    ---------
    select c.id, c.geo_ach, sum(c.geo_ach_covd) as geo_ach_covd
    into #ach
    from (
    select a.id, a.geo_ach,
      case
        /**if coverage period fully contains date range then person time is just date range */
        when b.from_date <= {from_date} and b.to_date >= {to_date} then
          datediff(day, {from_date}, {to_date}) + 1
        /**if coverage period begins before date range start and ends within date range */
        when b.from_date <= {from_date} and b.to_date < {to_date} and b.to_date >= {from_date} then 
         datediff(day, {from_date}, b.to_date) + 1
        /**if coverage period begins within date range and ends after date range end */
        when b.from_date > {from_date} and b.to_date >= {to_date} and b.from_date <= {to_date} then 
         datediff(day, b.from_date, {to_date}) + 1
        /**if coverage period begins after date range start and ends before date range end */
        when b.from_date > {from_date} and b.to_date < {to_date} then datediff(day, b.from_date, b.to_date) + 1
        else null
      end as geo_ach_covd
    from (select id, geo_ach from #geo) as a
    inner join (select id_apcd, geo_ach, from_date, to_date from phclaims.final.apcd_elig_timevar) as b
    on a.id = b.id_apcd
    where b.from_date <= {to_date} and b.to_date >= {from_date} and a.geo_ach = b.geo_ach
    ) as c
    group by c.id, c.geo_ach;
     		
    
    --------------------------
    --STEP 7: Join coverage and geo, and pull in demographics
    --------------------------
    if object_id('tempdb..#merge1') is not null drop table #merge1;
    select a.id as id_apcd, 
      
      --DEMOGRAPHICS
      b.geo_zip, b.geo_county, b.geo_ach, c.geo_ach_covd, 
      cast((c.geo_ach_covd * 1.0) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as geo_ach_covper, d.age,
    	case
    			when d.age >= 0 and d.age < 5 then '0-4'
    			when d.age >= 5 and d.age < 12 then '5-11'
    			when d.age >= 12 and d.age < 18 then '12-17'
    			when d.age >= 18 and d.age < 25 then '18-24'
    			when d.age >= 25 and d.age < 45 then '25-44'
    			when d.age >= 45 and d.age < 65 then '45-64'
    			when d.age >= 65 or d.ninety_only = 1 then '65 and over'
    	end as age_grp7,
    	d.gender_me, d.gender_recent, d.gender_female, d.gender_male,
    	
      --COVERAGE STATS
      a.med_total_covd, a.med_total_covper, a.full_benefit_covd,
      cast((a.full_benefit_covd * 1.0) / ((datediff(day, {from_date}, {to_date}) + 1) * 1.0) * 100.0 as decimal(4,1)) as full_benefit_covper,
      a.dual_covd, a.dual_covper, a.dual_flag, a.med_medicaid_covd, a.med_medicare_covd, a.med_commercial_covd,
      a.med_medicaid_covper, a.med_medicare_covper, a.med_commercial_covper, a.med_total_ccovd_max, a.med_medicaid_ccovd_max, a.med_medicare_ccovd_max,
      a.med_commercial_ccovd_max, a.med_total_covgap_max, a.med_medicaid_covgap_max, a.med_medicare_covgap_max, a.med_commercial_covgap_max,
      a.pharm_total_covd, a.pharm_total_covper, a.pharm_medicaid_covd, a.pharm_medicare_covd, a.pharm_commercial_covd, a.pharm_medicaid_covper, 
      a.pharm_medicare_covper, a.pharm_commercial_covper, a.pharm_total_ccovd_max, a.pharm_medicaid_ccovd_max, a.pharm_medicare_ccovd_max,
      a.pharm_commercial_ccovd_max, a.pharm_total_covgap_max, a.pharm_medicaid_covgap_max, a.pharm_medicare_covgap_max, a.pharm_commercial_covgap_max
    
    into #merge1
    from #cov3 as a
    left join #geo as b
    on a.id = b.id
    left join #ach as c
    on a.id = c.id
    left join (
    select *, case
    	when (floor((datediff(day, dob, {to_date}) + 1) / 365.25) >= 90) or (ninety_only = 1) then 90
    	when floor((datediff(day, dob, {to_date}) + 1) / 365.25) >=0 then floor((datediff(day, dob, {to_date}) + 1) / 365.25)
    	when floor((datediff(day, dob, {to_date}) + 1) / 365.25) = -1 then 0
    end as age
    from phclaims.final.apcd_elig_demo
    ) as d
    on a.id = d.id_apcd;
    
    
    --------------------------
    --STEP 8: Create final coverage cohort variables and select into table shell
    --------------------------
    insert into PHClaims.stage.{`table_name_year`} with (tablock)
    select id_apcd, 
    
    --coverage cohorts, state-level
    --overall_wa marks members in WA state
    --must incorproate DSRIP comprehensive coverage criteria (which can only be fairly applied if member has RAC codes for full Medicaid eligibility period)
    --must incorproate dual exclusion and pseudo-TPL exclusion (based on commercial medical coverage)
    case when geo_county is not null then 1 else 0 end as geo_wa,
    case when (geo_county is not null and (med_medicaid_covd >= 1 or pharm_medicaid_covd >= 1)) then 1 else 0 end as overall_mcaid,
    case when (geo_county is not null and med_medicaid_covd >= 1) then 1 else 0 end as overall_mcaid_med,
    case when (geo_county is not null and pharm_medicaid_covd >= 1) then 1 else 0 end as overall_mcaid_pharm,
    case when geo_county is not null and full_benefit_covper >= 91.7 and dual_covper < 8.3 and med_commercial_covper < 8.3
    	then 1 else 0 end as performance_11_wa,
    case when geo_county is not null and full_benefit_covper >= 58.3 and dual_covper < 41.7 and med_commercial_covper < 41.7
    	then 1 else 0 end as performance_7_wa,
      
    --coverage cohorts, ach-level
    --same as state-level with addition of ACH-level attribution logic
    case when full_benefit_covper >= 91.7 and dual_covper < 8.3 and med_commercial_covper < 8.3 and geo_ach_covper >= 91.7
    	then 1 else 0 end as performance_11_ach,
    case when full_benefit_covper >= 58.3 and dual_covper < 41.7 and med_commercial_covper < 41.7 and geo_ach_covper >= 58.3
    	then 1 else 0 end as performance_7_ach,
    geo_zip, geo_county, geo_ach, geo_ach_covd, geo_ach_covper, age, age_grp7, gender_me, gender_recent, gender_female, gender_male, med_total_covd, med_total_covper, 
    full_benefit_covd, full_benefit_covper, dual_covd, dual_covper, dual_flag, med_medicaid_covd, med_medicare_covd, med_commercial_covd,
    med_medicaid_covper, med_medicare_covper, med_commercial_covper, med_total_ccovd_max, med_medicaid_ccovd_max, med_medicare_ccovd_max,
    med_commercial_ccovd_max, med_total_covgap_max, med_medicaid_covgap_max, med_medicare_covgap_max, med_commercial_covgap_max,
    pharm_total_covd, pharm_total_covper, pharm_medicaid_covd, pharm_medicare_covd, pharm_commercial_covd, pharm_medicaid_covper, 
    pharm_medicare_covper, pharm_commercial_covper, pharm_total_ccovd_max, pharm_medicaid_ccovd_max, pharm_medicare_ccovd_max,
    pharm_commercial_ccovd_max, pharm_total_covgap_max, pharm_medicaid_covgap_max, pharm_medicare_covgap_max, pharm_commercial_covgap_max,
    getdate() as last_run
    from #merge1;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.apcd_elig_plr_f <- function() {
}