#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_HEADER
# Eli Kern, PHSKC (APDE)
#
# 2019-10

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/apcd/master_apcd_analytic.R

#### Load script ####
load_stage.apcd_claim_header_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(db_claims, glue::glue_sql(
    "
    ------------------
    --STEP 1: Do all line-level transformations that don't require ICD-CM, procedure, or provider information
    --Exclude all denied and orphaned claim headers
    --Acute inpatient stay defined through Susan Hernandez's work and dialogue with OnPoint
    --Max of discharge dt grouped by claim header will take latest discharge date when >1 discharge dt
    --Run time: ~30-40 min
    -------------------
    if object_id('tempdb..#temp1') is not null drop table #temp1;
    select internal_member_id as id_apcd, 
    medical_claim_header_id as claim_header_id,
    min(case when product_code_id in (-1,-2) then null else product_code_id end) as product_code_id,
    min(first_service_dt) as first_service_date,
    max(last_service_dt) as last_service_date,
    min(first_paid_dt) as first_paid_date,
    max(last_paid_dt) as last_paid_date,
    min(case when claim_status_id in (-1,-2) then null else claim_status_id end) as claim_status_id,
    min(case when type_of_bill_code in (-1,-2) then null else type_of_bill_code end) as type_of_bill_code,
    
    --concatenate claim type variables
    cast(convert(varchar(100), max(claim_type_id))
    	+ '.' + convert(varchar(100), max(type_of_setting_id))
    	+ '.' + convert(varchar(100), min(case when place_of_setting_id in (-1,-2) then null else place_of_setting_id end))
    as varchar(100)) as claim_type_apcd_id,
    
    --ED performance temp flags (RDA measure)
    cast(max(case when emergency_room_flag = 'Y' then 1 else 0 end) as tinyint) as ed_perform_temp,
    
    --ED population health temp flags (Yale measure)
    max(case when place_of_service_code = '23' then 1 else 0 end) as ed_pos_temp,
    max(case when revenue_code like '045[01269]' or revenue_code = '0981' then 1 else 0 end) as ed_revenue_code_temp,
    
    --inpatient visit
    max(case when claim_type_id = '1' and type_of_setting_id = '1' and place_of_setting_id = '1'
    	and claim_status_id in (-1, -2, 1, 5, 2, 6) -- only include primary and secondary claims
    	and discharge_dt is not null
    then 1 else 0 end) as ipt_flag,
    max(discharge_dt) as discharge_date
    
    into #temp1
    from PHClaims.stage.apcd_medical_claim as a
    --exclude denined/orphaned claims
    left join PHClaims.ref.apcd_denied_orphaned_header as b
    on a.medical_claim_header_id = b.claim_header_id
    where b.denied_header_min = 0 and b.orphaned_header_min = 0
    --grouping statement for consolidation to person-header level
    group by a.internal_member_id, a.medical_claim_header_id;
    
    
    ------------------
    --STEP 2: Procedure code query for ED visits
    --Subset to relevant claims as last step to minimize temp table size
    --Run time: 15 min
    -------------------
    if object_id('tempdb..#ed_procedure_code') is not null drop table #ed_procedure_code;
    select x.medical_claim_header_id, x.ed_procedure_code_temp
    into #ed_procedure_code
    from (
      select a.medical_claim_header_id,
      	max(case when b.procedure_code like '9928[12345]' or b.procedure_code = '99291' then 1 else 0 end) as ed_procedure_code_temp
      from PHClaims.stage.apcd_medical_claim as a
      --procedure code table
      left join PHClaims.final.apcd_claim_procedure as b
      on a.medical_claim_header_id = b.claim_header_id
      --exclude denined/orphaned claims
      left join PHClaims.ref.apcd_denied_orphaned_header as c
      on a.medical_claim_header_id = c.claim_header_id
      where c.denied_header_min = 0 and c.orphaned_header_min = 0
      --cluster to claim header
      group by a.medical_claim_header_id
    ) as x
    where x.ed_procedure_code_temp = 1;
    
    
    ------------------
    --STEP 3: Primary care visit query
    --Run time: 7 min (failed after 1 hour before I separated out joins as inner joins)
    -------------------
    if object_id('tempdb..#pc_visit') is not null drop table #pc_visit;
    select x.medical_claim_header_id, x.pc_procedure_temp, x.pc_taxonomy_temp, x.pc_zcode_temp
    into #pc_visit
    from (
    select a.medical_claim_header_id,
    --primary care visit temp flags
    max(case when b.code is not null then 1 else 0 end) as pc_procedure_temp,
    max(case when c.code is not null then 1 else 0 end) as pc_zcode_temp,
    max(case when d.code is not null then 1 else 0 end) as pc_taxonomy_temp
    from PHClaims.stage.apcd_medical_claim as a
    
    --procedure codes
    left join (
    	select b1.claim_header_id, b2.code
    	--procedure code table
    	from PHClaims.final.apcd_claim_procedure as b1
    	--primary care-relevant procedure codes
    	inner join (select code from PHClaims.ref.pc_visit_oregon where code_system in ('cpt', 'hcpcs')) as b2
    	on b1.procedure_code = b2.code
    ) as b
    on a.medical_claim_header_id = b.claim_header_id
    
    --ICD-CM codes
    left join (
    	select c1.claim_header_id, c2.code
    	--ICD-CM table
    	from PHClaims.final.apcd_claim_icdcm_header as c1
    	--primary care-relevant ICD-10-CM codes
    	inner join (select code from PHClaims.ref.pc_visit_oregon where code_system = 'icd10cm') as c2
    	on (c1.icdcm_norm = c2.code) and (c1.icdcm_version = 10)
    ) as c
    on a.medical_claim_header_id = c.claim_header_id
    
    --provider taxonomies
    left join (
    	select d1.claim_header_id, d4.code
    	--rendering and attending providers
    	from (select * from PHClaims.final.apcd_claim_provider where provider_type in ('rendering', 'attending')) as d1
    	--NPIs for each provider
    	inner join PHClaims.ref.apcd_provider_npi as d2
    	on d1.provider_id_apcd = d2.provider_id_apcd
    	--taxonomy codes for rendering and attending providers
    	inner join PHClaims.ref.kc_provider_master as d3
    	on d2.npi = d3.npi
    	--primary care-relevant provider taxonomy codes
    	inner join (select code from PHClaims.ref.pc_visit_oregon where code_system = 'provider_taxonomy') as d4
    	on (d3.primary_taxonomy = d4.code) or (d3.secondary_taxonomy = d4.code)
    ) as d
    on a.medical_claim_header_id = d.claim_header_id
    
    --exclude denined/orphaned claims
    left join PHClaims.ref.apcd_denied_orphaned_header as e
    on a.medical_claim_header_id = e.claim_header_id
    where e.denied_header_min = 0 and e.orphaned_header_min = 0
    
    --cluster to claim header
    group by a.medical_claim_header_id
    ) as x
    where (x.pc_procedure_temp = 1 or x.pc_zcode_temp = 1) and x.pc_taxonomy_temp = 1;
    
    
    ------------------
    --STEP 4: Prepare header-level charge_amt separately as denied and orphaned amounts must be included in sum
    --Run time: 4 min
    -------------------
    if object_id('tempdb..#charge') is not null drop table #charge;
    select medical_claim_header_id, sum(charge_amt) as charge_amt
    into #charge
    from PHClaims.stage.apcd_medical_claim as a
    --exclude denined/orphaned claims
    left join PHClaims.ref.apcd_denied_orphaned_header as b
    on a.medical_claim_header_id = b.claim_header_id
    where b.denied_header_min = 0 and b.orphaned_header_min = 0
    group by a.medical_claim_header_id;
    
    
    ------------------
    --STEP 5: Extract primary diagnosis, take first ordered ICD-CM code when >1 primary per header
    --Run time: 20 min
    ------------------
    if object_id('tempdb..#icd1') is not null drop table #icd1;
    select claim_header_id,
    min(icdcm_norm) as primary_diagnosis,
    min(icdcm_version) as icdcm_version
    into #icd1
    from PHClaims.final.apcd_claim_icdcm_header
    where icdcm_number = '01'
    group by claim_header_id;
    
    
    ------------------
    --STEP 6: Prepare header-level concepts using analytic claim tables
    --Add in charge amounts and principal diagnosis
    --Run time: 55 min
    -------------------
    if object_id('tempdb..#temp2') is not null drop table #temp2;
    select distinct a.id_apcd, 
    a.claim_header_id,
    a.product_code_id,
    a.first_service_date,
    a.last_service_date,
    a.first_paid_date,
    a.last_paid_date,
    c.charge_amt,
    d.primary_diagnosis,
    d.icdcm_version,
    a.claim_status_id,
    a.claim_type_apcd_id,
    b.kc_clm_type_id as claim_type_id,
    a.type_of_bill_code,
    
    --ED performance (RDA measure)
    case when a.ed_perform_temp = 1 and b.kc_clm_type_id = 4 then 1 else 0 end as ed_perform,
    
    --ED population health (Yale measure)
    case when b.kc_clm_type_id = 5 and e.ed_procedure_code_temp = 1 and a.ed_pos_temp = 1 then 1 else 0 end as ed_yale_carrier,
    case when b.kc_clm_type_id = 4 and a.ed_revenue_code_temp = 1 then 1 else 0 end as ed_yale_opt,
    case when b.kc_clm_type_id = 1 and a.ed_revenue_code_temp = 1 then 1 else 0 end as ed_yale_ipt,
    
    --Inpatient visit
    ipt_flag as inpatient,
    discharge_date,
    
    --Primary care visit (Oregon)
    case when (f.pc_procedure_temp = 1 or f.pc_zcode_temp = 1) and f.pc_taxonomy_temp = 1
	    and a.claim_type_apcd_id not in ('1.1.1', '1.1.14', '1.1.2', '2.3.8', '2.3.2', '1.2.8') --exclude inpatient, swing bed, free-standing ambulatory
    	and a.claim_status_id in (-1, -2, 1, 5, 2, 6) -- only include primary and secondary claim headers
    	then 1 else 0
    end as pc_visit
    
    into #temp2
    from #temp1 as a
    left join (select * from PHClaims.ref.kc_claim_type_crosswalk where source_desc = 'apcd') as b
    on a.claim_type_apcd_id = b.source_clm_type_id
    left join #charge as c
    on a.claim_header_id = c.medical_claim_header_id
    left join #icd1 as d
    on a.claim_header_id = d.claim_header_id
    left join #ed_procedure_code as e
    on a.claim_header_id = e.medical_claim_header_id
    left join #pc_visit as f
    on a.claim_header_id = f.medical_claim_header_id;
    
    --drop other temp tables to make space
    if object_id('tempdb..#temp1') is not null drop table #temp1;
    if object_id('tempdb..#charge') is not null drop table #charge;
    if object_id('tempdb..#icd1') is not null drop table #icd1;
    if object_id('tempdb..#ed_procedure_code') is not null drop table #ed_procedure_code;
    if object_id('tempdb..#pc_visit') is not null drop table #pc_visit;
    
    
    ------------------
    --STEP 7: Assign unique ID to healthcare utilization concepts that are grouped by person, service date
    --Run time: 39 min
    -------------------
    if object_id('tempdb..#temp3') is not null drop table #temp3;
    select *,
    --primary care visits
    case when pc_visit = 0 then null
    else dense_rank() over
    	(order by case when pc_visit = 0 then 2 else 1 end, --sorts non-relevant claims to bottom
    	id_apcd, first_service_date)
    end as pc_visit_id,
    
    --inpatient stays
    case when inpatient = 0 then null
    else dense_rank() over
    	(order by case when inpatient = 0 then 2 else 1 end, --sorts non-relevant claims to bottom
    	id_apcd, discharge_date)
    end as inpatient_id,
    
    --ED performance (RDA measure)
    case when ed_perform = 0 then null
    else dense_rank() over
    	(order by case when ed_perform = 0 then 2 else 1 end, --sorts non-relevant claims to bottom
    	id_apcd, first_service_date)
    end as ed_perform_id
    into #temp3
    from #temp2;
    
    --drop other temp tables to make space
    if object_id('tempdb..#temp2') is not null drop table #temp2;
    
    
    ------------------
    --STEP 8: Conduct overlap and clustering for ED population health measure (Yale measure)
    --Adaptation of Philip's Medicaid code, which is adaptation of Eli's original code
    --Run time: 12 min
    -------------------

    -----
    --Union carrier, outpatient and inpatient ED visits
    -----
    --extract carrier ED visits and create left and right matching windows
    if object_id('tempdb..#ed_yale_1') is not null drop table #ed_yale_1;
    select id_apcd, claim_header_id, first_service_date, last_service_date, 'Carrier' as ed_type
    into #ed_yale_1
    from #temp3
    where ed_yale_carrier = 1
    
    union
    select id_apcd, claim_header_id, first_service_date, last_service_date, 'Outpatient' as ed_type
    from #temp3 where ed_yale_opt = 1
    
    union
    select id_apcd, claim_header_id, first_service_date, last_service_date, 'Inpatient' as ed_type
    from #temp3 where ed_yale_ipt = 1;
    
    -----
    --label duplicate/adjacent visits with a single [ed_pophealth_id]
    -----
    
    --Set date of service matching window
    declare @match_window int;
    set @match_window = 1;
    
    if object_id('tempdb..#ed_yale_final') is not null 
    drop table #ed_yale_final;
    WITH [increment_stays_by_person] AS
    (
    SELECT
     [id_apcd]
    ,[claim_header_id]
    -- If [prior_first_service_date] IS NULL, then it is the first chronological [first_service_date] for the person
    ,LAG([first_service_date]) OVER(PARTITION BY [id_apcd] ORDER BY [first_service_date], [last_service_date], [claim_header_id]) AS [prior_first_service_date]
    ,[first_service_date]
    ,[last_service_date]
    ,[ed_type]
    -- Number of days between consecutive rows
    ,DATEDIFF(DAY, LAG([first_service_date]) OVER(PARTITION BY [id_apcd] 
     ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) AS [date_diff]
    /*
    Create a chronological (0, 1) indicator column.
    If 0, it is the first ED visit for the person OR the ED visit appears to be a duplicate
    (overlapping service dates) of the prior visit.
    If 1, the prior ED visit appears to be distinct from the following stay.
    This indicator column will be summed to create an episode_id.
    */
    ,CASE WHEN ROW_NUMBER() OVER(PARTITION BY [id_apcd] 
          ORDER BY [first_service_date], [last_service_date], [claim_header_id]) = 1 THEN 0
          WHEN DATEDIFF(DAY, LAG([first_service_date]) OVER(PARTITION BY [id_apcd]
    	  ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) <= @match_window THEN 0
    	  WHEN DATEDIFF(DAY, LAG(first_service_date) OVER(PARTITION BY [id_apcd]
    	  ORDER BY [first_service_date], [last_service_date], [claim_header_id]), [first_service_date]) > @match_window THEN 1
     END AS [increment]
    FROM #ed_yale_1
    --ORDER BY [id_apcd], [first_service_date], [last_service_date], [claim_header_id]
    ),
    
    /*
    Sum [increment] column (Cumulative Sum) within person to create an stay_id that
    combines duplicate/overlapping ED visits.
    */
    [create_within_person_stay_id] AS
    (
    SELECT
     id_apcd
    ,[claim_header_id]
    ,[prior_first_service_date]
    ,[first_service_date]
    ,[last_service_date]
    ,[ed_type]
    ,[date_diff]
    ,[increment]
    ,SUM([increment]) OVER(PARTITION BY [id_apcd] ORDER BY [first_service_date], [last_service_date], [claim_header_id] ROWS UNBOUNDED PRECEDING) + 1 AS [within_person_stay_id]
    FROM [increment_stays_by_person]
    --ORDER BY [id_apcd], [first_service_date], [last_service_date], [claim_header_id]
    )
    
    SELECT
     id_apcd
    ,[claim_header_id]
    ,[prior_first_service_date]
    ,[first_service_date]
    ,[last_service_date]
    ,[ed_type]
    ,[date_diff]
    ,[increment]
    ,[within_person_stay_id]
    ,DENSE_RANK() OVER(ORDER BY [id_apcd], [within_person_stay_id]) AS [ed_pophealth_id]
    
    ,FIRST_VALUE([first_service_date]) OVER(PARTITION BY [id_apcd], [within_person_stay_id] 
     ORDER BY [id_apcd], [within_person_stay_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_first_service_date]
    ,LAST_VALUE([last_service_date]) OVER(PARTITION BY [id_apcd], [within_person_stay_id] 
     ORDER BY [id_apcd], [within_person_stay_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS [episode_last_service_date]
    
    INTO #ed_yale_final
    FROM [create_within_person_stay_id]
    ORDER BY id_apcd, [first_service_date], [last_service_date], [claim_header_id];
    
    --drop other temp tables to make space
    if object_id('tempdb..#ed_yale_1') is not null drop table #ed_yale_1;
    
    
    ------------------
    --STEP 9: Join back Yale table with header table on claim header ID
    --Run time: 19 min
    -------------------
    insert into PHClaims.stage.apcd_claim_header with (tablock)
    select distinct
    a.id_apcd,
    a.claim_header_id,
    a.product_code_id,
    a.first_service_date,
    a.last_service_date,
    a.first_paid_date,
    a.last_paid_date,
    a.charge_amt,
    a.primary_diagnosis,
    a.icdcm_version,
    a.claim_status_id,
    a.claim_type_apcd_id,
    a.claim_type_id,
    a.type_of_bill_code,
    a.ed_perform_id,
    b.ed_pophealth_id,
    a.inpatient_id,
    a.discharge_date,
    a.pc_visit_id,
    getdate() as last_run
    from #temp3 as a
    left join #ed_yale_final as b
    on a.claim_header_id = b.claim_header_id;",
    .con = db_claims))
}

#### Table-level QA script ####
qa_stage.apcd_claim_header_f <- function() {
  
  #confirm that claim header is distinct
  res1 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_header' as 'table', '# of non-distinct headers, expect 0' as qa_type,
    count(a.claim_header_id) as qa1, qa2 = null
    from (
      select claim_header_id, count(*) as header_cnt
      from PHClaims.stage.apcd_claim_header
      group by claim_header_id
    ) as a
    where a.header_cnt > 1;",
    .con = db_claims))
  
  #compare member and claim header counts wth raw data
  res2 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_header' as 'table', 'qa1 = distinct IDs, qa2 = distinct headers' as qa_type,
    count(distinct id_apcd) as qa1, count(distinct claim_header_id) as qa2
    from PHClaims.stage.apcd_claim_header;",
    .con = db_claims))
  
  res3 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_medical_claim' as 'table', 'qa1 = distinct IDs, qa2 = distinct headers' as qa_type,
    count(distinct internal_member_id) as qa1, count(distinct medical_claim_header_id) as qa2
    from PHClaims.stage.apcd_medical_claim as a
    --exclude denined/orphaned claims
    left join PHClaims.ref.apcd_denied_orphaned_header as b
    on a.medical_claim_header_id = b.claim_header_id
    where b.denied_header_min = 0 and b.orphaned_header_min = 0;",
    .con = db_claims))
  
  #all members should be in elig_demo table
  res4 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_header' as 'table', '# of members not in elig_demo, expect 0' as qa_type,
    count(a.id_apcd) as qa1, qa2 = null
    from PHClaims.stage.apcd_claim_header as a
    left join PHClaims.final.apcd_elig_demo as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = db_claims))
  
  #all members should be in elig_timevar table
  res5 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_header' as 'table', '# of members not in elig_timevar, expect 0' as qa_type,
    count(a.id_apcd) as qa1, qa2 = null
    from PHClaims.stage.apcd_claim_header as a
    left join PHClaims.final.apcd_elig_timevar as b
    on a.id_apcd = b.id_apcd
    where b.id_apcd is null;",
    .con = db_claims))
  
  #count unmatched claim types
  res6 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_header' as 'table', '# of claims with unmatched claim type, expect 0' as qa_type,
    count(*) as qa1, qa2 = null
    from PHClaims.stage.apcd_claim_header
    where claim_type_id is null or claim_type_apcd_id is null;",
    .con = db_claims))
  
  #verify that all inpatient stays have discharge date
  res7 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_header' as 'table', '# of ipt stays with no discharge date, expect 0' as qa_type,
    count(*) as qa1, qa2 = null
    from PHClaims.stage.apcd_claim_header
    where inpatient_id is not null and discharge_date is null;",
    .con = db_claims))
  
  #verify that no ed_pophealth_id value is used for more than one person
  res8 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_header' as 'table', '# of ed_pophealth_id values used for >1 person, expect 0' as qa_type,
    count(a.ed_pophealth_id) as qa1, qa2 = null
    from (
      select ed_pophealth_id, count(distinct id_apcd) as id_dcount
      from PHClaims.stage.apcd_claim_header
      group by ed_pophealth_id
    ) as a
    where a.id_dcount > 1;",
    .con = db_claims))
  
  #verify that ed_pophealth_id does not skip any values
  res9 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_header' as 'table', 'qa1 = distinct ed_pophealth_id, qa2 = max - min + 1' as qa_type,
    count(distinct ed_pophealth_id) as qa1, cast(max(ed_pophealth_id) - min(ed_pophealth_id) + 1 as int) as qa2
    from PHClaims.stage.apcd_claim_header;",
    .con = db_claims))
  
  #verify that there are no rows with ed_perform_id without ed_pophealth_id
  res10 <- dbGetQuery(conn = db_claims, glue_sql(
    "select 'stage.apcd_claim_header' as 'table', '# of ed_perform rows with no ed_pophealth, expect 0' as qa_type,
    count(*) as qa1, qa2 = null
    from PHClaims.stage.apcd_claim_header
    where ed_perform_id is not null and ed_pophealth_id is null;",
    .con = db_claims))
  
  #verify that 1-day overlap window was implemented correctly with ed_pophealth_id
  res11 <- dbGetQuery(conn = db_claims, glue_sql(
    "with cte as
    (
    select * 
    ,lag(ed_pophealth_id) over(partition by id_apcd, ed_pophealth_id order by first_service_date) as lag_ed_pophealth_id
    ,lag(first_service_date) over(partition by id_apcd, ed_pophealth_id order by first_service_date) as lag_first_service_date
    from PHClaims.stage.apcd_claim_header
    where [ed_pophealth_id] is not null
    )
    select 'stage.apcd_claim_header' as 'table', '# of ed_pophealth visits where the overlap date is greater than 1 day, expect 0' as 'qa_type',
      count(*) as qa1, qa2 = null
    from PHClaims.stage.apcd_claim_header
    where [ed_pophealth_id] in (select ed_pophealth_id from cte where abs(datediff(day, lag_first_service_date, first_service_date)) > 1);",
    .con = db_claims))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}