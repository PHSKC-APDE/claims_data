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
    --Exclude all denied and orphaned claim lines
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
    from PHClaims.stage.apcd_medical_claim
    --exclusions
    where denied_claim_flag = 'N' and orphaned_adjustment_flag = 'N'
    --internal_member_id in (11059447694, 12761029412, 11268493312, 11061932071, 11268509776, 11277972181, 11307944287)
    --grouping statement for consolidation to person-header level
    group by internal_member_id, medical_claim_header_id;
    
    
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
    --exclusions
    where a.denied_claim_flag = 'N' and a.orphaned_adjustment_flag = 'N'
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
    
    --exclusions
    where a.denied_claim_flag = 'N' and a.orphaned_adjustment_flag = 'N'
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
    from PHClaims.stage.apcd_medical_claim
    --where internal_member_id in (11059447694, 12761029412, 11268493312, 11061932071, 11268509776, 11277972181, 11307944287)
    group by medical_claim_header_id;
    
    
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
    --and internal_member_id in (11059447694, 12761029412, 11268493312, 11061932071, 11268509776, 11277972181, 11307944287);
    
    
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
    --Run time: 12 min
    -------------------
    
    --Set date of service matching window
    declare @match_window int;
    set @match_window = 1;
    
    -----
    --Overlap between Carrier and outpatient
    -----
    --extract carrier ED visits and create left and right matching windows
    if object_id('tempdb..#ed_yale_1') is not null drop table #ed_yale_1;
    select id_apcd, claim_header_id,
    	first_service_date, dateadd(day, -1*@match_window, first_service_date) as window_left, 
    	dateadd(day, @match_window, first_service_date) as window_right,
    ed_yale_carrier
    into #ed_yale_1
    from #temp3
    where ed_yale_carrier = 1;
    
    --full join with outpatient ED visits by ID
    if object_id('tempdb..#ed_yale_2') is not null drop table #ed_yale_2;
    select a.*, b.id_apcd as ed_opt_id_apcd, b.claim_header_id as ed_opt_claim_header_id, b.first_service_date as ed_opt_date, ed_yale_opt
    into #ed_yale_2
    from #ed_yale_1 a
    full join (select id_apcd, claim_header_id, first_service_date, ed_yale_opt from #temp3 where ed_yale_opt = 1) as b
    on a.id_apcd = b.id_apcd;
    
    --flag outpatient ED visits that are within match window, aggregate to outpatient ED visit date
    if object_id('tempdb..#ed_yale_3') is not null drop table #ed_yale_3;
    select a.ed_opt_id_apcd as id_apcd, a.ed_opt_claim_header_id, a.ed_opt_date, max(a.ed_yale_opt) as ed_yale_opt,
    	max(a.ed_yale_dup) as ed_yale_dup
    into #ed_yale_3
    from (
    select *,
    case
    	when ed_opt_date < window_left or ed_opt_date > window_right then 0
    	else 1
    end as ed_yale_dup
    from #ed_yale_2
    ) as a
    where ed_opt_date is not null
    group by a.ed_opt_id_apcd, a.ed_opt_claim_header_id, a.ed_opt_date;
    
    ------
    --Overlap between Carrier/outpatient and inpatient
    ------
    --prepare new carrier + outpatient table for joining to inpatient
    if object_id('tempdb..#ed_yale_4') is not null drop table #ed_yale_4;
    select id_apcd, claim_header_id, first_service_date, window_left, window_right
    into #ed_yale_4
    from #ed_yale_1
    union
    select id_apcd, ed_opt_claim_header_id as claim_header_id, ed_opt_date as first_service_date,
    	dateadd(day, -1*@match_window, ed_opt_date) as window_left, dateadd(day, @match_window, ed_opt_date) as window_right
    from #ed_yale_3
    where ed_yale_dup = 0;
    
    --full join with inpatient ED visits by ID
    if object_id('tempdb..#ed_yale_5') is not null drop table #ed_yale_5;
    select a.*, b.id_apcd as ed_ipt_id_apcd, b.claim_header_id as ed_ipt_claim_header_id,
    	b.first_service_date as ed_ipt_date, ed_yale_ipt
    into #ed_yale_5
    from #ed_yale_4 a
    full join (select id_apcd, claim_header_id, first_service_date, ed_yale_ipt from #temp3 where ed_yale_ipt = 1) as b
    on a.id_apcd = b.id_apcd;
    
    --flag inpatient ED visits that are within match window, aggregate to outpatient ED visit date
    if object_id('tempdb..#ed_yale_6') is not null drop table #ed_yale_6;
    select a.ed_ipt_id_apcd as id_apcd, a.ed_ipt_claim_header_id, a.ed_ipt_date,
    	max(a.ed_yale_ipt) as ed_yale_ipt, max(a.ed_yale_dup) as ed_yale_dup
    into #ed_yale_6
    from (
    select *,
    case
    	when ed_ipt_date < window_left or ed_ipt_date > window_right then 0
    	else 1
    end as ed_yale_dup
    from #ed_yale_5
    ) as a
    where ed_ipt_date is not null
    group by a.ed_ipt_id_apcd, a.ed_ipt_claim_header_id, a.ed_ipt_date;
    
    --union all ED visits into final table
    if object_id('tempdb..#ed_yale_7') is not null drop table #ed_yale_7;
    select id_apcd, claim_header_id, first_service_date, 'carrier' as ed_type, 0 as ed_yale_dup
    into #ed_yale_7
    from #ed_yale_1
    union select id_apcd, ed_opt_claim_header_id as claim_header_id, ed_opt_date as first_service_date, 'opt' as ed_type, ed_yale_dup
    from #ed_yale_3
    union select id_apcd, ed_ipt_claim_header_id as claim_header_id, ed_ipt_date as first_service_date, 'ipt' as ed_type, ed_yale_dup
    from #ed_yale_6;
    
    --assign ED ID to non-duplicate ED visits
    if object_id('tempdb..#ed_yale_8') is not null drop table #ed_yale_8;
    select *,
    case 
    	when ed_yale_dup = 1 then null
    	else dense_rank() over
    		(order by case when ed_yale_dup = 1 then 2 else 1 end, --sorts non-relevant claims to bottom
    		id_apcd, first_service_date)
    end as ed_yale_id
    into #ed_yale_8
    from #ed_yale_7;
    
    --spread ED IDs to duplicate ED visits using count window aggregate function
    if object_id('tempdb..#ed_yale_final') is not null drop table #ed_yale_final;
    select a.id_apcd, a.claim_header_id, a.first_service_date, a.ed_type, a.ed_yale_dup,
    	max(a.ed_yale_id) over (partition by a.id_apcd, a.c) as ed_pophealth_id
    into #ed_yale_final
    from (
    	select *,
    	count(ed_yale_id) over (order by id_apcd, first_service_date, ed_yale_dup, ed_type) as c
    	from #ed_yale_8
    ) as a;
    
    --drop other temp tables to make space
    if object_id('tempdb..#ed_yale_1') is not null drop table #ed_yale_1;
    if object_id('tempdb..#ed_yale_2') is not null drop table #ed_yale_2;
    if object_id('tempdb..#ed_yale_3') is not null drop table #ed_yale_3;
    if object_id('tempdb..#ed_yale_4') is not null drop table #ed_yale_4;
    if object_id('tempdb..#ed_yale_5') is not null drop table #ed_yale_5;
    if object_id('tempdb..#ed_yale_6') is not null drop table #ed_yale_6;
    if object_id('tempdb..#ed_yale_7') is not null drop table #ed_yale_7;
    if object_id('tempdb..#ed_yale_8') is not null drop table #ed_yale_8;
    
    
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
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}