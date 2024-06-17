#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_claim_moud
# Eli Kern, PHSKC (APDE)
#
# 2024-06
#
# 2024-06-17 Eli update: use supplied days supply column for all MOUD types, no estimation needed for methadone

### Run from 02_master_mcare_claims_analytic.R script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/02_master_mcare_claims_analytic.R

#### Load script ####
load_stage.mcare_claim_moud_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(inthealth, glue::glue_sql(
    "--Create a table that identifies and quantifies MOUD in Medicare beneficiaries
    --Methods adapted from Busch et al. 2023 paper and RDA value sets
    --Eli Kern, PHSKC, APDE
    --Nov 2023
    --Medicare data stored on HHSAW
        
    --Updated 4/2/2023 JL:
    --For pharmacy data, using rx_days_supply instead of rx_quantity
    --If both NDCs and HCPCS codes were for the same MOUD for same method of 
    --		administration on the same day, then only NDC info used
        
    --Updated 4/19/2023 JL:
    --Per David's feedback:
    --	Confirming OUD diagnosis for naltrexone (only for HCPCS) because oral and injectable are also used for AUD
    --		(oral naltrexone more likely to be used for AUD than and injectable approved for both AUD and OUD)
    --		Not requirement for NDCs because pharmacy claims aren't required to have diagnosis codes while medical claims do
    --	Remove morphine-naltrexone from NDCs because only prescribed for pain and not OUD
    --Per Brad's feedback:
    --	Add admin method to final output 
        ---------------------------------------
        
    ---------------------------
    --STEP 1: Flag methadone episodes using HCPCS codes from 1/1/2016 onward
    --Codes come from Busch et al paper, RDA value sets, and MAT/billing guide review
    --Updated 4/2/2023 JL: added method of administration info
    ---------------------------
    if object_id('tempdb..#mcare_moud_proc_1') is not null drop table #mcare_moud_proc_1;
    select distinct id_mcare, claim_header_id, first_service_date, last_service_date, procedure_code,
        
    case when procedure_code in ('H0033') then 1 else 0 end as moud_proc_flag_tbd,
    case when procedure_code in ('H0020', 'S0109', 'G2078', 'G2067') then 1 else 0 end as meth_proc_flag,
    case when procedure_code in ('J0571', 'J0572', 'J0573', 'J0574', 'J0575', 'G2068', 'G2079', 'Q9991', 'Q9992', 'G2069', 'G2070', 'G2072', 'J0570')
        then 1 else 0 end as bup_proc_flag,
    case when procedure_code in ('96372', '11981', '11983', 'G0516', 'G0518') then 1 else 0 end as bup_proc_flag_tbd,
    case when procedure_code in ('G2073', 'J2315') then 1 else 0 end as nal_proc_flag,
    case when procedure_code in ('G2074', 'G2075', 'G2076', 'G2077', 'G2080', 'G2086', 'G2087', 'G2088', 'G2213') then 1 else 0 end as unspec_proc_flag,
        
    case
        when procedure_code in ('H0033', 'H0020', 'S0109', 'J0571', 'J0572', 'J0573', 'J0574', 'J0575', '96372', 'J2315') then 1
        when procedure_code in ('G2078', 'G2067', 'G2068', 'G2079', 'G2073') then 7
        when procedure_code in ('Q9991', 'Q9992', 'G2069') then 30
        when procedure_code in ('G2070', 'G2072', 'J0570', '11981', '11983', 'G0516', 'G0518') then 180
        else 0
    end as moud_days_supply,
        
    case 
        when procedure_code in ('H0033', 'H0020', 'S0109', 'G2078', 'G2067', 'J0571', 'J0572', 'J0573', 'J0574', 'J0575', 'G2068', 'G2079', 'G2073', '96372') then 'oral'
        when procedure_code in ('Q9991', 'Q9992', 'G2069', 'G2070', 'G2072', 'J0570', '11981', '11983', 'G0516', 'G0518', 'J2315') then 'injection/implant'
        else null
    end as admin_method
        
    into #mcare_moud_proc_1
    from stg_claims.final_mcare_claim_procedure 
    where last_service_date >= '2016-01-01'
        and procedure_code in (
        'H0033',
        'H0020', 'S0109', 'G2078', 'G2067',
        'J0571', 'J0572', 'J0573', 'J0574', 'J0575', 'G2068', 'G2079', 'Q9991', 'Q9992', 'G2069', 'G2070', 'G2072', 'J0570',
        '96372', '11981', '11983', 'G0516', 'G0518',
        'G2073', 'J2315',
        'G2074', 'G2075', 'G2076', 'G2077', 'G2080', 'G2086', 'G2087', 'G2088', 'G2213');
        
        
        
    ---------------------------
    --STEP 2: Bring in primary diagnosis information
    --Method from Busch et al. paper
    ---------------------------
    if object_id('tempdb..#mcare_moud_proc_2') is not null drop table #mcare_moud_proc_2;
    select distinct a.*,
    max(case when c.sub_group_condition = 'sud_opioid' then 1 else 0 end) over(partition by a.claim_header_id) as oud_dx1_flag
    into #mcare_moud_proc_2
    from #mcare_moud_proc_1 as a
    left join stg_claims.final_mcare_claim_header as b
    on a.claim_header_id = b.claim_header_id
    left join (
        select distinct code, icdcm_version, sub_group_condition
        from stg_claims.ref_rda_value_sets_apde where sub_group_condition = 'sud_opioid' and data_source_type = 'diagnosis'
    ) as c
    on (b.primary_diagnosis = c.code) and (b.icdcm_version = c.icdcm_version);
        
        
    ---------------------------
    --STEP 3: Subset methadone HCPCS codes by considering primary diagnosis
    --Method from Busch et al paper, Appendix Table 1
    --Consolidate values for bup_proc_flag
    --Updated 4/19/2024 JL: added naltrexone codes to also require diagnosis of OUD
    ---------------------------
    if object_id('tempdb..#mcare_moud_proc_3') is not null drop table #mcare_moud_proc_3;
    select distinct
        id_mcare,
        claim_header_id,
        first_service_date,
        last_service_date,
        procedure_code,
        moud_proc_flag_tbd,
        meth_proc_flag,
        case
        	when bup_proc_flag = 1 then 1
        	when bup_proc_flag_tbd = 1 then 1
        	else 0
        end as bup_proc_flag,
        nal_proc_flag,
        unspec_proc_flag,
        admin_method,
        moud_days_supply,
        oud_dx1_flag
    into #mcare_moud_proc_3
    from #mcare_moud_proc_2
    where 
        --codes not requiring primary diagnosis of OUD
        procedure_code in (
        'H0020', 'S0109', 'G2078', 'G2067',
        'J0571', 'J0572', 'J0573', 'J0574', 'J0575', 'G2068', 'G2079', 'Q9991', 'Q9992', 'G2069', 'G2070', 'G2072', 'J0570',
        'G2073', 'J2315',
        'G2074', 'G2075', 'G2076', 'G2077', 'G2080', 'G2086', 'G2087', 'G2088', 'G2213')
        --codes requiring primary diagnosis of OUD
        or (procedure_code in ('H0033') and oud_dx1_flag = 1)
        or (procedure_code in ('96372', '11981', '11983', 'G0516', 'G0518') and oud_dx1_flag = 1)
        or (procedure_code in ('G2073', 'J2315') and oud_dx1_flag = 1);
        
        
    ---------------------------
    --STEP 4: Pull pharmacy fill data for bup and naltrexone prescriptions
    --Codes come from RDA value sets
    --Updated 4/2/2024 JL: linked to ref.ndc_codes to get method of administration info, using rx_days_supply
    ---------------------------
    if object_id('tempdb..#mcare_moud_pharm_1') is not null drop table #mcare_moud_pharm_1;
    select distinct a.id_mcare, a.claim_header_id, a.last_service_date as first_service_date, a.last_service_date as last_service_date, a.ndc,
    case when b.sub_group_pharmacy in ('pharm_buprenorphine', 'pharm_buprenorphine_naloxone') then 1 else 0 end as bup_rx_flag,
    case when b.sub_group_pharmacy = 'pharm_naltrexone_rx' then 1 else 0 end as nal_rx_flag,
    case 
        when c.DOSAGEFORMNAME like 'FILM%' or c.DOSAGEFORMNAME like 'TABLET%' then 'oral'
        when c.DOSAGEFORMNAME like 'KIT%' or c.DOSAGEFORMNAME like 'SOLUTION%' then 'injection/implant'
        else null
    end as admin_method,
    a.days_suply_num as moud_days_supply
    into #mcare_moud_pharm_1
    from stg_claims.final_mcare_claim_pharm as a
    inner join (
        select distinct code, sub_group_pharmacy
        from stg_claims.ref_rda_value_sets_apde
        where sub_group_pharmacy in ('pharm_buprenorphine', 'pharm_buprenorphine_naloxone', 'pharm_naltrexone_rx')
    ) as b
    on a.ndc = b.code
    left join (
        select ndc, DOSAGEFORMNAME
        from stg_claims.ref_ndc_codes) as c
    on b.code = c.ndc
    where a.last_service_date >= '2016-01-01';
    
    if object_id('tempdb..#mcare_moud_pharm_2') is not null drop table #mcare_moud_pharm_2;
    select id_mcare, claim_header_id, first_service_date, last_service_date, ndc, bup_rx_flag, nal_rx_flag, 
    case when ndc = '00093572156' or ndc = '00093572056' or ndc = '49452483501'  or ndc = '00378876616' then 'oral' 
        else admin_method 
    end as admin_method, moud_days_supply
    into #mcare_moud_pharm_2
    from #mcare_moud_pharm_1;
        
    /*--Below code is to see if there are any new NDCs that are missing in the ref.ndc_codes table
    select *
    from #mcare_moud_pharm_2
    where admin_method is null
    */
        
    --QA/Explore outliers for dispensed supply (days given assumption of 1-day supply per pill)
    --select top 100 * from #mcare_moud_pharm_1
    --where moud_days_supply >180
    --order by moud_days_supply;
        
    --select moud_days_supply, count(*) as row_count
    --from #mcare_moud_pharm_1
    --group by moud_days_supply
    --order by row_count desc;
        
        
    ---------------------------
    --STEP 5: Union procedure code and pharmacy fill data
    ---------------------------
    if object_id('tempdb..#mcare_moud_union_1') is not null drop table #mcare_moud_union_1;
    select 
        id_mcare,
        claim_header_id,
        first_service_date,
        last_service_date,
        procedure_code,
        moud_proc_flag_tbd,
        meth_proc_flag,
        bup_proc_flag,
        nal_proc_flag,
        unspec_proc_flag,
        admin_method,
        null as ndc,
        null as bup_rx_flag,
        null as nal_rx_flag,
        cast(moud_days_supply as numeric(8,1)) as moud_days_supply,
        oud_dx1_flag
    into #mcare_moud_union_1
    from #mcare_moud_proc_3
    union select
        id_mcare,
        claim_header_id,
        first_service_date,
        last_service_date,
        null as procedure_code,
        null as moud_proc_flag_tbd,
        null as meth_proc_flag,
        null as bup_proc_flag,
        null as nal_proc_flag,
        null as unspec_proc_flag,
        admin_method,
        ndc,
        bup_rx_flag,
        nal_rx_flag,
        moud_days_supply,
        null as oud_dx1_flag
    from #mcare_moud_pharm_2;
    
    --One-time QA to understand methadone billing in Medicare (which began in Jan 2020) 
    --select year(last_service_date) as service_year, procedure_code, count(*) as row_count
    --from #mcare_moud_union_1
    --where procedure_code is not null
    --group by year(last_service_date), procedure_code
    --order by service_year, procedure_code;
    
    --select procedure_code, count(*) as row_count
    --from #mcare_moud_union_1
    --where procedure_code is not null
    --group by procedure_code
    --order by row_count desc;
    
    --select distinct procedure_code
    --from #mcare_moud_union_1
    --where meth_proc_flag = 1;
        
    ---------------------------
    --STEP 6: Assign MOUD type to procedure code H0033 (could be methadone or bup) depending on monthly sums of either med
    --Method from Busch et al. paper
    ---------------------------
        
    --Create table to hold person IDs and ever flags for H0033
    if object_id('tempdb..#temp1') is not null drop table #temp1;
    select distinct id_mcare,
    max(case when procedure_code = 'H0033' then 1 else 0 end) over(partition by id_mcare) as proc_h0033_flag
    into #temp1
    from #mcare_moud_union_1;
        
    --Join back to person-date-level data and sum MOUD codes (procedure and rx fills) by month
    if object_id('tempdb..#temp2') is not null drop table #temp2;
    select c.year_month, b.id_mcare,
    sum(isnull(meth_proc_flag,0)) as meth_proc_month_sum,
    sum(isnull(bup_proc_flag,0)) as bup_proc_month_sum,
    sum(isnull(nal_proc_flag,0)) as nal_proc_month_sum,
    sum(isnull(bup_rx_flag,0)) as bup_rx_month_sum,
    sum(isnull(nal_rx_flag,0)) as nal_rx_month_sum
    into #temp2
    from (select distinct id_mcare from #temp1 where proc_h0033_flag = 1) as a
    inner join #mcare_moud_union_1 as b
    on a.id_mcare = b.id_mcare
    left join (select distinct [date], year_month from stg_claims.ref_date) as c
    on b.last_service_date = c.[date]
    group by c.year_month, b.id_mcare;
        
    --Join again to person-date-level data and use monthly sums to allocate H0033 encounters to either methadone or bup
    if object_id('tempdb..#temp3') is not null drop table #temp3;
    select a.id_mcare,
    a.claim_header_id,
    a.first_service_date,
    a.last_service_date,
    a.procedure_code,
    case
        when a.procedure_code = 'H0033' and c.meth_proc_month_sum = 0 and c.bup_proc_month_sum = 0 and c.bup_rx_month_sum = 0 then 1
        when a.procedure_code = 'H0033' and c.meth_proc_month_sum = 0 and c.bup_proc_month_sum = 0 and c.bup_rx_month_sum > 0 then 0
        when a.procedure_code = 'H0033' and	c.meth_proc_month_sum >= c.bup_proc_month_sum and c.meth_proc_month_sum != 0 then 1
        when a.procedure_code = 'H0033' and c.meth_proc_month_sum < c.bup_proc_month_sum then 0
        else a.meth_proc_flag
    end as meth_proc_flag,
    case
        when a.procedure_code = 'H0033' and c.meth_proc_month_sum = 0 and c.bup_proc_month_sum = 0 and c.bup_rx_month_sum = 0 then 0
        when a.procedure_code = 'H0033' and c.meth_proc_month_sum = 0 and c.bup_proc_month_sum = 0 and c.bup_rx_month_sum > 0 then 1
        when a.procedure_code = 'H0033' and	c.meth_proc_month_sum >= c.bup_proc_month_sum and c.meth_proc_month_sum != 0 then 0
        when a.procedure_code = 'H0033' and c.meth_proc_month_sum < c.bup_proc_month_sum then 1
        else a.bup_proc_flag
    end as bup_proc_flag,
    a.nal_proc_flag,
    a.unspec_proc_flag,
    a.admin_method,
    a.ndc,
    a.bup_rx_flag,
    a.nal_rx_flag,
    a.moud_days_supply,
    a.oud_dx1_flag,
    c.year_month,
    c.meth_proc_month_sum,
    c.bup_proc_month_sum,
    c.nal_proc_month_sum,
    c.bup_rx_month_sum,
    c.nal_rx_month_sum
    into #temp3
    from #mcare_moud_union_1 as a
    left join stg_claims.ref_date as b
    on a.last_service_date = b.[date]
    left join #temp2 as c
    on (a.id_mcare = c.id_mcare) and (b.year_month = c.year_month);
        
    --QA
    --select top 10 * from #temp3 where procedure_code = 'H0033'
    --select * from #temp3 where id_mcare = 'BLANK' order by last_service_date;
        
    ----Review H0033 claims based on comparing monthly methadone to bup
    --select id_mcare, claim_header_id, last_service_date, oud_dx1_flag, procedure_code, meth_proc_flag, bup_proc_flag, nal_proc_flag,
    --meth_proc_month_sum, bup_proc_month_sum, nal_proc_month_sum, bup_rx_month_sum, nal_rx_month_sum
    --from #temp3 where procedure_code = 'H0033'
    --and bup_proc_month_sum = meth_proc_month_sum;
    --and bup_proc_month_sum != meth_proc_month_sum;
        
    /*
    --Summarize # of people and H0033 claims based on methadone-bup monthly encounter comparison
    select 'Monthly methadone and bup encounters = ZERO and bup rx fills = ZERO' as metric, count(distinct id_mcare) as id_dcount,
    count(distinct claim_header_id) as claim_dcount, 'Methadone' as 'algorithm_assigns'
    from #temp3
    where procedure_code = 'H0033' and meth_proc_month_sum = 0 and bup_proc_month_sum = 0 and bup_rx_month_sum = 0
        
    union select 'Monthly methadone and bup encounters = ZERO, bup rx fills > 0' as metric, count(distinct id_mcare) as id_dcount,
    count(distinct claim_header_id) as claim_dcount, 'Buprenorphine' as 'algorithm_assigns'
    from #temp3
    where procedure_code = 'H0033' and  meth_proc_month_sum = 0 and bup_proc_month_sum = 0 and bup_rx_month_sum > 0
        
    --Removed naltrexone from consideration as there were 0 people/claims identified by the below query
    --union select 'Monthly methadone and bup encounters and bup rx fills = ZERO, nal encounters OR nal rx fills >0' as metric, count(distinct id_mcare) as id_dcount,
    --count(distinct claim_header_id) as claim_dcount, 'Naltrexone' as 'algorithm_assigns'
    --from #temp3
    --where procedure_code = 'H0033' and bup_proc_month_sum = 0 and meth_proc_month_sum = 0 and bup_rx_month_sum = 0
    --	and (nal_proc_month_sum > 0 or nal_rx_month_sum > 0)
        
    union select 'Methadone monthly encounters = bup encounters, neither = ZERO' as metric, count(distinct id_mcare) as id_dcount,
    count(distinct claim_header_id) as claim_dcount, 'Methadone' as 'algorithm_assigns'
    from #temp3
    where procedure_code = 'H0033' and bup_proc_month_sum = meth_proc_month_sum and meth_proc_month_sum != 0 and bup_proc_month_sum != 0
        
    union select 'Methadone monthly encounters > bup encounters' as metric, count(distinct id_mcare) as id_dcount,
    count(distinct claim_header_id) as claim_dcount, 'Methadone' as 'algorithm_assigns'
    from #temp3
    where procedure_code = 'H0033' and meth_proc_month_sum > bup_proc_month_sum
        
    union select 'Methadone monthly encounters < bup encounters' as metric, count(distinct id_mcare) as id_dcount,
    count(distinct claim_header_id) as claim_dcount, 'Buprenorphine' as 'algorithm_assigns'
    from #temp3
    where procedure_code = 'H0033' and meth_proc_month_sum < bup_proc_month_sum;
    */
        
    --Collapse to distinct ID, last_service_date and moud_specific_flags, and sum moud_days_supply
    --This inflates moud_days_supply for methadone administration (given many duplicate claims for same service date), but next-service-date quantification method
        --will address this below
    if object_id('tempdb..#mcare_moud_union_2') is not null drop table #mcare_moud_union_2;
    select id_mcare,
    last_service_date,
    meth_proc_flag,
    bup_proc_flag,
    nal_proc_flag,
    unspec_proc_flag,
    bup_rx_flag,
    nal_rx_flag,
    sum(moud_days_supply) as moud_days_supply,
    admin_method
    into #mcare_moud_union_2
    from #temp3
    group by id_mcare, last_service_date, meth_proc_flag, bup_proc_flag, nal_proc_flag, unspec_proc_flag, bup_rx_flag, nal_rx_flag, admin_method;
        
        
    ---------------------------
    --Added this step 4/2/2024 JL
    --STEP 7: Identify same MOUDs with same method of administration occurring on the same day
    --For those fulfilling above criteria, will only use NDCs for moud days instead of using both NDC and HCPCS codes
    ---------------------------
        
    --Creating a single moud type variable that is very broad
    if object_id('tempdb..#mcare_moud_union_3') is not null drop table #mcare_moud_union_3;
    select *, 
    case when bup_proc_flag = 1 or bup_rx_flag = 1 then 'buprenorphine'
        when nal_proc_flag = 1 or nal_rx_flag = 1 then 'naltrexone'
        else null
    end as moudtype, --only doing bupe and naltrexone 
    case when bup_proc_flag = 1 or nal_proc_flag = 1 then 'hcpcs'
        when bup_rx_flag = 1 or nal_rx_flag = 1 then 'ndc'
        else null 
    end as codetype --only doing bupe and naltrexone
    into #mcare_moud_union_3
    from #mcare_moud_union_2;
        
    --Identify id_mcare, last_service_dates, and moudtype variables that are the same and link to full data to create temp data
    if object_id('tempdb..#tempcolumns') is not null drop table #tempcolumns;
    select count(*) as dupdate, id_mcare, last_service_date, moudtype, admin_method
    into #tempcolumns
    from #mcare_moud_union_3
    group by id_mcare, last_service_date, moudtype, admin_method
    having count(*) > 1;
    
    if object_id('tempdb..#tempcolumns2') is not null drop table #tempcolumns2;
    select a.dupdate, b.*
    into #tempcolumns2
    from #tempcolumns as a 
    right join 
    #mcare_moud_union_3 as b
    on (a.id_mcare = b.id_mcare) and (a.last_service_date = b.last_service_date) and (a.moudtype = b.moudtype)
    where dupdate is not null;
        
    --Create indicator in temp dataset for rows that shouldn't be counted for MOUD days
    if object_id('tempdb..#tempcolumns3') is not null drop table #tempcolumns3;
    select *, 
    case when codetype = 'hcpcs' then 1
        else 0
    end as dupmoud_todelete
    into #tempcolumns3
    from #tempcolumns2
    where case when codetype = 'hcpcs' then 1 else 0 end = 1;
        
    --Joining temp dataset back to the full dataset with the newly created indicator
    if object_id('tempdb..#mcare_moud_union_4') is not null drop table #mcare_moud_union_4;
    select a.*, b.dupmoud_todelete
    into #mcare_moud_union_4
    from #mcare_moud_union_3 as a
    left join
    #tempcolumns3 as b
    on (a.id_mcare = b.id_mcare) and (a.last_service_date = b.last_service_date) and (a.moudtype = b.moudtype) and (a.admin_method = b.admin_method) and (a.codetype = b.codetype);
    
    if object_id('tempdb..#mcare_moud_union_final') is not null drop table #mcare_moud_union_final;
    select id_mcare, last_service_date, meth_proc_flag, bup_proc_flag, nal_proc_flag, unspec_proc_flag, bup_rx_flag, nal_rx_flag, moud_days_supply, admin_method
    into #mcare_moud_union_final
    from #mcare_moud_union_4
    where dupmoud_todelete is null;
        
        
    ---------------------------
    --STEP 8: Add time period columns for easier tabulation
    --Note there is no need to estimate methadone days supply because Medicare only allows G codes for MOUD, each of which have specific days
    --supply, and ResDAC data files have a days supply column that is already good to use for summing days supply
    --Note that Medicare only began covering MOUD in Jan 2020
    ---------------------------
        
    --Add year_half column using ref_date table
    if object_id('tempdb..#temp_meth_1') is not null drop table #temp_meth_1;
    select a.*, b.year_month, b.year_quarter, b.year_half, b.[year]
    into #temp_meth_1
    from #mcare_moud_union_final as a
    left join (select [date], [year_month], [year_quarter],
        case
        	when right(year_quarter,2) in (1,2) then left(year_quarter,4) + '_top'
        	when right(year_quarter,2) in (3,4) then left(year_quarter,4) + '_bottom'
        end as year_half,
        [year]
        from stg_claims.ref_date) as b
    on a.last_service_date = b.[date];
        
    --Insert into final table shell
    insert into stg_claims.stage_mcare_claim_moud
    select
    id_mcare,
    last_service_date,
    [year] as service_year,
    year_quarter as service_quarter,
    year_month as service_month,
    meth_proc_flag,
    bup_proc_flag,
    nal_proc_flag,
    unspec_proc_flag,
    bup_rx_flag,
    nal_rx_flag,
    admin_method,
        
    --count moud type flags
    isnull(meth_proc_flag,0) + isnull(bup_proc_flag,0) + isnull(nal_proc_flag,0) + isnull(bup_rx_flag,0) + isnull(nal_rx_flag,0) as moud_flag_count ,
        
    moud_days_supply,
    getdate() as last_run
        
    from #temp_meth_1;",
            .con = inthealth))
        }

#### Table-level QA script ####
qa_stage.mcare_claim_moud_qa_f <- function() {
  
  #make sure everyone is in bene_enrollment table
  res1 <- dbGetQuery(conn = inthealth, glue_sql(
  "select 'stg_claims.stage_mcare_claim_moud' as 'table', '# members not in bene_enrollment, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stg_claims.stage_mcare_claim_moud as a
    left join stg_claims.mcare_bene_enrollment as b
    on a.id_mcare = b.bene_id
    where b.bene_id is null;",
  .con = inthealth))
  
  #confirm no rows with unspecified procedure flag and non-ZERO MOUD supply
  res2 <- dbGetQuery(conn = inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_moud' as 'table', '# of rows with unspec proc and non-zero supply, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_mcare_claim_moud
    where unspec_proc_flag = 1 and moud_days_supply_new_year_quarter > 0;",
    .con = inthealth))
  
  #confirm no rows with more than one type of MOUD flag
  res3 <- dbGetQuery(conn = inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_moud' as 'table', '# of rows with more than 1 type of MOUD flag, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_mcare_claim_moud
    where moud_flag_count > 1;",
    .con = inthealth))

res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}