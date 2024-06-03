#### CODE TO LOAD & TABLE-LEVEL QA STAGE.APCD_CLAIM_PREG_EPISODE
# Eli Kern, PHSKC (APDE)
#
# 2024-06

### Run from master_apcd_analytic script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/apcd/07_apcd_create_analytic_tables.R

#### Load script ####
load_stage.apcd_claim_preg_episode_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(dw_inthealth, glue::glue_sql(
    "
    ---------------------------------
    --Eli Kern, APDE, PHSKC
    --January 2023
    --Code to create a pregnancy endpoint analytic table, based on Moll et al. 2021 study
    ---------------------------------
    
    --------------------------
    --STEP 1: Find claims with a ICD-10-CM code relevant to pregnancy endpoints
    --Restrict to last service date >= 1/1/2016 to restrict to ICD-10-CM era
    --Based on exploratory analysis, an exact join on ICD-10-CM will miss some ICD-10-CM codes where ICD-10-CM codes in claims are more
    	--detailed than in the pregnancy endpoint reference table
    --Efficient approach is to create a new temporary crosswalk using distinct ICD-10-CM codes in claims, then join claim data to this ref table
    --------------------------
    
    --Pull out distinct ICD-10-CM codes from claims >= 2016-01-01
    IF OBJECT_ID(N'tempdb..#dx_distinct') IS NOT NULL drop table #dx_distinct;
    select distinct icdcm_norm
    into #dx_distinct
    from stg_claims.stage_apcd_claim_icdcm_header
    where last_service_date >= '2016-01-01';
    
    --Join distinct ICD-10-CM codes to pregnancy endpoint reference table using LIKE join
    IF OBJECT_ID(N'tempdb..#ref_dx') IS NOT NULL drop table #ref_dx;
    select distinct a.icdcm_norm, b.lb, b.ect, b.ab, b.sa, b.sb, b.tro, b.deliv
    into #ref_dx
    from #dx_distinct as a
    inner join (select * from stg_claims.ref_moll_preg_endpoint where code_type = 'icd10cm') as b
    on a.icdcm_norm like b.code_like;
    
    --Join new reference table to claims data using EXACT join
    IF OBJECT_ID(N'tempdb..#preg_dx') IS NOT NULL drop table #preg_dx;
    select a.id_apcd, a.claim_header_id, a.last_service_date, a.icdcm_norm, a.icdcm_version, 
    	a.icdcm_number, b.lb, b.ect, b.ab, b.sa, b.sb, b.tro, b.deliv
    into #preg_dx
    from stg_claims.stage_apcd_claim_icdcm_header as a
    inner join #ref_dx as b
    on a.icdcm_norm = b.icdcm_norm
    where a.last_service_date >= '2016-01-01'
	option (label = 'preg_dx');
    
    
    --------------------------
    --STEP 2: Find claims with a procedure code relevant to pregnancy endpoints
    --Restrict to first service date >= 1/1/2016 to restrict to ICD-10-CM era
    --------------------------
    
    --Pull out distinct procedure codes from claims >= 2016-01-01
    IF OBJECT_ID(N'tempdb..#px_distinct') IS NOT NULL drop table #px_distinct;
    select distinct procedure_code
    into #px_distinct
    from stg_claims.stage_apcd_claim_procedure
    where last_service_date >= '2016-01-01';
    
    --Join distinct procedure codes to pregnancy endpoint reference table using LIKE join
    IF OBJECT_ID(N'tempdb..#ref_px') IS NOT NULL drop table #ref_px;
    select distinct a.procedure_code, b.lb, b.ect, b.ab, b.sa, b.sb, b.tro, b.deliv
    into #ref_px
    from #px_distinct as a
    inner join (select * from stg_claims.ref_moll_preg_endpoint where code_type in ('icd10pcs', 'hcpcs', 'cpt_hcpcs')) as b
    on a.procedure_code like b.code_like;
    
    --Join new reference table to claims data using EXACT join
    IF OBJECT_ID(N'tempdb..#preg_px') IS NOT NULL drop table #preg_px;
    select a.id_apcd, a.claim_header_id, a.last_service_date, a.procedure_code, 
    	a.procedure_code_number, b.lb, b.ect, b.ab, b.sa, b.sb, b.tro, b.deliv
    into #preg_px
    from stg_claims.stage_apcd_claim_procedure as a
    inner join #ref_px as b
    on a.procedure_code = b.procedure_code
    where a.last_service_date >= '2016-01-01'
	option (label = 'preg_px');
    
    
    --------------------------
    --STEP 3: Union dx and px-based datasets, subsetting to common columns to collapse to distinct claim headers
    --------------------------
    
    IF OBJECT_ID(N'tempdb..#temp1') IS NOT NULL drop table #temp1;
    select id_apcd, claim_header_id, last_service_date, lb, ect, ab, sa, sb, tro, deliv
    into #temp1
    from #preg_dx
    union
    select id_apcd, claim_header_id, last_service_date, lb, ect, ab, sa, sb, tro, deliv
    from #preg_px;
    
    --Convert all NULLS to ZEROES and cast as TINYINT for later math
    IF OBJECT_ID(N'tempdb..#preg_dx_px') IS NOT NULL drop table #preg_dx_px;
    select id_apcd, claim_header_id, last_service_date,
    	cast(isnull(lb,0) as tinyint) as lb, cast(isnull(ect,0) as tinyint) as ect, cast(isnull(ab,0) as tinyint) as ab,
    	cast(isnull(sa,0) as tinyint) as sa, cast(isnull(sb,0) as tinyint) as sb, cast(isnull(tro,0) as tinyint) as tro,
    	cast(isnull(deliv,0) as tinyint) as deliv
    into #preg_dx_px
    from #temp1
	option (label = 'preg_dx_px');
    
    --------------------------
    --STEP 4: Group by ID-service date and count # of distinct endpoints (not including DELIV)
    --------------------------
    
    --Group by ID-service date and take max of each endpoint column
    IF OBJECT_ID(N'tempdb..#temp2') IS NOT NULL drop table #temp2;
    select id_apcd, last_service_date, max(lb) as lb, max(ect) as ect, max(ab) as ab, max(sa) as sa, max(sb) as sb,
    	max(tro) as tro, max(deliv) as deliv
    into #temp2
    from #preg_dx_px
    group by id_apcd, last_service_date;
    
    --Count # of distinct endpoints, not including DELIV
    IF OBJECT_ID(N'tempdb..#temp3') IS NOT NULL drop table #temp3;
    select id_apcd, last_service_date, lb, ect, ab, sa, sb, tro, deliv,
    	lb + ect + ab + sa + sb + tro as endpoint_dcount
    into #temp3
    from #temp2;
    
    --Recode DELIV to 0 when there's another valid endpoint
    IF OBJECT_ID(N'tempdb..#temp4') IS NOT NULL drop table #temp4;
    select id_apcd, last_service_date, lb, ect, ab, sa, sb, tro,
    	case when endpoint_dcount = 0 then deliv else 0 end as deliv,
    	endpoint_dcount
    into #temp4
    from #temp3;
    
    --Drop ID-service dates that contain >1 distinct endpoint (excluding DELIV)
    --Also restructure endpoint as a single variable, and add hierarchy variable
    IF OBJECT_ID(N'tempdb..#temp5') IS NOT NULL drop table #temp5;
    select id_apcd, last_service_date, lb, ect, ab, sa, sb, tro, deliv,
    
    --mutually exclusive pregnancy endpoint variable
    case when lb = 1 then 'lb'
    	when ect = 1 then 'ect'
    	when ab = 1 then 'ab'
    	when sa = 1 then 'sa'
    	when sb = 1 then 'sb'
    	when tro = 1 then 'tro'
    	when deliv = 1 then 'deliv'
    else null end as preg_endpoint,
    
    --pregnancy episode hierarchy
    case when lb = 1 then 1
    	when sb = 1 then 2
    	when deliv = 1 then 3
    	when tro = 1 then 4
    	when ect = 1 then 5
    	when ab = 1 then 6
    	when sa = 1 then 7
    else null end as preg_hier
    
    into #temp5
    from #temp4
    where endpoint_dcount <=1;
    
    --Add ranking variable within each pregnancy endpoint type
    IF OBJECT_ID(N'tempdb..#preg_endpoint') IS NOT NULL drop table #preg_endpoint;
    select *, rank() over (partition by id_apcd, preg_endpoint order by last_service_date) as preg_endpoint_rank 
    into #preg_endpoint
    from #temp5
	option (label = 'preg_endpoint');
    
    
    --------------------------
    --STEP 5: Hierarchical assessment of pregnancy outcomes to create pregnancy episodes for each woman
    --------------------------
    
    -------
    --Step 5A: Group livebirth service days into distinct pregnancy episodes
    -------
    
    --Count days between each service day (<1 min)
    if object_id(N'tempdb..#lb_step1') is not null drop table #lb_step1;
    select a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_endpoint_rank, a.date_compare_lag1,
    	datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into #lb_step1
    from(
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
    		lag(last_service_date, 1, last_service_date) over (partition by id_apcd order by last_service_date) as date_compare_lag1
    	from #preg_endpoint
    	where preg_endpoint = 'lb'
    ) as a;
    
    --Group pregnancy endpoints into episodes based on minimum spacing (<1 min)
    if object_id(N'tempdb..#lb_final') is not null drop table #lb_final;
    with
    t as (
        select t.id_apcd, t.last_service_date, t.preg_endpoint, t.preg_hier, t.preg_endpoint_rank, t.days_diff
    	from #lb_step1 as t
    ),
    cte as (
        select t.*, t.days_diff as days_diff_cum, 1 as timeline_include -- to keep 1st LB endpoint
        from t
        where t.preg_endpoint_rank = 1
        union all
        select t.*,
    	--generate cumulative days diff that resets when threshold is reached
    	case
    		when cte.days_diff_cum + t.days_diff > 182 then 0
            else cte.days_diff_cum + t.days_diff
        end as days_diff_cum,
    	--generate variable to flag inclusion on timeline
    	case
    		when cte.days_diff_cum + t.days_diff > 182 then 1
    		else 0
    	end as timeline_include
        from cte
    	inner join t
        on (t.id_apcd = cte.id_apcd) and (t.preg_endpoint_rank = cte.preg_endpoint_rank + 1)
    )
    --Clean up group column, keep only endpoints added to the timeline, and select columns to keep
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
    	rank() over (partition by id_apcd order by last_service_date) as preg_episode_id
    into #lb_final
    from cte
    where timeline_include = 1;
    
    --Clean up temp tables
    if object_id(N'tempdb..#lb_step1') is not null drop table #lb_step1;
    
    
    -------
    --Step 5B: PROCESS STILLBIRTH EPISODES
    -------
    
    --Union LB and SB endpoints
    if object_id(N'tempdb..#sb_step1') is not null drop table #sb_step1;
    select *, last_service_date as prior_lb_date, last_service_date as next_lb_date into #sb_step1 from #lb_final
    union
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
    	null as prior_lb_date, null as next_lb_date
    from #preg_endpoint where preg_endpoint = 'sb'
    order by last_service_date;
    
    --Create column to hold dates of LB endpoints for comparison
    if object_id(N'tempdb..#sb_step2') is not null drop table #sb_step2;
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
    --create column to hold date of prior LB
    case when preg_endpoint = 'sb' then
    ISNULL(prior_lb_date, (SELECT TOP 1 last_service_date FROM #sb_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_lb_date IS NOT NULL ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_lb_date,
    --create column to hold date of next LB
    case when preg_endpoint = 'sb' then
    ISNULL(next_lb_date, (SELECT TOP 1 last_service_date FROM #sb_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_lb_date IS NOT NULL ORDER BY id_apcd, last_service_date))
    else null
    end as next_lb_date
    into #sb_step2
    from #sb_step1 as t;
    
    --For each SB endpoint, count days between it and prior and next LB endpoint
    if object_id(N'tempdb..#sb_step3') is not null drop table #sb_step3;
    select *,	
    	case when preg_endpoint = 'sb' then datediff(day, prior_lb_date, last_service_date) else null end as days_diff_back_lb,
    	case when preg_endpoint = 'sb' then datediff(day, next_lb_date, last_service_date) else null end as days_diff_ahead_lb
    into #sb_step3
    from #sb_step2;
    
    --Pull out SB timepoints that potentially can be placed on timeline - COMPARE TO LB ENDPOINTS
    if object_id(N'tempdb..#sb_step4') is not null drop table #sb_step4;
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
    into #sb_step4
    from #sb_step3
    where preg_endpoint = 'sb'
    	and (days_diff_back_lb is null or days_diff_back_lb > 182)
    	and (days_diff_ahead_lb is null or days_diff_ahead_lb < -182);
    
    --Count days between each SB endpoint and regenerate preg_endpoint_rank variable
    if object_id(N'tempdb..#sb_step5') is not null drop table #sb_step5;
    select a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier,
    	rank() over (partition by a.id_apcd, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
    	datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into #sb_step5
    from(
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
    	lag(last_service_date, 1, last_service_date) over (partition by id_apcd order by last_service_date) as date_compare_lag1
    	from #sb_step4
    ) as a;
    
    --Group SB endpoints into episodes based on minimum spacing
    if object_id(N'tempdb..#sb_final') is not null drop table #sb_final;
    with
    t as (
        select t.id_apcd, t.last_service_date, t.preg_endpoint, t.preg_hier, t.preg_endpoint_rank, t.days_diff
    	from #sb_step5 as t
    ),
    cte as (
        select t.*, t.days_diff as days_diff_cum, 1 as timeline_include -- to keep first endpoint
        from t
        where t.preg_endpoint_rank = 1
        union all
        select t.*,
    	--generate cumulative days diff that resets when threshold is reached
    	case
    		when cte.days_diff_cum + t.days_diff > 168 then 0
            else cte.days_diff_cum + t.days_diff
        end as days_diff_cum,
    	--generate variable to flag inclusion on timeline
    	case
    		when cte.days_diff_cum + t.days_diff > 168 then 1
    		else 0
    	end as timeline_include
        from cte
    	inner join t
        on (t.id_apcd = cte.id_apcd) and (t.preg_endpoint_rank = cte.preg_endpoint_rank + 1)
    )
    --Clean up group column, keep only endpoints added to the timeline, and select columns to keep
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
    	rank() over (partition by id_apcd order by last_service_date) as preg_episode_id
    into #sb_final
    from cte
    where timeline_include = 1;
    
    --Union LB and SB endpoints placed on timeline
    if object_id(N'tempdb..#lb_sb_final') is not null drop table #lb_sb_final;
    select * into #lb_sb_final from #lb_final
    union
    select * from #sb_final;
    
    --Clean up temp tables
    if object_id(N'tempdb..#sb_step1') is not null drop table #sb_step1;
    if object_id(N'tempdb..#sb_step2') is not null drop table #sb_step2;
    if object_id(N'tempdb..#sb_step3') is not null drop table #sb_step3;
    if object_id(N'tempdb..#sb_step4') is not null drop table #sb_step4;
    if object_id(N'tempdb..#sb_step5') is not null drop table #sb_step5;
    if object_id(N'tempdb..#lb_final') is not null drop table #lb_final;
    if object_id(N'tempdb..#sb_final') is not null drop table #sb_final;
    
    
    -------
    --Step 5C: PROCESS DELIV EPISODES
    -------
    
    --Union LB-SB and DELIV endpoints
    if object_id(N'tempdb..#deliv_step1') is not null drop table #deliv_step1;
    select *, last_service_date as prior_date, last_service_date as next_date into #deliv_step1 from #lb_sb_final
    union
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
    	null as prior_date, null as next_date
    from #preg_endpoint where preg_endpoint = 'deliv'
    order by last_service_date;
    
    --Create column to hold dates of LB and SB endpoints for comparison
    if object_id(N'tempdb..#deliv_step2') is not null drop table #deliv_step2;
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
    --create column to hold date of prior LB
    case when preg_endpoint = 'deliv' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #deliv_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'lb'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_lb_date,
    --create column to hold date of next LB
    case when preg_endpoint = 'deliv' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #deliv_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'lb'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_lb_date,
    --create column to hold date of prior SB
    case when preg_endpoint = 'deliv' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #deliv_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'sb'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_sb_date,
    --create column to hold date of next SB
    case when preg_endpoint = 'deliv' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #deliv_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'sb'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_sb_date
    into #deliv_step2
    from #deliv_step1 as t;
    
    --For each DELIV endpoint, count days between it and prior and next LB and SB endpoints
    if object_id(N'tempdb..#deliv_step3') is not null drop table #deliv_step3;
    select *,	
    	case when preg_endpoint = 'deliv' then datediff(day, prior_lb_date, last_service_date) else null end as days_diff_back_lb,
    	case when preg_endpoint = 'deliv' then datediff(day, next_lb_date, last_service_date) else null end as days_diff_ahead_lb,
    	case when preg_endpoint = 'deliv' then datediff(day, prior_sb_date, last_service_date) else null end as days_diff_back_sb,
    	case when preg_endpoint = 'deliv' then datediff(day, next_sb_date, last_service_date) else null end as days_diff_ahead_sb
    into #deliv_step3
    from #deliv_step2;
    
    --Pull out DELIV timepoints that potentially can be placed on timeline - COMPARE TO LB and SB ENDPOINTS
    if object_id(N'tempdb..#deliv_step4') is not null drop table #deliv_step4;
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
    into #deliv_step4
    from #deliv_step3
    where preg_endpoint = 'deliv'
    	and (days_diff_back_lb is null or days_diff_back_lb > 182)
    	and (days_diff_ahead_lb is null or days_diff_ahead_lb < -182)
    	and (days_diff_back_sb is null or days_diff_back_sb > 168)
    	and (days_diff_ahead_sb is null or days_diff_ahead_sb < -168);
    
    --Count days between each DELIV endpoint and regenerate preg_endpoint_rank variable
    if object_id(N'tempdb..#deliv_step5') is not null drop table #deliv_step5;
    select a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier,
    	rank() over (partition by a.id_apcd, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
    	datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into #deliv_step5
    from(
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
    	lag(last_service_date, 1, last_service_date) over (partition by id_apcd order by last_service_date) as date_compare_lag1
    	from #deliv_step4
    ) as a;
    
    --Group DELIV endpoints into episodes based on minimum spacing
    if object_id(N'tempdb..#deliv_final') is not null drop table #deliv_final;
    with
    t as (
        select t.id_apcd, t.last_service_date, t.preg_endpoint, t.preg_hier, t.preg_endpoint_rank, t.days_diff
    	from #deliv_step5 as t
    ),
    cte as (
        select t.*, t.days_diff as days_diff_cum, 1 as timeline_include -- to keep first endpoint
        from t
        where t.preg_endpoint_rank = 1
        union all
        select t.*,
    	--generate cumulative days diff that resets when threshold is reached
    	case
    		when cte.days_diff_cum + t.days_diff > 168 then 0
            else cte.days_diff_cum + t.days_diff
        end as days_diff_cum,
    	--generate variable to flag inclusion on timeline
    	case
    		when cte.days_diff_cum + t.days_diff > 168 then 1
    		else 0
    	end as timeline_include
        from cte
    	inner join t
        on (t.id_apcd = cte.id_apcd) and (t.preg_endpoint_rank = cte.preg_endpoint_rank + 1)
    )
    --Clean up group column, keep only endpoints added to the timeline, and select columns to keep
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
    	rank() over (partition by id_apcd order by last_service_date) as preg_episode_id
    into #deliv_final
    from cte
    where timeline_include = 1;
    
    --Union LB, SB and DELIV endpoints placed on timeline
    if object_id(N'tempdb..#lb_sb_deliv_final') is not null drop table #lb_sb_deliv_final;
    select * into #lb_sb_deliv_final from #lb_sb_final
    union
    select * from #deliv_final;
    
    --Clean up temp tables
    if object_id(N'tempdb..#deliv_step1') is not null drop table #deliv_step1;
    if object_id(N'tempdb..#deliv_step2') is not null drop table #deliv_step2;
    if object_id(N'tempdb..#deliv_step3') is not null drop table #deliv_step3;
    if object_id(N'tempdb..#deliv_step4') is not null drop table #deliv_step4;
    if object_id(N'tempdb..#deliv_step5') is not null drop table #deliv_step5;
    if object_id(N'tempdb..#lb_sb_final') is not null drop table #lb_sb_final;
    if object_id(N'tempdb..#deliv_final') is not null drop table #deliv_final;
    
    
    -------
    --Step 5D: PROCESS TRO EPISODES
    -------
    
    --Union LB-SB-DELIV and TRO endpoints
    if object_id(N'tempdb..#tro_step1') is not null drop table #tro_step1;
    select *, last_service_date as prior_date, last_service_date as next_date into #tro_step1 from #lb_sb_deliv_final
    union
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
    	null as prior_date, null as next_date
    from #preg_endpoint where preg_endpoint = 'tro'
    order by last_service_date;
    
    --Create column to hold dates of LB and SB endpoints for comparison
    if object_id(N'tempdb..#tro_step2') is not null drop table #tro_step2;
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
    --create column to hold date of prior LB
    case when preg_endpoint = 'tro' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #tro_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'lb'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_lb_date,
    --create column to hold date of next LB
    case when preg_endpoint = 'tro' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #tro_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'lb'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_lb_date,
    --create column to hold date of prior SB
    case when preg_endpoint = 'tro' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #tro_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'sb'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_sb_date,
    --create column to hold date of next SB
    case when preg_endpoint = 'tro' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #tro_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'sb'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_sb_date,
    --create column to hold date of prior DELIV
    case when preg_endpoint = 'tro' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #tro_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'deliv'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_deliv_date,
    --create column to hold date of next DELIV
    case when preg_endpoint = 'tro' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #tro_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'deliv'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_deliv_date
    into #tro_step2
    from #tro_step1 as t;
    
    --For each TRO endpoint, count days between it and prior and next LB, SB and DELIV endpoints
    if object_id(N'tempdb..#tro_step3') is not null drop table #tro_step3;
    select *,	
    	case when preg_endpoint = 'tro' then datediff(day, prior_lb_date, last_service_date) else null end as days_diff_back_lb,
    	case when preg_endpoint = 'tro' then datediff(day, next_lb_date, last_service_date) else null end as days_diff_ahead_lb,
    	case when preg_endpoint = 'tro' then datediff(day, prior_sb_date, last_service_date) else null end as days_diff_back_sb,
    	case when preg_endpoint = 'tro' then datediff(day, next_sb_date, last_service_date) else null end as days_diff_ahead_sb,
    	case when preg_endpoint = 'tro' then datediff(day, prior_deliv_date, last_service_date) else null end as days_diff_back_deliv,
    	case when preg_endpoint = 'tro' then datediff(day, next_deliv_date, last_service_date) else null end as days_diff_ahead_deliv
    into #tro_step3
    from #tro_step2;
    
    --Pull out TRO timepoints that potentially can be placed on timeline - COMPARE TO LB, SB, and DELIV ENDPOINTS
    if object_id(N'tempdb..#tro_step4') is not null drop table #tro_step4;
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
    into #tro_step4
    from #tro_step3
    where preg_endpoint = 'tro'
    	and (days_diff_back_lb is null or days_diff_back_lb > 168)
    	and (days_diff_ahead_lb is null or days_diff_ahead_lb < -168)
    	and (days_diff_back_sb is null or days_diff_back_sb > 154)
    	and (days_diff_ahead_sb is null or days_diff_ahead_sb < -154)
    	and (days_diff_back_deliv is null or days_diff_back_deliv > 154)
    	and (days_diff_ahead_deliv is null or days_diff_ahead_deliv < -154);
    
    --Count days between each TRO endpoint and regenerate preg_endpoint_rank variable
    if object_id(N'tempdb..#tro_step5') is not null drop table #tro_step5;
    select a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier,
    	rank() over (partition by a.id_apcd, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
    	datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into #tro_step5
    from(
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
    	lag(last_service_date, 1, last_service_date) over (partition by id_apcd order by last_service_date) as date_compare_lag1
    	from #tro_step4
    ) as a;
    
    --Group TRO endpoints into episodes based on minimum spacing
    if object_id(N'tempdb..#tro_final') is not null drop table #tro_final;
    with
    t as (
        select t.id_apcd, t.last_service_date, t.preg_endpoint, t.preg_hier, t.preg_endpoint_rank, t.days_diff
    	from #tro_step5 as t
    ),
    cte as (
        select t.*, t.days_diff as days_diff_cum, 1 as timeline_include -- to keep first endpoint
        from t
        where t.preg_endpoint_rank = 1
        union all
        select t.*,
    	--generate cumulative days diff that resets when threshold is reached
    	case
    		when cte.days_diff_cum + t.days_diff > 56 then 0
            else cte.days_diff_cum + t.days_diff
        end as days_diff_cum,
    	--generate variable to flag inclusion on timeline
    	case
    		when cte.days_diff_cum + t.days_diff > 56 then 1
    		else 0
    	end as timeline_include
        from cte
    	inner join t
        on (t.id_apcd = cte.id_apcd) and (t.preg_endpoint_rank = cte.preg_endpoint_rank + 1)
    )
    --Clean up group column, keep only endpoints added to the timeline, and select columns to keep
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
    	rank() over (partition by id_apcd order by last_service_date) as preg_episode_id
    into #tro_final
    from cte
    where timeline_include = 1;
    
    --Union LB, SB, DELIV and TRO endpoints placed on timeline
    if object_id(N'tempdb..#lb_sb_deliv_tro_final') is not null drop table #lb_sb_deliv_tro_final;
    select * into #lb_sb_deliv_tro_final from #lb_sb_deliv_final
    union
    select * from #tro_final;
    
    --Clean up temp tables
    if object_id(N'tempdb..#tro_step1') is not null drop table #tro_step1;
    if object_id(N'tempdb..#tro_step2') is not null drop table #tro_step2;
    if object_id(N'tempdb..#tro_step3') is not null drop table #tro_step3;
    if object_id(N'tempdb..#tro_step4') is not null drop table #tro_step4;
    if object_id(N'tempdb..#tro_step5') is not null drop table #tro_step5;
    if object_id(N'tempdb..#lb_sb_deliv_final') is not null drop table #lb_sb_deliv_final;
    if object_id(N'tempdb..#tro_final') is not null drop table #tro_final;
    
    
    -------
    --Step 5E: PROCESS ECT EPISODES
    -------
    
    --Union LB-SB, DELIV, TRO and ECT endpoints
    if object_id(N'tempdb..#ect_step1') is not null drop table #ect_step1;
    select *, last_service_date as prior_date, last_service_date as next_date into #ect_step1 from #lb_sb_deliv_tro_final
    union
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
    	null as prior_date, null as next_date
    from #preg_endpoint where preg_endpoint = 'ect'
    order by last_service_date;
    
    --Create column to hold dates of LB, SB, DELIV and TRO endpoints for comparison
    if object_id(N'tempdb..#ect_step2') is not null drop table #ect_step2;
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
    --create column to hold date of prior LB
    case when preg_endpoint = 'ect' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ect_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'lb'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_lb_date,
    --create column to hold date of next LB
    case when preg_endpoint = 'ect' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ect_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'lb'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_lb_date,
    --create column to hold date of prior SB
    case when preg_endpoint = 'ect' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ect_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'sb'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_sb_date,
    --create column to hold date of next SB
    case when preg_endpoint = 'ect' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ect_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'sb'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_sb_date,
    --create column to hold date of prior DELIV
    case when preg_endpoint = 'ect' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ect_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'deliv'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_deliv_date,
    --create column to hold date of next DELIV
    case when preg_endpoint = 'ect' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ect_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'deliv'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_deliv_date,
    --create column to hold date of prior TRO
    case when preg_endpoint = 'ect' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ect_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'tro'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_tro_date,
    --create column to hold date of next TRO
    case when preg_endpoint = 'ect' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ect_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'tro'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_tro_date
    into #ect_step2
    from #ect_step1 as t;
    
    --For each ECT endpoint, count days between it and prior and next LB, SB, DELIV, and TRO endpoints
    if object_id(N'tempdb..#ect_step3') is not null drop table #ect_step3;
    select *,	
    	case when preg_endpoint = 'ect' then datediff(day, prior_lb_date, last_service_date) else null end as days_diff_back_lb,
    	case when preg_endpoint = 'ect' then datediff(day, next_lb_date, last_service_date) else null end as days_diff_ahead_lb,
    	case when preg_endpoint = 'ect' then datediff(day, prior_sb_date, last_service_date) else null end as days_diff_back_sb,
    	case when preg_endpoint = 'ect' then datediff(day, next_sb_date, last_service_date) else null end as days_diff_ahead_sb,
    	case when preg_endpoint = 'ect' then datediff(day, prior_deliv_date, last_service_date) else null end as days_diff_back_deliv,
    	case when preg_endpoint = 'ect' then datediff(day, next_deliv_date, last_service_date) else null end as days_diff_ahead_deliv,
    	case when preg_endpoint = 'ect' then datediff(day, prior_tro_date, last_service_date) else null end as days_diff_back_tro,
    	case when preg_endpoint = 'ect' then datediff(day, next_tro_date, last_service_date) else null end as days_diff_ahead_tro
    into #ect_step3
    from #ect_step2;
    
    --Pull out ECT timepoints that potentially can be placed on timeline - COMPARE TO LB, SB, DELIV and TRO ENDPOINTS
    if object_id(N'tempdb..#ect_step4') is not null drop table #ect_step4;
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
    into #ect_step4
    from #ect_step3
    where preg_endpoint = 'ect'
    	and (days_diff_back_lb is null or days_diff_back_lb > 168)
    	and (days_diff_ahead_lb is null or days_diff_ahead_lb < -168)
    	and (days_diff_back_sb is null or days_diff_back_sb > 154)
    	and (days_diff_ahead_sb is null or days_diff_ahead_sb < -154)
    	and (days_diff_back_deliv is null or days_diff_back_deliv > 154)
    	and (days_diff_ahead_deliv is null or days_diff_ahead_deliv < -154)
    	and (days_diff_back_tro is null or days_diff_back_tro > 56)
    	and (days_diff_ahead_tro is null or days_diff_ahead_tro < -56);
    
    --Count days between each ECT endpoint and regenerate preg_endpoint_rank variable
    if object_id(N'tempdb..#ect_step5') is not null drop table #ect_step5;
    select a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier,
    	rank() over (partition by a.id_apcd, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
    	datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into #ect_step5
    from(
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
    	lag(last_service_date, 1, last_service_date) over (partition by id_apcd order by last_service_date) as date_compare_lag1
    	from #ect_step4
    ) as a;
    
    --Group ECT endpoints into episodes based on minimum spacing
    if object_id(N'tempdb..#ect_final') is not null drop table #ect_final;
    with
    t as (
        select t.id_apcd, t.last_service_date, t.preg_endpoint, t.preg_hier, t.preg_endpoint_rank, t.days_diff
    	from #ect_step5 as t
    ),
    cte as (
        select t.*, t.days_diff as days_diff_cum, 1 as timeline_include -- to keep first endpoint
        from t
        where t.preg_endpoint_rank = 1
        union all
        select t.*,
    	--generate cumulative days diff that resets when threshold is reached
    	case
    		when cte.days_diff_cum + t.days_diff > 56 then 0
            else cte.days_diff_cum + t.days_diff
        end as days_diff_cum,
    	--generate variable to flag inclusion on timeline
    	case
    		when cte.days_diff_cum + t.days_diff > 56 then 1
    		else 0
    	end as timeline_include
        from cte
    	inner join t
        on (t.id_apcd = cte.id_apcd) and (t.preg_endpoint_rank = cte.preg_endpoint_rank + 1)
    )
    --Clean up group column, keep only endpoints added to the timeline, and select columns to keep
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
    	rank() over (partition by id_apcd order by last_service_date) as preg_episode_id
    into #ect_final
    from cte
    where timeline_include = 1;
    
    --Union LB, SB, DELIV, TRO, and ECT endpoints placed on timeline
    if object_id(N'tempdb..#lb_sb_deliv_tro_ect_final') is not null drop table #lb_sb_deliv_tro_ect_final;
    select * into #lb_sb_deliv_tro_ect_final from #lb_sb_deliv_tro_final
    union
    select * from #ect_final;
    
    --Clean up temp tables
    if object_id(N'tempdb..#ect_step1') is not null drop table #ect_step1;
    if object_id(N'tempdb..#ect_step2') is not null drop table #ect_step2;
    if object_id(N'tempdb..#ect_step3') is not null drop table #ect_step3;
    if object_id(N'tempdb..#ect_step4') is not null drop table #ect_step4;
    if object_id(N'tempdb..#ect_step5') is not null drop table #ect_step5;
    if object_id(N'tempdb..#lb_sb_deliv_tro_final') is not null drop table #lb_sb_deliv_tro_final;
    if object_id(N'tempdb..#ect_final') is not null drop table #ect_final;
    
    
    -------
    --Step 5F: PROCESS AB EPISODES
    -------
    
    --Union LB-SB, DELIV, TRO, ECT, an AB endpoints
    if object_id(N'tempdb..#ab_step1') is not null drop table #ab_step1;
    select *, last_service_date as prior_date, last_service_date as next_date into #ab_step1 from #lb_sb_deliv_tro_ect_final
    union
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
    	null as prior_date, null as next_date
    from #preg_endpoint where preg_endpoint = 'ab'
    order by last_service_date;
    
    --Create column to hold dates of LB, SB, DELIV, TRO, and ECT endpoints for comparison
    if object_id(N'tempdb..#ab_step2') is not null drop table #ab_step2;
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
    --create column to hold date of prior LB
    case when preg_endpoint = 'ab' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ab_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'lb'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_lb_date,
    --create column to hold date of next LB
    case when preg_endpoint = 'ab' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ab_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'lb'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_lb_date,
    --create column to hold date of prior SB
    case when preg_endpoint = 'ab' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ab_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'sb'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_sb_date,
    --create column to hold date of next SB
    case when preg_endpoint = 'ab' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ab_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'sb'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_sb_date,
    --create column to hold date of prior DELIV
    case when preg_endpoint = 'ab' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ab_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'deliv'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_deliv_date,
    --create column to hold date of next DELIV
    case when preg_endpoint = 'ab' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ab_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'deliv'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_deliv_date,
    --create column to hold date of prior TRO
    case when preg_endpoint = 'ab' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ab_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'tro'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_tro_date,
    --create column to hold date of next TRO
    case when preg_endpoint = 'ab' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ab_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'tro'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_tro_date,
    --create column to hold date of prior ECT
    case when preg_endpoint = 'ab' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ab_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'ect'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_ect_date,
    --create column to hold date of next ECT
    case when preg_endpoint = 'ab' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ab_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'ect'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_ect_date
    into #ab_step2
    from #ab_step1 as t;
    
    --For each AB endpoint, count days between it and prior and next LB, SB, DELIV, TRO, and ECT endpoints
    if object_id(N'tempdb..#ab_step3') is not null drop table #ab_step3;
    select *,	
    	case when preg_endpoint = 'ab' then datediff(day, prior_lb_date, last_service_date) else null end as days_diff_back_lb,
    	case when preg_endpoint = 'ab' then datediff(day, next_lb_date, last_service_date) else null end as days_diff_ahead_lb,
    	case when preg_endpoint = 'ab' then datediff(day, prior_sb_date, last_service_date) else null end as days_diff_back_sb,
    	case when preg_endpoint = 'ab' then datediff(day, next_sb_date, last_service_date) else null end as days_diff_ahead_sb,
    	case when preg_endpoint = 'ab' then datediff(day, prior_deliv_date, last_service_date) else null end as days_diff_back_deliv,
    	case when preg_endpoint = 'ab' then datediff(day, next_deliv_date, last_service_date) else null end as days_diff_ahead_deliv,
    	case when preg_endpoint = 'ab' then datediff(day, prior_tro_date, last_service_date) else null end as days_diff_back_tro,
    	case when preg_endpoint = 'ab' then datediff(day, next_tro_date, last_service_date) else null end as days_diff_ahead_tro,
    	case when preg_endpoint = 'ab' then datediff(day, prior_ect_date, last_service_date) else null end as days_diff_back_ect,
    	case when preg_endpoint = 'ab' then datediff(day, next_ect_date, last_service_date) else null end as days_diff_ahead_ect
    into #ab_step3
    from #ab_step2;
    
    --Pull out AB timepoints that potentially can be placed on timeline - COMPARE TO LB, SB, DELIV, TRO, and ECT ENDPOINTS
    if object_id(N'tempdb..#ab_step4') is not null drop table #ab_step4;
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
    into #ab_step4
    from #ab_step3
    where preg_endpoint = 'ab'
    	and (days_diff_back_lb is null or days_diff_back_lb > 168)
    	and (days_diff_ahead_lb is null or days_diff_ahead_lb < -168)
    	and (days_diff_back_sb is null or days_diff_back_sb > 154)
    	and (days_diff_ahead_sb is null or days_diff_ahead_sb < -154)
    	and (days_diff_back_deliv is null or days_diff_back_deliv > 154)
    	and (days_diff_ahead_deliv is null or days_diff_ahead_deliv < -154)
    	and (days_diff_back_tro is null or days_diff_back_tro > 56)
    	and (days_diff_ahead_tro is null or days_diff_ahead_tro < -56)
    	and (days_diff_back_ect is null or days_diff_back_ect > 56)
    	and (days_diff_ahead_ect is null or days_diff_ahead_ect < -56);
    
    --Count days between each AB endpoint and regenerate preg_endpoint_rank variable
    if object_id(N'tempdb..#ab_step5') is not null drop table #ab_step5;
    select a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier,
    	rank() over (partition by a.id_apcd, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
    	datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into #ab_step5
    from(
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
    	lag(last_service_date, 1, last_service_date) over (partition by id_apcd order by last_service_date) as date_compare_lag1
    	from #ab_step4
    ) as a;
    
    --Group AB endpoints into episodes based on minimum spacing
    if object_id(N'tempdb..#ab_final') is not null drop table #ab_final;
    with
    t as (
        select t.id_apcd, t.last_service_date, t.preg_endpoint, t.preg_hier, t.preg_endpoint_rank, t.days_diff
    	from #ab_step5 as t
    ),
    cte as (
        select t.*, t.days_diff as days_diff_cum, 1 as timeline_include -- to keep first endpoint
        from t
        where t.preg_endpoint_rank = 1
        union all
        select t.*,
    	--generate cumulative days diff that resets when threshold is reached
    	case
    		when cte.days_diff_cum + t.days_diff > 56 then 0
            else cte.days_diff_cum + t.days_diff
        end as days_diff_cum,
    	--generate variable to flag inclusion on timeline
    	case
    		when cte.days_diff_cum + t.days_diff > 56 then 1
    		else 0
    	end as timeline_include
        from cte
    	inner join t
        on (t.id_apcd = cte.id_apcd) and (t.preg_endpoint_rank = cte.preg_endpoint_rank + 1)
    )
    --Clean up group column, keep only endpoints added to the timeline, and select columns to keep
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
    	rank() over (partition by id_apcd order by last_service_date) as preg_episode_id
    into #ab_final
    from cte
    where timeline_include = 1;
    
    --Union LB, SB, DELIV, TRO, ECT and AB endpoints placed on timeline
    if object_id(N'tempdb..#lb_sb_deliv_tro_ect_ab_final') is not null drop table #lb_sb_deliv_tro_ect_ab_final;
    select * into #lb_sb_deliv_tro_ect_ab_final from #lb_sb_deliv_tro_ect_final
    union
    select * from #ab_final;
    
    --Clean up temp tables
    if object_id(N'tempdb..#ab_step1') is not null drop table #ab_step1;
    if object_id(N'tempdb..#ab_step2') is not null drop table #ab_step2;
    if object_id(N'tempdb..#ab_step3') is not null drop table #ab_step3;
    if object_id(N'tempdb..#ab_step4') is not null drop table #ab_step4;
    if object_id(N'tempdb..#ab_step5') is not null drop table #ab_step5;
    if object_id(N'tempdb..#lb_sb_deliv_tro_ect_final') is not null drop table #lb_sb_deliv_tro_ect_final;
    if object_id(N'tempdb..#ab_final') is not null drop table #ab_final;
    
    
    -------
    --Step 5G: PROCESS SA EPISODES
    -------
    
    --Union LB-SB, DELIV, TRO, ECT, AB, and SA endpoints
    if object_id(N'tempdb..#sa_step1') is not null drop table #sa_step1;
    select *, last_service_date as prior_date, last_service_date as next_date into #sa_step1 from #lb_sb_deliv_tro_ect_ab_final
    union
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
    	null as prior_date, null as next_date
    from #preg_endpoint where preg_endpoint = 'sa'
    order by last_service_date;
    
    --Create column to hold dates of LB, SB, DELIV, TRO, ECT, and AB endpoints for comparison
    if object_id(N'tempdb..#sa_step2') is not null drop table #sa_step2;
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
    --create column to hold date of prior LB
    case when preg_endpoint = 'sa' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #sa_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'lb'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_lb_date,
    --create column to hold date of next LB
    case when preg_endpoint = 'sa' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #sa_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'lb'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_lb_date,
    --create column to hold date of prior SB
    case when preg_endpoint = 'sa' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #sa_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'sb'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_sb_date,
    --create column to hold date of next SB
    case when preg_endpoint = 'sa' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #sa_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'sb'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_sb_date,
    --create column to hold date of prior DELIV
    case when preg_endpoint = 'sa' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #sa_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'deliv'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_deliv_date,
    --create column to hold date of next DELIV
    case when preg_endpoint = 'sa' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #sa_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'deliv'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_deliv_date,
    --create column to hold date of prior TRO
    case when preg_endpoint = 'sa' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #sa_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'tro'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_tro_date,
    --create column to hold date of next TRO
    case when preg_endpoint = 'sa' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #sa_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'tro'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_tro_date,
    --create column to hold date of prior ECT
    case when preg_endpoint = 'sa' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #sa_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'ect'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_ect_date,
    --create column to hold date of next ECT
    case when preg_endpoint = 'sa' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #sa_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'ect'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_ect_date,
    --create column to hold date of prior AB
    case when preg_endpoint = 'sa' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #sa_step1
    	WHERE id_apcd = t.id_apcd and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'ab'
    	ORDER BY id_apcd, last_service_date DESC))
    else null
    end as prior_ab_date,
    --create column to hold date of next AB
    case when preg_endpoint = 'sa' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #sa_step1
    	WHERE id_apcd = t.id_apcd and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'ab'
    	ORDER BY id_apcd, last_service_date))
    else null
    end as next_ab_date
    into #sa_step2
    from #sa_step1 as t;
    
    --For each SA endpoint, count days between it and prior and next LB, SB, DELIV, TRO, ECT, and AB endpoints
    if object_id(N'tempdb..#sa_step3') is not null drop table #sa_step3;
    select *,	
    	case when preg_endpoint = 'sa' then datediff(day, prior_lb_date, last_service_date) else null end as days_diff_back_lb,
    	case when preg_endpoint = 'sa' then datediff(day, next_lb_date, last_service_date) else null end as days_diff_ahead_lb,
    	case when preg_endpoint = 'sa' then datediff(day, prior_sb_date, last_service_date) else null end as days_diff_back_sb,
    	case when preg_endpoint = 'sa' then datediff(day, next_sb_date, last_service_date) else null end as days_diff_ahead_sb,
    	case when preg_endpoint = 'sa' then datediff(day, prior_deliv_date, last_service_date) else null end as days_diff_back_deliv,
    	case when preg_endpoint = 'sa' then datediff(day, next_deliv_date, last_service_date) else null end as days_diff_ahead_deliv,
    	case when preg_endpoint = 'sa' then datediff(day, prior_tro_date, last_service_date) else null end as days_diff_back_tro,
    	case when preg_endpoint = 'sa' then datediff(day, next_tro_date, last_service_date) else null end as days_diff_ahead_tro,
    	case when preg_endpoint = 'sa' then datediff(day, prior_ect_date, last_service_date) else null end as days_diff_back_ect,
    	case when preg_endpoint = 'sa' then datediff(day, next_ect_date, last_service_date) else null end as days_diff_ahead_ect,
    	case when preg_endpoint = 'sa' then datediff(day, prior_ab_date, last_service_date) else null end as days_diff_back_ab,
    	case when preg_endpoint = 'sa' then datediff(day, next_ab_date, last_service_date) else null end as days_diff_ahead_ab
    into #sa_step3
    from #sa_step2;
    
    --Pull out SA timepoints that potentially can be placed on timeline - COMPARE TO LB, SB, DELIV, TRO, ECT, and AB ENDPOINTS
    if object_id(N'tempdb..#sa_step4') is not null drop table #sa_step4;
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
    into #sa_step4
    from #sa_step3
    where preg_endpoint = 'sa'
    	and (days_diff_back_lb is null or days_diff_back_lb > 168)
    	and (days_diff_ahead_lb is null or days_diff_ahead_lb < -168)
    	and (days_diff_back_sb is null or days_diff_back_sb > 154)
    	and (days_diff_ahead_sb is null or days_diff_ahead_sb < -154)
    	and (days_diff_back_deliv is null or days_diff_back_deliv > 154)
    	and (days_diff_ahead_deliv is null or days_diff_ahead_deliv < -154)
    	and (days_diff_back_tro is null or days_diff_back_tro > 56)
    	and (days_diff_ahead_tro is null or days_diff_ahead_tro < -56)
    	and (days_diff_back_ect is null or days_diff_back_ect > 56)
    	and (days_diff_ahead_ect is null or days_diff_ahead_ect < -56)
    	and (days_diff_back_ab is null or days_diff_back_ab > 56)
    	and (days_diff_ahead_ab is null or days_diff_ahead_ab < -56);
    
    --Count days between each SA endpoint and regenerate preg_endpoint_rank variable
    if object_id(N'tempdb..#sa_step5') is not null drop table #sa_step5;
    select a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier,
    	rank() over (partition by a.id_apcd, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
    	datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into #sa_step5
    from(
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
    	lag(last_service_date, 1, last_service_date) over (partition by id_apcd order by last_service_date) as date_compare_lag1
    	from #sa_step4
    ) as a;
    
    --Group SA endpoints into episodes based on minimum spacing
    if object_id(N'tempdb..#sa_final') is not null drop table #sa_final;
    with
    t as (
        select t.id_apcd, t.last_service_date, t.preg_endpoint, t.preg_hier, t.preg_endpoint_rank, t.days_diff
    	from #sa_step5 as t
    ),
    cte as (
        select t.*, t.days_diff as days_diff_cum, 1 as timeline_include -- to keep first endpoint
        from t
        where t.preg_endpoint_rank = 1
        union all
        select t.*,
    	--generate cumulative days diff that resets when threshold is reached
    	case
    		when cte.days_diff_cum + t.days_diff > 42 then 0
            else cte.days_diff_cum + t.days_diff
        end as days_diff_cum,
    	--generate variable to flag inclusion on timeline
    	case
    		when cte.days_diff_cum + t.days_diff > 42 then 1
    		else 0
    	end as timeline_include
        from cte
    	inner join t
        on (t.id_apcd = cte.id_apcd) and (t.preg_endpoint_rank = cte.preg_endpoint_rank + 1)
    )
    --Clean up group column, keep only endpoints added to the timeline, and select columns to keep
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
    	rank() over (partition by id_apcd order by last_service_date) as preg_episode_id
    into #sa_final
    from cte
    where timeline_include = 1;
    
    --Union LB, SB, DELIV, TRO, ECT, AB, and SA endpoints placed on timeline
    if object_id(N'tempdb..#preg_endpoint_union') is not null drop table #preg_endpoint_union;
    select * into #preg_endpoint_union from #lb_sb_deliv_tro_ect_ab_final
    union
    select * from #sa_final
	option (label = 'preg_endpoint_union');
    
    --Clean up temp tables
    if object_id(N'tempdb..#sa_step1') is not null drop table #sa_step1;
    if object_id(N'tempdb..#sa_step2') is not null drop table #sa_step2;
    if object_id(N'tempdb..#sa_step3') is not null drop table #sa_step3;
    if object_id(N'tempdb..#sa_step4') is not null drop table #sa_step4;
    if object_id(N'tempdb..#sa_step5') is not null drop table #sa_step5;
    if object_id(N'tempdb..#lb_sb_deliv_tro_ect_ab_final') is not null drop table #lb_sb_deliv_tro_ect_ab_final;
    if object_id(N'tempdb..#sa_final') is not null drop table #sa_final;
    
    
    --------------------------
    --STEP 6: Regenerate pregnancy episode ID to be unique across dataset
    --Drop preg_endpoint_rank variable as it is no longer needed
    --------------------------
    if object_id(N'tempdb..#episode_0') is not null drop table #episode_0;
    select id_apcd, last_service_date, preg_endpoint, preg_hier,
    	dense_rank() over (order by id_apcd, last_service_date) as preg_episode_id
    into #episode_0
    from #preg_endpoint_union
	option (label = 'episode_0');
    
    
    --------------------------
    --STEP 7: Define prenatal window for each pregnancy episode (<1 min)
    --------------------------
    
    --Create columns to hold information about prior pregnancy episode
    IF OBJECT_ID(N'tempdb..#episode_1') IS NOT NULL drop table #episode_1;
    select *,
    
    --calculate days difference from prior pregnancy outcome
    datediff(day,
    	lag(last_service_date, 1, null) over (partition by id_apcd order by last_service_date),
    	last_service_date) as days_diff_prior,
    
    --create column to hold minumum days buffer between pregnancy episodes
    case when lag(preg_endpoint, 1, null) over (partition by id_apcd order by last_service_date) in ('lb','sb','deliv') then 28
    when lag(preg_endpoint, 1, null) over (partition by id_apcd order by last_service_date) in ('tro','ect','ab','sa') then 14
    else null
    end as days_buffer
    
    into #episode_1
    from #episode_0;
    
    --Calculate start and end dates for each pregnancy episode
    IF OBJECT_ID(N'tempdb..#episode_2') IS NOT NULL drop table #episode_2;
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
    
    --start date of each pregnancy episode, adjusted as needed by prior pregnancies
    case when preg_endpoint in ('lb','sb','deliv') and (days_diff_prior is null or days_diff_prior >= 301) then dateadd(day, -301, last_service_date)
    when preg_endpoint in ('lb','sb','deliv') and days_diff_prior < 301 then dateadd(day, (-1 * days_diff_prior) + days_buffer, last_service_date)
    
    when preg_endpoint = 'tro' and (days_diff_prior is null or days_diff_prior >= 112) then dateadd(day, -112, last_service_date)
    when preg_endpoint = 'tro' and days_diff_prior < 112 then dateadd(day, (-1 * days_diff_prior) + days_buffer, last_service_date)
    
    when preg_endpoint = 'ect' and (days_diff_prior is null or days_diff_prior >= 84) then dateadd(day, -84, last_service_date)
    when preg_endpoint = 'ect' and days_diff_prior < 84 then dateadd(day, (-1 * days_diff_prior) + days_buffer, last_service_date)
    
    when preg_endpoint = 'ab' and (days_diff_prior is null or days_diff_prior >= 168) then dateadd(day, -168, last_service_date)
    when preg_endpoint = 'ab' and days_diff_prior < 168 then dateadd(day, (-1 * days_diff_prior) + days_buffer, last_service_date)
    
    when preg_endpoint = 'sa' and (days_diff_prior is null or days_diff_prior >= 133) then dateadd(day, -133, last_service_date)
    when preg_endpoint = 'sa' and days_diff_prior < 133 then dateadd(day, (-1 * days_diff_prior) + days_buffer, last_service_date)
    
    else null
    end as preg_start_date,
    
    --end date of each pregnancy episode
    last_service_date as preg_end_date
    
    into #episode_2
    from #episode_1 order by last_service_date;
    
    --Confirm that there are no pregnancy episodes with a null start or end date
    --select count (*) as qa_count from #episode_2 where preg_start_date is null or preg_end_date is null;
    
    --Add columns to hold earliest and latest pregnancy start date for later processing
    IF OBJECT_ID(N'tempdb..#preg_episode_temp') IS NOT NULL drop table #preg_episode_temp;
    select *,
    
    --earliest pregnancy start date
    case when preg_endpoint in ('lb','sb','deliv') then dateadd(day, -301, preg_end_date)
    when preg_endpoint = 'tro' then dateadd(day, -112, preg_end_date)
    when preg_endpoint = 'ect' then dateadd(day, -84, preg_end_date)
    when preg_endpoint = 'ab' then dateadd(day, -168, preg_end_date)
    when preg_endpoint = 'sa' then dateadd(day, -133, preg_end_date)
    else null
    end as preg_start_date_max,
    
    --earliest pregnancy start date
    case when preg_endpoint = 'lb' then dateadd(day, -154, preg_end_date)
    when preg_endpoint in ('sb','deliv') then dateadd(day, -140, preg_end_date)
    when preg_endpoint in ('tro','ect', 'ab') then dateadd(day, -42, preg_end_date)
    when preg_endpoint = 'sa' then dateadd(day, -28, preg_end_date)
    else null
    end as preg_start_date_min
    
    into #preg_episode_temp
    from #episode_2
	option (label = 'preg_episode_temp');
    
    
    --------------------------
    --STEP 8: Use claims that provide information about gestational age to correct pregnancy outcome and start date
    --Uses information in Supplemental Table 5
    --------------------------
    
    ------------
    --Step 8A: Intrauterine insemination/embryo transfer
    ------------
    
    --Pull out pregnany episodes that have relevant procedure codes during prenatal window
    IF OBJECT_ID(N'tempdb..#ga_1a') IS NOT NULL drop table #ga_1a;
    select a.*, b.last_service_date as procedure_date, b.procedure_code
    into #ga_1a
    from #preg_episode_temp as a
    inner join stg_claims.stage_apcd_claim_procedure as b
    on a.id_apcd = b.id_apcd
    where b.procedure_code in ('58321', '58322', 'S4035', '58974', '58976', 'S4037')
    	and b.last_service_date between a.preg_start_date and a.preg_end_date;
    
    --Create column to hold corrected start date
    IF OBJECT_ID(N'tempdb..#ga_1b') IS NOT NULL drop table #ga_1b;
    select distinct id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
    dateadd(day, -13, procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
    into #ga_1b
    from #ga_1a;
    
    --For episodes with more than corrected start, select record that is closet to pregnancy end date
    IF OBJECT_ID(N'tempdb..#ga_1c') IS NOT NULL drop table #ga_1c;
    select a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date_correct, a.preg_end_date,
    	a.preg_start_date_max, a.preg_start_date_min
    into #ga_1c
    from (select *, rank() over (partition by preg_episode_id order by preg_start_date_correct desc) as rank_col from #ga_1b) as a
    where a.rank_col = 1;
    
    --Calculate gestational age in days and weeks
    IF OBJECT_ID(N'tempdb..#ga_1d') IS NOT NULL drop table #ga_1d;
    select *,
    datediff(day, preg_start_date_correct, preg_end_date) + 1 as ga_days,
    cast(round((datediff(day, preg_start_date_correct, preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
    into #ga_1d
    from #ga_1c;
    
    --Create final dataset with flags for plausible pregnancy start date and GA
    IF OBJECT_ID(N'tempdb..#ga_1_final') IS NOT NULL drop table #ga_1_final;
    select *,
    --valid pregnancy start date flag
    case when preg_start_date_correct between preg_start_date_max and preg_start_date_min then 1 else 0 end as valid_start_date,
    --valid GA
    case
    	when preg_endpoint = 'lb' and ga_weeks < 22 then 0
    	when preg_endpoint = 'sb' and ga_weeks < 20 then 0
    	when preg_endpoint = 'sa' and ga_weeks >= 20 then 0
    	else 1
    end as valid_ga,
    --classification of LB episodes
    case
    	when preg_endpoint = 'lb' and ga_weeks >= 37 then 'ftb'
    	when preg_endpoint = 'lb' and ga_weeks < 37 then 'ptb'
    	else null
    end as lb_type,
    --GA estimation step
    1 as ga_estimation_step
    into #ga_1_final
    from #ga_1d;
    
	
    ------------
    --Step 8B: Z3A code on 1st trimester ultrasound
    ------------
    
    --Pull forward episodes not assigned in prior steps
    IF OBJECT_ID(N'tempdb..#ga_2a') IS NOT NULL drop table #ga_2a;
    select a.*
    into #ga_2a
    from #preg_episode_temp as a
    left join #ga_1_final as b
    on a.preg_episode_id = b.preg_episode_id
    where b.preg_episode_id is null;
    
    --Pull out pregnany episodes that have a procedure code for a 1st trimester ultrasound during the prenatal window
    IF OBJECT_ID(N'tempdb..#ga_2b') IS NOT NULL drop table #ga_2b;
    select a.*, b.last_service_date as procedure_date, b.claim_header_id 
    into #ga_2b
    from #ga_2a as a
    inner join stg_claims.stage_apcd_claim_procedure as b
    on a.id_apcd = b.id_apcd
    where b.procedure_code in ('76801', '76802')
    	and b.last_service_date between a.preg_start_date and a.preg_end_date;
    
    --Further subset to pregnancy episodes that have a Z3A code
    IF OBJECT_ID(N'tempdb..#ga_2c') IS NOT NULL drop table #ga_2c;
    select a.*, right(icdcm_norm, 2) as ga_weeks_int
    into #ga_2c
    from #ga_2b as a
    inner join stg_claims.stage_apcd_claim_icdcm_header as b
    on a.claim_header_id = b.claim_header_id
    where b.icdcm_version = 10 and b.icdcm_norm in ('Z3A08', 'Z3A09', 'Z3A10', 'Z3A11', 'Z3A12', 'Z3A13', 'Z3A14', 'Z3A15', 'Z3A16', 'Z3A17', 'Z3A18', 'Z3A19',
    'Z3A20', 'Z3A21', 'Z3A22', 'Z3A23', 'Z3A24', 'Z3A25', 'Z3A26', 'Z3A27', 'Z3A28', 'Z3A29', 'Z3A30', 'Z3A31', 'Z3A32', 'Z3A33', 'Z3A34', 'Z3A35', 'Z3A36',
    'Z3A37', 'Z3A38', 'Z3A39', 'Z3A40', 'Z3A41', 'Z3A42');
    
    --Convert extracted gestational age to integer and collapse to distinct rows
    IF OBJECT_ID(N'tempdb..#ga_2d') IS NOT NULL drop table #ga_2d;
    select distinct id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date,
    	preg_end_date, preg_start_date_max, preg_start_date_min, procedure_date, cast(ga_weeks_int as tinyint) as ga_weeks_int
    into #ga_2d
    from #ga_2c;
    
    --For episodes with multiple first trimester ultrasounds, select first (ranked by procedure date and GA)
    IF OBJECT_ID(N'tempdb..#ga_2e') IS NOT NULL drop table #ga_2e;
    select a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
    	a.preg_start_date_max, a.preg_start_date_min, a.procedure_date, a.ga_weeks_int
    into #ga_2e
    from (select *, rank() over (partition by preg_episode_id order by procedure_date, ga_weeks_int) as rank_col from #ga_2d) as a
    where a.rank_col = 1;
    
    --Create column to hold corrected start date and calculate GA in days and weeks
    IF OBJECT_ID(N'tempdb..#ga_2f') IS NOT NULL drop table #ga_2f;
    select a.*,
    	datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
    	cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
    into #ga_2f
    from (
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
    	dateadd(day, ((-1 * ga_weeks_int * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
    	from #ga_2e
    ) as a;
    
    --Create final dataset with flags for plausible pregnancy start date and GA
    IF OBJECT_ID(N'tempdb..#ga_2_final') IS NOT NULL drop table #ga_2_final;
    select *,
    --valid pregnancy start date flag
    case when preg_start_date_correct between preg_start_date_max and preg_start_date_min then 1 else 0 end as valid_start_date,
    --valid GA
    case
    	when preg_endpoint = 'lb' and ga_weeks < 22 then 0
    	when preg_endpoint = 'sb' and ga_weeks < 20 then 0
    	when preg_endpoint = 'sa' and ga_weeks >= 20 then 0
    	else 1
    end as valid_ga,
    --classification of LB episodes
    case
    	when preg_endpoint = 'lb' and ga_weeks >= 37 then 'ftb'
    	when preg_endpoint = 'lb' and ga_weeks < 37 then 'ptb'
    	else null
    end as lb_type,
    --GA estimation step
    2 as ga_estimation_step
    into #ga_2_final
    from #ga_2f;
 
    --Union assigned episodes thus far
    IF OBJECT_ID(N'tempdb..#ga_1to2_final') IS NOT NULL drop table #ga_1to2_final;
    select * into #ga_1to2_final
    from #ga_1_final union select * from #ga_2_final;
    
    
    ------------
    --Step 8C: Z3A code on NT scan
    ------------
    
    --Pull forward episodes not assigned in prior steps
    IF OBJECT_ID(N'tempdb..#ga_3a') IS NOT NULL drop table #ga_3a;
    select a.*
    into #ga_3a
    from #preg_episode_temp as a
    left join #ga_1to2_final as b
    on a.preg_episode_id = b.preg_episode_id
    where b.preg_episode_id is null;
    
    --Pull out pregnany episodes that have a procedure code for an NT scan during the prenatal window
    IF OBJECT_ID(N'tempdb..#ga_3b') IS NOT NULL drop table #ga_3b;
    select a.*, b.last_service_date as procedure_date, b.claim_header_id 
    into #ga_3b
    from #ga_3a as a
    inner join stg_claims.stage_apcd_claim_procedure as b
    on a.id_apcd = b.id_apcd
    where b.procedure_code in ('76813', '76814')
    	and b.last_service_date between a.preg_start_date and a.preg_end_date;
    
    --Further subset to pregnancy episodes that have a Z3A code
    IF OBJECT_ID(N'tempdb..#ga_3c') IS NOT NULL drop table #ga_3c;
    select a.*, right(icdcm_norm, 2) as ga_weeks_int
    into #ga_3c
    from #ga_3b as a
    inner join stg_claims.stage_apcd_claim_icdcm_header as b
    on a.claim_header_id = b.claim_header_id
    where b.icdcm_version = 10 and b.icdcm_norm in ('Z3A08', 'Z3A09', 'Z3A10', 'Z3A11', 'Z3A12', 'Z3A13', 'Z3A14', 'Z3A15', 'Z3A16', 'Z3A17', 'Z3A18', 'Z3A19',
    'Z3A20', 'Z3A21', 'Z3A22', 'Z3A23', 'Z3A24', 'Z3A25', 'Z3A26', 'Z3A27', 'Z3A28', 'Z3A29', 'Z3A30', 'Z3A31', 'Z3A32', 'Z3A33', 'Z3A34', 'Z3A35', 'Z3A36',
    'Z3A37', 'Z3A38', 'Z3A39', 'Z3A40', 'Z3A41', 'Z3A42');
    
    --Convert extracted gestational age to integer and collapse to distinct rows
    IF OBJECT_ID(N'tempdb..#ga_3d') IS NOT NULL drop table #ga_3d;
    select distinct id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date,
    	preg_end_date, preg_start_date_max, preg_start_date_min, procedure_date, cast(ga_weeks_int as tinyint) as ga_weeks_int
    into #ga_3d
    from #ga_3c;
    
    --For episodes with multiple NT scans, select first (ranked by procedure date and GA)
    IF OBJECT_ID(N'tempdb..#ga_3e') IS NOT NULL drop table #ga_3e;
    select a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
    	a.preg_start_date_max, a.preg_start_date_min, a.procedure_date, a.ga_weeks_int
    into #ga_3e
    from (select *, rank() over (partition by preg_episode_id order by procedure_date, ga_weeks_int) as rank_col from #ga_3d) as a
    where a.rank_col = 1;
    
    --Create column to hold corrected start date and calculate GA in days and weeks
    IF OBJECT_ID(N'tempdb..#ga_3f') IS NOT NULL drop table #ga_3f;
    select a.*,
    	datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
    	cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
    into #ga_3f
    from (
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
    	dateadd(day, ((-1 * ga_weeks_int * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
    	from #ga_3e
    ) as a;
    
    --Create final dataset with flags for plausible pregnancy start date and GA
    IF OBJECT_ID(N'tempdb..#ga_3_final') IS NOT NULL drop table #ga_3_final;
    select *,
    --valid pregnancy start date flag
    case when preg_start_date_correct between preg_start_date_max and preg_start_date_min then 1 else 0 end as valid_start_date,
    --valid GA
    case
    	when preg_endpoint = 'lb' and ga_weeks < 22 then 0
    	when preg_endpoint = 'sb' and ga_weeks < 20 then 0
    	when preg_endpoint = 'sa' and ga_weeks >= 20 then 0
    	else 1
    end as valid_ga,
    --classification of LB episodes
    case
    	when preg_endpoint = 'lb' and ga_weeks >= 37 then 'ftb'
    	when preg_endpoint = 'lb' and ga_weeks < 37 then 'ptb'
    	else null
    end as lb_type,
    --GA estimation step
    3 as ga_estimation_step
    into #ga_3_final
    from #ga_3f;
    
    --Union assigned episodes thus far
    IF OBJECT_ID(N'tempdb..#ga_1to3_final') IS NOT NULL drop table #ga_1to3_final;
    select * into #ga_1to3_final from #ga_1to2_final
    union select * from #ga_3_final;
    
    --Drop prior union table
    IF OBJECT_ID(N'tempdb..#ga_1to2_final') IS NOT NULL drop table #ga_1to2_final;
    
    
    ------------
    --Step 8D: Z3A code on anatomic ultrasound
    ------------
    
    --Pull forward episodes not assigned in prior steps
    IF OBJECT_ID(N'tempdb..#ga_4a') IS NOT NULL drop table #ga_4a;
    select a.*
    into #ga_4a
    from #preg_episode_temp as a
    left join #ga_1to3_final as b
    on a.preg_episode_id = b.preg_episode_id
    where b.preg_episode_id is null;
    
    --Pull out pregnany episodes that have a procedure code for an anatomic ultrasound during the prenatal window
    IF OBJECT_ID(N'tempdb..#ga_4b') IS NOT NULL drop table #ga_4b;
    select a.*, b.last_service_date as procedure_date, b.claim_header_id 
    into #ga_4b
    from #ga_4a as a
    inner join stg_claims.stage_apcd_claim_procedure as b
    on a.id_apcd = b.id_apcd
    where b.procedure_code in ('76811', '76812')
    	and b.last_service_date between a.preg_start_date and a.preg_end_date;
    
    --Further subset to pregnancy episodes that have a Z3A code
    IF OBJECT_ID(N'tempdb..#ga_4c') IS NOT NULL drop table #ga_4c;
    select a.*, right(icdcm_norm, 2) as ga_weeks_int
    into #ga_4c
    from #ga_4b as a
    inner join stg_claims.stage_apcd_claim_icdcm_header as b
    on a.claim_header_id = b.claim_header_id
    where b.icdcm_version = 10 and b.icdcm_norm in ('Z3A08', 'Z3A09', 'Z3A10', 'Z3A11', 'Z3A12', 'Z3A13', 'Z3A14', 'Z3A15', 'Z3A16', 'Z3A17', 'Z3A18', 'Z3A19',
    'Z3A20', 'Z3A21', 'Z3A22', 'Z3A23', 'Z3A24', 'Z3A25', 'Z3A26', 'Z3A27', 'Z3A28', 'Z3A29', 'Z3A30', 'Z3A31', 'Z3A32', 'Z3A33', 'Z3A34', 'Z3A35', 'Z3A36',
    'Z3A37', 'Z3A38', 'Z3A39', 'Z3A40', 'Z3A41', 'Z3A42');
    
    --Convert extracted gestational age to integer and collapse to distinct rows
    IF OBJECT_ID(N'tempdb..#ga_4d') IS NOT NULL drop table #ga_4d;
    select distinct id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date,
    	preg_end_date, preg_start_date_max, preg_start_date_min, procedure_date, cast(ga_weeks_int as tinyint) as ga_weeks_int
    into #ga_4d
    from #ga_4c
    where ga_weeks_int not in (0, 1, 49);
    
    --For episodes with multiple anatomic ultrasounds, select first (ranked by procedure date and GA)
    IF OBJECT_ID(N'tempdb..#ga_4e') IS NOT NULL drop table #ga_4e;
    select a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
    	a.preg_start_date_max, a.preg_start_date_min, a.procedure_date, a.ga_weeks_int
    into #ga_4e
    from (select *, rank() over (partition by preg_episode_id order by procedure_date, ga_weeks_int) as rank_col from #ga_4d) as a
    where a.rank_col = 1;
    
    --Create column to hold corrected start date and calculate GA in days and weeks
    IF OBJECT_ID(N'tempdb..#ga_4f') IS NOT NULL drop table #ga_4f;
    select a.*,
    	datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
    	cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
    into #ga_4f
    from (
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
    	dateadd(day, ((-1 * ga_weeks_int * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
    	from #ga_4e
    ) as a;
    
    --Create final dataset with flags for plausible pregnancy start date and GA
    IF OBJECT_ID(N'tempdb..#ga_4_final') IS NOT NULL drop table #ga_4_final;
    select *,
    --valid pregnancy start date flag
    case when preg_start_date_correct between preg_start_date_max and preg_start_date_min then 1 else 0 end as valid_start_date,
    --valid GA
    case
    	when preg_endpoint = 'lb' and ga_weeks < 22 then 0
    	when preg_endpoint = 'sb' and ga_weeks < 20 then 0
    	when preg_endpoint = 'sa' and ga_weeks >= 20 then 0
    	else 1
    end as valid_ga,
    --classification of LB episodes
    case
    	when preg_endpoint = 'lb' and ga_weeks >= 37 then 'ftb'
    	when preg_endpoint = 'lb' and ga_weeks < 37 then 'ptb'
    	else null
    end as lb_type,
    --GA estimation step
    4 as ga_estimation_step
    into #ga_4_final
    from #ga_4f;
    
    --Union assigned episodes thus far
    IF OBJECT_ID(N'tempdb..#ga_1to4_final') IS NOT NULL drop table #ga_1to4_final;
    select * into #ga_1to4_final from #ga_1to3_final
    union select * from #ga_4_final;
    
    --Drop prior union table
    IF OBJECT_ID(N'tempdb..#ga_1to3_final') IS NOT NULL drop table #ga_1to3_final;
    
    
    ------------
    --Step 8E: Z3A code on another type of prenatal service
    ------------
    
    --Pull forward episodes not assigned in prior steps
    IF OBJECT_ID(N'tempdb..#ga_5a') IS NOT NULL drop table #ga_5a;
    select a.*
    into #ga_5a
    from #preg_episode_temp as a
    left join #ga_1to4_final as b
    on a.preg_episode_id = b.preg_episode_id
    where b.preg_episode_id is null;
    
    --Pull out pregnany episodes that have a Z3A code during the prenatal window
    IF OBJECT_ID(N'tempdb..#ga_5b') IS NOT NULL drop table #ga_5b;
    select a.*, b.last_service_date as procedure_date, right(b.icdcm_norm, 2) as ga_weeks_int
    into #ga_5b
    from #ga_5a as a
    inner join stg_claims.stage_apcd_claim_icdcm_header as b
    on a.id_apcd = b.id_apcd
    where (b.icdcm_version = 10) and b.icdcm_norm in ('Z3A08', 'Z3A09', 'Z3A10', 'Z3A11', 'Z3A12', 'Z3A13', 'Z3A14', 'Z3A15', 'Z3A16', 'Z3A17', 'Z3A18', 'Z3A19',
    'Z3A20', 'Z3A21', 'Z3A22', 'Z3A23', 'Z3A24', 'Z3A25', 'Z3A26', 'Z3A27', 'Z3A28', 'Z3A29', 'Z3A30', 'Z3A31', 'Z3A32', 'Z3A33', 'Z3A34', 'Z3A35', 'Z3A36',
    'Z3A37', 'Z3A38', 'Z3A39', 'Z3A40', 'Z3A41', 'Z3A42') and (b.last_service_date between a.preg_start_date and a.preg_end_date);
    
    --Convert ZA3 codes to integer (needed to be a separate step to avoid a cast error)
    IF OBJECT_ID(N'tempdb..#ga_5c') IS NOT NULL drop table #ga_5c;
    select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date, preg_end_date,
    	preg_start_date_max, preg_start_date_min, procedure_date, cast(ga_weeks_int as tinyint) as ga_weeks_int
    into #ga_5c
    from #ga_5b;
    
    --Collapse to distinct rows
    IF OBJECT_ID(N'tempdb..#ga_5d') IS NOT NULL drop table #ga_5d;
    select distinct id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date,
    	preg_end_date, preg_start_date_max, preg_start_date_min, procedure_date, ga_weeks_int
    into #ga_5d
    from #ga_5c;
    
    --Create column to hold corrected start date and calculate GA in days and weeks
    IF OBJECT_ID(N'tempdb..#ga_5e') IS NOT NULL drop table #ga_5e;
    select a.*,
    	datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
    	cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
    into #ga_5e
    from (
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
    	dateadd(day, ((-1 * ga_weeks_int * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
    	from #ga_5d
    ) as a;
    
    --Count distinct preg start dates for each episode and calculate mode and median for those with >1 start date
    IF OBJECT_ID(N'tempdb..#ga_5f') IS NOT NULL drop table #ga_5f;
    select c.*,
    --Descending rank of pregnany start date count
    rank() over (partition by preg_episode_id order by preg_start_row_count desc) as preg_start_mode_rank
    into #ga_5f
    from (
    	select a.*, b.preg_start_dcount,
    
    	--Count of start dates within each pregnancy episode
    	case when b.preg_start_dcount > 1 then count(*) over (partition by a.preg_episode_id, a.preg_start_date_correct order by a.preg_start_date_correct)
    	else null
    	end as preg_start_row_count,
    
    	--Median of start dates
    	case when b.preg_start_dcount > 1 then percentile_disc(0.5) within group (order by a.preg_start_date_correct) over (partition by a.preg_episode_id)
    	else null
    	end as preg_start_median
    
    	from #ga_5e as a
    	left join (
    		--Join to counts of distinct pregnancy start dates within each episode
    		select preg_episode_id, count(distinct preg_start_date_correct) as preg_start_dcount
    		from #ga_5e
    		group by preg_episode_id
    	) as b
    	on a.preg_episode_id = b.preg_episode_id
    ) as c;
    
    --Count number of distinct start dates by preg episode and rank
    IF OBJECT_ID(N'tempdb..#ga_5g') IS NOT NULL drop table #ga_5g;
    select a.*, b.preg_start_rank_dcount
    into #ga_5g
    from #ga_5f as a
    left join(
    	select preg_episode_id, preg_start_mode_rank, count(distinct preg_start_date_correct) as preg_start_rank_dcount
    	from #ga_5f
    	group by preg_episode_id, preg_start_mode_rank
    ) as b
    on (a.preg_episode_id = b.preg_episode_id) and (a.preg_start_mode_rank) = (b.preg_start_mode_rank);
    
    --Create flag to indicate whether mode exists for episodes with >1 start date
    IF OBJECT_ID(N'tempdb..#ga_5h') IS NOT NULL drop table #ga_5h;
    select *,
    max(case when preg_start_mode_rank = 1 and preg_start_rank_dcount = 1 then 1 else 0 end)
    	over (partition by preg_episode_id) as preg_start_mode_present
    into #ga_5h
    from #ga_5g;
    
    --Create flag to indicate pregnancy start date to keep
    IF OBJECT_ID(N'tempdb..#ga_5i') IS NOT NULL drop table #ga_5i;
    select *,
    case
    	when preg_start_dcount = 1 then 1
    	when preg_start_mode_rank = 1 and preg_start_rank_dcount = 1 then 1
    	when preg_start_mode_present = 0 and preg_start_date_correct = preg_start_median then 1
    	else 0
    end as preg_start_date_keep
    into #ga_5i
    from #ga_5h;
    
    --Keep one pregnancy start date per episode
    IF OBJECT_ID(N'tempdb..#ga_5j') IS NOT NULL drop table #ga_5j;
    select distinct id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date_correct,
    	preg_end_date, preg_start_date_max, preg_start_date_min,
    	ga_days,
    	ga_weeks
    into #ga_5j
    from #ga_5i
    where preg_start_date_keep = 1;
    
    --Create final dataset with flags for plausible pregnancy start date and GA
    IF OBJECT_ID(N'tempdb..#ga_5_final') IS NOT NULL drop table #ga_5_final;
    select *,
    --valid pregnancy start date flag
    case when preg_start_date_correct between preg_start_date_max and preg_start_date_min then 1 else 0 end as valid_start_date,
    --valid GA
    case
    	when preg_endpoint = 'lb' and ga_weeks < 22 then 0
    	when preg_endpoint = 'sb' and ga_weeks < 20 then 0
    	when preg_endpoint = 'sa' and ga_weeks >= 20 then 0
    	else 1
    end as valid_ga,
    --classification of LB episodes
    case
    	when preg_endpoint = 'lb' and ga_weeks >= 37 then 'ftb'
    	when preg_endpoint = 'lb' and ga_weeks < 37 then 'ptb'
    	else null
    end as lb_type,
    --GA estimation step
    5 as ga_estimation_step
    into #ga_5_final
    from #ga_5j;
    
    --Union assigned episodes thus far
    --Beginning with this step (5), propagate episodes with invalid start date to next step
    IF OBJECT_ID(N'tempdb..#ga_1to5_final') IS NOT NULL drop table #ga_1to5_final;
    select * into #ga_1to5_final from #ga_1to4_final
    union select * from #ga_5_final where valid_start_date = 1 and valid_ga = 1;
    
    --Drop prior union table
    IF OBJECT_ID(N'tempdb..#ga_1to4_final') IS NOT NULL drop table #ga_1to4_final;
    
    
    ------------
    --Step 8F: Nuchal translucency (NT) scan without Z3A code
    ------------
    
    --Pull forward episodes not assigned in prior steps
    IF OBJECT_ID(N'tempdb..#ga_6a') IS NOT NULL drop table #ga_6a;
    select a.*
    into #ga_6a
    from #preg_episode_temp as a
    left join #ga_1to5_final as b
    on a.preg_episode_id = b.preg_episode_id
    where b.preg_episode_id is null;
    
    --Pull out pregnany episodes that have a procedure code for an NT scan during the prenatal window, collapse to distinct rows
    IF OBJECT_ID(N'tempdb..#ga_6b') IS NOT NULL drop table #ga_6b;
    select distinct a.*, b.last_service_date as procedure_date
    into #ga_6b
    from #ga_6a as a
    inner join stg_claims.stage_apcd_claim_procedure as b
    on a.id_apcd = b.id_apcd
    where b.procedure_code in ('76813', '76814')
    	and b.last_service_date between a.preg_start_date and a.preg_end_date;
    
    --For episodes with multiple NT scans, select first (ranked by procedure date) and take distinct rows
    IF OBJECT_ID(N'tempdb..#ga_6c') IS NOT NULL drop table #ga_6c;
    select a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
    	a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
    into #ga_6c
    from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from #ga_6b) as a
    where a.rank_col = 1;
    
    --Create column to hold corrected start date and calculate GA in days and weeks
    IF OBJECT_ID(N'tempdb..#ga_6d') IS NOT NULL drop table #ga_6d;
    select a.*,
    	datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
    	cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
    into #ga_6d
    from (
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
    	dateadd(day, -89, procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
    	from #ga_6c
    ) as a;
    
    --Create final dataset with flags for plausible pregnancy start date and GA
    IF OBJECT_ID(N'tempdb..#ga_6_final') IS NOT NULL drop table #ga_6_final;
    select *,
    --valid pregnancy start date flag
    case when preg_start_date_correct between preg_start_date_max and preg_start_date_min then 1 else 0 end as valid_start_date,
    --valid GA
    case
    	when preg_endpoint = 'lb' and ga_weeks < 22 then 0
    	when preg_endpoint = 'sb' and ga_weeks < 20 then 0
    	when preg_endpoint = 'sa' and ga_weeks >= 20 then 0
    	else 1
    end as valid_ga,
    --classification of LB episodes
    case
    	when preg_endpoint = 'lb' and ga_weeks >= 37 then 'ftb'
    	when preg_endpoint = 'lb' and ga_weeks < 37 then 'ptb'
    	else null
    end as lb_type,
    --GA estimation step
    6 as ga_estimation_step
    into #ga_6_final
    from #ga_6d;
    
    --Union assigned episodes thus far
    IF OBJECT_ID(N'tempdb..#ga_1to6_final') IS NOT NULL drop table #ga_1to6_final;
    select * into #ga_1to6_final from #ga_1to5_final
    union select * from #ga_6_final where valid_start_date = 1 and valid_ga = 1;
    
    --Drop prior union table
    IF OBJECT_ID(N'tempdb..#ga_1to5_final') IS NOT NULL drop table #ga_1to5_final;
    
    
    ------------
    --Step 8G: Chorionic Villus Sampling (CVS)
    ------------
    
    --Pull forward episodes not assigned in prior steps
    IF OBJECT_ID(N'tempdb..#ga_7a') IS NOT NULL drop table #ga_7a;
    select a.*
    into #ga_7a
    from #preg_episode_temp as a
    left join #ga_1to6_final as b
    on a.preg_episode_id = b.preg_episode_id
    where b.preg_episode_id is null;
    
    --Pull out pregnany episodes that have a procedure code for CVS during the prenatal window, collapse to distinct rows
    IF OBJECT_ID(N'tempdb..#ga_7b') IS NOT NULL drop table #ga_7b;
    select distinct a.*, b.last_service_date as procedure_date
    into #ga_7b
    from #ga_7a as a
    inner join stg_claims.stage_apcd_claim_procedure as b
    on a.id_apcd = b.id_apcd
    where b.procedure_code in ('59015', '76945')
    	and b.last_service_date between a.preg_start_date and a.preg_end_date;
    
    --For episodes with multiple CVS services, select first (ranked by procedure date)
    IF OBJECT_ID(N'tempdb..#ga_7c') IS NOT NULL drop table #ga_7c;
    select a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
    	a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
    into #ga_7c
    from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from #ga_7b) as a
    where a.rank_col = 1;
    
    --Create column to hold corrected start date and calculate GA in days and weeks
    IF OBJECT_ID(N'tempdb..#ga_7d') IS NOT NULL drop table #ga_7d;
    select a.*,
    	datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
    	cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
    into #ga_7d
    from (
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
    	dateadd(day, ((-1 * 12 * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
    	from #ga_7c
    ) as a;
    
    --Create final dataset with flags for plausible pregnancy start date and GA
    IF OBJECT_ID(N'tempdb..#ga_7_final') IS NOT NULL drop table #ga_7_final;
    select *,
    --valid pregnancy start date flag
    case when preg_start_date_correct between preg_start_date_max and preg_start_date_min then 1 else 0 end as valid_start_date,
    --valid GA
    case
    	when preg_endpoint = 'lb' and ga_weeks < 22 then 0
    	when preg_endpoint = 'sb' and ga_weeks < 20 then 0
    	when preg_endpoint = 'sa' and ga_weeks >= 20 then 0
    	else 1
    end as valid_ga,
    --classification of LB episodes
    case
    	when preg_endpoint = 'lb' and ga_weeks >= 37 then 'ftb'
    	when preg_endpoint = 'lb' and ga_weeks < 37 then 'ptb'
    	else null
    end as lb_type,
    --GA estimation step
    7 as ga_estimation_step
    into #ga_7_final
    from #ga_7d;
    
    --Union assigned episodes thus far
    IF OBJECT_ID(N'tempdb..#ga_1to7_final') IS NOT NULL drop table #ga_1to7_final;
    select * into #ga_1to7_final from #ga_1to6_final
    union select * from #ga_7_final where valid_start_date = 1 and valid_ga = 1;
    
    --Drop prior union table
    IF OBJECT_ID(N'tempdb..#ga_1to6_final') IS NOT NULL drop table #ga_1to6_final;
    
    
    ------------
    --Step 8H: Cell free fetal DNA screening
    ------------
    
    --Pull forward episodes not assigned in prior steps
    IF OBJECT_ID(N'tempdb..#ga_8a') IS NOT NULL drop table #ga_8a;
    select a.*
    into #ga_8a
    from #preg_episode_temp as a
    left join #ga_1to7_final as b
    on a.preg_episode_id = b.preg_episode_id
    where b.preg_episode_id is null;
    
    --Pull out pregnany episodes that have a procedure code for cell-free DNA sampling during the prenatal window, collapse to distinct rows
    IF OBJECT_ID(N'tempdb..#ga_8b') IS NOT NULL drop table #ga_8b;
    select distinct a.*, b.last_service_date as procedure_date
    into #ga_8b
    from #ga_8a as a
    inner join stg_claims.stage_apcd_claim_procedure as b
    on a.id_apcd = b.id_apcd
    where b.procedure_code in ('81420', '81507')
    	and b.last_service_date between a.preg_start_date and a.preg_end_date;
    
    --For episodes with multiple cell-free DNA sampling services, select first (ranked by procedure date)
    IF OBJECT_ID(N'tempdb..#ga_8c') IS NOT NULL drop table #ga_8c;
    select a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
    	a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
    into #ga_8c
    from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from #ga_8b) as a
    where a.rank_col = 1;
    
    --Create column to hold corrected start date and calculate GA in days and weeks
    IF OBJECT_ID(N'tempdb..#ga_8d') IS NOT NULL drop table #ga_8d;
    select a.*,
    	datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
    	cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
    into #ga_8d
    from (
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
    	dateadd(day, ((-1 * 12 * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
    	from #ga_8c
    ) as a;
    
    --Create final dataset with flags for plausible pregnancy start date and GA
    IF OBJECT_ID(N'tempdb..#ga_8_final') IS NOT NULL drop table #ga_8_final;
    select *,
    --valid pregnancy start date flag
    case when preg_start_date_correct between preg_start_date_max and preg_start_date_min then 1 else 0 end as valid_start_date,
    --valid GA
    case
    	when preg_endpoint = 'lb' and ga_weeks < 22 then 0
    	when preg_endpoint = 'sb' and ga_weeks < 20 then 0
    	when preg_endpoint = 'sa' and ga_weeks >= 20 then 0
    	else 1
    end as valid_ga,
    --classification of LB episodes
    case
    	when preg_endpoint = 'lb' and ga_weeks >= 37 then 'ftb'
    	when preg_endpoint = 'lb' and ga_weeks < 37 then 'ptb'
    	else null
    end as lb_type,
    --GA estimation step
    8 as ga_estimation_step
    into #ga_8_final
    from #ga_8d;

    --Union assigned episodes thus far
    IF OBJECT_ID(N'tempdb..#ga_1to8_final') IS NOT NULL drop table #ga_1to8_final;
    select * into #ga_1to8_final from #ga_1to7_final
    union select * from #ga_8_final where valid_start_date = 1 and valid_ga = 1;
    
    --Drop prior union table
    IF OBJECT_ID(N'tempdb..#ga_1to7_final') IS NOT NULL drop table #ga_1to7_final;
    
    
    ------------
    --Step 8I: Full-term code for live birth or stillbirth within 7 days of outcome date
    ------------
    
    --Pull forward episodes not assigned in prior steps
    IF OBJECT_ID(N'tempdb..#ga_9a') IS NOT NULL drop table #ga_9a;
    select a.*
    into #ga_9a
    from #preg_episode_temp as a
    left join #ga_1to8_final as b
    on a.preg_episode_id = b.preg_episode_id
    where b.preg_episode_id is null;
    
    --Pull out distinct ICD-10-CM codes from claims >= 2016-01-01 (<1 min)
    IF OBJECT_ID(N'tempdb..#dx_distinct_step8') IS NOT NULL drop table #dx_distinct_step8;
    select distinct icdcm_norm
    into #dx_distinct_step8
    from stg_claims.stage_apcd_claim_icdcm_header
    where last_service_date >= '2016-01-01';
    
    --Create temp table holding full-term status ICD-10-CM codes in LIKE format
    IF OBJECT_ID(N'tempdb..#dx_fullterm') IS NOT NULL drop table #dx_fullterm;
    create table #dx_fullterm (code_like varchar(255));
    insert into #dx_fullterm (code_like) values ('O6020%');
    insert into #dx_fullterm (code_like) values ('O6022%');
    insert into #dx_fullterm (code_like) values ('O6023%');
    insert into #dx_fullterm (code_like) values ('O4202%');
    insert into #dx_fullterm (code_like) values ('O4292%');
    insert into #dx_fullterm (code_like) values ('O471%');
    insert into #dx_fullterm (code_like) values ('O80%');
    
    --Join distinct ICD-10-CM codes to full-term ICD-10-CM reference table (<1 min)
    IF OBJECT_ID(N'tempdb..#ref_dx_fullterm') IS NOT NULL drop table #ref_dx_fullterm;
    select distinct a.icdcm_norm, b.code_like
    into #ref_dx_fullterm
    from #dx_distinct_step8 as a
    inner join #dx_fullterm as b
    on a.icdcm_norm like b.code_like;
    
    --Join new reference table to claims data using EXACT join (1 min)
    IF OBJECT_ID(N'tempdb..#fullterm_dx') IS NOT NULL drop table #fullterm_dx;
    select a.id_apcd, a.last_service_date, a.icdcm_norm
    into #fullterm_dx
    from stg_claims.stage_apcd_claim_icdcm_header as a
    inner join #ref_dx_fullterm as b
    on a.icdcm_norm = b.icdcm_norm
    where a.last_service_date >= '2016-01-01';
    
    --Pull out pregnancy episodes that have a full-term status ICD-10-CM code within 7 days of the outcome
    --Only include livebirth and stillbirth outcomes
    IF OBJECT_ID(N'tempdb..#ga_9b') IS NOT NULL drop table #ga_9b;
    select a.*, b.last_service_date as procedure_date
    into #ga_9b
    from #ga_9a as a
    inner join #fullterm_dx as b
    on a.id_apcd = b.id_apcd
    where (b.last_service_date between dateadd(day, -7, a.preg_end_date) and dateadd(day, 7, a.preg_end_date))
    	and a.preg_endpoint in ('lb', 'sb');
    
    --For episodes with multiple relevant services, select first (ranked by procedure date) and take distinct rows
    IF OBJECT_ID(N'tempdb..#ga_9c') IS NOT NULL drop table #ga_9c;
    select distinct a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
    	a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
    into #ga_9c
    from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from #ga_9b) as a
    where a.rank_col = 1;
    
    --Create column to hold corrected start date and calculate GA in days and weeks
    IF OBJECT_ID(N'tempdb..#ga_9d') IS NOT NULL drop table #ga_9d;
    select a.*,
    	datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
    	cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
    into #ga_9d
    from (
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
    	dateadd(day, ((-1 * 39 * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
    	from #ga_9c
    ) as a;
    
    --Create final dataset with flags for plausible pregnancy start date and GA
    IF OBJECT_ID(N'tempdb..#ga_9_final') IS NOT NULL drop table #ga_9_final;
    select *,
    --valid pregnancy start date flag
    case when preg_start_date_correct between preg_start_date_max and preg_start_date_min then 1 else 0 end as valid_start_date,
    --valid GA
    case
    	when preg_endpoint = 'lb' and ga_weeks < 22 then 0
    	when preg_endpoint = 'sb' and ga_weeks < 20 then 0
    	when preg_endpoint = 'sa' and ga_weeks >= 20 then 0
    	else 1
    end as valid_ga,
    --classification of LB episodes
    case
    	when preg_endpoint = 'lb' and ga_weeks >= 37 then 'ftb'
    	when preg_endpoint = 'lb' and ga_weeks < 37 then 'ptb'
    	else null
    end as lb_type,
    --GA estimation step
    9 as ga_estimation_step
    into #ga_9_final
    from #ga_9d;
    
    --Union assigned episodes thus far
    IF OBJECT_ID(N'tempdb..#ga_1to9_final') IS NOT NULL drop table #ga_1to9_final;
    select * into #ga_1to9_final from #ga_1to8_final
    union select * from #ga_9_final where valid_start_date = 1 and valid_ga = 1;
    
    --Drop prior union table
    IF OBJECT_ID(N'tempdb..#ga_1to8_final') IS NOT NULL drop table #ga_1to8_final;
    
    
    ------------
    --Step 8J: Trimester codes within 7 days of outcome date
    ------------
    
    --Pull forward episodes not assigned in prior steps
    IF OBJECT_ID(N'tempdb..#ga_10a') IS NOT NULL drop table #ga_10a;
    select a.*
    into #ga_10a
    from #preg_episode_temp as a
    left join #ga_1to9_final as b
    on a.preg_episode_id = b.preg_episode_id
    where b.preg_episode_id is null;
    
    --Join distinct ICD-10-CM codes to ICD-10-CM codes in trimester codes reference table (<1 min)
    IF OBJECT_ID(N'tempdb..#ref_dx_trimester') IS NOT NULL drop table #ref_dx_trimester;
    select distinct a.icdcm_norm, b.code_like, b.trimester
    into #ref_dx_trimester
    from #dx_distinct_step8 as a
    inner join (select * from stg_claims.ref_moll_trimester where code_type = 'icd10cm') as b
    on a.icdcm_norm like b.code_like;
    
    --Join new reference table to claims data using EXACT join (1 min)
    IF OBJECT_ID(N'tempdb..#trimester_dx') IS NOT NULL drop table #trimester_dx;
    select a.id_apcd, a.last_service_date, b.trimester
    into #trimester_dx
    from stg_claims.stage_apcd_claim_icdcm_header as a
    inner join #ref_dx_trimester as b
    on a.icdcm_norm = b.icdcm_norm
    where a.last_service_date >= '2016-01-01';
    
    --Pull out distinct procedure codes from claims >= 2016-01-01 (<1 min)
    IF OBJECT_ID(N'tempdb..#px_distinct_step10') IS NOT NULL drop table #px_distinct_step10;
    select distinct procedure_code
    into #px_distinct_step10
    from stg_claims.stage_apcd_claim_procedure
    where last_service_date >= '2016-01-01';
    
    --Join distinct procedure codes to procedure codes in trimester codes reference table using LIKE join (<1 min)
    IF OBJECT_ID(N'tempdb..#ref_px_trimester') IS NOT NULL drop table #ref_px_trimester;
    select distinct a.procedure_code, b.code_like, b.trimester
    into #ref_px_trimester
    from #px_distinct_step10 as a
    inner join (select * from stg_claims.ref_moll_trimester where code_type = 'cpt_hcpcs') as b
    on a.procedure_code like b.code_like;
    
    --Join new reference table to claims data using EXACT join (1 min)
    IF OBJECT_ID(N'tempdb..#trimester_px') IS NOT NULL drop table #trimester_px;
    select a.id_apcd, a.last_service_date, b.trimester
    into #trimester_px
    from stg_claims.stage_apcd_claim_procedure as a
    inner join #ref_px_trimester as b
    on a.procedure_code = b.procedure_code
    where a.last_service_date >= '2016-01-01';
    
    --Union diagnosis and procedure code tables
    IF OBJECT_ID(N'tempdb..#trimester_dx_px') IS NOT NULL drop table #trimester_dx_px;
    select * into #trimester_dx_px from #trimester_dx
    union select * from #trimester_px;
    
    --Pull out pregnany episodes that have a trimester code within 7 days of the outcome
    IF OBJECT_ID(N'tempdb..#ga_10b') IS NOT NULL drop table #ga_10b;
    select a.*, b.last_service_date as procedure_date, b.trimester
    into #ga_10b
    from #ga_10a as a
    inner join #trimester_dx_px as b
    on a.id_apcd = b.id_apcd
    where (b.last_service_date between dateadd(day, -7, a.preg_end_date) and dateadd(day, 7, a.preg_end_date));
    
    --For episodes with multiple relevant services, select first (ranked by procedure date DESC, trimester) and take distinct rows
    IF OBJECT_ID(N'tempdb..#ga_10c') IS NOT NULL drop table #ga_10c;
    select distinct a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
    	a.preg_start_date_max, a.preg_start_date_min, a.procedure_date, a.trimester
    into #ga_10c
    from (select *, rank() over (partition by preg_episode_id order by procedure_date desc, trimester) as rank_col from #ga_10b) as a
    where a.rank_col = 1;
    
    --Create column to hold corrected start date and calculate GA in days and weeks
    IF OBJECT_ID(N'tempdb..#ga_10d') IS NOT NULL drop table #ga_10d;
    select a.*,
    	datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
    	cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
    into #ga_10d
    from (
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
    	
    	--use trimester code to calculate corrected start date
    	case when trimester = 1 then dateadd(day, -69, procedure_date)
    	when trimester = 2 then dateadd(day, -146, procedure_date)
    	when trimester = 3 then dateadd(day, -240, procedure_date)
    	else null
    	end as preg_start_date_correct,
    	
    	preg_end_date, preg_start_date_max, preg_start_date_min
    	from #ga_10c
    ) as a;
    
    --Create final dataset with flags for plausible pregnancy start date and GA
    IF OBJECT_ID(N'tempdb..#ga_10_final') IS NOT NULL drop table #ga_10_final;
    select *,
    --valid pregnancy start date flag
    case when preg_start_date_correct between preg_start_date_max and preg_start_date_min then 1 else 0 end as valid_start_date,
    --valid GA
    case
    	when preg_endpoint = 'lb' and ga_weeks < 22 then 0
    	when preg_endpoint = 'sb' and ga_weeks < 20 then 0
    	when preg_endpoint = 'sa' and ga_weeks >= 20 then 0
    	else 1
    end as valid_ga,
    --classification of LB episodes
    case
    	when preg_endpoint = 'lb' and ga_weeks >= 37 then 'ftb'
    	when preg_endpoint = 'lb' and ga_weeks < 37 then 'ptb'
    	else null
    end as lb_type,
    --GA estimation step
    10 as ga_estimation_step
    into #ga_10_final
    from #ga_10d;
    
    --Union assigned episodes thus far
    IF OBJECT_ID(N'tempdb..#ga_1to10_final') IS NOT NULL drop table #ga_1to10_final;
    select * into #ga_1to10_final from #ga_1to9_final
    union select * from #ga_10_final where valid_start_date = 1 and valid_ga = 1;
    
    --Drop prior union table
    IF OBJECT_ID(N'tempdb..#ga_1to9_final') IS NOT NULL drop table #ga_1to9_final;
    
    
    ------------
    --Step 8K: Preterm code within 7 days of outcome date
    ------------
    
    --Pull forward episodes not assigned in prior steps
    IF OBJECT_ID(N'tempdb..#ga_11a') IS NOT NULL drop table #ga_11a;
    select a.*
    into #ga_11a
    from #preg_episode_temp as a
    left join #ga_1to10_final as b
    on a.preg_episode_id = b.preg_episode_id
    where b.preg_episode_id is null;
    
    --Create temp table holding preterm status ICD-10-CM codes in LIKE format
    IF OBJECT_ID(N'tempdb..#dx_preterm') IS NOT NULL drop table #dx_preterm;
    create table #dx_preterm (code_like varchar(255));
    insert into #dx_preterm (code_like) values ('O6010%');
    insert into #dx_preterm (code_like) values ('O6012%');
    insert into #dx_preterm (code_like) values ('O6013%');
    insert into #dx_preterm (code_like) values ('O6014%');
    insert into #dx_preterm (code_like) values ('O4201%');
    insert into #dx_preterm (code_like) values ('O4211%');
    insert into #dx_preterm (code_like) values ('O4291%');
  
    --Join distinct ICD-10-CM codes to preterm ICD-10-CM reference table (<1 min)
    IF OBJECT_ID(N'tempdb..#ref_dx_preterm') IS NOT NULL drop table #ref_dx_preterm;
    select distinct a.icdcm_norm, b.code_like
    into #ref_dx_preterm
    from #dx_distinct_step8 as a
    inner join #dx_preterm as b
    on a.icdcm_norm like b.code_like;
    
    --Join new reference table to claims data using EXACT join (1 min)
    IF OBJECT_ID(N'tempdb..#preterm_dx') IS NOT NULL drop table #preterm_dx;
    select a.id_apcd, a.last_service_date, a.icdcm_norm
    into #preterm_dx
    from stg_claims.stage_apcd_claim_icdcm_header as a
    inner join #ref_dx_preterm as b
    on a.icdcm_norm = b.icdcm_norm
    where a.last_service_date >= '2016-01-01';
    
    --Pull out pregnancy episodes that have a preterm status ICD-10-CM code within 7 days of the outcome
    --Only include livebirth and stillbirth outcomes
    IF OBJECT_ID(N'tempdb..#ga_11b') IS NOT NULL drop table #ga_11b;
    select a.*, b.last_service_date as procedure_date
    into #ga_11b
    from #ga_11a as a
    inner join #preterm_dx as b
    on a.id_apcd = b.id_apcd
    where (b.last_service_date between dateadd(day, -7, a.preg_end_date) and dateadd(day, 7, a.preg_end_date));
    
    --For episodes with multiple relevant services, select first (ranked by procedure date) and take distinct rows
    IF OBJECT_ID(N'tempdb..#ga_11c') IS NOT NULL drop table #ga_11c;
    select distinct a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
    	a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
    into #ga_11c
    from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from #ga_11b) as a
    where a.rank_col = 1;
    
    --Create column to hold corrected start date and calculate GA in days and weeks
    IF OBJECT_ID(N'tempdb..#ga_11d') IS NOT NULL drop table #ga_11d;
    select a.*,
    	datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
    	cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
    into #ga_11d
    from (
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
    	dateadd(day, ((-1 * 35 * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
    	from #ga_11c
    ) as a;
    
    --Create final dataset with flags for plausible pregnancy start date and GA
    IF OBJECT_ID(N'tempdb..#ga_11_final') IS NOT NULL drop table #ga_11_final;
    select *,
    --valid pregnancy start date flag
    case when preg_start_date_correct between preg_start_date_max and preg_start_date_min then 1 else 0 end as valid_start_date,
    --valid GA
    case
    	when preg_endpoint = 'lb' and ga_weeks < 22 then 0
    	when preg_endpoint = 'sb' and ga_weeks < 20 then 0
    	when preg_endpoint = 'sa' and ga_weeks >= 20 then 0
    	else 1
    end as valid_ga,
    --classification of LB episodes
    case
    	when preg_endpoint = 'lb' and ga_weeks >= 37 then 'ftb'
    	when preg_endpoint = 'lb' and ga_weeks < 37 then 'ptb'
    	else null
    end as lb_type,
    --GA estimation step
    11 as ga_estimation_step
    into #ga_11_final
    from #ga_11d;
    
    --Union assigned episodes thus far
    IF OBJECT_ID(N'tempdb..#ga_1to11_final') IS NOT NULL drop table #ga_1to11_final;
    select * into #ga_1to11_final from #ga_1to10_final
    union select * from #ga_11_final where valid_start_date = 1 and valid_ga = 1;
    
    --Drop prior union table
    IF OBJECT_ID(N'tempdb..#ga_1to10_final') IS NOT NULL drop table #ga_1to10_final;
    
    
    ------------
    --Step 8L: First glucose screening or tolerance test
    ------------
    
    --Pull forward episodes not assigned in prior steps
    IF OBJECT_ID(N'tempdb..#ga_12a') IS NOT NULL drop table #ga_12a;
    select a.*
    into #ga_12a
    from #preg_episode_temp as a
    left join #ga_1to11_final as b
    on a.preg_episode_id = b.preg_episode_id
    where b.preg_episode_id is null;
    
    --Pull out pregnany episodes that have a procedure code for glucose screening/tolerance test during the prenatal window, collapse to distinct rows
    IF OBJECT_ID(N'tempdb..#ga_12b') IS NOT NULL drop table #ga_12b;
    select distinct a.*, b.last_service_date as procedure_date
    into #ga_12b
    from #ga_12a as a
    inner join stg_claims.stage_apcd_claim_procedure as b
    on a.id_apcd = b.id_apcd
    where b.procedure_code in ('82950', '82951', '82952')
    	and b.last_service_date between a.preg_start_date and a.preg_end_date;
    
    --For episodes with multiple glucose screenings, select first (ranked by procedure date)
    IF OBJECT_ID(N'tempdb..#ga_12c') IS NOT NULL drop table #ga_12c;
    select a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
    	a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
    into #ga_12c
    from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from #ga_12b) as a
    where a.rank_col = 1;
    
    --Create column to hold corrected start date and calculate GA in days and weeks
    IF OBJECT_ID(N'tempdb..#ga_12d') IS NOT NULL drop table #ga_12d;
    select a.*,
    	datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
    	cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
    into #ga_12d
    from (
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
    	dateadd(day, ((-1 * 26 * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
    	from #ga_12c
    ) as a;
    
    --Create final dataset with flags for plausible pregnancy start date and GA
    IF OBJECT_ID(N'tempdb..#ga_12_final') IS NOT NULL drop table #ga_12_final;
    select *,
    --valid pregnancy start date flag
    case when preg_start_date_correct between preg_start_date_max and preg_start_date_min then 1 else 0 end as valid_start_date,
    --valid GA
    case
    	when preg_endpoint = 'lb' and ga_weeks < 22 then 0
    	when preg_endpoint = 'sb' and ga_weeks < 20 then 0
    	when preg_endpoint = 'sa' and ga_weeks >= 20 then 0
    	else 1
    end as valid_ga,
    --classification of LB episodes
    case
    	when preg_endpoint = 'lb' and ga_weeks >= 37 then 'ftb'
    	when preg_endpoint = 'lb' and ga_weeks < 37 then 'ptb'
    	else null
    end as lb_type,
    --GA estimation step
    12 as ga_estimation_step
    into #ga_12_final
    from #ga_12d;
    
    --Union assigned episodes thus far
    IF OBJECT_ID(N'tempdb..#ga_1to12_final') IS NOT NULL drop table #ga_1to12_final;
    select * into #ga_1to12_final from #ga_1to11_final
    union select * from #ga_12_final where valid_start_date = 1 and valid_ga = 1;
    
    --Drop prior union table
    IF OBJECT_ID(N'tempdb..##ga_1to11_final') IS NOT NULL drop table #ga_1to11_final;
    
    
    ------------
    --Step 8M: Prenatal service > 7 days before outcome date with a trimester code
    ------------
    
    --Pull forward episodes not assigned in prior steps
    IF OBJECT_ID(N'tempdb..#ga_13a') IS NOT NULL drop table #ga_13a;
    select a.*
    into #ga_13a
    from #preg_episode_temp as a
    left join #ga_1to12_final as b
    on a.preg_episode_id = b.preg_episode_id
    where b.preg_episode_id is null;
    
    --Pull out pregnany episodes that have a trimester code > 7 days before outcome
    IF OBJECT_ID(N'tempdb..#ga_13b') IS NOT NULL drop table #ga_13b;
    select a.*, b.last_service_date as procedure_date, b.trimester
    into #ga_13b
    from #ga_13a as a
    inner join #trimester_dx_px as b
    on a.id_apcd = b.id_apcd
    where (b.last_service_date < dateadd(day, -7, a.preg_end_date));
    
    --For episodes with multiple relevant services, select first (ranked by procedure date DESC, trimester) and take distinct rows
    IF OBJECT_ID(N'tempdb..#ga_13c') IS NOT NULL drop table #ga_13c;
    select distinct a.id_apcd, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
    	a.preg_start_date_max, a.preg_start_date_min, a.procedure_date, a.trimester
    into #ga_13c
    from (select *, rank() over (partition by preg_episode_id order by procedure_date desc, trimester) as rank_col from #ga_13b) as a
    where a.rank_col = 1;
    
    --Create column to hold corrected start date and calculate GA in days and weeks
    IF OBJECT_ID(N'tempdb..#ga_13d') IS NOT NULL drop table #ga_13d;
    select a.*,
    	datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
    	cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
    into #ga_13d
    from (
    	select id_apcd, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
    	
    	--use trimester code to calculate corrected start date
    	case when trimester = 1 then dateadd(day, -69, procedure_date)
    	when trimester = 2 then dateadd(day, -146, procedure_date)
    	when trimester = 3 then dateadd(day, -240, procedure_date)
    	else null
    	end as preg_start_date_correct,
    	
    	preg_end_date, preg_start_date_max, preg_start_date_min
    	from #ga_13c
    ) as a;
    
    --Create final dataset with flags for plausible pregnancy start date and GA
    IF OBJECT_ID(N'tempdb..#ga_13_final') IS NOT NULL drop table #ga_13_final;
    select *,
    --valid pregnancy start date flag
    case when preg_start_date_correct between preg_start_date_max and preg_start_date_min then 1 else 0 end as valid_start_date,
    --valid GA
    case
    	when preg_endpoint = 'lb' and ga_weeks < 22 then 0
    	when preg_endpoint = 'sb' and ga_weeks < 20 then 0
    	when preg_endpoint = 'sa' and ga_weeks >= 20 then 0
    	else 1
    end as valid_ga,
    --classification of LB episodes
    case
    	when preg_endpoint = 'lb' and ga_weeks >= 37 then 'ftb'
    	when preg_endpoint = 'lb' and ga_weeks < 37 then 'ptb'
    	else null
    end as lb_type,
    --GA estimation step
    13 as ga_estimation_step
    into #ga_13_final
    from #ga_13d;
    
    --Union assigned episodes thus far
    IF OBJECT_ID(N'tempdb..#ga_1to13_final') IS NOT NULL drop table #ga_1to13_final;
    select * into #ga_1to13_final from #ga_1to12_final
    union select * from #ga_13_final where valid_start_date = 1 and valid_ga = 1;
    
    --Drop prior union table
    IF OBJECT_ID(N'tempdb..#ga_1to12_final') IS NOT NULL drop table #ga_1to12_final;
    
    
    ---------------
    --Union episodes that were not flagged by any of the 13 steps
    ---------------
    IF OBJECT_ID(N'tempdb..#preg_episode') IS NOT NULL drop table #preg_episode;
    select id_apcd, preg_episode_id, preg_endpoint, preg_hier, preg_start_date_correct as preg_start_date,
    	preg_end_date, ga_days, ga_weeks, valid_start_date, valid_ga, lb_type, ga_estimation_step
    into #preg_episode
    from #ga_1to13_final
    
    union
    
    select a.id_apcd, a.preg_episode_id, a.preg_endpoint, a.preg_hier, a.preg_start_date,
    	a.preg_end_date, null as ga_days, null as ga_weeks, 0 as valid_start_date, 0 as valid_ga,
    	null as lb_type, null as ga_estimation_step
    
    from #preg_episode_temp as a
    left join #ga_1to13_final as b
    on a.preg_episode_id = b.preg_episode_id
    where b.preg_episode_id is null
	option (label = 'preg_episode');
    

    --------------------------
    --STEP 9: Join to eligibility data to bring in age for subset
    --Also add a summary flag for valid start date and GA
    --------------------------
    IF OBJECT_ID(N'tempdb..#preg_episode_age_all') IS NOT NULL drop table #preg_episode_age_all;
    select a.id_apcd,  
    case
        when (floor((datediff(day, b.dob, a.preg_end_date) + 1) / 365.25) >= 90) or (b.ninety_only = 1) then 90
        when floor((datediff(day, b.dob, a.preg_end_date) + 1) / 365.25) >=0 then floor((datediff(day, b.dob, a.preg_end_date) + 1) / 365.25)
        when floor((datediff(day, b.dob, a.preg_end_date) + 1) / 365.25) = -1 then 0
    end as age_at_outcome,
    a.preg_episode_id, a.preg_endpoint, a.preg_hier, a.preg_start_date, a.preg_end_date, a.ga_days, a.ga_weeks,
    a.valid_start_date, a.valid_ga, case when a.valid_start_date = 1 and a.valid_ga = 1 then 1 else 0 end as valid_both,
    a.lb_type, a.ga_estimation_step, getdate() as last_run
    into #preg_episode_age_all
    from #preg_episode as a
    left join stg_claims.stage_apcd_elig_demo as b
    on a.id_apcd = b.id_apcd
	option (label = 'preg_episode_age_all');
    
    --Create final table subset to ages 12 to 55 per Moll et al. method
    --Also add an age group variable
    insert into stg_claims.stage_apcd_claim_preg_episode
    select id_apcd, age_at_outcome,
    
    case when age_at_outcome between 12 and 19 then '12-19'
    when age_at_outcome between 20 and 24 then '20-24'
    when age_at_outcome between 25 and 29 then '25-29'
    when age_at_outcome between 30 and 34 then '30-34'
    when age_at_outcome between 35 and 39 then '35-39'
    when age_at_outcome between 40 and 55 then '40-55'
    end as age_at_outcome_cat6,
    
    preg_episode_id, preg_endpoint, preg_hier, preg_start_date, preg_end_date, ga_days, ga_weeks,
    valid_start_date, valid_ga, valid_both, lb_type, ga_estimation_step, getdate() as last_run
    
    from #preg_episode_age_all
    where age_at_outcome between 12 and 55
	option (label = 'apcd_claim_preg_episode');",
    .con = dw_inthealth))
}

#### Table-level QA script ####
qa_stage.apcd_claim_preg_episode_f <- function() {
  
  #minimum age check
  res1 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_preg_episode' as 'table', 'minimum age, expect 12' as qa_type,
    min(age_at_outcome) as qa
    from stg_claims.stage_apcd_claim_preg_episode;",
    .con = dw_inthealth))
  
  #maximum age check
  res2 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_preg_episode' as 'table', 'minimum age, expect 55' as qa_type,
    max(age_at_outcome) as qa
    from stg_claims.stage_apcd_claim_preg_episode;",
    .con = dw_inthealth))
  
  #confirm no null dates for pregnancy start and end
  res3 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_preg_episode' as 'table', '# of rows with null start or end date, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_apcd_claim_preg_episode
    where preg_start_date is null or preg_end_date is null;",
    .con = dw_inthealth))
  
  #confirm no null values for GA columns when GA estimation is valid
  res4 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_preg_episode' as 'table', '# of valid GA rows with null GA columns, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_apcd_claim_preg_episode
    where valid_ga = 1 and (ga_days is null or ga_weeks is null or ga_estimation_step is null);",
    .con = dw_inthealth))
  
  #count number of distinct pregnancy endpoint types
  res5 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_preg_episode' as 'table', '# of distinct preg endpoint types, expect 7' as qa_type,
    count(distinct preg_endpoint) as qa
    from stg_claims.stage_apcd_claim_preg_episode;",
    .con = dw_inthealth))
  
  #count number of live birth endpoints where GA is valid and lb_type is null
  res6 <- dbGetQuery(conn = dw_inthealth, glue_sql(
    "select 'stg_claims.stage_apcd_claim_preg_episode' as 'table', '# of LB records with valid GA and null lb_type, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_apcd_claim_preg_episode
    where preg_endpoint = 'lb' and valid_ga = 1 and lb_type is null;",
    .con = dw_inthealth))
  
  res_final <- mget(ls(pattern="^res")) %>% bind_rows()
  
}