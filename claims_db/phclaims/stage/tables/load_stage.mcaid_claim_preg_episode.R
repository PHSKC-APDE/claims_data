# This code creates the the mcaid claim preg episode table
# Create a reference table for pregnancy episodes distributed in mcaid claims
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# R script developed by Jeremy Whitehurst using SQL scripts from Eli Kern, Jennifer Liu and Spencer Hensley
#
## 

## Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims

load_stage_mcaid_claim_preg_episode_f <- function(conn = NULL,
                                          server = c("hhsaw", "phclaims"),
										                      config = NULL,
                                          get_config = F) {
  
  # Set up variables specific to the server
  server <- match.arg(server)
  
  # READ IN CONFIG FILE ----
  # NOTE: The mcaid_mcare YAML files still have the older structure (no server, etc.)
  # If the format is updated, need to add in more robust code here to identify schema and table names
  if (get_config == T){
    if (stringr::str_detect(config, "^http")) {
      config <- yaml::yaml.load(getURL(config))
    } else{
      stop("A URL must be specified in config if using get_config = T")
    }
  }
  
  # VARIABLES ----
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  final_schema <- config[[server]][["final_schema"]]
  final_table <- ifelse(is.null(config[[server]][["final_table"]]), '',
                        config[[server]][["final_table"]])
  ref_schema <- config[[server]][["ref_schema"]]
  ref_table <- ifelse(is.null(config[[server]][["ref_table"]]), '',
                      config[[server]][["ref_table"]])

  message("Creating ", to_schema, ".", to_table, ".")
  time_start <- Sys.time()
  
  #### DROP EXISTING TABLE TO USE SELECT INTO ####
  try(DBI::dbRemoveTable(conn, DBI::Id(schema = to_schema, table = to_table)), silent = T)
  
  #### LOAD TABLE ####
  message("STEP 1: Find claims with a ICD-10-CM code relevant to pregnancy endpoints")
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Pull out distinct ICD-10-CM codes from claims >= 2016-01-01 (<1 min)
    IF OBJECT_ID(N'tempdb..#dx_distinct') IS NOT NULL DROP TABLE #dx_distinct;
    select distinct icdcm_norm
    into #dx_distinct
    from {`final_schema`}.{`paste0(final_table, 'mcaid_claim_icdcm_header')`}
    where last_service_date >= '2016-01-01';", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Join distinct ICD-10-CM codes to pregnancy endpoint reference table using LIKE join (<1 min)
    IF OBJECT_ID(N'tempdb..#ref_dx') IS NOT NULL DROP TABLE #ref_dx;
    select distinct a.icdcm_norm, b.lb, b.ect, b.ab, b.sa, b.sb, b.tro, b.deliv
    into #ref_dx
    from #dx_distinct as a
    inner join (select * from {`ref_schema`}.{`paste0(ref_table, 'moll_preg_endpoint')`} where code_type = 'icd10cm') as b
    on a.icdcm_norm like b.code_like;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Join new reference table to claims data using EXACT join (1 min)
    IF OBJECT_ID(N'tempdb..#preg_dx') IS NOT NULL DROP TABLE #preg_dx;
    select a.id_mcaid, a.claim_header_id, a.last_service_date, a.icdcm_norm, a.icdcm_version, 
	    a.icdcm_number, b.lb, b.ect, b.ab, b.sa, b.sb, b.tro, b.deliv
    into #preg_dx
    from {`final_schema`}.{`paste0(final_table, 'mcaid_claim_icdcm_header')`} as a
    inner join #ref_dx as b
    on a.icdcm_norm = b.icdcm_norm
    where a.last_service_date >= '2016-01-01';", .con = conn))
  
  message("STEP 2: Find claims with a procedure code relevant to pregnancy endpoints")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	  --Pull out distinct procedure codes from claims >= 2016-01-01 (<1 min)
	  IF OBJECT_ID(N'tempdb..#px_distinct') IS NOT NULL DROP TABLE #px_distinct;
    select distinct procedure_code
    into #px_distinct
    from {`final_schema`}.{`paste0(final_table, 'mcaid_claim_procedure')`}
    where last_service_date >= '2016-01-01';", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Join distinct procedure codes to pregnancy endpoint reference table using LIKE join (<1 min)
    IF OBJECT_ID(N'tempdb..#ref_px') IS NOT NULL DROP TABLE #ref_px;
    select distinct a.procedure_code, b.lb, b.ect, b.ab, b.sa, b.sb, b.tro, b.deliv
    into #ref_px
    from #px_distinct as a
    inner join (select * from {`ref_schema`}.{`paste0(ref_table, 'moll_preg_endpoint')`} where code_type in ('icd10pcs', 'hcpcs', 'cpt_hcpcs')) as b
      on a.procedure_code like b.code_like;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Join new reference table to claims data using EXACT join (1 min)
    IF OBJECT_ID(N'tempdb..#preg_px') IS NOT NULL DROP TABLE #preg_px;
    select a.id_mcaid, a.claim_header_id, a.last_service_date, a.procedure_code, 
	    b.lb, b.ect, b.ab, b.sa, b.sb, b.tro, b.deliv
    into #preg_px
    from {`final_schema`}.{`paste0(final_table, 'mcaid_claim_procedure')`} as a
    inner join #ref_px as b
      on a.procedure_code = b.procedure_code
    where a.last_service_date >= '2016-01-01';", .con = conn))

  message("STEP 3: Union dx and px-based datasets, subsetting to common columns to collapse to distinct claim headers")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	  IF OBJECT_ID(N'tempdb..#temp_1') IS NOT NULL DROP TABLE #temp_1;
	  select id_mcaid, claim_header_id, last_service_date, lb, ect, ab, sa, sb, tro, deliv
    into #temp_1
    from #preg_dx
    union
    select id_mcaid, claim_header_id, last_service_date, lb, ect, ab, sa, sb, tro, deliv
    from #preg_px", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    IF OBJECT_ID(N'tempdb..#preg_dx_px') IS NOT NULL DROP TABLE #preg_dx_px;
    --Convert all NULLS to ZEROES and cast as TINYINT for later math
    select id_mcaid, claim_header_id, last_service_date,
	    cast(isnull(lb,0) as tinyint) as lb, cast(isnull(ect,0) as tinyint) as ect, cast(isnull(ab,0) as tinyint) as ab,
	    cast(isnull(sa,0) as tinyint) as sa, cast(isnull(sb,0) as tinyint) as sb, cast(isnull(tro,0) as tinyint) as tro,
	    cast(isnull(deliv,0) as tinyint) as deliv
    into #preg_dx_px
    from #temp_1;", .con = conn))
  
  message("STEP 4: Group by ID-service date and count # of distinct endpoints (not including DELIV)")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	  --Group by ID-service date and take max of each endpoint column
    IF OBJECT_ID(N'tempdb..#temp_2') IS NOT NULL DROP TABLE #temp_2;
    select id_mcaid, last_service_date, max(lb) as lb, max(ect) as ect, max(ab) as ab, max(sa) as sa, max(sb) as sb,
	    max(tro) as tro, max(deliv) as deliv
    into #temp_2
    from #preg_dx_px
    group by id_mcaid, last_service_date;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Count # of distinct endpoints, not including DELIV
    IF OBJECT_ID(N'tempdb..#temp_3') IS NOT NULL DROP TABLE #temp_3;
    select id_mcaid, last_service_date, lb, ect, ab, sa, sb, tro, deliv,
	    lb + ect + ab + sa + sb + tro as endpoint_dcount
    into #temp_3
    from #temp_2;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Recode DELIV to 0 when there's another valid endpoint
    IF OBJECT_ID(N'tempdb..#temp_4') IS NOT NULL DROP TABLE #temp_4;
    select id_mcaid, last_service_date, lb, ect, ab, sa, sb, tro,
	    case when endpoint_dcount = 0 then deliv else 0 end as deliv,
	    endpoint_dcount
    into #temp_4
    from #temp_3;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Drop ID-service dates that contain >1 distinct endpoint (excluding DELIV)
    --NOTE - THIS REMOVES PREGNANCIES THAT HAD MULTIPLE GESTATIONS WITH TWO OR MORE DIFFERENT ENDPOINTS (e.g. liveborn and still birth)
    --Also restructure endpoint as a single variable, and add hierarchy variable
    IF OBJECT_ID(N'tempdb..#temp_5') IS NOT NULL DROP TABLE #temp_5;
    select id_mcaid, last_service_date, lb, ect, ab, sa, sb, tro, deliv,
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
    into #temp_5
    from #temp_4
    where endpoint_dcount <= 1;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_preg_endpoint',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_preg_endpoint;
    CREATE TABLE {`to_schema`}.tmp_mcaid_claim_preg_endpoint (
    id_mcaid varchar(255),
    last_service_date date,
    lb tinyint,
    ect tinyint,
    ab tinyint,
    sa tinyint,
    sb tinyint,
    tro tinyint,
    deliv tinyint,
    preg_endpoint varchar(255),
    preg_hier tinyint,
    preg_endpoint_rank int
    ) with (heap);", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Add ranking variable within each pregnancy endpoint type
    insert into {`to_schema`}.tmp_mcaid_claim_preg_endpoint
    select *, rank() over (partition by id_mcaid, preg_endpoint order by last_service_date) as preg_endpoint_rank
    from #temp_5
    option (label = 'preg_endpoint');", .con = conn))
  
  message("STEP 5: Hierarchical assessment of pregnancy outcomes to create pregnancy episodes for each woman")
  message("--STEP 5A: Group livebirth service days into distinct pregnancy episodes")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	  --Count days between each service day (<1 min)
    IF OBJECT_ID(N'tempdb..#lb_step1') IS NOT NULL DROP TABLE #lb_step1;
    select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_endpoint_rank, a.date_compare_lag1,
	    datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into #lb_step1
    from(
	    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
		    lag(last_service_date, 1, last_service_date) over (partition by id_mcaid order by last_service_date) as date_compare_lag1
	     from {`to_schema`}.tmp_mcaid_claim_preg_endpoint
	    where preg_endpoint = 'lb'
      ) as a
    option (label = 'lb_step1');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Group pregnancy endpoints into episodes based on minimum spacing
    --Seed table table with first-ranked preg endpoints
    IF OBJECT_ID(N'tempdb..#lb_step2') IS NOT NULL DROP TABLE #lb_step2;
    select *, days_diff as days_diff_cum, 1 as timeline_include -- to keep 1st LB endpoint
    into #lb_step2
    from #lb_step1
    where preg_endpoint_rank = 1
    option (label = 'lb_step2');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Loop over all preg endpoints to identify endpoints to include on each woman's timeline
    --set counter initially at 1
    declare @counter_lb int = 1;
    --create while condition (rows exist with given preg_endpoint_rank + 1, as each endpoint is compared to subsequent)
    while exists (select * from #lb_step1 where preg_endpoint_rank = @counter_lb + 1)
    --begin loop
    begin
    insert into #lb_step2 --insert rows for next preg_endpoint_rank (looping over counter)
    select b.*,
    --generate cumulative days diff that resets when threshold is reached
    case
    	when a.days_diff_cum + b.days_diff > 182 then 0
    	else a.days_diff_cum + b.days_diff
    end as days_diff_cum,
    --generate variable to flag inclusion on timeline
    case
    	when a.days_diff_cum + b.days_diff > 182 then 1
    	else 0
    end as timeline_include
    from (select * from #lb_step2 where preg_endpoint_rank = @counter_lb) as a --refers to table receiving inserted rows
    inner join (select * from #lb_step1 where preg_endpoint_rank = @counter_lb + 1) as b --refers to initial table
    on (a.id_mcaid = b.id_mcaid) and (a.preg_endpoint_rank + 1 = b.preg_endpoint_rank)
    option (label = 'lb_step2_loop');
    --advance counter by 1
    set @counter_lb = @counter_lb + 1;
    -- break in case infinite loop (defined as counter greater than 100
    if @counter_lb > 100 begin raiserror('Too many loops!', 16, 1) break end;
    --end loop
    end;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("  
    --Create preg_episode_id variable and subset results to endpoints included on timeline
    --Save data thus far in persistent heap table
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_lb_final',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_lb_final;
    create table {`to_schema`}.tmp_mcaid_claim_lb_final (
    id_mcaid bigint,
    last_service_date date,
    preg_endpoint varchar(255),
    preg_hier tinyint,
    preg_endpoint_rank int,
    preg_episode_id bigint
    ) with (heap);", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    insert into {`to_schema`}.tmp_mcaid_claim_lb_final
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
        rank() over (partition by id_mcaid order by last_service_date) as preg_episode_id
    from #lb_step2
    where timeline_include = 1
    option (label = 'lb_final');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Clean up temp tables
    IF OBJECT_ID(N'tempdb..#lb_step1') IS NOT NULL DROP TABLE #lb_step1;
    IF OBJECT_ID(N'tempdb..#lb_step2') IS NOT NULL DROP TABLE #lb_step2;", .con = conn))
  
  message("--STEP 5B: PROCESS STILLBIRTH EPISODES")
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Union LB and SB endpoints
    IF OBJECT_ID(N'tempdb..#sb_step1') IS NOT NULL DROP TABLE #sb_step1;
    select *, last_service_date as prior_lb_date, last_service_date as next_lb_date into #sb_step1 from {`to_schema`}.tmp_mcaid_claim_lb_final
    union
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
        null as prior_lb_date, null as next_lb_date
    from {`to_schema`}.tmp_mcaid_claim_preg_endpoint where preg_endpoint = 'sb'
    option (label = 'sb_step1');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Create column to hold dates of LB endpoints for comparison
    IF OBJECT_ID(N'tempdb..#sb_step2') IS NOT NULL DROP TABLE #sb_step2;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
      --create column to hold date of prior LB
      case when preg_endpoint = 'sb' then
        ISNULL(prior_lb_date, (SELECT TOP 1 last_service_date FROM #sb_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_lb_date IS NOT NULL ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_lb_date,
      --create column to hold date of next LB
      case when preg_endpoint = 'sb' then
        ISNULL(next_lb_date, (SELECT TOP 1 last_service_date FROM #sb_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_lb_date IS NOT NULL ORDER BY id_mcaid, last_service_date))
        else null
        end as next_lb_date
    into #sb_step2
    from #sb_step1 as t
    option (label = 'sb_step2');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --For each SB endpoint, count days between it and prior and next LB endpoint
    IF OBJECT_ID(N'tempdb..#sb_step3') IS NOT NULL DROP TABLE #sb_step3;
    select *,	
	    case when preg_endpoint = 'sb' then datediff(day, prior_lb_date, last_service_date) else null end as days_diff_back_lb,
	    case when preg_endpoint = 'sb' then datediff(day, next_lb_date, last_service_date) else null end as days_diff_ahead_lb
    into #sb_step3
    from #sb_step2
    option (label = 'sb_step3');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Pull out SB timepoints that potentially can be placed on timeline - COMPARE TO LB ENDPOINTS
    IF OBJECT_ID(N'tempdb..#sb_step4') IS NOT NULL DROP TABLE #sb_step4;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
    into #sb_step4
    from #sb_step3
    where preg_endpoint = 'sb'
	    and (days_diff_back_lb is null or days_diff_back_lb > 182)
	    and (days_diff_ahead_lb is null or days_diff_ahead_lb < -182)
    option (label = 'sb_step4');", .con = conn))
	DBI::dbExecute(conn = conn, glue::glue_sql("
    --Count days between each SB endpoint and regenerate preg_endpoint_rank variable
    IF OBJECT_ID(N'tempdb..#sb_step5') IS NOT NULL DROP TABLE #sb_step5;
    select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier,
	    rank() over (partition by a.id_mcaid, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
	    datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into #sb_step5
    from(
	    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
	      lag(last_service_date, 1, last_service_date) over (partition by id_mcaid order by last_service_date) as date_compare_lag1
	     from #sb_step4
      ) as a
    option (label = 'sb_step5');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Group SB endpoints into episodes based on minimum spacing
    --Seed table table with first-ranked preg endpoints
    IF OBJECT_ID(N'tempdb..#sb_step6') IS NOT NULL DROP TABLE #sb_step6;
    select *, days_diff as days_diff_cum, 1 as timeline_include -- to keep 1st LB endpoint
    into #sb_step6
    from #sb_step5
    where preg_endpoint_rank = 1
    option (label = 'sb_step6');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Loop over all preg endpoints to identify endpoints to include on each woman's timeline
    --set counter initially at 1
    declare @counter_sb int = 1;
    --create while condition (rows exist with given preg_endpoint_rank + 1, as each endpoint is compared to subsequent)
    while exists (select * from #sb_step5 where preg_endpoint_rank = @counter_sb + 1)
    --begin loop
    begin
    insert into #sb_step6 --insert rows for next preg_endpoint_rank (looping over counter)
    select b.*,
    --generate cumulative days diff that resets when threshold is reached
    case
    	when a.days_diff_cum + b.days_diff > 168 then 0
    	else a.days_diff_cum + b.days_diff
    end as days_diff_cum,
    --generate variable to flag inclusion on timeline
    case
    	when a.days_diff_cum + b.days_diff > 168 then 1
    	else 0
    end as timeline_include
    from (select * from #sb_step6 where preg_endpoint_rank = @counter_sb) as a --refers to table receiving inserted rows
    inner join (select * from #sb_step5 where preg_endpoint_rank = @counter_sb + 1) as b --refers to initial table
    on (a.id_mcaid = b.id_mcaid) and (a.preg_endpoint_rank + 1 = b.preg_endpoint_rank)
    option (label = 'sb_step6_loop');
    --advance counter by 1
    set @counter_sb = @counter_sb + 1;
    -- break in case infinite loop (defined as counter greater than 100
    if @counter_sb > 100 begin raiserror('Too many loops!', 16, 1) break end;
    --end loop
    end;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Create preg_episode_id variable and subset results to endpoints included on timeline
    IF OBJECT_ID(N'tempdb..#sb_final') IS NOT NULL DROP TABLE #sb_final;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
        rank() over (partition by id_mcaid order by last_service_date) as preg_episode_id
    into #sb_final
    from #sb_step6
    where timeline_include = 1
    option (label = 'sb_final');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Union LB and SB endpoints placed on timeline
    --Save data thus far in persistent heap table
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_lb_sb_final',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_lb_sb_final;
    create table {`to_schema`}.tmp_mcaid_claim_lb_sb_final (
    id_mcaid varchar(255),
    last_service_date date,
    preg_endpoint varchar(255),
    preg_hier tinyint,
    preg_endpoint_rank int,
    preg_episode_id bigint
    ) with (heap);", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    insert into {`to_schema`}.tmp_mcaid_claim_lb_sb_final
    select * from {`to_schema`}.tmp_mcaid_claim_lb_final
    union
    select * from #sb_final
    option (label = 'lb_sb_final');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Clean up temp tables
    IF OBJECT_ID(N'tempdb..#sb_step1') IS NOT NULL DROP TABLE #sb_step1;
    IF OBJECT_ID(N'tempdb..#sb_step2') IS NOT NULL DROP TABLE #sb_step2;
    IF OBJECT_ID(N'tempdb..#sb_step3') IS NOT NULL DROP TABLE #sb_step3;
    IF OBJECT_ID(N'tempdb..#sb_step4') IS NOT NULL DROP TABLE #sb_step4;
    IF OBJECT_ID(N'tempdb..#sb_step5') IS NOT NULL DROP TABLE #sb_step5;
    IF OBJECT_ID(N'tempdb..#sb_step6') IS NOT NULL DROP TABLE #sb_step6;
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_lb_final',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_lb_final;
    IF OBJECT_ID(N'tempdb..#sb_final') IS NOT NULL DROP TABLE #sb_final;", .con = conn))
  
  message("--STEP 5C: PROCESS DELIV EPISODES")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	  --Union LB-SB and DELIV endpoints
    IF OBJECT_ID(N'tempdb..#deliv_step1') IS NOT NULL DROP TABLE #deliv_step1;
    select *, last_service_date as prior_date, last_service_date as next_date into #deliv_step1 from {`to_schema`}.tmp_mcaid_claim_lb_sb_final
    union
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
        null as prior_date, null as next_date
    from {`to_schema`}.tmp_mcaid_claim_preg_endpoint where preg_endpoint = 'deliv'
    option (label = 'deliv_step1');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Create column to hold dates of LB and SB endpoints for comparison
    IF OBJECT_ID(N'tempdb..#deliv_step2') IS NOT NULL DROP TABLE #deliv_step2;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
    --create column to hold date of prior LB
    case when preg_endpoint = 'deliv' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #deliv_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'lb'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_lb_date,
    --create column to hold date of next LB
    case when preg_endpoint = 'deliv' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #deliv_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'lb'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_lb_date,
    --create column to hold date of prior SB
    case when preg_endpoint = 'deliv' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #deliv_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'sb'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_sb_date,
    --create column to hold date of next SB
    case when preg_endpoint = 'deliv' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #deliv_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'sb'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_sb_date
    into #deliv_step2
    from #deliv_step1 as t
    option (label = 'deliv_step2');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --For each DELIV endpoint, count days between it and prior and next LB and SB endpoints
    IF OBJECT_ID(N'tempdb..#deliv_step3') IS NOT NULL DROP TABLE #deliv_step3;
    select *,	
        case when preg_endpoint = 'deliv' then datediff(day, prior_lb_date, last_service_date) else null end as days_diff_back_lb,
        case when preg_endpoint = 'deliv' then datediff(day, next_lb_date, last_service_date) else null end as days_diff_ahead_lb,
        case when preg_endpoint = 'deliv' then datediff(day, prior_sb_date, last_service_date) else null end as days_diff_back_sb,
        case when preg_endpoint = 'deliv' then datediff(day, next_sb_date, last_service_date) else null end as days_diff_ahead_sb
    into #deliv_step3
    from #deliv_step2
    option (label = 'deliv_step3');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Pull out DELIV timepoints that potentially can be placed on timeline - COMPARE TO LB and SB ENDPOINTS
    IF OBJECT_ID(N'tempdb..#deliv_step4') IS NOT NULL DROP TABLE #deliv_step4;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
    into #deliv_step4
    from #deliv_step3
    where preg_endpoint = 'deliv'
        and (days_diff_back_lb is null or days_diff_back_lb > 182)
        and (days_diff_ahead_lb is null or days_diff_ahead_lb < -182)
        and (days_diff_back_sb is null or days_diff_back_sb > 168)
        and (days_diff_ahead_sb is null or days_diff_ahead_sb < -168)
    option (label = 'deliv_step4');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Count days between each DELIV endpoint and regenerate preg_endpoint_rank variable
    IF OBJECT_ID(N'tempdb..#deliv_step5') IS NOT NULL DROP TABLE #deliv_step5;
    select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier,
        rank() over (partition by a.id_mcaid, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
        datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into #deliv_step5
    from(
        select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
        lag(last_service_date, 1, last_service_date) over (partition by id_mcaid order by last_service_date) as date_compare_lag1
        from #deliv_step4
    ) as a
    option (label = 'deliv_step5');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Group DELIV endpoints into episodes based on minimum spacing
    --Seed table table with first-ranked preg endpoints
    IF OBJECT_ID(N'tempdb..#deliv_step6') IS NOT NULL DROP TABLE #deliv_step6;
    select *, days_diff as days_diff_cum, 1 as timeline_include -- to keep 1st LB endpoint
    into #deliv_step6
    from #deliv_step5
    where preg_endpoint_rank = 1
    option (label = 'deliv_step6');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Loop over all preg endpoints to identify endpoints to include on each woman's timeline
    --set counter initially at 1
    declare @counter_deliv int = 1;
    --create while condition (rows exist with given preg_endpoint_rank + 1, as each endpoint is compared to subsequent)
    while exists (select * from #deliv_step5 where preg_endpoint_rank = @counter_deliv + 1)
    --begin loop
    begin
    insert into #deliv_step6 --insert rows for next preg_endpoint_rank (looping over counter)
    select b.*,
    --generate cumulative days diff that resets when threshold is reached
    case
    	when a.days_diff_cum + b.days_diff > 168 then 0
    	else a.days_diff_cum + b.days_diff
    end as days_diff_cum,
    --generate variable to flag inclusion on timeline
    case
    	when a.days_diff_cum + b.days_diff > 168 then 1
    	else 0
    end as timeline_include
    from (select * from #deliv_step6 where preg_endpoint_rank = @counter_deliv) as a --refers to table receiving inserted rows
    inner join (select * from #deliv_step5 where preg_endpoint_rank = @counter_deliv + 1) as b --refers to initial table
    on (a.id_mcaid = b.id_mcaid) and (a.preg_endpoint_rank + 1 = b.preg_endpoint_rank)
    option (label = 'deliv_step6_loop');
    --advance counter by 1
    set @counter_deliv = @counter_deliv + 1;
    -- break in case infinite loop (defined as counter greater than 100
    if @counter_deliv > 100 begin raiserror('Too many loops!', 16, 1) break end;
    --end loop
    end;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Create preg_episode_id variable and subset results to endpoints included on timeline
    IF OBJECT_ID(N'tempdb..#deliv_final') IS NOT NULL DROP TABLE #deliv_final;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
        rank() over (partition by id_mcaid order by last_service_date) as preg_episode_id
    into #deliv_final
    from #deliv_step6
    where timeline_include = 1
    option (label = 'deliv_final');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Union LB, SB and DELIV endpoints placed on timeline
    --Save data thus far in persistent heap table
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_final',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_final;
    create table {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_final (
    id_mcaid varchar(255),
    last_service_date date,
    preg_endpoint varchar(255),
    preg_hier tinyint,
    preg_endpoint_rank int,
    preg_episode_id bigint
    ) with (heap);", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    insert into {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_final
    select * from {`to_schema`}.tmp_mcaid_claim_lb_sb_final
    union
    select * from #deliv_final
    option (label = 'lb_sb_deliv_final');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Clean up temp tables
    IF OBJECT_ID(N'tempdb..#deliv_step1') IS NOT NULL DROP TABLE #deliv_step1;
    IF OBJECT_ID(N'tempdb..#deliv_step2') IS NOT NULL DROP TABLE #deliv_step2;
    IF OBJECT_ID(N'tempdb..#deliv_step3') IS NOT NULL DROP TABLE #deliv_step3;
    IF OBJECT_ID(N'tempdb..#deliv_step4') IS NOT NULL DROP TABLE #deliv_step4;
    IF OBJECT_ID(N'tempdb..#deliv_step5') IS NOT NULL DROP TABLE #deliv_step5;
    IF OBJECT_ID(N'tempdb..#deliv_step6') IS NOT NULL DROP TABLE #deliv_step6;
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_lb_sb_final',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_lb_sb_final;
    IF OBJECT_ID(N'tempdb..#deliv_final') IS NOT NULL DROP TABLE #deliv_final;", .con = conn))
  
  message("--STEP 5D: PROCESS TRO EPISODES")
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Union LB-SB-DELIV and TRO endpoints
    IF OBJECT_ID(N'tempdb..#tro_step1') IS NOT NULL DROP TABLE #tro_step1;
    select *, last_service_date as prior_date, last_service_date as next_date into #tro_step1 from {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_final
    union
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
        null as prior_date, null as next_date
    from {`to_schema`}.tmp_mcaid_claim_preg_endpoint where preg_endpoint = 'tro'
    option (label = 'tro_step1');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Create column to hold dates of LB and SB endpoints for comparison
    IF OBJECT_ID(N'tempdb..#tro_step2') IS NOT NULL DROP TABLE #tro_step2;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
    --create column to hold date of prior LB
    case when preg_endpoint = 'tro' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #tro_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'lb'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_lb_date,
    --create column to hold date of next LB
    case when preg_endpoint = 'tro' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #tro_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'lb'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_lb_date,
    --create column to hold date of prior SB
    case when preg_endpoint = 'tro' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #tro_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'sb'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_sb_date,
    --create column to hold date of next SB
    case when preg_endpoint = 'tro' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #tro_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'sb'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_sb_date,
    --create column to hold date of prior DELIV
    case when preg_endpoint = 'tro' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #tro_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'deliv'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_deliv_date,
    --create column to hold date of next DELIV
    case when preg_endpoint = 'tro' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #tro_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'deliv'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_deliv_date
    into #tro_step2
    from #tro_step1 as t
    option (label = 'tro_step2');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --For each TRO endpoint, count days between it and prior and next LB, SB and DELIV endpoints
    IF OBJECT_ID(N'tempdb..#tro_step3') IS NOT NULL DROP TABLE #tro_step3;
    select *,	
        case when preg_endpoint = 'tro' then datediff(day, prior_lb_date, last_service_date) else null end as days_diff_back_lb,
        case when preg_endpoint = 'tro' then datediff(day, next_lb_date, last_service_date) else null end as days_diff_ahead_lb,
        case when preg_endpoint = 'tro' then datediff(day, prior_sb_date, last_service_date) else null end as days_diff_back_sb,
        case when preg_endpoint = 'tro' then datediff(day, next_sb_date, last_service_date) else null end as days_diff_ahead_sb,
        case when preg_endpoint = 'tro' then datediff(day, prior_deliv_date, last_service_date) else null end as days_diff_back_deliv,
        case when preg_endpoint = 'tro' then datediff(day, next_deliv_date, last_service_date) else null end as days_diff_ahead_deliv
    into #tro_step3
    from #tro_step2
    option (label = 'tro_step3');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Pull out TRO timepoints that potentially can be placed on timeline - COMPARE TO LB, SB, and DELIV ENDPOINTS
    IF OBJECT_ID(N'tempdb..#tro_step4') IS NOT NULL DROP TABLE #tro_step4;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
    into #tro_step4
    from #tro_step3
    where preg_endpoint = 'tro'
        and (days_diff_back_lb is null or days_diff_back_lb > 168)
        and (days_diff_ahead_lb is null or days_diff_ahead_lb < -168)
        and (days_diff_back_sb is null or days_diff_back_sb > 154)
        and (days_diff_ahead_sb is null or days_diff_ahead_sb < -154)
        and (days_diff_back_deliv is null or days_diff_back_deliv > 154)
        and (days_diff_ahead_deliv is null or days_diff_ahead_deliv < -154)
    option (label = 'tro_step4');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Count days between each TRO endpoint and regenerate preg_endpoint_rank variable
    IF OBJECT_ID(N'tempdb..#tro_step5') IS NOT NULL DROP TABLE #tro_step5;
    select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier,
        rank() over (partition by a.id_mcaid, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
        datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into #tro_step5
    from(
        select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
        lag(last_service_date, 1, last_service_date) over (partition by id_mcaid order by last_service_date) as date_compare_lag1
        from #tro_step4
    ) as a
    option (label = 'tro_step5');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Group TRO endpoints into episodes based on minimum spacing
    --Seed table table with first-ranked preg endpoints
    IF OBJECT_ID(N'tempdb..#tro_step6') IS NOT NULL DROP TABLE #tro_step6;
    select *, days_diff as days_diff_cum, 1 as timeline_include -- to keep 1st LB endpoint
    into #tro_step6
    from #tro_step5
    where preg_endpoint_rank = 1
    option (label = 'tro_step6');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Loop over all preg endpoints to identify endpoints to include on each woman's timeline
    --set counter initially at 1
    declare @counter_tro int = 1;
    --create while condition (rows exist with given preg_endpoint_rank + 1, as each endpoint is compared to subsequent)
    while exists (select * from #tro_step5 where preg_endpoint_rank = @counter_tro + 1)
    --begin loop
    begin
    insert into #tro_step6 --insert rows for next preg_endpoint_rank (looping over counter)
    select b.*,
    --generate cumulative days diff that resets when threshold is reached
    case
    	when a.days_diff_cum + b.days_diff > 56 then 0
    	else a.days_diff_cum + b.days_diff
    end as days_diff_cum,
    --generate variable to flag inclusion on timeline
    case
    	when a.days_diff_cum + b.days_diff > 56 then 1
    	else 0
    end as timeline_include
    from (select * from #tro_step6 where preg_endpoint_rank = @counter_tro) as a --refers to table receiving inserted rows
    inner join (select * from #tro_step5 where preg_endpoint_rank = @counter_tro + 1) as b --refers to initial table
    on (a.id_mcaid = b.id_mcaid) and (a.preg_endpoint_rank + 1 = b.preg_endpoint_rank)
    option (label = 'tro_step6_loop');
    --advance counter by 1
    set @counter_tro = @counter_tro + 1;
    -- break in case infinite loop (defined as counter greater than 100
    if @counter_tro > 100 begin raiserror('Too many loops!', 16, 1) break end;
    --end loop
    end;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Create preg_episode_id variable and subset results to endpoints included on timeline
    IF OBJECT_ID(N'tempdb..#tro_final') IS NOT NULL DROP TABLE #tro_final;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
        rank() over (partition by id_mcaid order by last_service_date) as preg_episode_id
    into #tro_final
    from #tro_step6
    where timeline_include = 1
    option (label = 'tro_final');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Union LB, SB, DELIV and TRO endpoints placed on timeline
    --Save data thus far in persistent heap table
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_final',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_final;
    create table {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_final (
    id_mcaid varchar(255),
    last_service_date date,
    preg_endpoint varchar(255),
    preg_hier tinyint,
    preg_endpoint_rank int,
    preg_episode_id bigint
    ) with (heap);", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    insert into {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_final
    select * from {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_final
    union
    select * from #tro_final
    option (label = 'lb_sb_deliv_tro_final');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Clean up temp tables
    IF OBJECT_ID(N'tempdb..#tro_step1') IS NOT NULL DROP TABLE #tro_step1;
    IF OBJECT_ID(N'tempdb..#tro_step2') IS NOT NULL DROP TABLE #tro_step2;
    IF OBJECT_ID(N'tempdb..#tro_step3') IS NOT NULL DROP TABLE #tro_step3;
    IF OBJECT_ID(N'tempdb..#tro_step4') IS NOT NULL DROP TABLE #tro_step4;
    IF OBJECT_ID(N'tempdb..#tro_step5') IS NOT NULL DROP TABLE #tro_step5;
    IF OBJECT_ID(N'tempdb..#tro_step6') IS NOT NULL DROP TABLE #tro_step6;
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_final',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_final;
    IF OBJECT_ID(N'tempdb..#tro_final') IS NOT NULL DROP TABLE #tro_final;", .con = conn))
  
  message("--STEP 5E: PROCESS ECT EPISODES")
  DBI::dbExecute(conn = conn, glue::glue_sql("
     --Union LB-SB, DELIV, TRO and ECT endpoints
    IF OBJECT_ID(N'tempdb..#ect_step1') IS NOT NULL DROP TABLE #ect_step1;
    select *, last_service_date as prior_date, last_service_date as next_date into #ect_step1 from {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_final
    union
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
        null as prior_date, null as next_date
    from {`to_schema`}.tmp_mcaid_claim_preg_endpoint where preg_endpoint = 'ect'
    option (label = 'ect_step1');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Create column to hold dates of LB, SB, DELIV and TRO endpoints for comparison
    IF OBJECT_ID(N'tempdb..#ect_step2') IS NOT NULL DROP TABLE #ect_step2;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
    --create column to hold date of prior LB
    case when preg_endpoint = 'ect' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ect_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'lb'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_lb_date,
    --create column to hold date of next LB
    case when preg_endpoint = 'ect' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ect_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'lb'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_lb_date,
    --create column to hold date of prior SB
    case when preg_endpoint = 'ect' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ect_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'sb'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_sb_date,
    --create column to hold date of next SB
    case when preg_endpoint = 'ect' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ect_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'sb'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_sb_date,
    --create column to hold date of prior DELIV
    case when preg_endpoint = 'ect' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ect_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'deliv'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_deliv_date,
    --create column to hold date of next DELIV
    case when preg_endpoint = 'ect' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ect_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'deliv'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_deliv_date,
    --create column to hold date of prior TRO
    case when preg_endpoint = 'ect' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ect_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'tro'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_tro_date,
    --create column to hold date of next TRO
    case when preg_endpoint = 'ect' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ect_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'tro'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_tro_date
    into #ect_step2
    from #ect_step1 as t
    option (label = 'ect_step2');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --For each ECT endpoint, count days between it and prior and next LB, SB, DELIV, and TRO endpoints
    IF OBJECT_ID(N'tempdb..#ect_step3') IS NOT NULL DROP TABLE #ect_step3;
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
    from #ect_step2
    option (label = 'ect_step3');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Pull out ECT timepoints that potentially can be placed on timeline - COMPARE TO LB, SB, DELIV and TRO ENDPOINTS
    IF OBJECT_ID(N'tempdb..#ect_step4') IS NOT NULL DROP TABLE #ect_step4;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
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
        and (days_diff_ahead_tro is null or days_diff_ahead_tro < -56)
    option (label = 'ect_step4');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Count days between each ECT endpoint and regenerate preg_endpoint_rank variable
    IF OBJECT_ID(N'tempdb..#ect_step5') IS NOT NULL DROP TABLE #ect_step5;
    select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier,
        rank() over (partition by a.id_mcaid, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
        datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into #ect_step5
    from(
        select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
        lag(last_service_date, 1, last_service_date) over (partition by id_mcaid order by last_service_date) as date_compare_lag1
        from #ect_step4
    ) as a
    option (label = 'ect_step5');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Group ECT endpoints into episodes based on minimum spacing
    --Seed table table with first-ranked preg endpoints
    IF OBJECT_ID(N'tempdb..#ect_step6') IS NOT NULL DROP TABLE #ect_step6;
    select *, days_diff as days_diff_cum, 1 as timeline_include -- to keep 1st LB endpoint
    into #ect_step6
    from #ect_step5
    where preg_endpoint_rank = 1
    option (label = 'ect_step6');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Loop over all preg endpoints to identify endpoints to include on each woman's timeline
    --set counter initially at 1
    declare @counter_ect int = 1;
    --create while condition (rows exist with given preg_endpoint_rank + 1, as each endpoint is compared to subsequent)
    while exists (select * from #ect_step5 where preg_endpoint_rank = @counter_ect + 1)
    --begin loop
    begin
    insert into #ect_step6 --insert rows for next preg_endpoint_rank (looping over counter)
    select b.*,
    --generate cumulative days diff that resets when threshold is reached
    case
    	when a.days_diff_cum + b.days_diff > 56 then 0
    	else a.days_diff_cum + b.days_diff
    end as days_diff_cum,
    --generate variable to flag inclusion on timeline
    case
    	when a.days_diff_cum + b.days_diff > 56 then 1
    	else 0
    end as timeline_include
    from (select * from #ect_step6 where preg_endpoint_rank = @counter_ect) as a --refers to table receiving inserted rows
    inner join (select * from #ect_step5 where preg_endpoint_rank = @counter_ect + 1) as b --refers to initial table
    on (a.id_mcaid = b.id_mcaid) and (a.preg_endpoint_rank + 1 = b.preg_endpoint_rank)
    option (label = 'ect_step6_loop');
    --advance counter by 1
    set @counter_ect = @counter_ect + 1;
    -- break in case infinite loop (defined as counter greater than 100
    if @counter_ect > 100 begin raiserror('Too many loops!', 16, 1) break end;
    --end loop
    end;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Create preg_episode_id variable and subset results to endpoints included on timeline
    IF OBJECT_ID(N'tempdb..#ect_final') IS NOT NULL DROP TABLE #ect_final;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
        rank() over (partition by id_mcaid order by last_service_date) as preg_episode_id
    into #ect_final
    from #ect_step6
    where timeline_include = 1
    option (label = 'ect_final');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Union LB, SB, DELIV, TRO, and ECT endpoints placed on timeline
    --Save data thus far in persistent heap table
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_final',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_final;
    create table {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_final (
    id_mcaid varchar(255),
    last_service_date date,
    preg_endpoint varchar(255),
    preg_hier tinyint,
    preg_endpoint_rank int,
    preg_episode_id bigint
    ) with (heap);", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    insert into {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_final
    select * from {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_final
    union
    select * from #ect_final
    option (label = 'lb_sb_deliv_tro_ect_final');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Clean up temp tables
    IF OBJECT_ID(N'tempdb..#ect_step1') IS NOT NULL DROP TABLE #ect_step1;
    IF OBJECT_ID(N'tempdb..#ect_step2') IS NOT NULL DROP TABLE #ect_step2;
    IF OBJECT_ID(N'tempdb..#ect_step3') IS NOT NULL DROP TABLE #ect_step3;
    IF OBJECT_ID(N'tempdb..#ect_step4') IS NOT NULL DROP TABLE #ect_step4;
    IF OBJECT_ID(N'tempdb..#ect_step5') IS NOT NULL DROP TABLE #ect_step5;
    IF OBJECT_ID(N'tempdb..#ect_step6') IS NOT NULL DROP TABLE #ect_step6;
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_final',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_final;
    IF OBJECT_ID(N'tempdb..#ect_final') IS NOT NULL DROP TABLE #ect_final;", .con = conn))

  message("--STEP 5F: PROCESS AB EPISODES")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	  --Union LB-SB, DELIV, TRO, ECT, an AB endpoints
    IF OBJECT_ID(N'tempdb..#ab_step1') IS NOT NULL DROP TABLE #ab_step1;
    select *, last_service_date as prior_date, last_service_date as next_date into #ab_step1 from {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_final
    union
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
        null as prior_date, null as next_date
    from {`to_schema`}.tmp_mcaid_claim_preg_endpoint where preg_endpoint = 'ab'
    option (label = 'ab_step1');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Create column to hold dates of LB, SB, DELIV, TRO, and ECT endpoints for comparison
    IF OBJECT_ID(N'tempdb..#ab_step2') IS NOT NULL DROP TABLE #ab_step2;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
    --create column to hold date of prior LB
    case when preg_endpoint = 'ab' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ab_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'lb'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_lb_date,
    --create column to hold date of next LB
    case when preg_endpoint = 'ab' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ab_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'lb'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_lb_date,
    --create column to hold date of prior SB
    case when preg_endpoint = 'ab' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ab_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'sb'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_sb_date,
    --create column to hold date of next SB
    case when preg_endpoint = 'ab' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ab_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'sb'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_sb_date,
    --create column to hold date of prior DELIV
    case when preg_endpoint = 'ab' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ab_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'deliv'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_deliv_date,
    --create column to hold date of next DELIV
    case when preg_endpoint = 'ab' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ab_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'deliv'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_deliv_date,
    --create column to hold date of prior TRO
    case when preg_endpoint = 'ab' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ab_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'tro'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_tro_date,
    --create column to hold date of next TRO
    case when preg_endpoint = 'ab' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ab_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'tro'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_tro_date,
    --create column to hold date of prior ECT
    case when preg_endpoint = 'ab' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #ab_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'ect'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_ect_date,
    --create column to hold date of next ECT
    case when preg_endpoint = 'ab' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #ab_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'ect'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_ect_date
    into #ab_step2
    from #ab_step1 as t
    option (label = 'ab_step2');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --For each AB endpoint, count days between it and prior and next LB, SB, DELIV, TRO, and ECT endpoints
    IF OBJECT_ID(N'tempdb..#ab_step3') IS NOT NULL DROP TABLE #ab_step3;
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
    from #ab_step2
    option (label = 'ab_step3');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Pull out AB timepoints that potentially can be placed on timeline - COMPARE TO LB, SB, DELIV, TRO, and ECT ENDPOINTS
    IF OBJECT_ID(N'tempdb..#ab_step4') IS NOT NULL DROP TABLE #ab_step4;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
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
        and (days_diff_ahead_ect is null or days_diff_ahead_ect < -56)
    option (label = 'ab_step4');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Count days between each AB endpoint and regenerate preg_endpoint_rank variable
    IF OBJECT_ID(N'tempdb..#ab_step5') IS NOT NULL DROP TABLE #ab_step5;
    select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier,
        rank() over (partition by a.id_mcaid, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
        datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into #ab_step5
    from(
        select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
        lag(last_service_date, 1, last_service_date) over (partition by id_mcaid order by last_service_date) as date_compare_lag1
        from #ab_step4
    ) as a
    option (label = 'ab_step5');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Group AB endpoints into episodes based on minimum spacing
    --Seed table table with first-ranked preg endpoints
    IF OBJECT_ID(N'tempdb..#ab_step6') IS NOT NULL DROP TABLE #ab_step6;
    select *, days_diff as days_diff_cum, 1 as timeline_include -- to keep 1st LB endpoint
    into #ab_step6
    from #ab_step5
    where preg_endpoint_rank = 1
    option (label = 'ab_step6');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Loop over all preg endpoints to identify endpoints to include on each woman's timeline
    --set counter initially at 1
    declare @counter_ab int = 1
    --create while condition (rows exist with given preg_endpoint_rank + 1, as each endpoint is compared to subsequent)
    while (@counter_ab + 1 <= (select max(preg_endpoint_rank) from #ab_step5))
    --begin loop
    begin
    insert into #ab_step6 --insert rows for next preg_endpoint_rank (looping over counter)
    select b.*,
    --generate cumulative days diff that resets when threshold is reached
    case
    	when a.days_diff_cum + b.days_diff > 56 then 0
    	else a.days_diff_cum + b.days_diff
    end as days_diff_cum,
    --generate variable to flag inclusion on timeline
    case
    	when a.days_diff_cum + b.days_diff > 56 then 1
    	else 0
    end as timeline_include
    from (select * from #ab_step6 where preg_endpoint_rank = @counter_ab) as a --refers to table receiving inserted rows
    inner join (select * from #ab_step5 where preg_endpoint_rank = @counter_ab + 1) as b --refers to initial table
    on (a.id_mcaid = b.id_mcaid) and (a.preg_endpoint_rank + 1 = b.preg_endpoint_rank)
    option (label = 'ab_step6_loop');
    --advance counter by 1
    set @counter_ab = @counter_ab + 1;
    -- break in case infinite loop (defined as counter greater than 100
    if @counter_ab > 100 begin raiserror('Too many loops!', 16, 1) break end;
    --end loop
    end;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Create preg_episode_id variable and subset results to endpoints included on timeline
    IF OBJECT_ID(N'tempdb..#ab_final') IS NOT NULL DROP TABLE #ab_final;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
        rank() over (partition by id_mcaid order by last_service_date) as preg_episode_id
    into #ab_final
    from #ab_step6
    where timeline_include = 1
    option (label = 'ab_final');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Union LB, SB, DELIV, TRO, ECT and AB endpoints placed on timeline
    --Save data thus far in persistent heap table
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_ab_final',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_ab_final;
    create table {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_ab_final (
    id_mcaid varchar(255),
    last_service_date date,
    preg_endpoint varchar(255),
    preg_hier tinyint,
    preg_endpoint_rank int,
    preg_episode_id bigint
    ) with (heap);", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    insert into {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_ab_final
    select * from {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_final
    union
    select * from #ab_final
    option (label = 'lb_sb_deliv_tro_ect_ab_final');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Clean up temp tables
    IF OBJECT_ID(N'tempdb..#ab_step1') IS NOT NULL DROP TABLE #ab_step1;
    IF OBJECT_ID(N'tempdb..#ab_step2') IS NOT NULL DROP TABLE #ab_step2;
    IF OBJECT_ID(N'tempdb..#ab_step3') IS NOT NULL DROP TABLE #ab_step3;
    IF OBJECT_ID(N'tempdb..#ab_step4') IS NOT NULL DROP TABLE #ab_step4;
    IF OBJECT_ID(N'tempdb..#ab_step5') IS NOT NULL DROP TABLE #ab_step5;
    IF OBJECT_ID(N'tempdb..#ab_step6') IS NOT NULL DROP TABLE #ab_step6;
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_final',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_final;
    IF OBJECT_ID(N'tempdb..#ab_final') IS NOT NULL DROP TABLE #ab_final;", .con = conn))

  message("--STEP 5G: PROCESS SA EPISODES")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	  --Union LB-SB, DELIV, TRO, ECT, AB, and SA endpoints
    IF OBJECT_ID(N'tempdb..#sa_step1') IS NOT NULL DROP TABLE #sa_step1;
    select *, last_service_date as prior_date, last_service_date as next_date into #sa_step1 from {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_ab_final
    union
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
        null as prior_date, null as next_date
    from {`to_schema`}.tmp_mcaid_claim_preg_endpoint where preg_endpoint = 'sa'
    option (label = 'sa_step1');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Create column to hold dates of LB, SB, DELIV, TRO, ECT, and AB endpoints for comparison
    IF OBJECT_ID(N'tempdb..#sa_step2') IS NOT NULL DROP TABLE #sa_step2;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
    --create column to hold date of prior LB
    case when preg_endpoint = 'sa' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #sa_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'lb'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_lb_date,
    --create column to hold date of next LB
    case when preg_endpoint = 'sa' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #sa_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'lb'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_lb_date,
    --create column to hold date of prior SB
    case when preg_endpoint = 'sa' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #sa_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'sb'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_sb_date,
    --create column to hold date of next SB
    case when preg_endpoint = 'sa' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #sa_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'sb'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_sb_date,
    --create column to hold date of prior DELIV
    case when preg_endpoint = 'sa' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #sa_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'deliv'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_deliv_date,
    --create column to hold date of next DELIV
    case when preg_endpoint = 'sa' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #sa_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'deliv'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_deliv_date,
    --create column to hold date of prior TRO
    case when preg_endpoint = 'sa' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #sa_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'tro'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_tro_date,
    --create column to hold date of next TRO
    case when preg_endpoint = 'sa' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #sa_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'tro'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_tro_date,
    --create column to hold date of prior ECT
    case when preg_endpoint = 'sa' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #sa_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'ect'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_ect_date,
    --create column to hold date of next ECT
    case when preg_endpoint = 'sa' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #sa_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'ect'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_ect_date,
    --create column to hold date of prior AB
    case when preg_endpoint = 'sa' then
    ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM #sa_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'ab'
        ORDER BY id_mcaid, last_service_date DESC))
    else null
    end as prior_ab_date,
    --create column to hold date of next AB
    case when preg_endpoint = 'sa' then
    ISNULL(next_date, (SELECT TOP 1 last_service_date FROM #sa_step1
        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'ab'
        ORDER BY id_mcaid, last_service_date))
    else null
    end as next_ab_date
    into #sa_step2
    from #sa_step1 as t
    option (label = 'sa_step2');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --For each SA endpoint, count days between it and prior and next LB, SB, DELIV, TRO, ECT, and AB endpoints
    IF OBJECT_ID(N'tempdb..#sa_step3') IS NOT NULL DROP TABLE #sa_step3;
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
    from #sa_step2
    option (label = 'sa_step3');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Pull out SA timepoints that potentially can be placed on timeline - COMPARE TO LB, SB, DELIV, TRO, ECT, and AB ENDPOINTS
    IF OBJECT_ID(N'tempdb..#sa_step4') IS NOT NULL DROP TABLE #sa_step4;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
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
        and (days_diff_ahead_ab is null or days_diff_ahead_ab < -56)
    option (label = 'sa_step4');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Count days between each SA endpoint and regenerate preg_endpoint_rank variable
    IF OBJECT_ID(N'tempdb..#sa_step5') IS NOT NULL DROP TABLE #sa_step5;
    select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier,
        rank() over (partition by a.id_mcaid, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
        datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into #sa_step5
    from(
        select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
        lag(last_service_date, 1, last_service_date) over (partition by id_mcaid order by last_service_date) as date_compare_lag1
        from #sa_step4
    ) as a
    option (label = 'sa_step5');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Group SA endpoints into episodes based on minimum spacing
    --Seed table table with first-ranked preg endpoints
    IF OBJECT_ID(N'tempdb..#sa_step6') IS NOT NULL DROP TABLE #sa_step6;
    select *, days_diff as days_diff_cum, 1 as timeline_include -- to keep 1st LB endpoint
    into #sa_step6
    from #sa_step5
    where preg_endpoint_rank = 1
    option (label = 'sa_step6');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Loop over all preg endpoints to identify endpoints to include on each woman's timeline
    --set counter initially at 1
    declare @counter_sa int = 1;
    --create while condition (rows exist with given preg_endpoint_rank + 1, as each endpoint is compared to subsequent)
    while exists (select * from #sa_step5 where preg_endpoint_rank = @counter_sa + 1)
    --begin loop
    begin
    insert into #sa_step6 --insert rows for next preg_endpoint_rank (looping over counter)
    select b.*,
    --generate cumulative days diff that resets when threshold is reached
    case
    	when a.days_diff_cum + b.days_diff > 42 then 0
    	else a.days_diff_cum + b.days_diff
    end as days_diff_cum,
    --generate variable to flag inclusion on timeline
    case
    	when a.days_diff_cum + b.days_diff > 42 then 1
    	else 0
    end as timeline_include
    from (select * from #sa_step6 where preg_endpoint_rank = @counter_sa) as a --refers to table receiving inserted rows
    inner join (select * from #sa_step5 where preg_endpoint_rank = @counter_sa + 1) as b --refers to initial table
    on (a.id_mcaid = b.id_mcaid) and (a.preg_endpoint_rank + 1 = b.preg_endpoint_rank)
    option (label = 'sa_step6_loop');
    --advance counter by 1
    set @counter_sa = @counter_sa + 1;
    -- break in case infinite loop (defined as counter greater than 100
    if @counter_sa > 100 begin raiserror('Too many loops!', 16, 1) break end;
    --end loop
    end;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Create preg_episode_id variable and subset results to endpoints included on timeline
    IF OBJECT_ID(N'tempdb..#sa_final') IS NOT NULL DROP TABLE #sa_final;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
        rank() over (partition by id_mcaid order by last_service_date) as preg_episode_id
    into #sa_final
    from #sa_step6
    where timeline_include = 1
    option (label = 'sa_final');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Union LB, SB, DELIV, TRO, ECT, AB, and SA endpoints placed on timeline
    --Save data thus far in persistent heap table
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_preg_endpoint_union',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_preg_endpoint_union;
    create table {`to_schema`}.tmp_mcaid_claim_preg_endpoint_union (
    id_mcaid varchar(255),
    last_service_date date,
    preg_endpoint varchar(255),
    preg_hier tinyint,
    preg_endpoint_rank int,
    preg_episode_id bigint
    ) with (heap);", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    insert into {`to_schema`}.tmp_mcaid_claim_preg_endpoint_union
    select * from {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_ab_final
    union
    select * from #sa_final
    option (label = 'preg_endpoint_union');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Clean up temp tables
    IF OBJECT_ID(N'tempdb..#sa_step1') IS NOT NULL DROP TABLE #sa_step1;
    IF OBJECT_ID(N'tempdb..#sa_step2') IS NOT NULL DROP TABLE #sa_step2;
    IF OBJECT_ID(N'tempdb..#sa_step3') IS NOT NULL DROP TABLE #sa_step3;
    IF OBJECT_ID(N'tempdb..#sa_step4') IS NOT NULL DROP TABLE #sa_step4;
    IF OBJECT_ID(N'tempdb..#sa_step5') IS NOT NULL DROP TABLE #sa_step5;
    IF OBJECT_ID(N'tempdb..#sa_step6') IS NOT NULL DROP TABLE #sa_step6;
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_ab_final',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_lb_sb_deliv_tro_ect_ab_final;
    IF OBJECT_ID(N'tempdb..#sa_final') IS NOT NULL DROP TABLE #sa_final;", .con = conn))

  message("STEP 6: Regenerate pregnancy episode ID to be unique across dataset")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	  --Drop preg_endpoint_rank variable as it is no longer needed
    --------------------------
    --Save data thus far in persistent heap table
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_preg_episode_0',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_preg_episode_0;
    create table {`to_schema`}.tmp_mcaid_claim_preg_episode_0 (
    id_mcaid varchar(255),
    last_service_date date,
    preg_endpoint varchar(255),
    preg_hier tinyint,
    preg_episode_id bigint
    ) with (heap);", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    insert into {`to_schema`}.tmp_mcaid_claim_preg_episode_0
    select id_mcaid, last_service_date, preg_endpoint, preg_hier,
        dense_rank() over (order by id_mcaid, last_service_date) as preg_episode_id
    from {`to_schema`}.tmp_mcaid_claim_preg_endpoint_union
    option (label = 'episode_0');",
    .con = conn))
  
  message("STEP 7: Define prenatal window for each pregnancy episode (<1 min)")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	  --Create columns to hold information about prior pregnancy episode
	  IF OBJECT_ID(N'tempdb..#episode_1') IS NOT NULL DROP TABLE #episode_1;
    select *,
      --calculate days difference from prior pregnancy outcome
      datediff(day,
	      lag(last_service_date, 1, null) over (partition by id_mcaid order by last_service_date),
	      last_service_date) as days_diff_prior,
      --create column to hold minumum days buffer between pregnancy episodes
      case when lag(preg_endpoint, 1, null) over (partition by id_mcaid order by last_service_date) in ('lb','sb','deliv') then 28
        when lag(preg_endpoint, 1, null) over (partition by id_mcaid order by last_service_date) in ('tro','ect','ab','sa') then 14
        else null
        end as days_buffer
    into #episode_1
    from {`to_schema`}.tmp_mcaid_claim_preg_episode_0
    option (label = 'episode_1');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    --Calculate start and end dates for each pregnancy episode
    IF OBJECT_ID(N'tempdb..#episode_2') IS NOT NULL DROP TABLE #episode_2;
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
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
    from #episode_1
    option (label = 'episode_2');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
     --Confirm that there are no pregnancy episodes with a null start or end date
    --select count (*) as qa_count from #episode_2 where preg_start_date is null or preg_end_date is null;
    --Add columns to hold earliest and latest pregnancy start date for later processing
    --Save data thus far in persistent heap table
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_preg_episode_temp',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_preg_episode_temp;
    create table {`to_schema`}.tmp_mcaid_claim_preg_episode_temp (
    id_mcaid varchar(255),
    last_service_date date,
    preg_endpoint varchar(255),
    preg_hier tinyint,
    preg_episode_id bigint,
    preg_start_date date,
    preg_end_date date,
    preg_start_date_max date,
    preg_start_date_min date
    ) with (heap);", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    insert into {`to_schema`}.tmp_mcaid_claim_preg_episode_temp
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
    from #episode_2
    option (label = 'preg_episode_temp');", .con = conn))

  message("STEP 8: Use claims that provide information about gestational age to correct pregnancy outcome and start date")
  message("--STEP 8A: Intrauterine insemination/embryo transfer")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	  --Pull out pregnany episodes that have relevant procedure codes during prenatal window
	  IF OBJECT_ID(N'tempdb..#ga_1a') IS NOT NULL DROP TABLE #ga_1a;
		select a.*, b.last_service_date as procedure_date, b.procedure_code
		into #ga_1a
		from {`to_schema`}.tmp_mcaid_claim_preg_episode_temp as a
		inner join {`final_schema`}.{`paste0(final_table, 'mcaid_claim_procedure')`} as b
			on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('58321', '58322', 'S4035', '58974', '58976', 'S4037')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create column to hold corrected start date
		IF OBJECT_ID(N'tempdb..#ga_1b') IS NOT NULL DROP TABLE #ga_1b;
		select distinct id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, -13, procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
		into #ga_1b
		from #ga_1a;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--For episodes with more than corrected start, select record that is closet to pregnancy end date
		IF OBJECT_ID(N'tempdb..#ga_1c') IS NOT NULL DROP TABLE #ga_1c;
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date_correct, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min
		into #ga_1c
		from (select *, rank() over (partition by preg_episode_id order by preg_start_date_correct desc) as rank_col from #ga_1b) as a
		where a.rank_col = 1;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Calculate gestational age in days and weeks
		IF OBJECT_ID(N'tempdb..#ga_1d') IS NOT NULL DROP TABLE #ga_1d;
		select *,
			datediff(day, preg_start_date_correct, preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, preg_start_date_correct, preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into #ga_1d
		from #ga_1c;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create final dataset with flags for plausible pregnancy start date and GA
		IF OBJECT_ID(N'tempdb..#ga_1_final') IS NOT NULL DROP TABLE #ga_1_final;
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
		from #ga_1d;", .con = conn))
  
  
  message("--STEP 8B: Z3A code on 1st trimester ultrasound")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps	
		IF OBJECT_ID(N'tempdb..#ga_2a') IS NOT NULL DROP TABLE #ga_2a;
		select a.*
		into #ga_2a
		from {`to_schema`}.tmp_mcaid_claim_preg_episode_temp as a
		left join #ga_1_final as b
			on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Pull out pregnany episodes that have a procedure code for a 1st trimester ultrasound during the prenatal window
		IF OBJECT_ID(N'tempdb..#ga_2b') IS NOT NULL DROP TABLE #ga_2b;
		select a.*, b.last_service_date as procedure_date, b.claim_header_id 
		into #ga_2b
		from #ga_2a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as b
			on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('76801', '76802')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Further subset to pregnancy episodes that have a Z3A code
		IF OBJECT_ID(N'tempdb..#ga_2c') IS NOT NULL DROP TABLE #ga_2c;
		select a.*, right(icdcm_norm, 2) as ga_weeks_int
		into #ga_2c
		from #ga_2b as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`} as b
			on a.claim_header_id = b.claim_header_id
		where b.icdcm_version = 10 and b.icdcm_norm in ('Z3A08', 'Z3A09', 'Z3A10', 'Z3A11', 'Z3A12', 'Z3A13', 'Z3A14', 'Z3A15', 'Z3A16', 'Z3A17', 'Z3A18', 'Z3A19',
			'Z3A20', 'Z3A21', 'Z3A22', 'Z3A23', 'Z3A24', 'Z3A25', 'Z3A26', 'Z3A27', 'Z3A28', 'Z3A29', 'Z3A30', 'Z3A31', 'Z3A32', 'Z3A33', 'Z3A34', 'Z3A35', 'Z3A36',
			'Z3A37', 'Z3A38', 'Z3A39', 'Z3A40', 'Z3A41', 'Z3A42');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Convert extracted gestational age to integer and collapse to distinct rows
		IF OBJECT_ID(N'tempdb..#ga_2d') IS NOT NULL DROP TABLE #ga_2d;
		select distinct id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date,
			preg_end_date, preg_start_date_max, preg_start_date_min, procedure_date, cast(ga_weeks_int as tinyint) as ga_weeks_int
		into #ga_2d
		from #ga_2c;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--For episodes with multiple first trimester ultrasounds, select first (ranked by procedure date and GA)
		IF OBJECT_ID(N'tempdb..#ga_2e') IS NOT NULL DROP TABLE #ga_2e;
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date, a.ga_weeks_int
		into #ga_2e
		from (select *, rank() over (partition by preg_episode_id order by procedure_date, ga_weeks_int) as rank_col from #ga_2d) as a
		where a.rank_col = 1;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create column to hold corrected start date and calculate GA in days and weeks
		IF OBJECT_ID(N'tempdb..#ga_2f') IS NOT NULL DROP TABLE #ga_2f;
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into #ga_2f
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
				dateadd(day, ((-1 * ga_weeks_int * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from #ga_2e
			) as a;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create final dataset with flags for plausible pregnancy start date and GA
		IF OBJECT_ID(N'tempdb..#ga_2_final') IS NOT NULL DROP TABLE #ga_2_final;
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
		from #ga_2f;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Union assigned episodes thus far
		IF OBJECT_ID(N'tempdb..#ga_1to2_final') IS NOT NULL DROP TABLE #ga_1to2_final;
		select * into #ga_1to2_final
		from #ga_1_final union select * from #ga_2_final;", .con = conn))
  
  message("--STEP 8C: Z3A code on NT scan")
  
  DBI::dbExecute(conn = conn, glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		IF OBJECT_ID(N'tempdb..#ga_3a') IS NOT NULL DROP TABLE #ga_3a;
		select a.*
		into #ga_3a
		from {`to_schema`}.tmp_mcaid_claim_preg_episode_temp as a
		left join #ga_1to2_final as b
			on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Pull out pregnany episodes that have a procedure code for an NT scan during the prenatal window
		IF OBJECT_ID(N'tempdb..#ga_3b') IS NOT NULL DROP TABLE #ga_3b;
		select a.*, b.last_service_date as procedure_date, b.claim_header_id 
		into #ga_3b
		from #ga_3a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as b
			on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('76813', '76814')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Further subset to pregnancy episodes that have a Z3A code
		IF OBJECT_ID(N'tempdb..#ga_3c') IS NOT NULL DROP TABLE #ga_3c;
		select a.*, right(icdcm_norm, 2) as ga_weeks_int
		into #ga_3c
		from #ga_3b as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`} as b
			on a.claim_header_id = b.claim_header_id
		where b.icdcm_version = 10 and b.icdcm_norm in ('Z3A08', 'Z3A09', 'Z3A10', 'Z3A11', 'Z3A12', 'Z3A13', 'Z3A14', 'Z3A15', 'Z3A16', 'Z3A17', 'Z3A18', 'Z3A19',
			'Z3A20', 'Z3A21', 'Z3A22', 'Z3A23', 'Z3A24', 'Z3A25', 'Z3A26', 'Z3A27', 'Z3A28', 'Z3A29', 'Z3A30', 'Z3A31', 'Z3A32', 'Z3A33', 'Z3A34', 'Z3A35', 'Z3A36',
			'Z3A37', 'Z3A38', 'Z3A39', 'Z3A40', 'Z3A41', 'Z3A42');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Convert extracted gestational age to integer and collapse to distinct rows
		IF OBJECT_ID(N'tempdb..#ga_3d') IS NOT NULL DROP TABLE #ga_3d;
		select distinct id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date,
			preg_end_date, preg_start_date_max, preg_start_date_min, procedure_date, cast(ga_weeks_int as tinyint) as ga_weeks_int
		into #ga_3d
		from #ga_3c;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--For episodes with multiple NT scans, select first (ranked by procedure date and GA)
		IF OBJECT_ID(N'tempdb..#ga_3e') IS NOT NULL DROP TABLE #ga_3e;
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date, a.ga_weeks_int
		into #ga_3e
		from (select *, rank() over (partition by preg_episode_id order by procedure_date, ga_weeks_int) as rank_col from #ga_3d) as a
		where a.rank_col = 1;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create column to hold corrected start date and calculate GA in days and weeks
		IF OBJECT_ID(N'tempdb..#ga_3f') IS NOT NULL DROP TABLE #ga_3f;
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into #ga_3f
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * ga_weeks_int * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from #ga_3e
		) as a;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create final dataset with flags for plausible pregnancy start date and GA
		IF OBJECT_ID(N'tempdb..#ga_3_final') IS NOT NULL DROP TABLE #ga_3_final;
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
		from #ga_3f;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Union assigned episodes thus far
		IF OBJECT_ID(N'tempdb..#ga_1to3_final') IS NOT NULL DROP TABLE #ga_1to3_final;
		select * into #ga_1to3_final from #ga_1to2_final
		union select * from #ga_3_final;", .con = conn))

  message("--STEP 8D: Z3A code on anatomic ultrasound")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	 IF OBJECT_ID(N'tempdb..#ga_4a') IS NOT NULL DROP TABLE #ga_4a;
	 select a.*
		into #ga_4a
		from {`to_schema`}.tmp_mcaid_claim_preg_episode_temp as a
		left join #ga_1to3_final as b
			on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Pull out pregnany episodes that have a procedure code for an anatomic ultrasound during the prenatal window
		IF OBJECT_ID(N'tempdb..#ga_4b') IS NOT NULL DROP TABLE #ga_4b;
		select a.*, b.last_service_date as procedure_date, b.claim_header_id 
		into #ga_4b
		from #ga_4a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as b
			on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('76811', '76812')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Further subset to pregnancy episodes that have a Z3A code
		IF OBJECT_ID(N'tempdb..#ga_4c') IS NOT NULL DROP TABLE #ga_4c;
		select a.*, right(icdcm_norm, 2) as ga_weeks_int
		into #ga_4c
		from #ga_4b as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`} as b
			on a.claim_header_id = b.claim_header_id
		where b.icdcm_version = 10 and b.icdcm_norm in ('Z3A08', 'Z3A09', 'Z3A10', 'Z3A11', 'Z3A12', 'Z3A13', 'Z3A14', 'Z3A15', 'Z3A16', 'Z3A17', 'Z3A18', 'Z3A19',
			'Z3A20', 'Z3A21', 'Z3A22', 'Z3A23', 'Z3A24', 'Z3A25', 'Z3A26', 'Z3A27', 'Z3A28', 'Z3A29', 'Z3A30', 'Z3A31', 'Z3A32', 'Z3A33', 'Z3A34', 'Z3A35', 'Z3A36',
			'Z3A37', 'Z3A38', 'Z3A39', 'Z3A40', 'Z3A41', 'Z3A42');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Convert extracted gestational age to integer and collapse to distinct rows
		IF OBJECT_ID(N'tempdb..#ga_4d') IS NOT NULL DROP TABLE #ga_4d;
		select distinct id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date,
			preg_end_date, preg_start_date_max, preg_start_date_min, procedure_date, cast(ga_weeks_int as tinyint) as ga_weeks_int
		into #ga_4d
		from #ga_4c;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--For episodes with multiple anatomic ultrasounds, select first (ranked by procedure date and GA)
		IF OBJECT_ID(N'tempdb..#ga_4e') IS NOT NULL DROP TABLE #ga_4e;
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date, a.ga_weeks_int
		into #ga_4e
		from (select *, rank() over (partition by preg_episode_id order by procedure_date, ga_weeks_int) as rank_col from #ga_4d) as a
		where a.rank_col = 1;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create column to hold corrected start date and calculate GA in days and weeks
		IF OBJECT_ID(N'tempdb..#ga_4f') IS NOT NULL DROP TABLE #ga_4f;
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into #ga_4f
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * ga_weeks_int * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from #ga_4e
		) as a;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create final dataset with flags for plausible pregnancy start date and GA
		IF OBJECT_ID(N'tempdb..#ga_4_final') IS NOT NULL DROP TABLE #ga_4_final;
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
		from #ga_4f;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Union assigned episodes thus far
		IF OBJECT_ID(N'tempdb..#ga_1to4_final') IS NOT NULL DROP TABLE #ga_1to4_final;
		select * into #ga_1to4_final from #ga_1to3_final
		union select * from #ga_4_final;", .con = conn))
  
  message("--STEP 8E: Z3A code on another type of prenatal service")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		IF OBJECT_ID(N'tempdb..#ga_5a') IS NOT NULL DROP TABLE #ga_5a;
		select a.*
		into #ga_5a
		from {`to_schema`}.tmp_mcaid_claim_preg_episode_temp as a
		left join #ga_1to4_final as b
			on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Pull out pregnany episodes that have a Z3A code during the prenatal window
		IF OBJECT_ID(N'tempdb..#ga_5b') IS NOT NULL DROP TABLE #ga_5b;
		select a.*, b.last_service_date as procedure_date, right(b.icdcm_norm, 2) as ga_weeks_int
		into #ga_5b
		from #ga_5a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`} as b
			on a.id_mcaid = b.id_mcaid
		where (b.icdcm_version = 10) and b.icdcm_norm in ('Z3A08', 'Z3A09', 'Z3A10', 'Z3A11', 'Z3A12', 'Z3A13', 'Z3A14', 'Z3A15', 'Z3A16', 'Z3A17', 'Z3A18', 'Z3A19',
			'Z3A20', 'Z3A21', 'Z3A22', 'Z3A23', 'Z3A24', 'Z3A25', 'Z3A26', 'Z3A27', 'Z3A28', 'Z3A29', 'Z3A30', 'Z3A31', 'Z3A32', 'Z3A33', 'Z3A34', 'Z3A35', 'Z3A36',
			'Z3A37', 'Z3A38', 'Z3A39', 'Z3A40', 'Z3A41', 'Z3A42') and (b.last_service_date between a.preg_start_date and a.preg_end_date);", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Convert ZA3 codes to integer (needed to be a separate step to avoid a cast error)
		IF OBJECT_ID(N'tempdb..#ga_5c') IS NOT NULL DROP TABLE #ga_5c;
		select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date, preg_end_date,
			preg_start_date_max, preg_start_date_min, procedure_date, cast(ga_weeks_int as tinyint) as ga_weeks_int
		into #ga_5c
		from #ga_5b;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Collapse to distinct rows
		IF OBJECT_ID(N'tempdb..#ga_5d') IS NOT NULL DROP TABLE #ga_5d;
		select distinct id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date,
			preg_end_date, preg_start_date_max, preg_start_date_min, procedure_date, ga_weeks_int
		into #ga_5d
		from #ga_5c;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create column to hold corrected start date and calculate GA in days and weeks
		IF OBJECT_ID(N'tempdb..#ga_5e') IS NOT NULL DROP TABLE #ga_5e;
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into #ga_5e
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * ga_weeks_int * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from #ga_5d
		) as a;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Count distinct preg start dates for each episode and calculate mode and median for those with >1 start date
		IF OBJECT_ID(N'tempdb..#ga_5f') IS NOT NULL DROP TABLE #ga_5f;
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
		) as c;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Count number of distinct start dates by preg episode and rank
		IF OBJECT_ID(N'tempdb..#ga_5g') IS NOT NULL DROP TABLE #ga_5g;
		select a.*, b.preg_start_rank_dcount
		into #ga_5g
		from #ga_5f as a
		left join(
			select preg_episode_id, preg_start_mode_rank, count(distinct preg_start_date_correct) as preg_start_rank_dcount
			from #ga_5f
			group by preg_episode_id, preg_start_mode_rank
		) as b
		on (a.preg_episode_id = b.preg_episode_id) and (a.preg_start_mode_rank) = (b.preg_start_mode_rank);", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create flag to indicate whether mode exists for episodes with >1 start date
		IF OBJECT_ID(N'tempdb..#ga_5h') IS NOT NULL DROP TABLE #ga_5h;
		select *,
		max(case when preg_start_mode_rank = 1 and preg_start_rank_dcount = 1 then 1 else 0 end)
			over (partition by preg_episode_id) as preg_start_mode_present
		into #ga_5h
		from #ga_5g;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create flag to indicate pregnancy start date to keep
		IF OBJECT_ID(N'tempdb..#ga_5i') IS NOT NULL DROP TABLE #ga_5i;
		select *,
			case
				when preg_start_dcount = 1 then 1
				when preg_start_mode_rank = 1 and preg_start_rank_dcount = 1 then 1
				when preg_start_mode_present = 0 and preg_start_date_correct = preg_start_median then 1
				else 0
				end as preg_start_date_keep
		into #ga_5i
		from #ga_5h;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Keep one pregnancy start date per episode
		IF OBJECT_ID(N'tempdb..#ga_5j') IS NOT NULL DROP TABLE #ga_5j;
		select distinct id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date_correct,
			preg_end_date, preg_start_date_max, preg_start_date_min,
			ga_days,
			ga_weeks
		into #ga_5j
		from #ga_5i
		where preg_start_date_keep = 1;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create final dataset with flags for plausible pregnancy start date and GA
		IF OBJECT_ID(N'tempdb..#ga_5_final') IS NOT NULL DROP TABLE #ga_5_final;
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
		from #ga_5j;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Union assigned episodes thus far
		--Beginning with this step (5), propagate episodes with invalid start date to next step
		IF OBJECT_ID(N'tempdb..#ga_1to5_final') IS NOT NULL DROP TABLE #ga_1to5_final;
		select * into #ga_1to5_final from #ga_1to4_final
		union select * from #ga_5_final where valid_start_date = 1 and valid_ga = 1;", .con = conn))

  message("--STEP 8F: Nuchal translucency (NT) scan without Z3A code")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		IF OBJECT_ID(N'tempdb..#ga_6a') IS NOT NULL DROP TABLE #ga_6a;
		select a.*
		into #ga_6a
		from {`to_schema`}.tmp_mcaid_claim_preg_episode_temp as a
		left join #ga_1to5_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Pull out pregnany episodes that have a procedure code for an NT scan during the prenatal window, collapse to distinct rows
		IF OBJECT_ID(N'tempdb..#ga_6b') IS NOT NULL DROP TABLE #ga_6b;
		select distinct a.*, b.last_service_date as procedure_date
		into #ga_6b
		from #ga_6a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as b
		on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('76813', '76814')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--For episodes with multiple NT scans, select first (ranked by procedure date) and take distinct rows
		IF OBJECT_ID(N'tempdb..#ga_6c') IS NOT NULL DROP TABLE #ga_6c;
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
		into #ga_6c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from #ga_6b) as a
		where a.rank_col = 1;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create column to hold corrected start date and calculate GA in days and weeks
		IF OBJECT_ID(N'tempdb..#ga_6d') IS NOT NULL DROP TABLE #ga_6d;
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into #ga_6d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, -89, procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from #ga_6c
		) as a;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create final dataset with flags for plausible pregnancy start date and GA
		IF OBJECT_ID(N'tempdb..#ga_6_final') IS NOT NULL DROP TABLE #ga_6_final;
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
		from #ga_6d;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Union assigned episodes thus far
		IF OBJECT_ID(N'tempdb..#ga_1to6_final') IS NOT NULL DROP TABLE #ga_1to6_final;
		select * into #ga_1to6_final from #ga_1to5_final
		union select * from #ga_6_final where valid_start_date = 1 and valid_ga = 1;", .con = conn))

  message("--STEP 8G: Chorionic Villus Sampling (CVS)")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		IF OBJECT_ID(N'tempdb..#ga_7a') IS NOT NULL DROP TABLE #ga_7a;
		select a.*
		into #ga_7a
		from {`to_schema`}.tmp_mcaid_claim_preg_episode_temp as a
		left join #ga_1to6_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Pull out pregnany episodes that have a procedure code for CVS during the prenatal window, collapse to distinct rows
		IF OBJECT_ID(N'tempdb..#ga_7b') IS NOT NULL DROP TABLE #ga_7b;
		select distinct a.*, b.last_service_date as procedure_date
		into #ga_7b
		from #ga_7a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as b
		on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('59015', '76945')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--For episodes with multiple CVS services, select first (ranked by procedure date)
		IF OBJECT_ID(N'tempdb..#ga_7c') IS NOT NULL DROP TABLE #ga_7c;
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
		into #ga_7c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from #ga_7b) as a
		where a.rank_col = 1;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create column to hold corrected start date and calculate GA in days and weeks
		IF OBJECT_ID(N'tempdb..#ga_7d') IS NOT NULL DROP TABLE #ga_7d;
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into #ga_7d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * 12 * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from #ga_7c
		) as a;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create final dataset with flags for plausible pregnancy start date and GA
		IF OBJECT_ID(N'tempdb..#ga_7_final') IS NOT NULL DROP TABLE #ga_7_final;
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
		from #ga_7d;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Union assigned episodes thus far
		IF OBJECT_ID(N'tempdb..#ga_1to7_final') IS NOT NULL DROP TABLE #ga_1to7_final;
		select * into #ga_1to7_final from #ga_1to6_final
		union select * from #ga_7_final where valid_start_date = 1 and valid_ga = 1;", .con = conn))
  
  message("--STEP 8H: Cell free fetal DNA screening")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		IF OBJECT_ID(N'tempdb..#ga_8a') IS NOT NULL DROP TABLE #ga_8a;
		select a.*
		into #ga_8a
		from {`to_schema`}.tmp_mcaid_claim_preg_episode_temp as a
		left join #ga_1to7_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Pull out pregnany episodes that have a procedure code for cell-free DNA sampling during the prenatal window, collapse to distinct rows
		IF OBJECT_ID(N'tempdb..#ga_8b') IS NOT NULL DROP TABLE #ga_8b;
		select distinct a.*, b.last_service_date as procedure_date
		into #ga_8b
		from #ga_8a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as b
		on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('81420', '81507')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--For episodes with multiple cell-free DNA sampling services, select first (ranked by procedure date)
		IF OBJECT_ID(N'tempdb..#ga_8c') IS NOT NULL DROP TABLE #ga_8c;
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
		into #ga_8c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from #ga_8b) as a
		where a.rank_col = 1;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create column to hold corrected start date and calculate GA in days and weeks
		IF OBJECT_ID(N'tempdb..#ga_8d') IS NOT NULL DROP TABLE #ga_8d;
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into #ga_8d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * 12 * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from #ga_8c
		) as a;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create final dataset with flags for plausible pregnancy start date and GA
		IF OBJECT_ID(N'tempdb..#ga_8_final') IS NOT NULL DROP TABLE #ga_8_final;
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
		from #ga_8d;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Union assigned episodes thus far
		IF OBJECT_ID(N'tempdb..#ga_1to8_final') IS NOT NULL DROP TABLE #ga_1to8_final;
		select * into #ga_1to8_final from #ga_1to7_final
		union select * from #ga_8_final where valid_start_date = 1 and valid_ga = 1;", .con = conn))

  message("--STEP 8I: Full-term code for live birth or stillbirth within 7 days of outcome date")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
	  IF OBJECT_ID(N'tempdb..#ga_9a') IS NOT NULL DROP TABLE #ga_9a;
		select a.*
		into #ga_9a
		from {`to_schema`}.tmp_mcaid_claim_preg_episode_temp as a
		left join #ga_1to8_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Pull out distinct ICD-10-CM codes from claims >= 2016-01-01 (<1 min)
		IF OBJECT_ID(N'tempdb..#dx_distinct_step8') IS NOT NULL DROP TABLE #dx_distinct_step8;
		select distinct icdcm_norm
		into #dx_distinct_step8
		from {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`}
		where last_service_date >= '2016-01-01';", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create temp table holding full-term status ICD-10-CM codes in LIKE format
		IF OBJECT_ID(N'tempdb..#dx_fullterm') IS NOT NULL DROP TABLE #dx_fullterm;
		create table #dx_fullterm (code_like varchar(255));
		insert into #dx_fullterm (code_like)
		SELECT 'O6020%' UNION ALL
    SELECT 'O6022%' UNION ALL
    SELECT 'O6023%' UNION ALL
    SELECT 'O4202%' UNION ALL
    SELECT 'O4292%' UNION ALL
    SELECT 'O471%' UNION ALL
    SELECT 'O80%';", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Join distinct ICD-10-CM codes to full-term ICD-10-CM reference table (<1 min)
		IF OBJECT_ID(N'tempdb..#ref_dx_fullterm') IS NOT NULL DROP TABLE #ref_dx_fullterm;
		select distinct a.icdcm_norm, b.code_like
		into #ref_dx_fullterm
		from #dx_distinct_step8 as a
		inner join #dx_fullterm as b
		on a.icdcm_norm like b.code_like;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Join new reference table to claims data using EXACT join (1 min)
		IF OBJECT_ID(N'tempdb..#fullterm_dx') IS NOT NULL DROP TABLE #fullterm_dx;
		select a.id_mcaid, a.last_service_date, a.icdcm_norm
		into #fullterm_dx
		from {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`} as a
		inner join #ref_dx_fullterm as b
		on a.icdcm_norm = b.icdcm_norm
		where a.last_service_date >= '2016-01-01';", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Pull out pregnancy episodes that have a full-term status ICD-10-CM code within 7 days of the outcome
		--Only include livebirth and stillbirth outcomes
		IF OBJECT_ID(N'tempdb..#ga_9b') IS NOT NULL DROP TABLE #ga_9b;
		select a.*, b.last_service_date as procedure_date
		into #ga_9b
		from #ga_9a as a
		inner join #fullterm_dx as b
		on a.id_mcaid = b.id_mcaid
		where (b.last_service_date between dateadd(day, -7, a.preg_end_date) and dateadd(day, 7, a.preg_end_date))
			and a.preg_endpoint in ('lb', 'sb');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--For episodes with multiple relevant services, select first (ranked by procedure date) and take distinct rows
		IF OBJECT_ID(N'tempdb..#ga_9c') IS NOT NULL DROP TABLE #ga_9c;
		select distinct a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
		into #ga_9c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from #ga_9b) as a
		where a.rank_col = 1;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create column to hold corrected start date and calculate GA in days and weeks
		IF OBJECT_ID(N'tempdb..#ga_9d') IS NOT NULL DROP TABLE #ga_9d;
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into #ga_9d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * 39 * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from #ga_9c
		) as a;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create final dataset with flags for plausible pregnancy start date and GA
		IF OBJECT_ID(N'tempdb..#ga_9_final') IS NOT NULL DROP TABLE #ga_9_final;
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
		from #ga_9d;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Union assigned episodes thus far
		IF OBJECT_ID(N'tempdb..#ga_1to9_final') IS NOT NULL DROP TABLE #ga_1to9_final;
		select * into #ga_1to9_final from #ga_1to8_final
		union select * from #ga_9_final where valid_start_date = 1 and valid_ga = 1;", .con = conn))
  
  message("--STEP 8J: Trimester codes within 7 days of outcome date")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		IF OBJECT_ID(N'tempdb..#ga_10a') IS NOT NULL DROP TABLE #ga_10a;
		select a.*
		into #ga_10a
		from {`to_schema`}.tmp_mcaid_claim_preg_episode_temp as a
		left join #ga_1to9_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Join distinct ICD-10-CM codes to ICD-10-CM codes in trimester codes reference table (<1 min)
		IF OBJECT_ID(N'tempdb..#ref_dx_trimester') IS NOT NULL DROP TABLE #ref_dx_trimester;
		select distinct a.icdcm_norm, b.code_like, b.trimester
		into #ref_dx_trimester
		from #dx_distinct_step8 as a
		inner join (select * from {`ref_schema`}.{`paste0(ref_table,'moll_trimester')`} where code_type = 'icd10cm') as b
		on a.icdcm_norm like b.code_like;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Join new reference table to claims data using EXACT join (1 min)
		IF OBJECT_ID(N'tempdb..#trimester_dx') IS NOT NULL DROP TABLE #trimester_dx;
		select a.id_mcaid, a.last_service_date, b.trimester
		into #trimester_dx
		from {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`} as a
		inner join #ref_dx_trimester as b
		on a.icdcm_norm = b.icdcm_norm
		where a.last_service_date >= '2016-01-01';", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Pull out distinct procedure codes from claims >= 2016-01-01 (<1 min)
		IF OBJECT_ID(N'tempdb..#px_distinct_step10') IS NOT NULL DROP TABLE #px_distinct_step10;
		select distinct procedure_code
		into #px_distinct_step10
		from {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`}
		where last_service_date >= '2016-01-01';", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Join distinct procedure codes to procedure codes in trimester codes reference table using LIKE join (<1 min)
		IF OBJECT_ID(N'tempdb..#ref_px_trimester') IS NOT NULL DROP TABLE #ref_px_trimester;
		select distinct a.procedure_code, b.code_like, b.trimester
		into #ref_px_trimester
		from #px_distinct_step10 as a
		inner join (select * from {`ref_schema`}.{`paste0(ref_table,'moll_trimester')`} where code_type = 'cpt_hcpcs') as b
		on a.procedure_code like b.code_like;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Join new reference table to claims data using EXACT join (1 min)
		IF OBJECT_ID(N'tempdb..#trimester_px') IS NOT NULL DROP TABLE #trimester_px;
		select a.id_mcaid, a.last_service_date, b.trimester
		into #trimester_px
		from {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as a
		inner join #ref_px_trimester as b
		on a.procedure_code = b.procedure_code
		where a.last_service_date >= '2016-01-01';", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Union diagnosis and procedure code tables
		IF OBJECT_ID(N'tempdb..#trimester_dx_px') IS NOT NULL DROP TABLE #trimester_dx_px;
		select * into #trimester_dx_px from #trimester_dx
		union select * from #trimester_px;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Pull out pregnany episodes that have a trimester code within 7 days of the outcome
		IF OBJECT_ID(N'tempdb..#ga_10b') IS NOT NULL DROP TABLE #ga_10b;
		select a.*, b.last_service_date as procedure_date, b.trimester
		into #ga_10b
		from #ga_10a as a
		inner join #trimester_dx_px as b
		on a.id_mcaid = b.id_mcaid
		where (b.last_service_date between dateadd(day, -7, a.preg_end_date) and dateadd(day, 7, a.preg_end_date));", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--For episodes with multiple relevant services, select first (ranked by procedure date DESC, trimester) and take distinct rows
		IF OBJECT_ID(N'tempdb..#ga_10c') IS NOT NULL DROP TABLE #ga_10c;
		select distinct a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date, a.trimester
		into #ga_10c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date desc, trimester) as rank_col from #ga_10b) as a
		where a.rank_col = 1;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create column to hold corrected start date and calculate GA in days and weeks
		IF OBJECT_ID(N'tempdb..#ga_10d') IS NOT NULL DROP TABLE #ga_10d;
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into #ga_10d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			--use trimester code to calculate corrected start date
			case when trimester = 1 then dateadd(day, -69, procedure_date)
			when trimester = 2 then dateadd(day, -146, procedure_date)
			when trimester = 3 then dateadd(day, -240, procedure_date)
			else null
			end as preg_start_date_correct,
			preg_end_date, preg_start_date_max, preg_start_date_min
			from #ga_10c
		) as a;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create final dataset with flags for plausible pregnancy start date and GA
		IF OBJECT_ID(N'tempdb..#ga_10_final') IS NOT NULL DROP TABLE #ga_10_final;
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
		from #ga_10d;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Union assigned episodes thus far
		IF OBJECT_ID(N'tempdb..#ga_1to10_final') IS NOT NULL DROP TABLE #ga_1to10_final;
		select * into #ga_1to10_final from #ga_1to9_final
		union select * from #ga_10_final where valid_start_date = 1 and valid_ga = 1;", .con = conn))

  message("--STEP 8K: Preterm code within 7 days of outcome date")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		IF OBJECT_ID(N'tempdb..#ga_11a') IS NOT NULL DROP TABLE #ga_11a;
		select a.*
		into #ga_11a
		from {`to_schema`}.tmp_mcaid_claim_preg_episode_temp as a
		left join #ga_1to10_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create temp table holding preterm status ICD-10-CM codes in LIKE format
		IF OBJECT_ID(N'tempdb..#dx_preterm') IS NOT NULL DROP TABLE #dx_preterm;
		create table #dx_preterm (code_like varchar(255));
		insert into #dx_preterm (code_like)
		SELECT 'O6010%' UNION ALL
    SELECT 'O6012%' UNION ALL
    SELECT 'O6013%' UNION ALL
    SELECT 'O6014%' UNION ALL
    SELECT 'O4201%' UNION ALL
    SELECT 'O4211%' UNION ALL
    SELECT 'O4291%';", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Join distinct ICD-10-CM codes to preterm ICD-10-CM reference table (<1 min)
		IF OBJECT_ID(N'tempdb..#ref_dx_preterm') IS NOT NULL DROP TABLE #ref_dx_preterm;
		select distinct a.icdcm_norm, b.code_like
		into #ref_dx_preterm
		from #dx_distinct_step8 as a
		inner join #dx_preterm as b
		on a.icdcm_norm like b.code_like;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Join new reference table to claims data using EXACT join (1 min)
		IF OBJECT_ID(N'tempdb..#preterm_dx') IS NOT NULL DROP TABLE #preterm_dx;
		select a.id_mcaid, a.last_service_date, a.icdcm_norm
		into #preterm_dx
		from {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`} as a
		inner join #ref_dx_preterm as b
		on a.icdcm_norm = b.icdcm_norm
		where a.last_service_date >= '2016-01-01';", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Pull out pregnancy episodes that have a preterm status ICD-10-CM code within 7 days of the outcome
		--Only include livebirth and stillbirth outcomes
		IF OBJECT_ID(N'tempdb..#ga_11b') IS NOT NULL DROP TABLE #ga_11b;
		select a.*, b.last_service_date as procedure_date
		into #ga_11b
		from #ga_11a as a
		inner join #preterm_dx as b
		on a.id_mcaid = b.id_mcaid
		where (b.last_service_date between dateadd(day, -7, a.preg_end_date) and dateadd(day, 7, a.preg_end_date));", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--For episodes with multiple relevant services, select first (ranked by procedure date) and take distinct rows
		IF OBJECT_ID(N'tempdb..#ga_11c') IS NOT NULL DROP TABLE #ga_11c;
		select distinct a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
		into #ga_11c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from #ga_11b) as a
		where a.rank_col = 1;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create column to hold corrected start date and calculate GA in days and weeks
		IF OBJECT_ID(N'tempdb..#ga_11d') IS NOT NULL DROP TABLE #ga_11d;
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into #ga_11d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * 35 * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from #ga_11c
		) as a;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create final dataset with flags for plausible pregnancy start date and GA
		IF OBJECT_ID(N'tempdb..#ga_11_final') IS NOT NULL DROP TABLE #ga_11_final;
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
		from #ga_11d;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Union assigned episodes thus far
		IF OBJECT_ID(N'tempdb..#ga_1to11_final') IS NOT NULL DROP TABLE #ga_1to11_final;
		select * into #ga_1to11_final from #ga_1to10_final
		union select * from #ga_11_final where valid_start_date = 1 and valid_ga = 1;", .con = conn))
  
  message("--STEP 8L: First glucose screening or tolerance test")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps	
		IF OBJECT_ID(N'tempdb..#ga_12a') IS NOT NULL DROP TABLE #ga_12a;
		select a.*
		into #ga_12a
		from {`to_schema`}.tmp_mcaid_claim_preg_episode_temp as a
		left join #ga_1to11_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Pull out pregnany episodes that have a procedure code for glucose screening/tolerance test during the prenatal window, collapse to distinct rows
		IF OBJECT_ID(N'tempdb..#ga_12b') IS NOT NULL DROP TABLE #ga_12b;
		select distinct a.*, b.last_service_date as procedure_date
		into #ga_12b
		from #ga_12a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as b
		on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('82950', '82951', '82952')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--For episodes with multiple glucose screenings, select first (ranked by procedure date)
		IF OBJECT_ID(N'tempdb..#ga_12c') IS NOT NULL DROP TABLE #ga_12c;
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
		into #ga_12c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from #ga_12b) as a
		where a.rank_col = 1;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create column to hold corrected start date and calculate GA in days and weeks
		IF OBJECT_ID(N'tempdb..#ga_12d') IS NOT NULL DROP TABLE #ga_12d;
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into #ga_12d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * 26 * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from #ga_12c
		) as a;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create final dataset with flags for plausible pregnancy start date and GA
		IF OBJECT_ID(N'tempdb..#ga_12_final') IS NOT NULL DROP TABLE #ga_12_final;
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
		from #ga_12d;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Union assigned episodes thus far
		IF OBJECT_ID(N'tempdb..#ga_1to12_final') IS NOT NULL DROP TABLE #ga_1to12_final;
		select * into #ga_1to12_final from #ga_1to11_final
		union select * from #ga_12_final where valid_start_date = 1 and valid_ga = 1;", .con = conn))

  message("--STEP 8M: Prenatal service > 7 days before outcome date with a trimester code")
  DBI::dbExecute(conn = conn, glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		IF OBJECT_ID(N'tempdb..#ga_13a') IS NOT NULL DROP TABLE #ga_13a;
		select a.*
		into #ga_13a
		from {`to_schema`}.tmp_mcaid_claim_preg_episode_temp as a
		left join #ga_1to12_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Pull out pregnany episodes that have a trimester code > 7 days before outcome
		IF OBJECT_ID(N'tempdb..#ga_13b') IS NOT NULL DROP TABLE #ga_13b;
		select a.*, b.last_service_date as procedure_date, b.trimester
		into #ga_13b
		from #ga_13a as a
		inner join #trimester_dx_px as b
		on a.id_mcaid = b.id_mcaid
		where (b.last_service_date < dateadd(day, -7, a.preg_end_date));", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--For episodes with multiple relevant services, select first (ranked by procedure date DESC, trimester) and take distinct rows
		IF OBJECT_ID(N'tempdb..#ga_13c') IS NOT NULL DROP TABLE #ga_13c;
		select distinct a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date, a.trimester
		into #ga_13c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date desc, trimester) as rank_col from #ga_13b) as a
		where a.rank_col = 1;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create column to hold corrected start date and calculate GA in days and weeks
		IF OBJECT_ID(N'tempdb..#ga_13d') IS NOT NULL DROP TABLE #ga_13d;
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into #ga_13d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			--use trimester code to calculate corrected start date
			case when trimester = 1 then dateadd(day, -69, procedure_date)
			when trimester = 2 then dateadd(day, -146, procedure_date)
			when trimester = 3 then dateadd(day, -240, procedure_date)
			else null
			end as preg_start_date_correct,
			preg_end_date, preg_start_date_max, preg_start_date_min
			from #ga_13c
		) as a;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create final dataset with flags for plausible pregnancy start date and GA
		IF OBJECT_ID(N'tempdb..#ga_13_final') IS NOT NULL DROP TABLE #ga_13_final;
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
		from #ga_13d;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Union assigned episodes thus far
		IF OBJECT_ID(N'tempdb..#ga_1to13_final') IS NOT NULL DROP TABLE #ga_1to13_final;
		select * into #ga_1to13_final from #ga_1to12_final
		union select * from #ga_13_final where valid_start_date = 1 and valid_ga = 1;", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		---------------
		--Union episodes that were not flagged by any of the 13 steps
		---------------
		--Save data thus far in persistent heap table
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_preg_episode',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_preg_episode;
    create table {`to_schema`}.tmp_mcaid_claim_preg_episode (
    id_mcaid varchar(255),
    preg_episode_id bigint,
    preg_endpoint varchar(255),
    preg_hier tinyint,
    preg_start_date date,
    preg_end_date date,
    ga_days int,
    ga_weeks numeric(4,1),
    valid_start_date tinyint,
    valid_ga tinyint,
    lb_type varchar(255),
    ga_estimation_step tinyint
    ) with (heap);", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
    insert into {`to_schema`}.tmp_mcaid_claim_preg_episode
    select id_mcaid, preg_episode_id, preg_endpoint, preg_hier, preg_start_date_correct as preg_start_date,
        preg_end_date, ga_days, ga_weeks, valid_start_date, valid_ga, lb_type, ga_estimation_step
    from #ga_1to13_final
    union
    select a.id_mcaid, a.preg_episode_id, a.preg_endpoint, a.preg_hier, a.preg_start_date,
        a.preg_end_date, null as ga_days, null as ga_weeks, 0 as valid_start_date, 0 as valid_ga,
        null as lb_type, null as ga_estimation_step
    from {`to_schema`}.tmp_mcaid_claim_preg_episode_temp as a
    left join #ga_1to13_final as b
    on a.preg_episode_id = b.preg_episode_id
    where b.preg_episode_id is null
    option (label = 'preg_episode');", .con = conn))

  message("STEP 9: Join to eligibility data to bring in age for subset")
  DBI::dbExecute(conn = conn, glue::glue_sql("
    IF OBJECT_ID(N'tempdb..#preg_episode_age_all') IS NOT NULL DROP TABLE #preg_episode_age_all;
    select a.id_mcaid,  
			case
				when floor((datediff(day, b.dob, a.preg_end_date) + 1) / 365.25) >=0 then floor((datediff(day, b.dob, a.preg_end_date) + 1) / 365.25)
				when floor((datediff(day, b.dob, a.preg_end_date) + 1) / 365.25) = -1 then 0
				end as age_at_outcome,
			a.preg_episode_id, a.preg_endpoint, a.preg_hier, a.preg_start_date, a.preg_end_date, a.ga_days, a.ga_weeks,
			a.valid_start_date, a.valid_ga, case when a.valid_start_date = 1 and a.valid_ga = 1 then 1 else 0 end as valid_both,
			a.lb_type, a.ga_estimation_step, getdate() as last_run
		into #preg_episode_age_all
		from {`to_schema`}.tmp_mcaid_claim_preg_episode as a
		left join {`final_schema`}.{`paste0(final_table,'mcaid_elig_demo')`} as b
			on a.id_mcaid = b.id_mcaid
			option (label = 'preg_episode_age_all');", .con = conn))
  DBI::dbExecute(conn = conn, glue::glue_sql("
		--Create table subset to ages 12 to 55 per Moll et al. method
		--Also add an age group variable
		select id_mcaid, age_at_outcome,		
			case when age_at_outcome between 12 and 19 then '12-19'
				when age_at_outcome between 20 and 24 then '20-24'
				when age_at_outcome between 25 and 29 then '25-29'
				when age_at_outcome between 30 and 34 then '30-34'
				when age_at_outcome between 35 and 39 then '35-39'
				when age_at_outcome between 40 and 55 then '40-55'
				end as age_at_outcome_cat6,
			preg_episode_id, preg_endpoint, preg_hier, preg_start_date, preg_end_date, ga_days, ga_weeks,
			valid_start_date, valid_ga, valid_both, lb_type, ga_estimation_step, getdate() as last_run
    into {`to_schema`}.{`to_table`}
		from #preg_episode_age_all
		where age_at_outcome between 12 and 55;", .con = conn))

  DBI::dbExecute(conn = conn, glue::glue_sql("
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_preg_endpoint',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_preg_endpoint;
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_preg_endpoint_union',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_preg_endpoint_union;
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_preg_episode',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_preg_episode;
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_preg_episode',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_preg_episode;
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_preg_episode_0',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_preg_episode_0;
    IF OBJECT_ID(N'{`to_schema`}.tmp_mcaid_claim_preg_episode_temp',N'U') IS NOT NULL DROP TABLE {`to_schema`}.tmp_mcaid_claim_preg_episode_temp;", .con = conn))
  
  time_end <- Sys.time()
  message("Loading took ", round(difftime(time_end, time_start, units = "secs"), 2), 
          " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
          " mins)")
  
  
  #### ADD INDEX ####
  #add_index_f(conn, server = server, table_config = config)
}