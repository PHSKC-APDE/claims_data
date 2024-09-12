# This code creates the the mcaid claim preg episode table
# Create a reference table for pregnancy episodes distributed in mcaid claims
#
# It is designed to be run as part of the master Medicaid script:
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcaid/master_mcaid_analytic.R
#
# R script developed by Jeremy Whitehurst using SQL scripts from Eli Kern, Jennifer Liu and Spencer Hensley
#
### 

### Function elements
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
  try(DBI::dbRemoveTable(conn, DBI::Id(schema = to_schema, table = to_table)))
  
  #### LOAD TABLE ####
  message("STEP 1: Find claims with a ICD-10-CM code relevant to pregnancy endpoints")
  try(odbc::dbRemoveTable(conn, "##pe_dx_distinct", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ref_dx", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_preg_dx", temporary = T), silent = T)
  
  step1_sql <- glue::glue_sql("
    --Pull out distinct ICD-10-CM codes from claims >= 2016-01-01 (<1 min)
    select distinct icdcm_norm
    into ##pe_dx_distinct
    from claims.final_mcaid_claim_icdcm_header
    where last_service_date >= '2016-01-01';

    --Join distinct ICD-10-CM codes to pregnancy endpoint reference table using LIKE join (<1 min)
    select distinct a.icdcm_norm, b.lb, b.ect, b.ab, b.sa, b.sb, b.tro, b.deliv
    into ##pe_ref_dx
    from ##pe_dx_distinct as a
    inner join (select * from {`ref_schema`}.{`paste0(ref_table, 'moll_preg_endpoint')`} where code_type = 'icd10cm') as b
    on a.icdcm_norm like b.code_like;

    --Join new reference table to claims data using EXACT join (1 min)
    select a.id_mcaid, a.claim_header_id, a.last_service_date, a.icdcm_norm, a.icdcm_version, 
	    a.icdcm_number, b.lb, b.ect, b.ab, b.sa, b.sb, b.tro, b.deliv
    into ##pe_preg_dx
    from {`final_schema`}.{`paste0(final_table, 'mcaid_claim_icdcm_header')`} as a
    inner join ##pe_ref_dx as b
    on a.icdcm_norm = b.icdcm_norm
    where a.last_service_date >= '2016-01-01';                          ", 
	  .con = conn)
  DBI::dbExecute(conn = conn, step1_sql)
    
  message("STEP 2: Find claims with a procedure code relevant to pregnancy endpoints")
  try(odbc::dbRemoveTable(conn, "##pe_px_distinct", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ref_px", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_preg_px", temporary = T), silent = T)
  step2_sql <- glue::glue_sql("
	  --Pull out distinct procedure codes from claims >= 2016-01-01 (<1 min)
    select distinct procedure_code
    into ##pe_px_distinct
    from {`final_schema`}.{`paste0(final_table, 'mcaid_claim_procedure')`}
    where last_service_date >= '2016-01-01';

    --Join distinct procedure codes to pregnancy endpoint reference table using LIKE join (<1 min)
    select distinct a.procedure_code, b.lb, b.ect, b.ab, b.sa, b.sb, b.tro, b.deliv
    into ##pe_ref_px
    from ##pe_px_distinct as a
    inner join (select * from {`ref_schema`}.{`paste0(ref_table, 'moll_preg_endpoint')`} where code_type in ('icd10pcs', 'hcpcs', 'cpt_hcpcs')) as b
      on a.procedure_code like b.code_like;

    --Join new reference table to claims data using EXACT join (1 min)
    select a.id_mcaid, a.claim_header_id, a.last_service_date, a.procedure_code, 
	    a.procedure_code_number, b.lb, b.ect, b.ab, b.sa, b.sb, b.tro, b.deliv
    into ##pe_preg_px
    from claims.final_mcaid_claim_procedure as a
    inner join ##pe_ref_px as b
      on a.procedure_code = b.procedure_code
    where a.last_service_date >= '2016-01-01';",
	  .con = conn)
  DBI::dbExecute(conn = conn, step2_sql)
  
  message("STEP 3: Union dx and px-based datasets, subsetting to common columns to collapse to distinct claim headers")
  try(odbc::dbRemoveTable(conn, "##pe_temp_1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_preg_dx_px", temporary = T), silent = T)
  step3_sql <- glue::glue_sql("
	  select id_mcaid, claim_header_id, last_service_date, lb, ect, ab, sa, sb, tro, deliv
    into ##pe_temp_1
    from ##pe_preg_dx
    union
    select id_mcaid, claim_header_id, last_service_date, lb, ect, ab, sa, sb, tro, deliv
    from ##pe_preg_px;

    --Convert all NULLS to ZEROES and cast as TINYINT for later math
    select id_mcaid, claim_header_id, last_service_date,
	    cast(isnull(lb,0) as tinyint) as lb, cast(isnull(ect,0) as tinyint) as ect, cast(isnull(ab,0) as tinyint) as ab,
	    cast(isnull(sa,0) as tinyint) as sa, cast(isnull(sb,0) as tinyint) as sb, cast(isnull(tro,0) as tinyint) as tro,
	    cast(isnull(deliv,0) as tinyint) as deliv
    into ##pe_preg_dx_px
    from ##pe_temp_1;",
    .con = conn)
  DBI::dbExecute(conn = conn, step3_sql)
  
  message("STEP 4: Group by ID-service date and count # of distinct endpoints (not including DELIV)")
  try(odbc::dbRemoveTable(conn, "##pe_temp_2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_temp_3", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_temp_4", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_temp_5", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_preg_endpoint", temporary = T), silent = T)
  step4_sql <- glue::glue_sql("
	  --Group by ID-service date and take max of each endpoint column
    select id_mcaid, last_service_date, max(lb) as lb, max(ect) as ect, max(ab) as ab, max(sa) as sa, max(sb) as sb,
	    max(tro) as tro, max(deliv) as deliv
    into ##pe_temp_2
    from ##pe_preg_dx_px
    group by id_mcaid, last_service_date;

    --Count # of distinct endpoints, not including DELIV
    select id_mcaid, last_service_date, lb, ect, ab, sa, sb, tro, deliv,
	    lb + ect + ab + sa + sb + tro as endpoint_dcount
    into ##pe_temp_3
    from ##pe_temp_2;

    --Recode DELIV to 0 when there's another valid endpoint
    select id_mcaid, last_service_date, lb, ect, ab, sa, sb, tro,
	    case when endpoint_dcount = 0 then deliv else 0 end as deliv,
	    endpoint_dcount
    into ##pe_temp_4
    from ##pe_temp_3;

    --Drop ID-service dates that contain >1 distinct endpoint (excluding DELIV)
    --NOTE - THIS REMOVES PREGNANCIES THAT HAD MULTIPLE GESTATIONS WITH TWO OR MORE DIFFERENT ENDPOINTS (e.g. liveborn and still birth)
    --Also restructure endpoint as a single variable, and add hierarchy variable
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
    into ##pe_temp_5
    from ##pe_temp_4
    where endpoint_dcount <= 1;

    --Add ranking variable within each pregnancy endpoint type
    select *, rank() over (partition by id_mcaid, preg_endpoint order by last_service_date) as preg_endpoint_rank 
    into ##pe_preg_endpoint
    from ##pe_temp_5;",
   .con = conn)
  DBI::dbExecute(conn = conn, step4_sql)
  
  message("STEP 5: Hierarchical assessment of pregnancy outcomes to create pregnancy episodes for each woman")
  message("--STEP 5A: Group livebirth service days into distinct pregnancy episodes")
  try(odbc::dbRemoveTable(conn, "##pe_lb_step1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_lb_final", temporary = T), silent = T)
  step5a_sql <- glue::glue_sql("
	  --Count days between each service day (<1 min)
    select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_endpoint_rank, a.date_compare_lag1,
	    datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into ##pe_lb_step1
    from(
	    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
		    lag(last_service_date, 1, last_service_date) over (partition by id_mcaid order by last_service_date) as date_compare_lag1
	     from ##pe_preg_endpoint
	    where preg_endpoint = 'lb'
      ) as a;

    --Group pregnancy endpoints into episodes based on minimum spacing (<1 min)
    with
      t as (
        select t.id_mcaid, t.last_service_date, t.preg_endpoint, t.preg_hier, t.preg_endpoint_rank, t.days_diff
	      from ##pe_lb_step1 as t
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
          on (t.id_mcaid = cte.id_mcaid) and (t.preg_endpoint_rank = cte.preg_endpoint_rank + 1)
        )
    --Clean up group column, keep only endpoints added to the timeline, and select columns to keep
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
	    rank() over (partition by id_mcaid order by last_service_date) as preg_episode_id
    into ##pe_lb_final
    from cte
    where timeline_include = 1;",
    .con = conn)
  DBI::dbExecute(conn = conn, step5a_sql)
  try(odbc::dbRemoveTable(conn, "##pe_lb_step1", temporary = T), silent = T)
  
  message("--STEP 5B: PROCESS STILLBIRTH EPISODES")
  try(odbc::dbRemoveTable(conn, "##pe_sb_step1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sb_step2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sb_step3", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sb_step4", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sb_step5", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sb_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_lb_sb_final", temporary = T), silent = T)
  step5b_sql <- glue::glue_sql("
    --Union LB and SB endpoints
    select *, last_service_date as prior_lb_date, last_service_date as next_lb_date into ##pe_sb_step1 from ##pe_lb_final
    union
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
	    null as prior_lb_date, null as next_lb_date
    from ##pe_preg_endpoint where preg_endpoint = 'sb'
    order by last_service_date;

    --Create column to hold dates of LB endpoints for comparison
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
      --create column to hold date of prior LB
      case when preg_endpoint = 'sb' then
        ISNULL(prior_lb_date, (SELECT TOP 1 last_service_date FROM ##pe_sb_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_lb_date IS NOT NULL ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_lb_date,
      --create column to hold date of next LB
      case when preg_endpoint = 'sb' then
        ISNULL(next_lb_date, (SELECT TOP 1 last_service_date FROM ##pe_sb_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_lb_date IS NOT NULL ORDER BY id_mcaid, last_service_date))
        else null
        end as next_lb_date
    into ##pe_sb_step2
    from ##pe_sb_step1 as t;

    --For each SB endpoint, count days between it and prior and next LB endpoint
    select *,	
	    case when preg_endpoint = 'sb' then datediff(day, prior_lb_date, last_service_date) else null end as days_diff_back_lb,
	    case when preg_endpoint = 'sb' then datediff(day, next_lb_date, last_service_date) else null end as days_diff_ahead_lb
    into ##pe_sb_step3
    from ##pe_sb_step2;

    --Pull out SB timepoints that potentially can be placed on timeline - COMPARE TO LB ENDPOINTS
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
    into ##pe_sb_step4
    from ##pe_sb_step3
    where preg_endpoint = 'sb'
	    and (days_diff_back_lb is null or days_diff_back_lb > 182)
	    and (days_diff_ahead_lb is null or days_diff_ahead_lb < -182);

    --Count days between each SB endpoint and regenerate preg_endpoint_rank variable
    select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier,
	    rank() over (partition by a.id_mcaid, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
	    datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into ##pe_sb_step5
    from(
	    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
	      lag(last_service_date, 1, last_service_date) over (partition by id_mcaid order by last_service_date) as date_compare_lag1
	     from ##pe_sb_step4
      ) as a;

    --Group SB endpoints into episodes based on minimum spacing
    with
    t as (
      select t.id_mcaid, t.last_service_date, t.preg_endpoint, t.preg_hier, t.preg_endpoint_rank, t.days_diff
    	from ##pe_sb_step5 as t
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
        on (t.id_mcaid = cte.id_mcaid) and (t.preg_endpoint_rank = cte.preg_endpoint_rank + 1)
      )
    --Clean up group column, keep only endpoints added to the timeline, and select columns to keep
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
	    rank() over (partition by id_mcaid order by last_service_date) as preg_episode_id
    into ##pe_sb_final
    from cte
    where timeline_include = 1;

    --Union LB and SB endpoints placed on timeline
    select * 
    into ##pe_lb_sb_final 
    from ##pe_lb_final
    union
    select * from ##pe_sb_final;",
    .con = conn)
  DBI::dbExecute(conn = conn, step5b_sql)
  try(odbc::dbRemoveTable(conn, "##pe_sb_step1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sb_step2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sb_step3", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sb_step4", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sb_step5", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sb_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_lb_final", temporary = T), silent = T)
  
  message("--STEP 5C: PROCESS DELIV EPISODES")
  try(odbc::dbRemoveTable(conn, "##pe_deliv_step1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_deliv_step2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_deliv_step3", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_deliv_step4", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_deliv_step5", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_deliv_final", temporary = T), silent = T)
  step5c_sql <- glue::glue_sql("
	  --Union LB-SB and DELIV endpoints
    select *, last_service_date as prior_date, last_service_date as next_date into ##pe_deliv_step1 from ##pe_lb_sb_final
    union
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
	    null as prior_date, null as next_date
    from ##pe_preg_endpoint where preg_endpoint = 'deliv'
    order by last_service_date;

    --Create column to hold dates of LB and SB endpoints for comparison
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
      --create column to hold date of prior LB
      case when preg_endpoint = 'deliv' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_deliv_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'lb'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_lb_date,
      --create column to hold date of next LB
      case when preg_endpoint = 'deliv' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_deliv_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'lb'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_lb_date,
      --create column to hold date of prior SB
      case when preg_endpoint = 'deliv' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_deliv_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'sb'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_sb_date,
      --create column to hold date of next SB
      case when preg_endpoint = 'deliv' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_deliv_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'sb'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_sb_date
    into ##pe_deliv_step2
    from ##pe_deliv_step1 as t;

    --For each DELIV endpoint, count days between it and prior and next LB and SB endpoints
    select *,	
	    case when preg_endpoint = 'deliv' then datediff(day, prior_lb_date, last_service_date) else null end as days_diff_back_lb,
	    case when preg_endpoint = 'deliv' then datediff(day, next_lb_date, last_service_date) else null end as days_diff_ahead_lb,
	    case when preg_endpoint = 'deliv' then datediff(day, prior_sb_date, last_service_date) else null end as days_diff_back_sb,
	    case when preg_endpoint = 'deliv' then datediff(day, next_sb_date, last_service_date) else null end as days_diff_ahead_sb
    into ##pe_deliv_step3
    from ##pe_deliv_step2;

    --Pull out DELIV timepoints that potentially can be placed on timeline - COMPARE TO LB and SB ENDPOINTS
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
    into ##pe_deliv_step4
    from ##pe_deliv_step3
    where preg_endpoint = 'deliv'
	    and (days_diff_back_lb is null or days_diff_back_lb > 182)
	    and (days_diff_ahead_lb is null or days_diff_ahead_lb < -182)
	    and (days_diff_back_sb is null or days_diff_back_sb > 168)
	    and (days_diff_ahead_sb is null or days_diff_ahead_sb < -168);

    --Count days between each DELIV endpoint and regenerate preg_endpoint_rank variable
    select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier,
	    rank() over (partition by a.id_mcaid, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
	    datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into ##pe_deliv_step5
    from(
	    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
	      lag(last_service_date, 1, last_service_date) over (partition by id_mcaid order by last_service_date) as date_compare_lag1
	     from ##pe_deliv_step4
      ) as a;

    --Group DELIV endpoints into episodes based on minimum spacing
    with
      t as (
        select t.id_mcaid, t.last_service_date, t.preg_endpoint, t.preg_hier, t.preg_endpoint_rank, t.days_diff
	      from ##pe_deliv_step5 as t
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
          on (t.id_mcaid = cte.id_mcaid) and (t.preg_endpoint_rank = cte.preg_endpoint_rank + 1)
        )
    --Clean up group column, keep only endpoints added to the timeline, and select columns to keep
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
	    rank() over (partition by id_mcaid order by last_service_date) as preg_episode_id
    into ##pe_deliv_final
    from cte
    where timeline_include = 1;

    --Union LB, SB and DELIV endpoints placed on timeline
    select * into ##pe_lb_sb_deliv_final from ##pe_lb_sb_final
    union
    select * from ##pe_deliv_final;",
    .con = conn)
  DBI::dbExecute(conn = conn, step5c_sql)
  try(odbc::dbRemoveTable(conn, "##pe_deliv_step1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_deliv_step2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_deliv_step3", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_deliv_step4", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_deliv_step5", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_deliv_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_lb_sb_final", temporary = T), silent = T)
  
  message("--STEP 5D: PROCESS TRO EPISODES")
  try(odbc::dbRemoveTable(conn, "##pe_tro_step1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_tro_step2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_tro_step3", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_tro_step4", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_tro_step5", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_tro_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_lb_sb_deliv_tro_final", temporary = T), silent = T)
  step5d_sql <- glue::glue_sql("
    --Union LB-SB-DELIV and TRO endpoints
    select *, last_service_date as prior_date, last_service_date as next_date into ##pe_tro_step1 from ##pe_lb_sb_deliv_final
    union
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
      null as prior_date, null as next_date
    from ##pe_preg_endpoint where preg_endpoint = 'tro'
    order by last_service_date;

    --Create column to hold dates of LB and SB endpoints for comparison
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
      --create column to hold date of prior LB
      case when preg_endpoint = 'tro' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_tro_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'lb'
	         ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_lb_date,
      --create column to hold date of next LB
      case when preg_endpoint = 'tro' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_tro_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'lb'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_lb_date,
      --create column to hold date of prior SB
      case when preg_endpoint = 'tro' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_tro_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'sb'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_sb_date,
      --create column to hold date of next SB
      case when preg_endpoint = 'tro' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_tro_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'sb'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_sb_date,
      --create column to hold date of prior DELIV
      case when preg_endpoint = 'tro' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_tro_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'deliv'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_deliv_date,
      --create column to hold date of next DELIV
      case when preg_endpoint = 'tro' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_tro_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'deliv'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_deliv_date
    into ##pe_tro_step2
    from ##pe_tro_step1 as t;

    --For each TRO endpoint, count days between it and prior and next LB, SB and DELIV endpoints
    select *,	
	    case when preg_endpoint = 'tro' then datediff(day, prior_lb_date, last_service_date) else null end as days_diff_back_lb,
	    case when preg_endpoint = 'tro' then datediff(day, next_lb_date, last_service_date) else null end as days_diff_ahead_lb,
	    case when preg_endpoint = 'tro' then datediff(day, prior_sb_date, last_service_date) else null end as days_diff_back_sb,
	    case when preg_endpoint = 'tro' then datediff(day, next_sb_date, last_service_date) else null end as days_diff_ahead_sb,
	    case when preg_endpoint = 'tro' then datediff(day, prior_deliv_date, last_service_date) else null end as days_diff_back_deliv,
	    case when preg_endpoint = 'tro' then datediff(day, next_deliv_date, last_service_date) else null end as days_diff_ahead_deliv
    into ##pe_tro_step3
    from ##pe_tro_step2;
  
    --Pull out TRO timepoints that potentially can be placed on timeline - COMPARE TO LB, SB, and DELIV ENDPOINTS
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
    into ##pe_tro_step4
    from ##pe_tro_step3
    where preg_endpoint = 'tro'
	    and (days_diff_back_lb is null or days_diff_back_lb > 168)
	    and (days_diff_ahead_lb is null or days_diff_ahead_lb < -168)
	    and (days_diff_back_sb is null or days_diff_back_sb > 154)
	    and (days_diff_ahead_sb is null or days_diff_ahead_sb < -154)
	    and (days_diff_back_deliv is null or days_diff_back_deliv > 154)
	    and (days_diff_ahead_deliv is null or days_diff_ahead_deliv < -154);
  
    --Count days between each TRO endpoint and regenerate preg_endpoint_rank variable
    select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier,
	    rank() over (partition by a.id_mcaid, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
	    datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into ##pe_tro_step5
    from(
	    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
	      lag(last_service_date, 1, last_service_date) over (partition by id_mcaid order by last_service_date) as date_compare_lag1
	      from ##pe_tro_step4
      ) as a;

    --Group TRO endpoints into episodes based on minimum spacing
    with
      t as (
        select t.id_mcaid, t.last_service_date, t.preg_endpoint, t.preg_hier, t.preg_endpoint_rank, t.days_diff
	      from ##pe_tro_step5 as t
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
          on (t.id_mcaid = cte.id_mcaid) and (t.preg_endpoint_rank = cte.preg_endpoint_rank + 1)
        )
    
    --Clean up group column, keep only endpoints added to the timeline, and select columns to keep
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
	    rank() over (partition by id_mcaid order by last_service_date) as preg_episode_id
    into ##pe_tro_final
    from cte
    where timeline_include = 1;

    --Union LB, SB, DELIV and TRO endpoints placed on timeline
    select * into ##pe_lb_sb_deliv_tro_final from ##pe_lb_sb_deliv_final
    union
    select * from ##pe_tro_final;",
    .con = conn)
  DBI::dbExecute(conn = conn, step5d_sql)
  try(odbc::dbRemoveTable(conn, "##pe_tro_step1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_tro_step2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_tro_step3", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_tro_step4", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_tro_step5", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_tro_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_lb_sb_deliv_final", temporary = T), silent = T)
  
  message("--STEP 5E: PROCESS ECT EPISODES")
  try(odbc::dbRemoveTable(conn, "##pe_ect_step1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ect_step2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ect_step3", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ect_step4", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ect_step5", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ect_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_lb_sb_deliv_tro_ect_final", temporary = T), silent = T)
  step5e_sql <- glue::glue_sql("
    --Union LB-SB, DELIV, TRO and ECT endpoints
    select *, last_service_date as prior_date, last_service_date as next_date into ##pe_ect_step1 from ##pe_lb_sb_deliv_tro_final
    union
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
	    null as prior_date, null as next_date
    from ##pe_preg_endpoint where preg_endpoint = 'ect'
    order by last_service_date;

    --Create column to hold dates of LB, SB, DELIV and TRO endpoints for comparison
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
      --create column to hold date of prior LB
      case when preg_endpoint = 'ect' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_ect_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'lb'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_lb_date,
      --create column to hold date of next LB
      case when preg_endpoint = 'ect' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_ect_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'lb'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_lb_date,
      --create column to hold date of prior SB
      case when preg_endpoint = 'ect' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_ect_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'sb'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_sb_date,
      --create column to hold date of next SB
      case when preg_endpoint = 'ect' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_ect_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'sb'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_sb_date,
      --create column to hold date of prior DELIV
      case when preg_endpoint = 'ect' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_ect_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'deliv'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_deliv_date,
      --create column to hold date of next DELIV
      case when preg_endpoint = 'ect' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_ect_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'deliv'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_deliv_date,
      --create column to hold date of prior TRO
      case when preg_endpoint = 'ect' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_ect_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'tro'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_tro_date,
      --create column to hold date of next TRO
      case when preg_endpoint = 'ect' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_ect_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'tro'
  	      ORDER BY id_mcaid, last_service_date))
        else null
        end as next_tro_date
    into ##pe_ect_step2
    from ##pe_ect_step1 as t;
  
    --For each ECT endpoint, count days between it and prior and next LB, SB, DELIV, and TRO endpoints
    select *,	
	    case when preg_endpoint = 'ect' then datediff(day, prior_lb_date, last_service_date) else null end as days_diff_back_lb,
	    case when preg_endpoint = 'ect' then datediff(day, next_lb_date, last_service_date) else null end as days_diff_ahead_lb,
	    case when preg_endpoint = 'ect' then datediff(day, prior_sb_date, last_service_date) else null end as days_diff_back_sb,
	    case when preg_endpoint = 'ect' then datediff(day, next_sb_date, last_service_date) else null end as days_diff_ahead_sb,
	    case when preg_endpoint = 'ect' then datediff(day, prior_deliv_date, last_service_date) else null end as days_diff_back_deliv,
	    case when preg_endpoint = 'ect' then datediff(day, next_deliv_date, last_service_date) else null end as days_diff_ahead_deliv,
	    case when preg_endpoint = 'ect' then datediff(day, prior_tro_date, last_service_date) else null end as days_diff_back_tro,
	    case when preg_endpoint = 'ect' then datediff(day, next_tro_date, last_service_date) else null end as days_diff_ahead_tro
    into ##pe_ect_step3
    from ##pe_ect_step2;
  
    --Pull out ECT timepoints that potentially can be placed on timeline - COMPARE TO LB, SB, DELIV and TRO ENDPOINTS
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
    into ##pe_ect_step4
    from ##pe_ect_step3
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
    select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier,
	    rank() over (partition by a.id_mcaid, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
	    datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into ##pe_ect_step5
    from(
	    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
	      lag(last_service_date, 1, last_service_date) over (partition by id_mcaid order by last_service_date) as date_compare_lag1
	      from ##pe_ect_step4
        ) as a;

    --Group ECT endpoints into episodes based on minimum spacing
    with
      t as (
        select t.id_mcaid, t.last_service_date, t.preg_endpoint, t.preg_hier, t.preg_endpoint_rank, t.days_diff
	      from ##pe_ect_step5 as t
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
          on (t.id_mcaid = cte.id_mcaid) and (t.preg_endpoint_rank = cte.preg_endpoint_rank + 1)
          )
    
    --Clean up group column, keep only endpoints added to the timeline, and select columns to keep
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
	    rank() over (partition by id_mcaid order by last_service_date) as preg_episode_id
    into ##pe_ect_final
    from cte
    where timeline_include = 1;

    --Union LB, SB, DELIV, TRO, and ECT endpoints placed on timeline
    select * into ##pe_lb_sb_deliv_tro_ect_final from ##pe_lb_sb_deliv_tro_final
    union
    select * from ##pe_ect_final;",
    .con = conn)
  DBI::dbExecute(conn = conn, step5e_sql)
  try(odbc::dbRemoveTable(conn, "##pe_ect_step1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ect_step2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ect_step3", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ect_step4", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ect_step5", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ect_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_lb_sb_deliv_tro_final", temporary = T), silent = T)
  
  message("--STEP 5F: PROCESS AB EPISODES")
  try(odbc::dbRemoveTable(conn, "##pe_ab_step1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ab_step2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ab_step3", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ab_step4", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ab_step5", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ab_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_lb_sb_deliv_tro_ect_ab_final", temporary = T), silent = T)
  step5f_sql <- glue::glue_sql("
	  --Union LB-SB, DELIV, TRO, ECT, an AB endpoints
    select *, last_service_date as prior_date, last_service_date as next_date into ##pe_ab_step1 from ##pe_lb_sb_deliv_tro_ect_final
    union
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
	    null as prior_date, null as next_date
    from ##pe_preg_endpoint where preg_endpoint = 'ab'
    order by last_service_date;

    --Create column to hold dates of LB, SB, DELIV, TRO, and ECT endpoints for comparison
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
      --create column to hold date of prior LB
      case when preg_endpoint = 'ab' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_ab_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'lb'
	       ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_lb_date,
      --create column to hold date of next LB
      case when preg_endpoint = 'ab' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_ab_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'lb'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_lb_date,
      --create column to hold date of prior SB
      case when preg_endpoint = 'ab' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_ab_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'sb'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_sb_date,
      --create column to hold date of next SB
      case when preg_endpoint = 'ab' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_ab_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'sb'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_sb_date,
      --create column to hold date of prior DELIV
      case when preg_endpoint = 'ab' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_ab_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'deliv'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_deliv_date,
      --create column to hold date of next DELIV
      case when preg_endpoint = 'ab' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_ab_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'deliv'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_deliv_date,
      --create column to hold date of prior TRO
      case when preg_endpoint = 'ab' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_ab_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'tro'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_tro_date,
      --create column to hold date of next TRO
      case when preg_endpoint = 'ab' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_ab_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'tro'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_tro_date,
      --create column to hold date of prior ECT
      case when preg_endpoint = 'ab' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_ab_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'ect'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_ect_date,
      --create column to hold date of next ECT
      case when preg_endpoint = 'ab' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_ab_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'ect'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_ect_date
    into ##pe_ab_step2
    from ##pe_ab_step1 as t;

    --For each AB endpoint, count days between it and prior and next LB, SB, DELIV, TRO, and ECT endpoints
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
    into ##pe_ab_step3
    from ##pe_ab_step2;

    --Pull out AB timepoints that potentially can be placed on timeline - COMPARE TO LB, SB, DELIV, TRO, and ECT ENDPOINTS
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
    into ##pe_ab_step4
    from ##pe_ab_step3
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
    select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier,
	    rank() over (partition by a.id_mcaid, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
	    datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into ##pe_ab_step5
    from(
	    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
	      lag(last_service_date, 1, last_service_date) over (partition by id_mcaid order by last_service_date) as date_compare_lag1
	      from ##pe_ab_step4
        ) as a;
  
    --Group AB endpoints into episodes based on minimum spacing
    with
      t as (
        select t.id_mcaid, t.last_service_date, t.preg_endpoint, t.preg_hier, t.preg_endpoint_rank, t.days_diff
	      from ##pe_ab_step5 as t
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
          on (t.id_mcaid = cte.id_mcaid) and (t.preg_endpoint_rank = cte.preg_endpoint_rank + 1)
          )
    --Clean up group column, keep only endpoints added to the timeline, and select columns to keep
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
	    rank() over (partition by id_mcaid order by last_service_date) as preg_episode_id
    into ##pe_ab_final
    from cte
    where timeline_include = 1;

    --Union LB, SB, DELIV, TRO, ECT and AB endpoints placed on timeline
    select * into ##pe_lb_sb_deliv_tro_ect_ab_final from ##pe_lb_sb_deliv_tro_ect_final
    union
    select * from ##pe_ab_final;",
    .con = conn)
  DBI::dbExecute(conn = conn, step5f_sql)
  try(odbc::dbRemoveTable(conn, "##pe_ab_step1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ab_step2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ab_step3", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ab_step4", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ab_step5", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ab_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_lb_sb_deliv_tro_ect_final", temporary = T), silent = T)
  
  message("--STEP 5G: PROCESS SA EPISODES")
  try(odbc::dbRemoveTable(conn, "##pe_sa_step1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sa_step2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sa_step3", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sa_step4", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sa_step5", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sa_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_lb_sb_deliv_tro_ect_ab_sa_final", temporary = T), silent = T)
  step5g_sql <- glue::glue_sql("
	  --Union LB-SB, DELIV, TRO, ECT, AB, and SA endpoints
    select *, last_service_date as prior_date, last_service_date as next_date into ##pe_sa_step1 from ##pe_lb_sb_deliv_tro_ect_ab_final
    union
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, null as preg_episode_id,
	    null as prior_date, null as next_date
    from ##pe_preg_endpoint where preg_endpoint = 'sa'
    order by last_service_date;

    --Create column to hold dates of LB, SB, DELIV, TRO, ECT, and AB endpoints for comparison
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id,
      --create column to hold date of prior LB
      case when preg_endpoint = 'sa' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_sa_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'lb'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_lb_date,
      --create column to hold date of next LB
      case when preg_endpoint = 'sa' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_sa_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'lb'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_lb_date,
      --create column to hold date of prior SB
      case when preg_endpoint = 'sa' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_sa_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'sb'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_sb_date,
      --create column to hold date of next SB
      case when preg_endpoint = 'sa' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_sa_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'sb'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_sb_date,
      --create column to hold date of prior DELIV
      case when preg_endpoint = 'sa' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_sa_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'deliv'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_deliv_date,
      --create column to hold date of next DELIV
      case when preg_endpoint = 'sa' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_sa_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'deliv'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_deliv_date,
      --create column to hold date of prior TRO
      case when preg_endpoint = 'sa' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_sa_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'tro'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_tro_date,
      --create column to hold date of next TRO
      case when preg_endpoint = 'sa' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_sa_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'tro'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_tro_date,
      --create column to hold date of prior ECT
      case when preg_endpoint = 'sa' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_sa_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'ect'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_ect_date,
      --create column to hold date of next ECT
      case when preg_endpoint = 'sa' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_sa_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'ect'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_ect_date,
      --create column to hold date of prior AB
      case when preg_endpoint = 'sa' then
        ISNULL(prior_date, (SELECT TOP 1 last_service_date FROM ##pe_sa_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date < t.last_service_date AND prior_date IS NOT NULL and preg_endpoint = 'ab'
	        ORDER BY id_mcaid, last_service_date DESC))
        else null
        end as prior_ab_date,
      --create column to hold date of next AB
      case when preg_endpoint = 'sa' then
        ISNULL(next_date, (SELECT TOP 1 last_service_date FROM ##pe_sa_step1
	        WHERE id_mcaid = t.id_mcaid and last_service_date > t.last_service_date AND next_date IS NOT NULL and preg_endpoint = 'ab'
	        ORDER BY id_mcaid, last_service_date))
        else null
        end as next_ab_date
    into ##pe_sa_step2
    from ##pe_sa_step1 as t;

    --For each SA endpoint, count days between it and prior and next LB, SB, DELIV, TRO, ECT, and AB endpoints
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
    into ##pe_sa_step3
    from ##pe_sa_step2;

    --Pull out SA timepoints that potentially can be placed on timeline - COMPARE TO LB, SB, DELIV, TRO, ECT, and AB ENDPOINTS
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank, preg_episode_id
    into ##pe_sa_step4
    from ##pe_sa_step3
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
    select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier,
	    rank() over (partition by a.id_mcaid, a.preg_endpoint order by a.last_service_date) as preg_endpoint_rank,
	    datediff(day, a.date_compare_lag1, a.last_service_date) as days_diff
    into ##pe_sa_step5
    from(
	    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
	      lag(last_service_date, 1, last_service_date) over (partition by id_mcaid order by last_service_date) as date_compare_lag1
	      from ##pe_sa_step4
        ) as a;
  
    --Group SA endpoints into episodes based on minimum spacing
    with
      t as (
        select t.id_mcaid, t.last_service_date, t.preg_endpoint, t.preg_hier, t.preg_endpoint_rank, t.days_diff
	      from ##pe_sa_step5 as t
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
          on (t.id_mcaid = cte.id_mcaid) and (t.preg_endpoint_rank = cte.preg_endpoint_rank + 1)
        )

    --Clean up group column, keep only endpoints added to the timeline, and select columns to keep
    select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_endpoint_rank,
	    rank() over (partition by id_mcaid order by last_service_date) as preg_episode_id
    into ##pe_sa_final
    from cte
    where timeline_include = 1;

    --Union LB, SB, DELIV, TRO, ECT, AB, and SA endpoints placed on timeline
    select * into ##pe_preg_endpoint_union from ##pe_lb_sb_deliv_tro_ect_ab_final
    union
    select * from ##pe_sa_final;",
     .con = conn)
  DBI::dbExecute(conn = conn, step5g_sql)
  try(odbc::dbRemoveTable(conn, "##pe_sa_step1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sa_step2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sa_step3", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sa_step4", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sa_step5", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_sa_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_lb_sb_deliv_tro_ect_ab_final", temporary = T), silent = T)
  
  
  message("STEP 6: Regenerate pregnancy episode ID to be unique across dataset")
  try(odbc::dbRemoveTable(conn, "##pe_episode_0", temporary = T), silent = T)
  step6_sql <- glue::glue_sql("
	  select id_mcaid, last_service_date, preg_endpoint, preg_hier,
	    dense_rank() over (order by id_mcaid, last_service_date) as preg_episode_id
    into ##pe_episode_0
    from ##pe_preg_endpoint_union;",
    .con = conn)
  DBI::dbExecute(conn = conn, step6_sql)
  
  message("STEP 7: Define prenatal window for each pregnancy episode (<1 min)")
  try(odbc::dbRemoveTable(conn, "##pe_episode_1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_episode_2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_episode_temp", temporary = T), silent = T)
  step7_sql <- glue::glue_sql("
	  --Create columns to hold information about prior pregnancy episode
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
    into ##pe_episode_1
    from ##pe_episode_0;

    --Calculate start and end dates for each pregnancy episode
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
    into ##pe_episode_2
    from ##pe_episode_1 order by last_service_date;

    --Add columns to hold earliest and latest pregnancy start date for later processing
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
    into ##pe_preg_episode_temp
    from ##pe_episode_2;",
    .con = conn)
  DBI::dbExecute(conn = conn, step7_sql)
  
  message("STEP 8: Use claims that provide information about gestational age to correct pregnancy outcome and start date")
  message("--STEP 8A: Intrauterine insemination/embryo transfer")
  try(odbc::dbRemoveTable(conn, "##pe_ga_1a", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_1b", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_1c", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_1d", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_1_final", temporary = T), silent = T)  
  step8a_sql <- glue::glue_sql("
	  --Pull out pregnany episodes that have relevant procedure codes during prenatal window
		select a.*, b.last_service_date as procedure_date, b.procedure_code
		into ##pe_ga_1a
		from ##pe_preg_episode_temp as a
		inner join {`final_schema`}.{`paste0(final_table, 'mcaid_claim_procedure')`} as b
			on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('58321', '58322', 'S4035', '58974', '58976', 'S4037')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;
		
		--Create column to hold corrected start date
		select distinct id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, -13, procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
		into ##pe_ga_1b
		from ##pe_ga_1a;
		
		--For episodes with more than corrected start, select record that is closet to pregnancy end date
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date_correct, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min
		into ##pe_ga_1c
		from (select *, rank() over (partition by preg_episode_id order by preg_start_date_correct desc) as rank_col from ##pe_ga_1b) as a
		where a.rank_col = 1;
		
		--Calculate gestational age in days and weeks
		select *,
			datediff(day, preg_start_date_correct, preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, preg_start_date_correct, preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into ##pe_ga_1d
		from ##pe_ga_1c;
		
		--Create final dataset with flags for plausible pregnancy start date and GA
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
		into ##pe_ga_1_final
		from ##pe_ga_1d;",
                             .con = conn)
  DBI::dbExecute(conn = conn, step8a_sql)
  try(odbc::dbRemoveTable(conn, "##pe_ga_1a", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_1b", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_1c", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_1d", temporary = T), silent = T) 
  
  message("--STEP 8B: Z3A code on 1st trimester ultrasound")
  try(odbc::dbRemoveTable(conn, "##pe_ga_2a", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_2b", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_2c", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_2d", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_2e", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_2f", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_2_final", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to2_final", temporary = T), silent = T) 
  step8b_sql <- glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps	
		select a.*
		into ##pe_ga_2a
		from ##pe_preg_episode_temp as a
		left join ##pe_ga_1_final as b
			on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;
		
		--Pull out pregnany episodes that have a procedure code for a 1st trimester ultrasound during the prenatal window
		select a.*, b.last_service_date as procedure_date, b.claim_header_id 
		into ##pe_ga_2b
		from ##pe_ga_2a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as b
			on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('76801', '76802')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;
		
		--Further subset to pregnancy episodes that have a Z3A code
		select a.*, right(icdcm_norm, 2) as ga_weeks_int
		into ##pe_ga_2c
		from ##pe_ga_2b as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`} as b
			on a.claim_header_id = b.claim_header_id
		where b.icdcm_version = 10 and b.icdcm_norm in ('Z3A08', 'Z3A09', 'Z3A10', 'Z3A11', 'Z3A12', 'Z3A13', 'Z3A14', 'Z3A15', 'Z3A16', 'Z3A17', 'Z3A18', 'Z3A19',
			'Z3A20', 'Z3A21', 'Z3A22', 'Z3A23', 'Z3A24', 'Z3A25', 'Z3A26', 'Z3A27', 'Z3A28', 'Z3A29', 'Z3A30', 'Z3A31', 'Z3A32', 'Z3A33', 'Z3A34', 'Z3A35', 'Z3A36',
			'Z3A37', 'Z3A38', 'Z3A39', 'Z3A40', 'Z3A41', 'Z3A42');
		
		--Convert extracted gestational age to integer and collapse to distinct rows
		select distinct id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date,
			preg_end_date, preg_start_date_max, preg_start_date_min, procedure_date, cast(ga_weeks_int as tinyint) as ga_weeks_int
		into ##pe_ga_2d
		from ##pe_ga_2c;
		
		--For episodes with multiple first trimester ultrasounds, select first (ranked by procedure date and GA)
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date, a.ga_weeks_int
		into ##pe_ga_2e
		from (select *, rank() over (partition by preg_episode_id order by procedure_date, ga_weeks_int) as rank_col from ##pe_ga_2d) as a
		where a.rank_col = 1;
		
		--Create column to hold corrected start date and calculate GA in days and weeks
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into ##pe_ga_2f
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
				dateadd(day, ((-1 * ga_weeks_int * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from ##pe_ga_2e
			) as a;
		
		--Create final dataset with flags for plausible pregnancy start date and GA
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
		into ##pe_ga_2_final
		from ##pe_ga_2f;	
	
		--Union assigned episodes thus far
		select * into ##pe_ga_1to2_final
		from ##pe_ga_1_final union select * from ##pe_ga_2_final;",
                              .con = conn)
  DBI::dbExecute(conn = conn, step8b_sql)
  try(odbc::dbRemoveTable(conn, "##pe_ga_2a", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_2b", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_2c", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_2d", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_2e", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_2f", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_2_final", temporary = T), silent = T)
  
  message("--STEP 8C: Z3A code on NT scan")
  try(odbc::dbRemoveTable(conn, "##pe_ga_3a", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_3b", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_3c", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_3d", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_3e", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_3f", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_3_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to3_final", temporary = T), silent = T) 
  
  step8c_sql <- glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		select a.*
		into ##pe_ga_3a
		from ##pe_preg_episode_temp as a
		left join ##pe_ga_1to2_final as b
			on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;
		
		--Pull out pregnany episodes that have a procedure code for an NT scan during the prenatal window
		select a.*, b.last_service_date as procedure_date, b.claim_header_id 
		into ##pe_ga_3b
		from ##pe_ga_3a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as b
			on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('76813', '76814')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;
		
		--Further subset to pregnancy episodes that have a Z3A code
		select a.*, right(icdcm_norm, 2) as ga_weeks_int
		into ##pe_ga_3c
		from ##pe_ga_3b as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`} as b
			on a.claim_header_id = b.claim_header_id
		where b.icdcm_version = 10 and b.icdcm_norm in ('Z3A08', 'Z3A09', 'Z3A10', 'Z3A11', 'Z3A12', 'Z3A13', 'Z3A14', 'Z3A15', 'Z3A16', 'Z3A17', 'Z3A18', 'Z3A19',
			'Z3A20', 'Z3A21', 'Z3A22', 'Z3A23', 'Z3A24', 'Z3A25', 'Z3A26', 'Z3A27', 'Z3A28', 'Z3A29', 'Z3A30', 'Z3A31', 'Z3A32', 'Z3A33', 'Z3A34', 'Z3A35', 'Z3A36',
			'Z3A37', 'Z3A38', 'Z3A39', 'Z3A40', 'Z3A41', 'Z3A42');
		
		--Convert extracted gestational age to integer and collapse to distinct rows
		select distinct id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date,
			preg_end_date, preg_start_date_max, preg_start_date_min, procedure_date, cast(ga_weeks_int as tinyint) as ga_weeks_int
		into ##pe_ga_3d
		from ##pe_ga_3c;
		
		--For episodes with multiple NT scans, select first (ranked by procedure date and GA)
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date, a.ga_weeks_int
		into ##pe_ga_3e
		from (select *, rank() over (partition by preg_episode_id order by procedure_date, ga_weeks_int) as rank_col from ##pe_ga_3d) as a
		where a.rank_col = 1;
		
		--Create column to hold corrected start date and calculate GA in days and weeks
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into ##pe_ga_3f
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * ga_weeks_int * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from ##pe_ga_3e
		) as a;
		
		--Create final dataset with flags for plausible pregnancy start date and GA
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
		into ##pe_ga_3_final
		from ##pe_ga_3f;
		
		--Union assigned episodes thus far
		select * into ##pe_ga_1to3_final from ##pe_ga_1to2_final
		union select * from ##pe_ga_3_final;",
                              .con = conn)
  DBI::dbExecute(conn = conn, step8c_sql)
  try(odbc::dbRemoveTable(conn, "##pe_ga_3a", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_3b", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_3c", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_3d", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_3e", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_3f", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_3_final", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to2_final", temporary = T), silent = T)
  
  message("--STEP 8D: Z3A code on anatomic ultrasound")
  try(odbc::dbRemoveTable(conn, "##pe_ga_4a", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_4b", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_4c", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_4d", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_4e", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_4f", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_4_final", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to4_final", temporary = T), silent = T)  
  step8d_sql <- glue::glue_sql("
	 select a.*
		into ##pe_ga_4a
		from ##pe_preg_episode_temp as a
		left join ##pe_ga_1to3_final as b
			on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;
		
		--Pull out pregnany episodes that have a procedure code for an anatomic ultrasound during the prenatal window
		select a.*, b.last_service_date as procedure_date, b.claim_header_id 
		into ##pe_ga_4b
		from ##pe_ga_4a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as b
			on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('76811', '76812')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;
		
		--Further subset to pregnancy episodes that have a Z3A code
		select a.*, right(icdcm_norm, 2) as ga_weeks_int
		into ##pe_ga_4c
		from ##pe_ga_4b as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`} as b
			on a.claim_header_id = b.claim_header_id
		where b.icdcm_version = 10 and b.icdcm_norm in ('Z3A08', 'Z3A09', 'Z3A10', 'Z3A11', 'Z3A12', 'Z3A13', 'Z3A14', 'Z3A15', 'Z3A16', 'Z3A17', 'Z3A18', 'Z3A19',
			'Z3A20', 'Z3A21', 'Z3A22', 'Z3A23', 'Z3A24', 'Z3A25', 'Z3A26', 'Z3A27', 'Z3A28', 'Z3A29', 'Z3A30', 'Z3A31', 'Z3A32', 'Z3A33', 'Z3A34', 'Z3A35', 'Z3A36',
			'Z3A37', 'Z3A38', 'Z3A39', 'Z3A40', 'Z3A41', 'Z3A42');
		
		--Convert extracted gestational age to integer and collapse to distinct rows
		select distinct id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date,
			preg_end_date, preg_start_date_max, preg_start_date_min, procedure_date, cast(ga_weeks_int as tinyint) as ga_weeks_int
		into ##pe_ga_4d
		from ##pe_ga_4c;
		
		--For episodes with multiple anatomic ultrasounds, select first (ranked by procedure date and GA)
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date, a.ga_weeks_int
		into ##pe_ga_4e
		from (select *, rank() over (partition by preg_episode_id order by procedure_date, ga_weeks_int) as rank_col from ##pe_ga_4d) as a
		where a.rank_col = 1;
		
		--Create column to hold corrected start date and calculate GA in days and weeks
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into ##pe_ga_4f
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * ga_weeks_int * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from ##pe_ga_4e
		) as a;
		
		--Create final dataset with flags for plausible pregnancy start date and GA
		
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
		into ##pe_ga_4_final
		from ##pe_ga_4f;
		
		--Union assigned episodes thus far
		select * into ##pe_ga_1to4_final from ##pe_ga_1to3_final
		union select * from ##pe_ga_4_final;",
                              .con = conn)
  DBI::dbExecute(conn = conn, step8d_sql)
  try(odbc::dbRemoveTable(conn, "##pe_ga_4a", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_4b", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_4c", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_4d", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_4e", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_4f", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_4_final", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to3_final", temporary = T), silent = T)
  
  message("--STEP 8E: Z3A code on another type of prenatal service")
  try(odbc::dbRemoveTable(conn, "##pe_ga_5a", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_5b", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_5c", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_5d", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_5e", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5f", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5g", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5h", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5i", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5j", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to5_final", temporary = T), silent = T)  
  step8e_sql <- glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		select a.*
		into ##pe_ga_5a
		from ##pe_preg_episode_temp as a
		left join ##pe_ga_1to4_final as b
			on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;
		
		--Pull out pregnany episodes that have a Z3A code during the prenatal window
		select a.*, b.last_service_date as procedure_date, right(b.icdcm_norm, 2) as ga_weeks_int
		into ##pe_ga_5b
		from ##pe_ga_5a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`} as b
			on a.id_mcaid = b.id_mcaid
		where (b.icdcm_version = 10) and b.icdcm_norm in ('Z3A08', 'Z3A09', 'Z3A10', 'Z3A11', 'Z3A12', 'Z3A13', 'Z3A14', 'Z3A15', 'Z3A16', 'Z3A17', 'Z3A18', 'Z3A19',
			'Z3A20', 'Z3A21', 'Z3A22', 'Z3A23', 'Z3A24', 'Z3A25', 'Z3A26', 'Z3A27', 'Z3A28', 'Z3A29', 'Z3A30', 'Z3A31', 'Z3A32', 'Z3A33', 'Z3A34', 'Z3A35', 'Z3A36',
			'Z3A37', 'Z3A38', 'Z3A39', 'Z3A40', 'Z3A41', 'Z3A42') and (b.last_service_date between a.preg_start_date and a.preg_end_date);
		
		--Convert ZA3 codes to integer (needed to be a separate step to avoid a cast error)
		select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date, preg_end_date,
			preg_start_date_max, preg_start_date_min, procedure_date, cast(ga_weeks_int as tinyint) as ga_weeks_int
		into ##pe_ga_5c
		from ##pe_ga_5b;
		
		--Collapse to distinct rows
		select distinct id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date,
			preg_end_date, preg_start_date_max, preg_start_date_min, procedure_date, ga_weeks_int
		into ##pe_ga_5d
		from ##pe_ga_5c;
		
		--Create column to hold corrected start date and calculate GA in days and weeks
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into ##pe_ga_5e
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * ga_weeks_int * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from ##pe_ga_5d
		) as a;
		
		--Count distinct preg start dates for each episode and calculate mode and median for those with >1 start date
		select c.*,
		--Descending rank of pregnany start date count
			rank() over (partition by preg_episode_id order by preg_start_row_count desc) as preg_start_mode_rank
		into ##pe_ga_5f
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
			from ##pe_ga_5e as a
			left join (
				--Join to counts of distinct pregnancy start dates within each episode
				select preg_episode_id, count(distinct preg_start_date_correct) as preg_start_dcount
				from ##pe_ga_5e
				group by preg_episode_id
			) as b
				on a.preg_episode_id = b.preg_episode_id
		) as c;
		
		--Count number of distinct start dates by preg episode and rank
		select a.*, b.preg_start_rank_dcount
		into ##pe_ga_5g
		from ##pe_ga_5f as a
		left join(
			select preg_episode_id, preg_start_mode_rank, count(distinct preg_start_date_correct) as preg_start_rank_dcount
			from ##pe_ga_5f
			group by preg_episode_id, preg_start_mode_rank
		) as b
		on (a.preg_episode_id = b.preg_episode_id) and (a.preg_start_mode_rank) = (b.preg_start_mode_rank);
		
		--Create flag to indicate whether mode exists for episodes with >1 start date
		select *,
		max(case when preg_start_mode_rank = 1 and preg_start_rank_dcount = 1 then 1 else 0 end)
			over (partition by preg_episode_id) as preg_start_mode_present
		into ##pe_ga_5h
		from ##pe_ga_5g;
		
		--Create flag to indicate pregnancy start date to keep
		select *,
			case
				when preg_start_dcount = 1 then 1
				when preg_start_mode_rank = 1 and preg_start_rank_dcount = 1 then 1
				when preg_start_mode_present = 0 and preg_start_date_correct = preg_start_median then 1
				else 0
				end as preg_start_date_keep
		into ##pe_ga_5i
		from ##pe_ga_5h;
		
		--Keep one pregnancy start date per episode
		select distinct id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id, preg_start_date_correct,
			preg_end_date, preg_start_date_max, preg_start_date_min,
			ga_days,
			ga_weeks
		into ##pe_ga_5j
		from ##pe_ga_5i
		where preg_start_date_keep = 1;
		
		--Create final dataset with flags for plausible pregnancy start date and GA
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
		into ##pe_ga_5_final
		from ##pe_ga_5j;
		
		--Union assigned episodes thus far
		--Beginning with this step (5), propagate episodes with invalid start date to next step
		select * into ##pe_ga_1to5_final from ##pe_ga_1to4_final
		union select * from ##pe_ga_5_final where valid_start_date = 1 and valid_ga = 1;",
                              .con = conn)
  DBI::dbExecute(conn = conn, step8e_sql)
  try(odbc::dbRemoveTable(conn, "##pe_ga_5a", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5b", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5c", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5d", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5e", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5f", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5g", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5h", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5i", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5j", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_5_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to4_final", temporary = T), silent = T)  
  
  message("--STEP 8F: Nuchal translucency (NT) scan without Z3A code")
  try(odbc::dbRemoveTable(conn, "##pe_ga_6a", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_6b", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_6c", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_6d", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_6_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to6_final", temporary = T), silent = T) 
  step8f_sql <- glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		select a.*
		into ##pe_ga_6a
		from ##pe_preg_episode_temp as a
		left join ##pe_ga_1to5_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;
		
		--Pull out pregnany episodes that have a procedure code for an NT scan during the prenatal window, collapse to distinct rows
		select distinct a.*, b.last_service_date as procedure_date
		into ##pe_ga_6b
		from ##pe_ga_6a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as b
		on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('76813', '76814')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;
		
		--For episodes with multiple NT scans, select first (ranked by procedure date) and take distinct rows
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
		into ##pe_ga_6c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from ##pe_ga_6b) as a
		where a.rank_col = 1;
		
		--Create column to hold corrected start date and calculate GA in days and weeks
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into ##pe_ga_6d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, -89, procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from ##pe_ga_6c
		) as a;

		--Create final dataset with flags for plausible pregnancy start date and GA
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
		into ##pe_ga_6_final
		from ##pe_ga_6d;

		--Union assigned episodes thus far
		select * into ##pe_ga_1to6_final from ##pe_ga_1to5_final
		union select * from ##pe_ga_6_final where valid_start_date = 1 and valid_ga = 1;",
                              .con = conn)
  DBI::dbExecute(conn = conn, step8f_sql)
  try(odbc::dbRemoveTable(conn, "##pe_ga_6a", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_6b", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_6c", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_6d", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_6_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to5_final", temporary = T), silent = T) 
  
  message("--STEP 8G: Chorionic Villus Sampling (CVS)")
  try(odbc::dbRemoveTable(conn, "##pe_ga_7a", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_7b", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_7c", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_7d", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_7_final", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to7_final", temporary = T), silent = T)
  step8g_sql <- glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		select a.*
		into ##pe_ga_7a
		from ##pe_preg_episode_temp as a
		left join ##pe_ga_1to6_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;
		
		--Pull out pregnany episodes that have a procedure code for CVS during the prenatal window, collapse to distinct rows
		select distinct a.*, b.last_service_date as procedure_date
		into ##pe_ga_7b
		from ##pe_ga_7a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as b
		on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('59015', '76945')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;
		
		--For episodes with multiple CVS services, select first (ranked by procedure date)
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
		into ##pe_ga_7c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from ##pe_ga_7b) as a
		where a.rank_col = 1;
		
		--Create column to hold corrected start date and calculate GA in days and weeks
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into ##pe_ga_7d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * 12 * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from ##pe_ga_7c
		) as a;
		
		--Create final dataset with flags for plausible pregnancy start date and GA
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
		into ##pe_ga_7_final
		from ##pe_ga_7d;

		--Union assigned episodes thus far
		select * into ##pe_ga_1to7_final from ##pe_ga_1to6_final
		union select * from ##pe_ga_7_final where valid_start_date = 1 and valid_ga = 1;",
                              .con = conn)
  DBI::dbExecute(conn = conn, step8g_sql)
  try(odbc::dbRemoveTable(conn, "##pe_ga_7a", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_7b", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_7c", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_7d", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_7_final", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to6_final", temporary = T), silent = T)
  
  message("--STEP 8H: Cell free fetal DNA screening")
  try(odbc::dbRemoveTable(conn, "##pe_ga_8a", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_8b", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_8c", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_8d", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_8_final", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to8_final", temporary = T), silent = T)   
  step8h_sql <- glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		select a.*
		into ##pe_ga_8a
		from ##pe_preg_episode_temp as a
		left join ##pe_ga_1to7_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;
		
		--Pull out pregnany episodes that have a procedure code for cell-free DNA sampling during the prenatal window, collapse to distinct rows
		select distinct a.*, b.last_service_date as procedure_date
		into ##pe_ga_8b
		from ##pe_ga_8a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as b
		on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('81420', '81507')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;
		
		--For episodes with multiple cell-free DNA sampling services, select first (ranked by procedure date)
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
		into ##pe_ga_8c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from ##pe_ga_8b) as a
		where a.rank_col = 1;
		
		--Create column to hold corrected start date and calculate GA in days and weeks
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into ##pe_ga_8d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * 12 * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from ##pe_ga_8c
		) as a;
		
		--Create final dataset with flags for plausible pregnancy start date and GA
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
		into ##pe_ga_8_final
		from ##pe_ga_8d;
		
		--Union assigned episodes thus far
		select * into ##pe_ga_1to8_final from ##pe_ga_1to7_final
		union select * from ##pe_ga_8_final where valid_start_date = 1 and valid_ga = 1;",
                              .con = conn)
  DBI::dbExecute(conn = conn, step8h_sql)
  try(odbc::dbRemoveTable(conn, "##pe_ga_8a", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_8b", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_8c", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_8d", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_8_final", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to7_final", temporary = T), silent = T)   
  
  message("--STEP 8I: Full-term code for live birth or stillbirth within 7 days of outcome date")
  try(odbc::dbRemoveTable(conn, "##pe_ga_9a", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_dx_distinct_step8", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_dx_fullterm", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ref_dx_fullterm", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_fullterm_dx", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_9b", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_9c", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_9d", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_9_final", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to9_final", temporary = T), silent = T)  
  step8i_sql <- glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		select a.*
		into ##pe_ga_9a
		from ##pe_preg_episode_temp as a
		left join ##pe_ga_1to8_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;
		
		--Pull out distinct ICD-10-CM codes from claims >= 2016-01-01 (<1 min)
		select distinct icdcm_norm
		into ##pe_dx_distinct_step8
		from {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`}
		where last_service_date >= '2016-01-01';
		
		--Create temp table holding full-term status ICD-10-CM codes in LIKE format
		create table ##pe_dx_fullterm (code_like varchar(255));
		insert into ##pe_dx_fullterm (code_like)
		values ('O6020%'), ('O6022%'), ('O6023%'), ('O4202%'), ('O4292%'), ('O471%'), ('O80%');
		
		--Join distinct ICD-10-CM codes to full-term ICD-10-CM reference table (<1 min)
		select distinct a.icdcm_norm, b.code_like
		into ##pe_ref_dx_fullterm
		from ##pe_dx_distinct_step8 as a
		inner join ##pe_dx_fullterm as b
		on a.icdcm_norm like b.code_like;
		
		--Join new reference table to claims data using EXACT join (1 min)
		select a.id_mcaid, a.last_service_date, a.icdcm_norm
		into ##pe_fullterm_dx
		from {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`} as a
		inner join ##pe_ref_dx_fullterm as b
		on a.icdcm_norm = b.icdcm_norm
		where a.last_service_date >= '2016-01-01';
		
		--Pull out pregnancy episodes that have a full-term status ICD-10-CM code within 7 days of the outcome
		--Only include livebirth and stillbirth outcomes
		select a.*, b.last_service_date as procedure_date
		into ##pe_ga_9b
		from ##pe_ga_9a as a
		inner join ##pe_fullterm_dx as b
		on a.id_mcaid = b.id_mcaid
		where (b.last_service_date between dateadd(day, -7, a.preg_end_date) and dateadd(day, 7, a.preg_end_date))
			and a.preg_endpoint in ('lb', 'sb');
		
		--For episodes with multiple relevant services, select first (ranked by procedure date) and take distinct rows
		select distinct a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
		into ##pe_ga_9c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from ##pe_ga_9b) as a
		where a.rank_col = 1;
		
		--Create column to hold corrected start date and calculate GA in days and weeks
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into ##pe_ga_9d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * 39 * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from ##pe_ga_9c
		) as a;
		
		--Create final dataset with flags for plausible pregnancy start date and GA
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
		into ##pe_ga_9_final
		from ##pe_ga_9d;
		
		--Union assigned episodes thus far
		select * into ##pe_ga_1to9_final from ##pe_ga_1to8_final
		union select * from ##pe_ga_9_final where valid_start_date = 1 and valid_ga = 1;",
                              .con = conn)
  DBI::dbExecute(conn = conn, step8i_sql)
  try(odbc::dbRemoveTable(conn, "##pe_ga_9a", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_dx_fullterm", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ref_dx_fullterm", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_fullterm_dx", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_9b", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_9c", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_9d", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_9_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to8_final", temporary = T), silent = T)  
  
  message("--STEP 8J: Trimester codes within 7 days of outcome date")
  try(odbc::dbRemoveTable(conn, "##pe_ga_10a", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ref_dx_trimester", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_trimester_dx", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_px_distinct_step10", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ref_px_trimester", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_trimester_px", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_trimester_dx_px", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_10b", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_10c", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_10d", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_10_final", temporary = T), silent = T)   
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to10_final", temporary = T), silent = T) 
  step8j_sql <- glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		select a.*
		into ##pe_ga_10a
		from ##pe_preg_episode_temp as a
		left join ##pe_ga_1to9_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;
		
		--Join distinct ICD-10-CM codes to ICD-10-CM codes in trimester codes reference table (<1 min)
		select distinct a.icdcm_norm, b.code_like, b.trimester
		into ##pe_ref_dx_trimester
		from ##pe_dx_distinct_step8 as a
		inner join (select * from {`ref_schema`}.{`paste0(ref_table,'moll_trimester')`} where code_type = 'icd10cm') as b
		on a.icdcm_norm like b.code_like;
		
		--Join new reference table to claims data using EXACT join (1 min)
		select a.id_mcaid, a.last_service_date, b.trimester
		into ##pe_trimester_dx
		from {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`} as a
		inner join ##pe_ref_dx_trimester as b
		on a.icdcm_norm = b.icdcm_norm
		where a.last_service_date >= '2016-01-01';
		
		--Pull out distinct procedure codes from claims >= 2016-01-01 (<1 min)
		select distinct procedure_code
		into ##pe_px_distinct_step10
		from {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`}
		where last_service_date >= '2016-01-01';
		
		--Join distinct procedure codes to procedure codes in trimester codes reference table using LIKE join (<1 min)
		select distinct a.procedure_code, b.code_like, b.trimester
		into ##pe_ref_px_trimester
		from ##pe_px_distinct_step10 as a
		inner join (select * from {`ref_schema`}.{`paste0(ref_table,'moll_trimester')`} where code_type = 'cpt_hcpcs') as b
		on a.procedure_code like b.code_like;
		
		--Join new reference table to claims data using EXACT join (1 min)
		select a.id_mcaid, a.last_service_date, b.trimester
		into ##pe_trimester_px
		from {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as a
		inner join ##pe_ref_px_trimester as b
		on a.procedure_code = b.procedure_code
		where a.last_service_date >= '2016-01-01';
		
		--Union diagnosis and procedure code tables
		select * into ##pe_trimester_dx_px from ##pe_trimester_dx
		union select * from ##pe_trimester_px;
		
		--Pull out pregnany episodes that have a trimester code within 7 days of the outcome
		select a.*, b.last_service_date as procedure_date, b.trimester
		into ##pe_ga_10b
		from ##pe_ga_10a as a
		inner join ##pe_trimester_dx_px as b
		on a.id_mcaid = b.id_mcaid
		where (b.last_service_date between dateadd(day, -7, a.preg_end_date) and dateadd(day, 7, a.preg_end_date));
		
		--For episodes with multiple relevant services, select first (ranked by procedure date DESC, trimester) and take distinct rows
		select distinct a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date, a.trimester
		into ##pe_ga_10c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date desc, trimester) as rank_col from ##pe_ga_10b) as a
		where a.rank_col = 1;
		
		--Create column to hold corrected start date and calculate GA in days and weeks
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into ##pe_ga_10d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			
			--use trimester code to calculate corrected start date
			case when trimester = 1 then dateadd(day, -69, procedure_date)
			when trimester = 2 then dateadd(day, -146, procedure_date)
			when trimester = 3 then dateadd(day, -240, procedure_date)
			else null
			end as preg_start_date_correct,
			
			preg_end_date, preg_start_date_max, preg_start_date_min
			from ##pe_ga_10c
		) as a;
		
		--Create final dataset with flags for plausible pregnancy start date and GA
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
		into ##pe_ga_10_final
		from ##pe_ga_10d;

		--Union assigned episodes thus far
		select * into ##pe_ga_1to10_final from ##pe_ga_1to9_final
		union select * from ##pe_ga_10_final where valid_start_date = 1 and valid_ga = 1;",
    .con = conn)
  
  DBI::dbExecute(conn = conn, step8j_sql)
  try(odbc::dbRemoveTable(conn, "##pe_ga_10a", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ref_dx_trimester", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_trimester_dx", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_px_distinct_step10", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ref_px_trimester", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_trimester_px", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_10b", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_10c", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_10d", temporary = T), silent = T)    
  try(odbc::dbRemoveTable(conn, "##pe_ga_10_final", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to9_final", temporary = T), silent = T)
  
  message("--STEP 8K: Preterm code within 7 days of outcome date")
  try(odbc::dbRemoveTable(conn, "##pe_ga_11a", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_dx_preterm", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ref_dx_preterm", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_preterm_dx", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_11b", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_11c", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_11d", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_11_final", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to11_final", temporary = T), silent = T) 
  step8k_sql <- glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		select a.*
		into ##pe_ga_11a
		from ##pe_preg_episode_temp as a
		left join ##pe_ga_1to10_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;
		
		--Create temp table holding preterm status ICD-10-CM codes in LIKE format
		create table ##pe_dx_preterm (code_like varchar(255));
		insert into ##pe_dx_preterm (code_like)
		values ('O6010%'), ('O6012%'), ('O6013%'), ('O6014%'), ('O4201%'), ('O4211%'), ('O4291%');
		
		--Join distinct ICD-10-CM codes to preterm ICD-10-CM reference table (<1 min)
		select distinct a.icdcm_norm, b.code_like
		into ##pe_ref_dx_preterm
		from ##pe_dx_distinct_step8 as a
		inner join ##pe_dx_preterm as b
		on a.icdcm_norm like b.code_like;
		
		--Join new reference table to claims data using EXACT join (1 min)
		select a.id_mcaid, a.last_service_date, a.icdcm_norm
		into ##pe_preterm_dx
		from {`final_schema`}.{`paste0(final_table,'mcaid_claim_icdcm_header')`} as a
		inner join ##pe_ref_dx_preterm as b
		on a.icdcm_norm = b.icdcm_norm
		where a.last_service_date >= '2016-01-01';
		
		--Pull out pregnancy episodes that have a preterm status ICD-10-CM code within 7 days of the outcome
		--Only include livebirth and stillbirth outcomes
		select a.*, b.last_service_date as procedure_date
		into ##pe_ga_11b
		from ##pe_ga_11a as a
		inner join ##pe_preterm_dx as b
		on a.id_mcaid = b.id_mcaid
		where (b.last_service_date between dateadd(day, -7, a.preg_end_date) and dateadd(day, 7, a.preg_end_date));
		
		--For episodes with multiple relevant services, select first (ranked by procedure date) and take distinct rows
		select distinct a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
		into ##pe_ga_11c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from ##pe_ga_11b) as a
		where a.rank_col = 1;
		
		--Create column to hold corrected start date and calculate GA in days and weeks
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into ##pe_ga_11d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * 35 * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from ##pe_ga_11c
		) as a;
		
		--Create final dataset with flags for plausible pregnancy start date and GA
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
		into ##pe_ga_11_final
		from ##pe_ga_11d;
		
		--Union assigned episodes thus far
		select * into ##pe_ga_1to11_final from ##pe_ga_1to10_final
		union select * from ##pe_ga_11_final where valid_start_date = 1 and valid_ga = 1;",
                              .con = conn)
  DBI::dbExecute(conn = conn, step8k_sql)
  try(odbc::dbRemoveTable(conn, "##pe_ga_11a", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_dx_preterm", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ref_dx_preterm", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_preterm_dx", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_11b", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_11c", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_11d", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_11_final", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to10_final", temporary = T), silent = T)  
  
  message("--STEP 8L: First glucose screening or tolerance test")
  try(odbc::dbRemoveTable(conn, "##pe_ga_12a", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_12b", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_12c", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_12d", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_12_final", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to12_final", temporary = T), silent = T)   
  step8l_sql <- glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps	
		select a.*
		into ##pe_ga_12a
		from ##pe_preg_episode_temp as a
		left join ##pe_ga_1to11_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;
		
		--Pull out pregnany episodes that have a procedure code for glucose screening/tolerance test during the prenatal window, collapse to distinct rows
		select distinct a.*, b.last_service_date as procedure_date
		into ##pe_ga_12b
		from ##pe_ga_12a as a
		inner join {`final_schema`}.{`paste0(final_table,'mcaid_claim_procedure')`} as b
		on a.id_mcaid = b.id_mcaid
		where b.procedure_code in ('82950', '82951', '82952')
			and b.last_service_date between a.preg_start_date and a.preg_end_date;
		
		--For episodes with multiple glucose screenings, select first (ranked by procedure date)
		select a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date
		into ##pe_ga_12c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date) as rank_col from ##pe_ga_12b) as a
		where a.rank_col = 1;
		
		--Create column to hold corrected start date and calculate GA in days and weeks
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into ##pe_ga_12d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			dateadd(day, ((-1 * 26 * 7) + 1), procedure_date) as preg_start_date_correct, preg_end_date, preg_start_date_max, preg_start_date_min
			from ##pe_ga_12c
		) as a;
		
		--Create final dataset with flags for plausible pregnancy start date and GA
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
		into ##pe_ga_12_final
		from ##pe_ga_12d;
		
		--Union assigned episodes thus far
		select * into ##pe_ga_1to12_final from ##pe_ga_1to11_final
		union select * from ##pe_ga_12_final where valid_start_date = 1 and valid_ga = 1;",
                              .con = conn)
  DBI::dbExecute(conn = conn, step8l_sql)
  try(odbc::dbRemoveTable(conn, "##pe_ga_12a", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_12b", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_12c", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_12d", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_12_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_##pe_ga_1to11_final", temporary = T), silent = T)
  
  message("--STEP 8M: Prenatal service > 7 days before outcome date with a trimester code")
  try(odbc::dbRemoveTable(conn, "##pe_ga_13a", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_13b", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_13c", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_13d", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_13_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to13_final", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_preg_episode", temporary = T), silent = T)   
  step8m_sql <- glue::glue_sql("
	 --Pull forward episodes not assigned in prior steps
		select a.*
		into ##pe_ga_13a
		from ##pe_preg_episode_temp as a
		left join ##pe_ga_1to12_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;
		
		--Pull out pregnany episodes that have a trimester code > 7 days before outcome
		select a.*, b.last_service_date as procedure_date, b.trimester
		into ##pe_ga_13b
		from ##pe_ga_13a as a
		inner join ##pe_trimester_dx_px as b
		on a.id_mcaid = b.id_mcaid
		where (b.last_service_date < dateadd(day, -7, a.preg_end_date));
		
		--For episodes with multiple relevant services, select first (ranked by procedure date DESC, trimester) and take distinct rows
		select distinct a.id_mcaid, a.last_service_date, a.preg_endpoint, a.preg_hier, a.preg_episode_id, a.preg_start_date, a.preg_end_date,
			a.preg_start_date_max, a.preg_start_date_min, a.procedure_date, a.trimester
		into ##pe_ga_13c
		from (select *, rank() over (partition by preg_episode_id order by procedure_date desc, trimester) as rank_col from ##pe_ga_13b) as a
		where a.rank_col = 1;
		
		--Create column to hold corrected start date and calculate GA in days and weeks
		select a.*,
			datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1 as ga_days,
			cast(round((datediff(day, a.preg_start_date_correct, a.preg_end_date) + 1)*1.0/7, 1) as numeric(4,1)) as ga_weeks
		into ##pe_ga_13d
		from (
			select id_mcaid, last_service_date, preg_endpoint, preg_hier, preg_episode_id,
			
			--use trimester code to calculate corrected start date
			case when trimester = 1 then dateadd(day, -69, procedure_date)
			when trimester = 2 then dateadd(day, -146, procedure_date)
			when trimester = 3 then dateadd(day, -240, procedure_date)
			else null
			end as preg_start_date_correct,
			
			preg_end_date, preg_start_date_max, preg_start_date_min
			from ##pe_ga_13c
		) as a;
		
		--Create final dataset with flags for plausible pregnancy start date and GA
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
		into ##pe_ga_13_final
		from ##pe_ga_13d;
		
		--Union assigned episodes thus far
		select * into ##pe_ga_1to13_final from ##pe_ga_1to12_final
		union select * from ##pe_ga_13_final where valid_start_date = 1 and valid_ga = 1;
				
		---------------
		--Union episodes that were not flagged by any of the 13 steps
		---------------
		select id_mcaid, preg_episode_id, preg_endpoint, preg_hier, preg_start_date_correct as preg_start_date,
			preg_end_date, ga_days, ga_weeks, valid_start_date, valid_ga, lb_type, ga_estimation_step
		into ##pe_preg_episode
		from ##pe_ga_1to13_final
		union
		select a.id_mcaid, a.preg_episode_id, a.preg_endpoint, a.preg_hier, a.preg_start_date,
			a.preg_end_date, null as ga_days, null as ga_weeks, 0 as valid_start_date, 0 as valid_ga,
			null as lb_type, null as ga_estimation_step
		from ##pe_preg_episode_temp as a
		left join ##pe_ga_1to13_final as b
		on a.preg_episode_id = b.preg_episode_id
		where b.preg_episode_id is null;",
                              .con = conn)
  DBI::dbExecute(conn = conn, step8m_sql)
  try(odbc::dbRemoveTable(conn, "##pe_ga_13a", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_13b", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_13c", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_13d", temporary = T), silent = T)  
  try(odbc::dbRemoveTable(conn, "##pe_ga_13_final", temporary = T), silent = T) 
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to12_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_ga_1to13_final", temporary = T), silent = T)
  
  message("STEP 9: Join to eligibility data to bring in age for subset")
  try(odbc::dbRemoveTable(conn, "##pe_preg_episode_age_all", temporary = T), silent = T)
  step9_sql <- glue::glue_sql("
    select a.id_mcaid,  
			case
				when floor((datediff(day, b.dob, a.preg_end_date) + 1) / 365.25) >=0 then floor((datediff(day, b.dob, a.preg_end_date) + 1) / 365.25)
				when floor((datediff(day, b.dob, a.preg_end_date) + 1) / 365.25) = -1 then 0
				end as age_at_outcome,
			a.preg_episode_id, a.preg_endpoint, a.preg_hier, a.preg_start_date, a.preg_end_date, a.ga_days, a.ga_weeks,
			a.valid_start_date, a.valid_ga, case when a.valid_start_date = 1 and a.valid_ga = 1 then 1 else 0 end as valid_both,
			a.lb_type, a.ga_estimation_step, getdate() as last_run
		into ##pe_preg_episode_age_all
		from ##pe_preg_episode as a
		left join {`final_schema`}.{`paste0(final_table,'mcaid_elig_demo')`} as b
			on a.id_mcaid = b.id_mcaid;
		
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
		from ##pe_preg_episode_age_all
		where age_at_outcome between 12 and 55;",
    .con = conn)
  DBI::dbExecute(conn = conn, step9_sql)
  try(odbc::dbRemoveTable(conn, "##pe_preg_episode", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_preg_episode_age_all", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##pe_trimester_dx_px", temporary = T), silent = T)
  
  time_end <- Sys.time()
  message("Loading took ", round(difftime(time_end, time_start, units = "secs"), 2), 
          " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
          " mins)")
  
  
  #### ADD INDEX ####
  add_index_f(conn, server = server, table_config = config)
}