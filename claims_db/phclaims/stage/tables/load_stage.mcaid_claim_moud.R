# This code creates the the mcaid claim moud table
# Create a table that identifies and quantifies MOUD in Medicaid beneficiaries
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

load_stage_mcaid_claim_moud_f <- function(conn = NULL,
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
  message("STEP 1: Flag methadone episodes using HCPCS codes from 1/1/2016 onward")
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_proc_1", temporary = T), silent = T)
  step1_sql <- glue::glue_sql("
	  select distinct 
	    id_mcaid, claim_header_id, first_service_date, last_service_date, procedure_code,		
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
	  into ##mcaid_moud_proc_1
	  from {`final_schema`}.{`paste0(final_table, 'mcaid_claim_procedure')`}
	  where last_service_date >= '2016-01-01'
  		and procedure_code in (
			  'H0033',
			  'H0020', 'S0109', 'G2078', 'G2067',
			  'J0571', 'J0572', 'J0573', 'J0574', 'J0575', 'G2068', 'G2079', 'Q9991', 'Q9992', 'G2069', 'G2070', 'G2072', 'J0570',
			  '96372', '11981', '11983', 'G0516', 'G0518',
			  'G2073', 'J2315',
			  'G2074', 'G2075', 'G2076', 'G2077', 'G2080', 'G2086', 'G2087', 'G2088', 'G2213');", 
	  .con = conn)
  DBI::dbExecute(conn = conn, step1_sql)
    
  message("STEP 2: Bring in primary diagnosis information")
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_proc_2", temporary = T), silent = T)
  step2_sql <- glue::glue_sql("
	  select distinct 
	    a.*,
		  max(case when c.sub_group_condition = 'sud_opioid' then 1 else 0 end) over(partition by a.claim_header_id) as oud_dx1_flag
	  into ##mcaid_moud_proc_2
	  from ##mcaid_moud_proc_1 as a
	  left join {`final_schema`}.{`paste0(final_table, 'mcaid_claim_header')`} as b
  		on a.claim_header_id = b.claim_header_id
  	left join (
		  select distinct code, icdcm_version, sub_group_condition
  			from ref.rda_value_sets_apde where sub_group_condition = 'sud_opioid' and data_source_type = 'diagnosis'
		  ) as c
		  on (b.primary_diagnosis = c.code) and (b.icdcm_version = c.icdcm_version);",
	  .con = conn)
  DBI::dbExecute(conn = conn, step2_sql)
  
  message("STEP 3: Subset methadone HCPCS codes by considering primary diagnosis")
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_proc_3", temporary = T), silent = T)
  step3_sql <- glue::glue_sql("
	  select distinct
		  id_mcaid,
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
	  into ##mcaid_moud_proc_3
	  from ##mcaid_moud_proc_2
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
		  or (procedure_code in ('G2073', 'J2315') and oud_dx1_flag = 1);",
	  .con = conn)
  DBI::dbExecute(conn = conn, step3_sql)
  
  message("STEP 4: Pull pharmacy fill data for bup and naltrexone prescriptions")
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_pharm_1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_pharm_2", temporary = T), silent = T)
  step4_sql <- glue::glue_sql("
	  select distinct 
	    a.id_mcaid, a.claim_header_id, a.rx_fill_date as first_service_date, a.rx_fill_date as last_service_date, a.ndc,
		  case when b.sub_group_pharmacy in ('pharm_buprenorphine', 'pharm_buprenorphine_naloxone') then 1 else 0 end as bup_rx_flag,
		  case when b.sub_group_pharmacy = 'pharm_naltrexone_rx' then 1 else 0 end as nal_rx_flag,
		  case 
  			when c.DOSAGEFORMNAME like 'FILM%' or c.DOSAGEFORMNAME like 'TABLET%' then 'oral'
			  when c.DOSAGEFORMNAME like 'KIT%' or c.DOSAGEFORMNAME like 'SOLUTION%' then 'injection/implant'
			  else null
			  end as admin_method,
		  a.rx_days_supply as moud_days_supply
	  into ##mcaid_moud_pharm_1
	  from {`final_schema`}.{`paste0(final_table, 'mcaid_claim_pharm')`} as a
	  inner join (
  		select distinct code, sub_group_pharmacy
		  from [ref].[rda_value_sets_apde]
		  where sub_group_pharmacy in ('pharm_buprenorphine', 'pharm_buprenorphine_naloxone', 'pharm_naltrexone_rx')
		  ) as b
  			on a.ndc = b.code
  	left join (
		  select ndc, DOSAGEFORMNAME
		  from ref.ndc_codes) as c
  			on b.code = c.ndc
  	where a.rx_fill_date >= '2016-01-01';
  
  	select 
  	  id_mcaid, claim_header_id, first_service_date, last_service_date, ndc, bup_rx_flag, nal_rx_flag, 
		  case when ndc = '00093572156' or ndc = '00093572056' or ndc = '49452483501'  or ndc = '00378876616' then 'oral' 
  			else admin_method 
			  end as admin_method, moud_days_supply
	  into ##mcaid_moud_pharm_2
	  from ##mcaid_moud_pharm_1;",
	  .con = conn)
  DBI::dbExecute(conn = conn, step4_sql)
  
  message("STEP 5: Union procedure code and pharmacy fill data")
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_union_1", temporary = T), silent = T)
  step5_sql <- glue::glue_sql("
	  select 
  		id_mcaid,
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
	  into ##mcaid_moud_union_1
	  from ##mcaid_moud_proc_3
	  union 
	  select
  		id_mcaid,
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
	  from ##mcaid_moud_pharm_2;",
	  .con = conn)
  DBI::dbExecute(conn = conn, step5_sql)
  
  message("STEP 6: Assign MOUD type to procedure code H0033 (could be methadone or bup) depending on monthly sums of either med")
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_temp_1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_temp_2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_temp_3", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_union_2", temporary = T), silent = T)
  step6_sql <- glue::glue_sql("
	  select distinct 
	    id_mcaid,
		  max(case when procedure_code = 'H0033' then 1 else 0 end) over(partition by id_mcaid) as proc_h0033_flag
	  into ##mcaid_moud_temp_1
	  from ##mcaid_moud_union_1;

  	select c.year_month, b.id_mcaid,
	  	sum(isnull(meth_proc_flag,0)) as meth_proc_month_sum,
  		sum(isnull(bup_proc_flag,0)) as bup_proc_month_sum,
		  sum(isnull(nal_proc_flag,0)) as nal_proc_month_sum,
		  sum(isnull(bup_rx_flag,0)) as bup_rx_month_sum,
		  sum(isnull(nal_rx_flag,0)) as nal_rx_month_sum
	  into ##mcaid_moud_temp_2
	  from (select distinct id_mcaid from #temp1 where proc_h0033_flag = 1) as a
	  inner join ##mcaid_moud_union_1 as b
  		on a.id_mcaid = b.id_mcaid
  	left join (select distinct [date], year_month from {`ref_schema`}ref_date) as c
		  on b.last_service_date = c.[date]
	  group by c.year_month, b.id_mcaid;
  
	  select 
	    a.id_mcaid,
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
	  into ##mcaid_moud_temp_3
	  from ##mcaid_moud_union_1 as a
	  left join {`ref_schema`}ref_date as b
  		on a.last_service_date = b.[date]
  	left join ##mcaid_moud_temp_2 as c
		  on (a.id_mcaid = c.id_mcaid) and (b.year_month = c.year_month);
  
  	select 
		  id_mcaid,
		  last_service_date,
		  meth_proc_flag,
		  bup_proc_flag,
		  nal_proc_flag,
		  unspec_proc_flag,
		  bup_rx_flag,
		  nal_rx_flag,
		  sum(moud_days_supply) as moud_days_supply,
		  admin_method
	  into ##mcaid_moud_union_2
	  from ##mcaid_moud_temp_3
	  group by id_mcaid, last_service_date, meth_proc_flag, bup_proc_flag, nal_proc_flag, unspec_proc_flag, bup_rx_flag, nal_rx_flag, admin_method;",
	  .con = conn)
  DBI::dbExecute(conn = conn, step6_sql)
  
  message("STEP 7: Identify same MOUDs with same method of administration occurring on the same day")
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_union_3", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_union_4", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_union_final", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_temp_columns_1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_temp_columns_2", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_temp_columns_3", temporary = T), silent = T)
  step7_sql <- glue::glue_sql("
	  select 
		  *, 
		  case when bup_proc_flag = 1 or bup_rx_flag = 1 then 'buprenorphine'
  			when nal_proc_flag = 1 or nal_rx_flag = 1 then 'naltrexone'
			  else null
			  end as moudtype, --only doing bupe and naltrexone 
		  case when bup_proc_flag = 1 or nal_proc_flag = 1 then 'hcpcs'
  			when bup_rx_flag = 1 or nal_rx_flag = 1 then 'ndc'
			  else null 
			  end as codetype --only doing bupe and naltrexone
	  into ##mcaid_moud_union_3
	  from ##mcaid_moud_union_2;
  
	  select 
  		count(*) as dupdate, id_mcaid, last_service_date, moudtype, admin_method
  	into ##mcaid_moud_temp_columns_1
  	from ##mcaid_moud_union_3
  	group by id_mcaid, last_service_date, moudtype, admin_method
  	having count(*) > 1;
  
  	select 
		  a.dupdate, b.*
	  into ##mcaid_moud_temp_columns_2
	  from ##mcaid_moud_temp_columns_1 as a 
	  right join ##mcaid_moud_union_3 as b
  		on (a.id_mcaid = b.id_mcaid) and (a.last_service_date = b.last_service_date) and (a.moudtype = b.moudtype)
  	where dupdate is not null;
  
  	select 
		  *, 
		  case when codetype = 'hcpcs' then 1
  			else 0
			  end as dupmoud_todelete
	  into ##mcaid_moud_temp_columns_3
	  from ##mcaid_moud_temp_columns_2
	  where case when codetype = 'hcpcs' then 1 else 0 end = 1;

	  select 
  		a.*, b.dupmoud_todelete
  	into ##mcaid_moud_union_4
  	from ##mcaid_moud_union_3 as a
  	left join #tempcolumns3 as b
		  on (a.id_mcaid = b.id_mcaid) and (a.last_service_date = b.last_service_date) and (a.moudtype = b.moudtype) and (a.admin_method = b.admin_method) and (a.codetype = b.codetype);

  	select 
		  id_mcaid, last_service_date, meth_proc_flag, bup_proc_flag, nal_proc_flag, 
		  unspec_proc_flag, bup_rx_flag, nal_rx_flag, moud_days_supply, admin_method
	  into ##mcaid_moud_union_final
	  from ##mcaid_moud_union_4
	  where dupmoud_todelete is null;",
	  .con = conn)
  DBI::dbExecute(conn = conn, step7_sql)
  
  message("STEP 8: Estimate methadone days supply based on next-service-date methodology")
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_temp_meth_1", temporary = T), silent = T)
  try(odbc::dbRemoveTable(conn, "##mcaid_moud_temp_meth_2", temporary = T), silent = T)
  step8_sql <- glue::glue_sql("
	  select 
		  a.*, b.year_month, b.year_quarter, b.year_half, b.[year],
		  case
  			when meth_proc_flag = 1 and lead(meth_proc_flag, 1) over(partition by id_mcaid order by last_service_date) = 1
				  then datediff(day, last_service_date, lead(last_service_date, 1) over(partition by id_mcaid order by last_service_date))
			  else null
			  end as next_meth_diff,
		  sum(meth_proc_flag) over(partition by id_mcaid, b.year_quarter) as meth_proc_sum_year_quarter
	  into ##mcaid_moud_temp_meth_1
	  from ##mcaid_moud_union_final as a
	  left join 
  		(select 
			  [date], [year_month], [year_quarter],
			  case
  				when right(year_quarter,2) in (1,2) then left(year_quarter,4) + '_top'
				  when right(year_quarter,2) in (3,4) then left(year_quarter,4) + '_bottom'
				  end as year_half,
			  [year]
		  from {`ref_schema`}ref_date) as b
  			on a.last_service_date = b.[date];
  
  	select 
		  *,
		  percentile_cont(0.5) within group (order by next_meth_diff) over(partition by id_mcaid, year_quarter) as next_meth_diff_median_year_quarter
	  into ##mcaid_moud_temp_meth_2
	  from ##mcaid_moud_temp_meth_1;
  
  	select
		  id_mcaid,
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
		  isnull(meth_proc_flag,0) + isnull(bup_proc_flag,0) + isnull(nal_proc_flag,0) + isnull(bup_rx_flag,0) + isnull(nal_rx_flag,0) as moud_flag_count ,
		  moud_days_supply,
		  next_meth_diff,
		  next_meth_diff_median_year_quarter,
		  meth_proc_sum_year_quarter,
		  case
  			when meth_proc_flag = 1 and meth_proc_sum_year_quarter <= 2 then moud_days_supply --low count of service dates exception
			  when meth_proc_flag = 1 and next_meth_diff > (1.5 * next_meth_diff_median_year_quarter) then next_meth_diff_median_year_quarter --skipped dose exception
			  when meth_proc_flag = 1 and next_meth_diff is null then next_meth_diff_median_year_quarter --no next service date
			  when meth_proc_flag = 1 then next_meth_diff --baseline rule
			  else moud_days_supply
			  end as moud_days_supply_new_year_quarter,
		  getdate() as last_run
	  into {`to_schema`}.{`to_table`}
	  from ##mcaid_moud_temp_meth_2;",
	  .con = conn)
  DBI::dbExecute(conn = conn, step8_sql)
  
  time_end <- Sys.time()
  message("Loading took ", round(difftime(time_end, time_start, units = "secs"), 2), 
          " secs (", round(difftime(time_end, time_start, units = "mins"), 2),
          " mins)")
  
  
  #### ADD INDEX ####
  add_index_f(conn, server = server, table_config = config)
}