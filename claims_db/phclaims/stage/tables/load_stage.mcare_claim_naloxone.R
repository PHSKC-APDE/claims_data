#### CODE TO LOAD & TABLE-LEVEL QA STAGE.MCARE_claim_naloxone
# Eli Kern, PHSKC (APDE)
#
# 2024-06
#

### Run from 02_master_mcare_claims_analytic.R script
# https://github.com/PHSKC-APDE/claims_data/blob/main/claims_db/db_loader/mcare/02_master_mcare_claims_analytic.R

#### Load script ####
load_stage.mcare_claim_naloxone_f <- function() {
  
  ### Run SQL query
  odbc::dbGetQuery(inthealth, glue::glue_sql(
    "----------------------------------------------------------------------
    --- CREATE REFERENCE TABLE FOR NALOXONE DISRIBUTED IN MCARE CLAIMS ---
    --- Eli Kern, adapted from Spencer Hensley ---------------------------
    --- June 2024 --------------------------------------------------------
    -----------------------------------------------------------------------
    
    --- CREATE TABLE TO HOLD NDC CODES IDENTIFYING NALOXONE FOR LIKE JOIN
    
    --First, create a table holding all NDC codes identifying naloxone
    if object_id(N'tempdb..#naloxone_ndc_list_prep') is not null drop table #naloxone_ndc_list_prep;
    create table #naloxone_ndc_list_prep (ndc varchar(255));
    
    insert into #naloxone_ndc_list_prep (ndc) values ('00932165');
    insert into #naloxone_ndc_list_prep (ndc) values ('04049921');
    insert into #naloxone_ndc_list_prep (ndc) values ('04049923');
    insert into #naloxone_ndc_list_prep (ndc) values ('04091782');
    insert into #naloxone_ndc_list_prep (ndc) values ('06416132');
    insert into #naloxone_ndc_list_prep (ndc) values ('06416260');
    insert into #naloxone_ndc_list_prep (ndc) values ('360000310');
    insert into #naloxone_ndc_list_prep (ndc) values ('435980750');
    insert into #naloxone_ndc_list_prep (ndc) values ('458020811');
    insert into #naloxone_ndc_list_prep (ndc) values ('500903294');
    insert into #naloxone_ndc_list_prep (ndc) values ('500905908');
    insert into #naloxone_ndc_list_prep (ndc) values ('500906710');
    insert into #naloxone_ndc_list_prep (ndc) values ('516621238');
    insert into #naloxone_ndc_list_prep (ndc) values ('516621240');
    insert into #naloxone_ndc_list_prep (ndc) values ('516621385');
    insert into #naloxone_ndc_list_prep (ndc) values ('516621495');
    insert into #naloxone_ndc_list_prep (ndc) values ('516621544');
    insert into #naloxone_ndc_list_prep (ndc) values ('516621620');
    insert into #naloxone_ndc_list_prep (ndc) values ('525840120');
    insert into #naloxone_ndc_list_prep (ndc) values ('551500327');
    insert into #naloxone_ndc_list_prep (ndc) values ('551500345');
    insert into #naloxone_ndc_list_prep (ndc) values ('594670679');
    insert into #naloxone_ndc_list_prep (ndc) values ('636299321');
    insert into #naloxone_ndc_list_prep (ndc) values ('674570299');
    insert into #naloxone_ndc_list_prep (ndc) values ('674570645');
    insert into #naloxone_ndc_list_prep (ndc) values ('674570992');
    insert into #naloxone_ndc_list_prep (ndc) values ('695470353');
    insert into #naloxone_ndc_list_prep (ndc) values ('700690071');
    insert into #naloxone_ndc_list_prep (ndc) values ('712050528');
    insert into #naloxone_ndc_list_prep (ndc) values ('718727009');
    insert into #naloxone_ndc_list_prep (ndc) values ('718727198');
    insert into #naloxone_ndc_list_prep (ndc) values ('718727219');
    insert into #naloxone_ndc_list_prep (ndc) values ('718727297');
    insert into #naloxone_ndc_list_prep (ndc) values ('725720450');
    insert into #naloxone_ndc_list_prep (ndc) values ('763293369');
    insert into #naloxone_ndc_list_prep (ndc) values ('786700140');
    insert into #naloxone_ndc_list_prep (ndc) values ('829540100');
    insert into #naloxone_ndc_list_prep (ndc) values ('04049920');
    insert into #naloxone_ndc_list_prep (ndc) values ('04049922');
    insert into #naloxone_ndc_list_prep (ndc) values ('04091215');
    insert into #naloxone_ndc_list_prep (ndc) values ('05912971');
    insert into #naloxone_ndc_list_prep (ndc) values ('06416205');
    insert into #naloxone_ndc_list_prep (ndc) values ('360000308');
    insert into #naloxone_ndc_list_prep (ndc) values ('420230224');
    insert into #naloxone_ndc_list_prep (ndc) values ('458020578');
    insert into #naloxone_ndc_list_prep (ndc) values ('500902422');
    insert into #naloxone_ndc_list_prep (ndc) values ('500905427');
    insert into #naloxone_ndc_list_prep (ndc) values ('500906491');
    insert into #naloxone_ndc_list_prep (ndc) values ('500906963');
    insert into #naloxone_ndc_list_prep (ndc) values ('516621239');
    insert into #naloxone_ndc_list_prep (ndc) values ('516621242');
    insert into #naloxone_ndc_list_prep (ndc) values ('516621426');
    insert into #naloxone_ndc_list_prep (ndc) values ('516621529');
    insert into #naloxone_ndc_list_prep (ndc) values ('516621586');
    insert into #naloxone_ndc_list_prep (ndc) values ('516621642');
    insert into #naloxone_ndc_list_prep (ndc) values ('542880124');
    insert into #naloxone_ndc_list_prep (ndc) values ('551500328');
    insert into #naloxone_ndc_list_prep (ndc) values ('557000985');
    insert into #naloxone_ndc_list_prep (ndc) values ('608420002');
    insert into #naloxone_ndc_list_prep (ndc) values ('674570292');
    insert into #naloxone_ndc_list_prep (ndc) values ('674570599');
    insert into #naloxone_ndc_list_prep (ndc) values ('674570987');
    insert into #naloxone_ndc_list_prep (ndc) values ('695470212');
    insert into #naloxone_ndc_list_prep (ndc) values ('695470627');
    insert into #naloxone_ndc_list_prep (ndc) values ('700690072');
    insert into #naloxone_ndc_list_prep (ndc) values ('712050707');
    insert into #naloxone_ndc_list_prep (ndc) values ('718727177');
    insert into #naloxone_ndc_list_prep (ndc) values ('718727215');
    insert into #naloxone_ndc_list_prep (ndc) values ('718727294');
    insert into #naloxone_ndc_list_prep (ndc) values ('718727299');
    insert into #naloxone_ndc_list_prep (ndc) values ('763291469');
    insert into #naloxone_ndc_list_prep (ndc) values ('763293469');
    insert into #naloxone_ndc_list_prep (ndc) values ('804250259');
    insert into #naloxone_ndc_list_prep (ndc) values ('830080007');
    
    --Second, add a column with % that can be used for a LIKE join
    if object_id(N'tempdb..#naloxone_ndc_list') is not null drop table #naloxone_ndc_list;
    select *, '%' + ndc + '%' as ndc_like
    into #naloxone_ndc_list
    from #naloxone_ndc_list_prep;
    
    --Third, LIKE join all distinct NDC codes to the list of naloxone NDC codes to create a data source-specific reference table
    --Then, use this custom reference table down below for an exact join
    if object_id(N'tempdb..#naloxone_ndc_ref_table') is not null drop table #naloxone_ndc_ref_table;
    select a.ndc, 1 as naloxone_flag
    into #naloxone_ndc_ref_table
    from (
    	select distinct ndc from stg_claims.final_mcare_claim_pharm
    ) as a
    inner join #naloxone_ndc_list as b
    on a.ndc like b.ndc_like;
    
    
    --- CREATE TABLE OF NALOXONE EVENTS ---
    
    insert into stg_claims.stage_mcare_claim_naloxone
    
    -- First get Naloxone distributed by pharmacies using NDC codes
    
    SELECT 
    	 a.id_mcare
    	,a.claim_header_id
    	,a.ndc as code
    	,UPPER(B.PROPRIETARYNAME) AS description
    	,a.last_service_date AS date
    	,A.qty_dspnsd_num AS quantity, 
    	case when DOSAGEFORMNAME LIKE '%SPRAY%' or A.NDC = '00093216519' THEN 'SPRAY' 
    		WHEN dosageformname LIKE '%INJECTION%' or A.NDC in ('55150034510', '55150032710', '00409121525')  THEN 'INJECTION' 
    		ELSE NULL END 
    		AS form,
    	CASE WHEN A.NDC = '00093216519' THEN 40.00
    	WHEN A.NDC = '55150034510' THEN 1
    	WHEN A.ndc = '55150032710' THEN 0.4
    	ELSE ACTIVE_NUMERATOR_STRENGTH / 
    		(case when ACTIVE_INGRED_UNIT = 'mg/.1mL' THEN .1 
    			WHEN  ACTIVE_INGRED_UNIT = 'mg/mL' THEN 1 ELSE NULL END) 
    			END
    			AS dosage_per_ml,
    	'PHARMACY' AS location,
    	getdate() as last_run
    
    FROM stg_claims.final_mcare_claim_pharm as a
    	left join stg_reference.ref_ndc_codes as b
    	on a.ndc = b.ndc
    
    	inner join #naloxone_ndc_ref_table as c
    	on a.ndc = c.ndc
    
    WHERE year(a.last_service_date) >= 2016
    	AND qty_dspnsd_num >= 1.00
    
    
    -- Next get Naloxone distributed as part of procedure codes
    
    UNION
    
    SELECT
    	id_mcare,
    	claim_header_id,
    	a.procedure_code AS code, 
    	UPPER(procedure_long_desc) as description,
    	last_service_date AS [date],
    	CASE WHEN a.procedure_code IN ('G1028', 'G2215') THEN 2
    		WHEN a.procedure_code IN ('G2216', 'J2310', 'J2311', 'J3490') THEN 1
    		ELSE NULL
    		END
    		AS quantity,
    	CASE WHEN a.procedure_code IN ('G1028', 'G2215') THEN 'SPRAY'
    		WHEN a.procedure_code IN ('G2216', 'J2310', 'J2311') THEN 'INJECTION'
    		WHEN a.procedure_code IN ('J3490') THEN 'UNKNOWN'
    		ELSE NULL
    		END
    		AS form,
    	CASE WHEN a.procedure_code IN ('G1028') THEN 80
    		WHEN a.procedure_code IN ('G2215') THEN 40
    		ELSE NULL
    		END
    		AS DOSAGE_IN_ML,
    		'PROCEDURE' AS location,
    		getdate() as last_run
    
    FROM stg_claims.final_mcare_claim_procedure AS A
    	LEFT JOIN stg_claims.ref_apcd_procedure_code AS B
    	ON A.procedure_code = B.procedure_code
    
    WHERE year(last_service_date) >= 2016 
    	and 
    	(a.procedure_code in ('G1028', 'G2215', 'G2216 ', 'J2310', 'J2311') 
    		or 
    	a.procedure_code = 'J3490' and (modifier_1 in ('HG', 'TG') 
    	    or modifier_2 in ('HG', 'TG')
    		or modifier_3 in ('HG', 'TG') or modifier_4 in ('HG', 'TG')));",
            .con = inthealth))
        }

#### Table-level QA script ####
qa_stage.mcare_claim_naloxone_qa_f <- function() {
  
  #make sure everyone is in bene_enrollment table
  res1 <- dbGetQuery(conn = inthealth, glue_sql(
  "select 'stg_claims.stage_mcare_claim_naloxone' as 'table', '# members not in bene_enrollment, expect 0' as qa_type,
    count(a.id_mcare) as qa
    from stg_claims.stage_mcare_claim_naloxone as a
    left join stg_claims.mcare_bene_enrollment as b
    on a.id_mcare = b.bene_id
    where b.bene_id is null;",
  .con = inthealth))
  
  #confirm no rows with null supply
  res2 <- dbGetQuery(conn = inthealth, glue_sql(
    "select 'stg_claims.stage_mcare_claim_naloxone' as 'table', '# of rows with null supply, expect 0' as qa_type,
    count(*) as qa
    from stg_claims.stage_mcare_claim_naloxone
    where quantity is null;",
    .con = inthealth))

res_final <- mget(ls(pattern="^res")) %>% bind_rows()
}