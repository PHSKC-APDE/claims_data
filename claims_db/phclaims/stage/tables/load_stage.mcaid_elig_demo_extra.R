#' @title load_stage.mcaid_elig_demo_extra
#' 
#' @description Utilize claims information to get a better idea of different
#' gender identities in Medicaid data and append that to the mcaid_elig_demo
#' table
#' 
#' @details 
#' 
#' Note: Need to use "set nocount on" to avoid temptable issues with R stopping the query early
#' 

# pacman::p_load(data.table, glue, keyring, lubridate, odbc)
# devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")
# 
# production_server <- F  # Change as needed!
# 
# schema <- "stg_claims"
# to_table <- "temp_noncisgender"
# proc_tbl <- "stage_mcaid_claim_procedure"
# pharm_tbl <- "stage_mcaid_claim_pharm"
# header_tbl <- "stage_mcaid_claim_icdcm_header"
# demog_tbl <- "stage_mcaid_elig_demo"
# ref_schema <- "stg_reference"
# ndc_tbl <- "ref_ndc_codes"
# 
# conn <- create_db_connection("inthealth", interactive = F, prod = production_server)

load_stage_mcaid_elig_demo_extra_f <- function(conn = NULL,
                                               server = c("hhsaw", "phclaims"),
                                               config = NULL,
                                               get_config = F) {
  
  # Set up variables specific to the server
  server <- match.arg(server)
  
  if (get_config == T){
    if (stringr::str_detect(config, "^http")) {
      config <- yaml::yaml.load(getURL(config))
    } else{
      stop("A URL must be specified in config if using get_config = T")
    }
  }
  schema <- config[[server]][["schema"]]
  to_table <- config[[server]][["to_table"]]
  proc_tbl <- config[[server]][["proc_tbl"]]
  pharm_tbl <- config[[server]][["pharm_tbl"]]
  header_tbl <- config[[server]][["header_tbl"]]
  demog_tbl <- config[[server]][["demog_tbl"]]
  ref_schema <- config[[server]][["ref_schema"]]
  ndc_tbl <- config[[server]][["ndc_tbl"]]
  config[[server]][["to_schema"]] <- schema
  
  message(glue::glue("Updating noncisgender column on [{schema}].[{demog_tbl}]."))
  
  # Query tables
  tbl1_dysphoria <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT id_mcaid, icdcm_norm, icdcm_version
     FROM {`schema`}.{`header_tbl`}
     WHERE icdcm_norm LIKE 'F64%'
     OR icdcm_norm LIKE 'F651%'
     OR icdcm_norm LIKE 'Z87890%'",
    .con = conn)))
  tbl2_endo_nos <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT id_mcaid, icdcm_norm, icdcm_version
     FROM {`schema`}.{`header_tbl`}
     WHERE icdcm_norm LIKE 'E34[89]%'
    OR icdcm_norm LIKE 'E0[01234567]%'
    OR icdcm_norm LIKE 'E2[01234567]%'
    OR icdcm_norm LIKE 'E31%'
    OR icdcm_norm LIKE 'E34[01234]%'
    OR icdcm_norm LIKE 'E7%'
    OR icdcm_norm LIKE 'E8[03457]%'
    OR icdcm_norm LIKE 'E88[01234]%'
    ",
    .con = conn)))
  tbl3a_ftm_proc <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT id_mcaid, procedure_code
     FROM {`schema`}.{`proc_tbl`}
     WHERE procedure_code IN ('0W4N071', '0W4N0J1', '0W4NOK1', '15757', '53410', '55175',
                      '55180', '55899', '55980', '57120', '64856')
    ",
    .con = conn)))
  tbl3b_ftm_no_uter <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "set nocount on
    --Pull relevant claims for gender reaffirming surgery FTM procedure codes (Table 3b)
     if object_id(N'tempdb..#ftm_no_uter') is not null drop table #ftm_no_uter;
     SELECT id_mcaid, claim_header_id, last_service_date, procedure_code
     into #ftm_no_uter
     FROM {`schema`}.{`proc_tbl`}
     WHERE procedure_code = '58661';
     
     --Pull codes for uterine, ovarian, and other cancers as exclusion criteria
     if object_id(N'tempdb..#exc_ftm_no_uter') is not null drop table #exc_ftm_no_uter;
     select distinct id_mcaid, claim_header_id, 1 as exc_ftm_no_uter
     into #exc_ftm_no_uter
     from {`schema`}.{`header_tbl`}
     where (icdcm_version = 9 and icdcm_norm like '183%')
     or (icdcm_version = 10 and icdcm_norm like 'C56%')
     or (icdcm_version = 10 and icdcm_norm like 'C57%');
     
     select *
     FROM #ftm_no_uter as a
     left join #exc_ftm_no_uter as b
     on a.claim_header_id = b.claim_header_id
     where b.exc_ftm_no_uter is null
    ",
    .con = conn)))
  tbl3c_ftm_no_vag <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "set nocount on
    --Pull relevant claims for gender reaffirming surgery FTM procedure codes (Table 3c)
     if object_id('tempdb..#ftm_no_vag') is not null drop table #ftm_no_vag;
     SELECT id_mcaid, claim_header_id, last_service_date, procedure_code
     into #ftm_no_vag
     FROM {`schema`}.{`proc_tbl`}
     WHERE procedure_code IN ('58661', '704', '7162', '0UTG0ZZ', '0UTG4ZZ',
       '0UTG7ZZ', '0UTG8ZZ', '0UTM0ZZ', '0UTMXZZ');
     
     --Pull codes for vaginal cancers as exclusion criteria
     if object_id('tempdb..#exc_ftm_no_vag') is not null drop table #exc_ftm_no_vag;
     select distinct id_mcaid, claim_header_id, 1 as exc_ftm_no_vag
     into #exc_ftm_no_vag
     from {`schema`}.{`header_tbl`}
     where (icdcm_version = 9 and icdcm_norm like '184%')
     or (icdcm_version = 10 and icdcm_norm like 'C51%')
     or (icdcm_version = 10 and icdcm_norm like 'C52%');
     
     select *
     FROM #ftm_no_vag as a
     left join #exc_ftm_no_vag as b
     on a.claim_header_id = b.claim_header_id
     where b.exc_ftm_no_vag is null;
    ",
    .con = conn)))
  tbl3d_mtf_proc <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT id_mcaid, procedure_code
     FROM {`schema`}.{`proc_tbl`}
     WHERE procedure_code IN ('0W4M070', '0W4M0J0', '0W4M0K0', '0W4M0Z0', '21209', '31899',
                      '53430', '54125', '55970', '56805', '57335', '58999')
    ",
    .con = conn)))
  tbl3e_mtf_no_test <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "set nocount on
    --Pull relevant claims for gender reaffirming surgery MTF procedure codes (Table 3e)
     if object_id(N'tempdb..#mtf_no_test') is not null drop table #mtf_no_test;
     SELECT id_mcaid, claim_header_id, last_service_date, procedure_code
     into #mtf_no_test
     FROM {`schema`}.{`proc_tbl`}
     where procedure_code in ('54520', '54690');
     
     --Pull codes for testicular cancers as exclusion criteria
     if object_id(N'tempdb..#exc_mtf_no_test') is not null drop table #exc_mtf_no_test;
     select distinct id_mcaid, claim_header_id, 1 as exc_mtf_no_test
     into #exc_mtf_no_test
     from {`schema`}.{`header_tbl`}
     where (icdcm_version = 9 and icdcm_norm like '187[56789]%')
     or (icdcm_version = 10 and icdcm_norm like 'C6[23]%');
     
     select *
     FROM #mtf_no_test as a
     left join #exc_mtf_no_test as b
     on a.claim_header_id = b.claim_header_id
     where b.exc_mtf_no_test is null
    ",
    .con = conn)))
  tbl3f_mtf_no_pen <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "set nocount on
    --Pull relevant claims for gender reaffirming surgery MTF procedure codes (Table 3e)
     if object_id(N'tempdb..#mtf_no_pen') is not null drop table #mtf_no_pen;
     SELECT id_mcaid, claim_header_id, last_service_date, procedure_code
     into #mtf_no_pen
     FROM {`schema`}.{`proc_tbl`}
     where procedure_code in ('643', '0VTS0ZZ', '0VTS4ZZ', '0VTSXZZ');
     
     --Pull codes for penile cancers as exclusion criteria
     if object_id(N'tempdb..#exc_mtf_no_pen') is not null drop table #exc_mtf_no_pen;
     select distinct id_mcaid, claim_header_id, 1 as exc_mtf_no_pen
     into #exc_mtf_no_pen
     from {`schema`}.{`header_tbl`}
     where (icdcm_version = 9 and icdcm_norm like '187[1234]%')
     or (icdcm_version = 10 and icdcm_norm like 'C60%');
     
     select *
     FROM #mtf_no_pen as a
     left join #exc_mtf_no_pen as b
     on a.claim_header_id = b.claim_header_id
     where b.exc_mtf_no_pen is null
    ",
    .con = conn)))
  
  tbl4a_ndc_codes <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT ndc, UPPER(NONPROPRIETARYNAME) AS ndc_name
     FROM {`ref_schema`}.{`ndc_tbl`}
     WHERE UPPER(NONPROPRIETARYNAME) LIKE '%ESTRAD%'
     OR UPPER(NONPROPRIETARYNAME) LIKE '%ESTRO%'
     OR UPPER(NONPROPRIETARYNAME) LIKE '%ESTRIOL%'
     OR UPPER(NONPROPRIETARYNAME) LIKE '%ESTR/PRG%'
    ",
    .con = conn)))
  tbl4a_fem_no_req <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT id_mcaid, ndc
     FROM {`schema`}.{`pharm_tbl`}
     WHERE ndc IN ({tbl4a_ndc_codes$ndc*})
    ",
    .con = conn)))
  
  tbl4b_ndc_codes <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT ndc, UPPER(NONPROPRIETARYNAME) AS ndc_name
     FROM {`ref_schema`}.{`ndc_tbl`}
     WHERE UPPER(NONPROPRIETARYNAME) LIKE '%DIHYDROTESTOSTERONE PROPIONATE%'
     OR UPPER(NONPROPRIETARYNAME) LIKE '%NANDROLONE%'
     OR UPPER(NONPROPRIETARYNAME) LIKE '%STANOLONE%'
     OR UPPER(NONPROPRIETARYNAME) LIKE '%STANOZOLOL%'
    ",
    .con = conn)))
  tbl4b_masc_no_req <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT id_mcaid, ndc
     FROM {`schema`}.{`pharm_tbl`}
     WHERE ndc IN ({tbl4b_ndc_codes$ndc*})
    ",
    .con = conn)))
  
  tbl4c_ndc_codes <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT ndc, UPPER(NONPROPRIETARYNAME) AS ndc_name, DOSAGEFORMNAME,
    ACTIVE_NUMERATOR_STRENGTH, ACTIVE_INGRED_UNIT
     FROM {`ref_schema`}.{`ndc_tbl`}
     WHERE UPPER(NONPROPRIETARYNAME) LIKE '%TESTOSTERONE%'
    ",
    .con = conn)))
  # > 7mg intramuscular drugs; >2mg gels/transdermal patches
  # NOTE: This misses solution, tablet, capsule, liquid, capsule liquid filled,
  # spray, solution/drops, powder, pellet implantable, pellet, cream, and kit
  # Taking us from around 80,000 to 60,000
  tbl4c_ndc_codes$ACTIVE_NUMERATOR_STRENGTH <- 
    vapply(strsplit(tbl4c_ndc_codes$ACTIVE_NUMERATOR_STRENGTH,";"), `[`, 1, FUN.VALUE=character(1))
  tbl4c_ndc_codes$ACTIVE_NUMERATOR_STRENGTH <- suppressWarnings(as.numeric(
    vapply(strsplit(tbl4c_ndc_codes$ACTIVE_NUMERATOR_STRENGTH," "), `[`, 1, FUN.VALUE=character(1))))
  tbl4c_ndc_codes <- tbl4c_ndc_codes[
    (ACTIVE_NUMERATOR_STRENGTH >= 7 & DOSAGEFORMNAME %in% c("INJECTION", "INJECTION, SOLUTION"))
    | (ACTIVE_NUMERATOR_STRENGTH >= 2 & DOSAGEFORMNAME %in% c("GEL", "PATCH", "GEL, METERED"))
    ,]
  tbl4c_masc_min_req <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT id_mcaid, ndc
     FROM {`schema`}.{`pharm_tbl`}
     WHERE ndc IN ({tbl4c_ndc_codes$ndc*})
    ",
    .con = conn)))
  
  tbl4d_ndc_codes <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT ndc, UPPER(NONPROPRIETARYNAME) AS ndc_name, DOSAGEFORMNAME,
    ACTIVE_NUMERATOR_STRENGTH, ACTIVE_INGRED_UNIT
     FROM {`ref_schema`}.{`ndc_tbl`}
     WHERE UPPER(NONPROPRIETARYNAME) LIKE '%SPIRONOLACTONE%'
    ",
    .con = conn)))
  # >50mg dose requirement
  # NOTE: This gets tablets, but misses solutions/drops, powder, and suspensions
  # Around 60,000 rows gone from final 4d
  tbl4d_ndc_codes$ACTIVE_NUMERATOR_STRENGTH <- as.numeric(
    sapply(strsplit(tbl4d_ndc_codes$ACTIVE_NUMERATOR_STRENGTH,";"), getElement, 1))
  tbl4d_ndc_codes <- tbl4d_ndc_codes[ACTIVE_NUMERATOR_STRENGTH >= 50,]
  tbl4d_fem_min_req <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT id_mcaid, ndc
     FROM {`schema`}.{`pharm_tbl`}
     WHERE ndc IN ({tbl4d_ndc_codes$ndc*})
    ",
    .con = conn)))
  
  tbl4e_exc_masc_hormone <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT id_mcaid, icdcm_norm, icdcm_version
     FROM {`schema`}.{`header_tbl`}
     WHERE icdcm_norm LIKE 'F52%'
     OR icdcm_norm LIKE 'R6882%'",
    .con = conn)))
  
  tbl4f_exc_spiro <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT id_mcaid, icdcm_norm, icdcm_version
     FROM {`schema`}.{`header_tbl`}
     WHERE icdcm_norm LIKE 'I09.9%'
     OR icdcm_norm LIKE 'I110%'
     OR icdcm_norm LIKE 'I130%'
     OR icdcm_norm LIKE 'I132%'
     OR icdcm_norm LIKE 'I255%'
     OR icdcm_norm LIKE 'I420%'
     OR icdcm_norm LIKE 'I425%'
     OR icdcm_norm LIKE 'I426%'
     OR icdcm_norm LIKE 'I427%'
     OR icdcm_norm LIKE 'I428%'
     OR icdcm_norm LIKE 'I429%'
     OR icdcm_norm LIKE 'I43%'
     OR icdcm_norm LIKE 'I50%'
     OR icdcm_norm LIKE 'I85%'
     OR icdcm_norm LIKE 'I864%'
     OR icdcm_norm LIKE 'K70%'
     OR icdcm_norm LIKE 'K717%'
     OR icdcm_norm LIKE 'K72%'
     OR icdcm_norm LIKE 'K74%'
     OR icdcm_norm LIKE 'K767%'
     OR icdcm_norm LIKE 'P290%'
    ",
    .con = conn)))
  
  
  # Mix-and-match
  # get all transmasc_proc, transferm_proc, and unclassified trans IDs
  transmasc_proc <- unique(c(tbl3a_ftm_proc$id_mcaid, tbl3b_ftm_no_uter$id_mcaid, tbl3c_ftm_no_vag$id_mcaid))
  transfem_proc <- unique(c(tbl3d_mtf_proc$id_mcaid, tbl3e_mtf_no_test$id_mcaid, tbl3f_mtf_no_pen$id_mcaid))
  trans_unknown <- unique(tbl1_dysphoria[!id_mcaid %in% c(transfem_proc, transmasc_proc),]$id_mcaid)
  
  # Subset ENOS to those with trans-related procedures
  enos_transmasc <- intersect(tbl2_endo_nos$id_mcaid, transmasc_proc)
  enos_transfem <- intersect(tbl2_endo_nos$id_mcaid, transfem_proc)
  
  # remove exclusions from hormone tables
  masc_hormones <- unique(c(tbl4b_masc_no_req$id_mcaid, tbl4c_masc_min_req$id_mcaid))
  fem_hormones <- unique(c(tbl4a_fem_no_req$id_mcaid, tbl4d_fem_min_req$id_mcaid))
  
  # trans proc and hormone therapy
  transmasc_and_hormones <- intersect(transmasc_proc, masc_hormones)
  transfem_and_hormones <- intersect(transfem_proc, fem_hormones)
  
  # enos and hormone
  enos_and_hormones_masc <- intersect(tbl2_endo_nos$id_mcaid, masc_hormones)
  enos_and_hormones_fem <- intersect(tbl2_endo_nos$id_mcaid, fem_hormones)
  
  # using gender_me
  enos_and_hormones_masc_f_sex <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT id_mcaid, gender_me
     FROM {`schema`}.{`demog_tbl`}
     WHERE gender_me = 'Female'
     AND id_mcaid IN ({enos_and_hormones_masc*})
    ",
    .con = conn)))
  m_sex <- setDT(DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT id_mcaid, gender_me
     FROM {`schema`}.{`demog_tbl`}
     WHERE gender_me = 'Male'
    ",
    .con = conn)))
  enos_and_hormones_fem_m_sex <- m_sex[id_mcaid %in% enos_and_hormones_fem,]
  rm(m_sex)
  
  # Reduce
  transmasc_ids <- Reduce(
    union, c(transmasc_proc, enos_transmasc, transmasc_and_hormones, enos_and_hormones_masc_f_sex))
  transfem_ids <- Reduce(
    union, c(transfem_proc, enos_transfem, transfem_and_hormones, enos_and_hormones_fem_m_sex))
  conflicting_ids <- intersect(transmasc_ids, transfem_ids)
  transmasc_ids_no_conflict <- setdiff(transmasc_ids, conflicting_ids)
  transfem_ids_no_conflict <- setdiff(transfem_ids, conflicting_ids)
  trans_unknown_ids <- setdiff(trans_unknown, transmasc_ids)  # this also removes the conflict ids
  trans_unknown_ids <- setdiff(trans_unknown_ids, transfem_ids)
  
  # Identified by any means
  all_ids <- Reduce(
    union, c(transmasc_ids_no_conflict, transfem_ids_no_conflict, trans_unknown_ids))  # 13766
  
  all_ids_tbl <- as.data.frame(all_ids)
  names(all_ids_tbl) <- c("id_mcaid")
  
  # Create temporary table with noncisgender IDs and load via BCP
  create_table_f(conn = conn, 
                 server = server,
                 config = config,
                 overwrite = T)
  load_df_bcp_f(dataset = all_ids_tbl,
             server = stg_server,
             db_name = "inthealth_edw",
             schema_name = schema,
             table_name = to_table,
             user = keyring::key_list(server)[["username"]],
             pass = keyring::key_get(server, keyring::key_list(server)[["username"]]))
  # Check if all ids loaded
  id_cnt <- DBI::dbGetQuery(conn,
                            glue::glue_sql("SELECT COUNT(*) FROM {`schema`}.{`to_table`}",
                                           .con = conn))[1,1]
  if(id_cnt != nrow(all_ids_tbl)) {
    stop("Not all IDs loaded into temp table!")
  }
  # Update mcaid_elig_demo table
  DBI::dbExecute(conn, glue::glue_sql(
    "
    UPDATE a
    SET noncisgender = 1
    FROM {`schema`}.{`demog_tbl`} a
    INNER JOIN {`schema`}.{`to_table`} b
    ON a.id_mcaid = b.id_mcaid
    ",
  .con = conn))
  # Delete the 'temporary' table
  DBI::dbExecute(conn, 
                 glue::glue_sql("DROP TABLE {`schema`}.{`to_table`}",
                                .con = conn))
  message(glue::glue("[{schema}].[{demog_tbl}] table has had the noncisgender column updated."))
}





