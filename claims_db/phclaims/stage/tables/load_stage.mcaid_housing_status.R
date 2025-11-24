#' @title load_stage.mcaid_housing_status
#' 
#' @description TBD
#' 
#' @details TBD
#' 
#' Note: 
#' 

## Set up constants and dbs ----
pacman::p_load(apde.etl, caret, data.table, glue, here, irr, lubridate, odbc, openxlsx, rads)

load_stage_mcaid_housing_status <- function(conn = NULL,
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
  timevar_table <- config[[server]][["timevar_table"]]
  icdcm_table <- config[[server]][["icdcm_header_table"]]
  kcids_table <- config[[server]][["kcids_table"]]
  z_code_crosswalk_path <- "https://raw.githubusercontent.com/PHSKC-APDE/claims_data/refs/heads/main/claims_db/db_loader/mcaid/mcaid_housing_status_crosswalk.xlsx"

  
  ## Medaid data load ----
  
  # Load Medicaid data
  mcaid <- setDT(
    DBI::dbGetQuery(conn,                                  
      glue::glue_sql("
      SELECT 
        a.id_mcaid, a.from_date, a.geo_add1, a.geo_hash_clean, b.icdcm_norm
      FROM
        (SELECT * FROM {`schema`}.{`timevar_table`}) a
          LEFT JOIN
          (SELECT id_mcaid, first_service_date, icdcm_norm
            FROM {`schema`}.{`icdcm_table`}
            WHERE icdcm_norm   in ( 'Z590', 'Z5900','Z5901', 'Z5902', 'Z591', 'Z5910', 'Z5919', 'Z5981%')) b
              ON a.id_mcaid = b.id_mcaid AND MONTH(a.from_date) = MONTH(b.first_service_date)",    
                     .con = conn)))
  
  # Remove any data that doesn't have a from_date, since it can't be used
  mcaid <- mcaid[!is.na(mcaid$from_date),]
  
  ## Create housing status columns ----
  
  # Load z-code to housing status crosswalk
  temp_file <- tempfile(fileext = ".xlsx")
  download.file(z_code_crosswalk_path,
                destfile = temp_file,
                mode = "wb")
  crosswalk_z_codes <- as.data.table(read.xlsx(xlsxFile = temp_file, sheet = "Z codes"))
  
  # Use mapping to create housing status from z-codes
  mcaid[crosswalk_z_codes, on = 'icdcm_norm == Z.Code', housing_status := i.Housing_Status]
  
  # add homeless address info (not overwriting existing info)
  mcaid$housing_status <- ifelse(mcaid$geo_add1 == "HOMELESS" & is.na(mcaid$housing_status), "homeless", mcaid$housing_status)
  
  # create column of "source" of housing status
  mcaid$housing_status_source <- data.table::fcase(
    mcaid$geo_add1 == "HOMELESS" &
      mcaid$icdcm_norm %in% c('Z590', 'Z5900','Z5901', 'Z5902', 'Z591'), "multiple",
    mcaid$geo_add1 == "HOMELESS" &
      mcaid$icdcm_norm %in% c('Z5910', 'Z5919', 'Z5981%'), "z_codes",  # currently z-codes higher in the hierarchy
    mcaid$geo_add1 == "HOMELESS" &
      !(mcaid$icdcm_norm %in% c('Z590', 'Z5900','Z5901', 'Z5902', 'Z591', 'Z5910', 'Z5919', 'Z5981%')), "homeless_address",
    mcaid$icdcm_norm %in% c('Z590', 'Z5900','Z5901', 'Z5902', 'Z591', 'Z5910', 'Z5919', 'Z5981%') & !(mcaid$geo_add1 == "HOMELESS"), "z_codes"
  )
  
  # Final columns: mcaid id, from date, housing status source, housing status
  mcaid_upload <- mcaid[!is.na(mcaid$housing_status),
                        c("id_mcaid", "from_date", "housing_status", "housing_status_source")]
  
  
  ## Upload ----
  # Add last run date/time
  full[, last_run := Sys.time()]
  
  # Write out table
  DBI::dbWriteTable(conn = db_hhsaw,
                    name = DBI::Id(schema = schema, table = to_table),
                    value = full,
                    overwrite = T)
  
}