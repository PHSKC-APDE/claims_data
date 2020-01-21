#################################################################################################
  # Author: Danny Colombara
  # Date: 2019/02/27
  # Purpose: Push Medicare MBSF ABCD data (2015-2016) to SQL. 
  # Notes: 
  #		      

## Clear memory and load packages ----
  rm(list=ls())
  pacman::p_load(data.table, lubridate, odbc, DBI, tidyr)

## set working directory ----
  setwd("//phdata01/DROF_Data/DOH DATA/Medicare/CMS_Drive/CMS_Drive/4749/New data/")
  
## Prevent scientific notation except for huge numbers ----
	options("scipen"=999) # turn off scientific notation

## Connect to the servers ----
  sql_server = "KCITSQLUTPDBH51"
  sql_server_odbc_name = "PHClaims51"
  db.claims51 <- dbConnect(odbc(), sql_server_odbc_name) ##Connect to SQL server

## Parameters for SQL table IDs and columns ----
  sql.columns <- c("bene_id" = "varchar(255)", "bene_enrollmt_ref_yr" = "INT", "enrl_src" = "varchar(255)", "sample_group" = "varchar(255)", 
                   "enhanced_five_percent_flag" = "varchar(255)", "crnt_bic_cd" = "varchar(255)", "state_code" = "varchar(255)", 
                   "county_cd" = "varchar(255)", "zip_cd" = "varchar(255)", "state_cnty_fips_cd_01" = "varchar(255)", 
                   "state_cnty_fips_cd_02" = "varchar(255)", "state_cnty_fips_cd_03" = "varchar(255)", "state_cnty_fips_cd_04" = "varchar(255)", 
                   "state_cnty_fips_cd_05" = "varchar(255)", "state_cnty_fips_cd_06" = "varchar(255)", "state_cnty_fips_cd_07" = "varchar(255)", 
                   "state_cnty_fips_cd_08" = "varchar(255)", "state_cnty_fips_cd_09" = "varchar(255)", "state_cnty_fips_cd_10" = "varchar(255)", 
                   "state_cnty_fips_cd_11" = "varchar(255)", "state_cnty_fips_cd_12" = "varchar(255)", "age_at_end_ref_yr" = "INT", 
                   "bene_birth_dt" = "DATE", "valid_death_dt_sw" = "varchar(255)", "bene_death_dt" = "DATE", "sex_ident_cd" = "varchar(255)", 
                   "bene_race_cd" = "varchar(255)", "rti_race_cd" = "varchar(255)", "covstart" = "DATE", "entlmt_rsn_orig" = "varchar(255)", 
                   "entlmt_rsn_curr" = "varchar(255)", "esrd_ind" = "varchar(255)", "mdcr_status_code_01" = "varchar(255)", 
                   "mdcr_status_code_02" = "varchar(255)", "mdcr_status_code_03" = "varchar(255)", "mdcr_status_code_04" = "varchar(255)", 
                   "mdcr_status_code_05" = "varchar(255)", "mdcr_status_code_06" = "varchar(255)", "mdcr_status_code_07" = "varchar(255)", 
                   "mdcr_status_code_08" = "varchar(255)", "mdcr_status_code_09" = "varchar(255)", "mdcr_status_code_10" = "varchar(255)", 
                   "mdcr_status_code_11" = "varchar(255)", "mdcr_status_code_12" = "varchar(255)", "bene_pta_trmntn_cd" = "varchar(255)", 
                   "bene_ptb_trmntn_cd" = "varchar(255)", "bene_hi_cvrage_tot_mons" = "INT", "bene_smi_cvrage_tot_mons" = "INT", 
                   "bene_state_buyin_tot_mons" = "INT", "bene_hmo_cvrage_tot_mons" = "INT", "ptd_plan_cvrg_mons" = "INT", "rds_cvrg_mons" = "INT", 
                   "dual_elgbl_mons" = "INT", "mdcr_entlmt_buyin_ind_01" = "varchar(255)", "mdcr_entlmt_buyin_ind_02" = "varchar(255)", 
                   "mdcr_entlmt_buyin_ind_03" = "varchar(255)", "mdcr_entlmt_buyin_ind_04" = "varchar(255)", "mdcr_entlmt_buyin_ind_05" = "varchar(255)", 
                   "mdcr_entlmt_buyin_ind_06" = "varchar(255)", "mdcr_entlmt_buyin_ind_07" = "varchar(255)", "mdcr_entlmt_buyin_ind_08" = "varchar(255)", 
                   "mdcr_entlmt_buyin_ind_09" = "varchar(255)", "mdcr_entlmt_buyin_ind_10" = "varchar(255)", "mdcr_entlmt_buyin_ind_11" = "varchar(255)", 
                   "mdcr_entlmt_buyin_ind_12" = "varchar(255)", "hmo_ind_01" = "varchar(255)", "hmo_ind_02" = "varchar(255)", 
                   "hmo_ind_03" = "varchar(255)", "hmo_ind_04" = "varchar(255)", "hmo_ind_05" = "varchar(255)", "hmo_ind_06" = "varchar(255)", 
                   "hmo_ind_07" = "varchar(255)", "hmo_ind_08" = "varchar(255)", "hmo_ind_09" = "varchar(255)", "hmo_ind_10" = "varchar(255)", 
                   "hmo_ind_11" = "varchar(255)", "hmo_ind_12" = "varchar(255)", "ptc_cntrct_id_01" = "varchar(255)", "ptc_cntrct_id_02" = "varchar(255)", 
                   "ptc_cntrct_id_03" = "varchar(255)", "ptc_cntrct_id_04" = "varchar(255)", "ptc_cntrct_id_05" = "varchar(255)", 
                   "ptc_cntrct_id_06" = "varchar(255)", "ptc_cntrct_id_07" = "varchar(255)", "ptc_cntrct_id_08" = "varchar(255)", 
                   "ptc_cntrct_id_09" = "varchar(255)", "ptc_cntrct_id_10" = "varchar(255)", "ptc_cntrct_id_11" = "varchar(255)", 
                   "ptc_cntrct_id_12" = "varchar(255)", "ptc_pbp_id_01" = "varchar(255)", "ptc_pbp_id_02" = "varchar(255)", 
                   "ptc_pbp_id_03" = "varchar(255)", "ptc_pbp_id_04" = "varchar(255)", "ptc_pbp_id_05" = "varchar(255)", 
                   "ptc_pbp_id_06" = "varchar(255)", "ptc_pbp_id_07" = "varchar(255)", "ptc_pbp_id_08" = "varchar(255)", 
                   "ptc_pbp_id_09" = "varchar(255)", "ptc_pbp_id_10" = "varchar(255)", "ptc_pbp_id_11" = "varchar(255)", 
                   "ptc_pbp_id_12" = "varchar(255)", "ptc_plan_type_cd_01" = "varchar(255)", "ptc_plan_type_cd_02" = "varchar(255)", 
                   "ptc_plan_type_cd_03" = "varchar(255)", "ptc_plan_type_cd_04" = "varchar(255)", "ptc_plan_type_cd_05" = "varchar(255)", 
                   "ptc_plan_type_cd_06" = "varchar(255)", "ptc_plan_type_cd_07" = "varchar(255)", "ptc_plan_type_cd_08" = "varchar(255)", 
                   "ptc_plan_type_cd_09" = "varchar(255)", "ptc_plan_type_cd_10" = "varchar(255)", "ptc_plan_type_cd_11" = "varchar(255)", 
                   "ptc_plan_type_cd_12" = "varchar(255)", "ptd_cntrct_id_01" = "varchar(255)", "ptd_cntrct_id_02" = "varchar(255)", 
                   "ptd_cntrct_id_03" = "varchar(255)", "ptd_cntrct_id_04" = "varchar(255)", "ptd_cntrct_id_05" = "varchar(255)", 
                   "ptd_cntrct_id_06" = "varchar(255)", "ptd_cntrct_id_07" = "varchar(255)", "ptd_cntrct_id_08" = "varchar(255)", 
                   "ptd_cntrct_id_09" = "varchar(255)", "ptd_cntrct_id_10" = "varchar(255)", "ptd_cntrct_id_11" = "varchar(255)", 
                   "ptd_cntrct_id_12" = "varchar(255)", "ptd_pbp_id_01" = "varchar(255)", "ptd_pbp_id_02" = "varchar(255)", 
                   "ptd_pbp_id_03" = "varchar(255)", "ptd_pbp_id_04" = "varchar(255)", "ptd_pbp_id_05" = "varchar(255)", 
                   "ptd_pbp_id_06" = "varchar(255)", "ptd_pbp_id_07" = "varchar(255)", "ptd_pbp_id_08" = "varchar(255)", 
                   "ptd_pbp_id_09" = "varchar(255)", "ptd_pbp_id_10" = "varchar(255)", "ptd_pbp_id_11" = "varchar(255)", 
                   "ptd_pbp_id_12" = "varchar(255)", "ptd_sgmt_id_01" = "varchar(255)", "ptd_sgmt_id_02" = "varchar(255)", 
                   "ptd_sgmt_id_03" = "varchar(255)", "ptd_sgmt_id_04" = "varchar(255)", "ptd_sgmt_id_05" = "varchar(255)", 
                   "ptd_sgmt_id_06" = "varchar(255)", "ptd_sgmt_id_07" = "varchar(255)", "ptd_sgmt_id_08" = "varchar(255)", 
                   "ptd_sgmt_id_09" = "varchar(255)", "ptd_sgmt_id_10" = "varchar(255)", "ptd_sgmt_id_11" = "varchar(255)", 
                   "ptd_sgmt_id_12" = "varchar(255)", "rds_ind_01" = "varchar(255)", "rds_ind_02" = "varchar(255)", "rds_ind_03" = "varchar(255)", 
                   "rds_ind_04" = "varchar(255)", "rds_ind_05" = "varchar(255)", "rds_ind_06" = "varchar(255)", "rds_ind_07" = "varchar(255)", 
                   "rds_ind_08" = "varchar(255)", "rds_ind_09" = "varchar(255)", "rds_ind_10" = "varchar(255)", "rds_ind_11" = "varchar(255)", 
                   "rds_ind_12" = "varchar(255)", "dual_stus_cd_01" = "varchar(255)", "dual_stus_cd_02" = "varchar(255)", 
                   "dual_stus_cd_03" = "varchar(255)", "dual_stus_cd_04" = "varchar(255)", "dual_stus_cd_05" = "varchar(255)", 
                   "dual_stus_cd_06" = "varchar(255)", "dual_stus_cd_07" = "varchar(255)", "dual_stus_cd_08" = "varchar(255)", 
                   "dual_stus_cd_09" = "varchar(255)", "dual_stus_cd_10" = "varchar(255)", "dual_stus_cd_11" = "varchar(255)", 
                   "dual_stus_cd_12" = "varchar(255)", "cst_shr_grp_cd_01" = "varchar(255)", "cst_shr_grp_cd_02" = "varchar(255)", 
                   "cst_shr_grp_cd_03" = "varchar(255)", "cst_shr_grp_cd_04" = "varchar(255)", "cst_shr_grp_cd_05" = "varchar(255)", 
                   "cst_shr_grp_cd_06" = "varchar(255)", "cst_shr_grp_cd_07" = "varchar(255)", "cst_shr_grp_cd_08" = "varchar(255)", 
                   "cst_shr_grp_cd_09" = "varchar(255)", "cst_shr_grp_cd_10" = "varchar(255)", "cst_shr_grp_cd_11" = "varchar(255)", 
                   "cst_shr_grp_cd_12" = "varchar(255)", "data_year" = "INT")
  sql_database_name <- "phclaims" ##Name of SQL database where table will be created
  sql_schema_name <- "load_raw" ##Name of schema where table will be created
  sql_table_name <- "mcare_mbsf_abcd"
  glue::glue({sql_database_name}.{sql_schema_name}.{sql_table_name})
  

## Create Empty Table ----
  # mbsf.create_table <- DBI::SQL(paste0(sql_database_name, ".", sql_schema_name, ".", sql_table_name))
  # dbRemoveTable(conn = db.claims51, name = mbsf.create_table) # delete table if it exists
  # dbCreateTable(conn = db.claims51, name = mbsf.create_table, fields = sql.columns, row.names = NULL)
  
## Get order of columns from SQL table ----
  mbsfabcd.names <- names(sql.columns)
  
## Write yearly MBSF_ABCD data ----
  mbsf.write.table <- DBI::Id(schema = sql_schema_name, table = sql_table_name)  
  for(yr in 17){
    # Import data####
      mbsf <- fread(paste0("//phdata01/DROF_Data/DOH DATA/Medicare/CMS_Drive/CMS_Drive/4749/New data/20", yr, "/mbsf_abcd_", yr, "_summary/mbsf_abcd_summary.csv"))

    # Drop the given year from the SQL data to avoid duplication   ----
      dbGetQuery(
        conn = db.claims51,
        glue::glue("DELETE FROM {sql_database_name}.{sql_schema_name}.{sql_table_name} where bene_enrollmt_ref_yr = 20{yr}", .con = db.claims51))

    # Change column names to lower case ----
      setnames(mbsf, names(mbsf), tolower(names(mbsf)))
      
    # Drop the useless rownumber indicator that was made by SAS ----
      suppressWarnings(mbsf[, c(grep("v1", names(mbsf), value = TRUE)) := NULL])      

    # Add a variable to indicate the year of the data upload ----
      mbsf[, data_year:=2000+yr]     
      
    # Set column order to match that in SQL to ensure proper appending ----
      setcolorder(mbsf, mbsfabcd.names)      
      
    # set dates to proper format ----
      date.vars<-c("bene_birth_dt", "bene_death_dt", "covstart")
      mbsf[, bene_birth_dt := dmy(bene_birth_dt)]
      mbsf[, bene_death_dt := dmy(bene_death_dt)]
      mbsf[, covstart := dmy(covstart)]

    # Append new data ----
      dbWriteTable(conn = db.claims51, name = mbsf.write.table, value = as.data.frame(mbsf), row.names = FALSE, header = T, append = T)
          
    # Basic QA ----
      # row counts
        count.sql <-  as.numeric(dbGetQuery(conn = db.claims51, glue::glue("SELECT count(*) FROM {sql_database_name}.{sql_schema_name}.{sql_table_name} where bene_enrollmt_ref_yr = 20{yr}", .con = db.claims51)))
        if(count.sql == nrow(mbsf)){
          print("All rows were copies to SQL")
        } else {stop("The number of rows copied to SQL does not equal the number of rows in the MBSF")}
      
      # column names
        names.sql <- tolower(names(dbGetQuery(conn = db.claims51, glue::glue("SELECT top(0) * FROM {sql_database_name}.{sql_schema_name}.{sql_table_name}", .con = db.claims51))))
        if( identical(names.sql, names(mbsf)) ){
          print("All columns were copied to SQL")
        } else {
          dbGetQuery(
            conn = db.claims51,
            glue::glue("DELETE FROM {sql_database_name}.{sql_schema_name}.{sql_table_name} where bene_enrollmt_ref_yr = 20{yr}", .con = db.claims51))
          stop("There is mismatch between SQL column names and the column names in R. 
               The 20{yr] data has been deleted from SQL. Identify the error and try again")}
      
  } # close loop for each year


# the end ----