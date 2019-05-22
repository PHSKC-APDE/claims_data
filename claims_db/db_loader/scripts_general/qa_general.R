#### GENERAL FUNCTIONS FOR QA PROCESSES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

#### FUNCTION FOR STANDARDIZED YAML ERROR CHECKS ####
qa_error_check_f <- function(config_url_chk = config_url,
                             config_file_chk = config_file,
                             overall_chk = overall,
                             ind_yr_chk = ind_yr) {
  
  #### BASIC ERROR CHECKS ####
  # Check that something will be run (but not both things)
  if (overall_chk == F & ind_yr_chk == F) {
    stop("At least one of 'overall and 'ind_yr' must be set to TRUE")
  }
  
  if (overall_chk == T & ind_yr_chk == T) {
    stop("Only one of 'overall and 'ind_yr' can be set to TRUE")
  }
  
  # Check if the config provided is a local file or on a webpage
  if (!is.null(config_url_chk) & !is.null(config_file_chk)) {
    stop("Specify either a config_url or config_file but not both")
  }
  
  # Check that the yaml config file exists in the right format
  if (!is.null(config_file_chk)) {
    # Check that the yaml config file exists in the right format
    if (file.exists(config_file_chk) == F) {
      stop("Config file does not exist, check file name")
    }
    
    if (is.yaml.file(config_file_chk) == F) {
      stop(paste0("Config file is not a YAML config file. \n", 
                  "Check there are no duplicate variables listed"))
    }
  }
}