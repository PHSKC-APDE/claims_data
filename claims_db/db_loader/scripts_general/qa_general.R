#### GENERAL FUNCTIONS FOR QA PROCESSES
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

#### FUNCTION FOR STANDARDIZED YAML ERROR CHECKS ####
qa_error_check_f <- function(config_chk = config,
                             config_url_chk = config_url,
                             config_file_chk = config_file) {
  
  #### BASIC ERROR CHECKS ####
  # Check if the config provided is a local object, file, or on a web page
  if (!is.null(config) & !is.null(config_url) & !is.null(config_file)) {
    stop("Specify either a local config object, config_url, or config_file but only one")
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