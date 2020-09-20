#### FUNCTIONS TO BRING IN A YAML FILE AND CHECK SOMETHING LOADED
# Alastair Matheson
# Created:        2020-09-20

yaml_import <- function(url = NULL) {
  
  config <- yaml::read_yaml(url)
  
  # Check things loaded properly
  ### Check for 404 errors
  if (config[[1]] == "Not Found") {
    stop("Error in config file URL")
  } else {
    return(config)
  }
}