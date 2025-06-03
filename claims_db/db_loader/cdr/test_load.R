# Jeremy Whitehurst
# APDE, PHSKC
# 2025-5
# Try to load snippet of CDR data files to explore using Eli's chunk method

library(tidyverse) # Manipulate data
library(dplyr) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(keyring) # Access stored credentials
library(stringr) # Various string functions
library(R.utils)
library(utils)
library(zip)
library(xlsx)
library(tibble)
library(AzureStor)
library(AzureAuth)
library(svDialogs)
library(data.table)
library(fpeek)
library(readr)


##### Set up global parameters and call in libraries #####
options(max.print = 350, tibble.print_max = 50, scipen = 999)
origin <- "1970-01-01" # Date origin
origin_excel <- "1899-12-30" # Date origin for JHS Epic Excel extract files
Sys.setenv(TZ="America/Los_Angeles") # Set Time Zone
pacman::p_load(tidyverse, rads, glue, keyring, data.table, future, future.apply, vroom, progressr)
#Sys.setlocale("LC_ALL", "en_US.UTF-8")

## Set up file paths
read_path <- "//dphcifs/APDE-CDIP/Mcaid-Mcare/cdr_raw/20250421/txt"

## Set up custom functions

# Function to read column names
read_column_names <- function(col_names_file, row_sep = "~@~", field_sep = "|@|") {
  # Read entire column names file as one big string
  raw_text <- readChar(col_names_file, file.info(col_names_file)$size, useBytes = TRUE)
  
  # Split by row separator to get rows (usually only one row expected)
  rows <- unlist(strsplit(raw_text, row_sep, fixed = TRUE))
  rows <- rows[rows != ""]  # remove empty strings
  
  # Usually just one row with all col names, but if multiple, pick first or handle accordingly
  first_row <- rows[1]
  
  # Split fields by field separator
  col_names <- unlist(strsplit(first_row, field_sep, fixed = TRUE))
  
  col_names
}

# Function to read in large data files using vroom
# Slower than fread, but vroom can handle multiple-character delimiters
# Sanitizes long notes by replacing CR and LF with blanks
# Converts encoding to UTF-8 to avoid invalid locale issues
# Uses parallel processing to speed up loading data
read_custom_large_file_parallel_vroom <- function(file_path,
                                                  row_sep = "~@~",
                                                  field_sep = "|@|",
                                                  chunk_size = 100000,
                                                  workers = 4,
                                                  max_rows = NULL) {
  
  # Set up parallel plan
  plan(multisession, workers = workers)
  
  con <- file(file_path, open = "rb")  # binary mode to avoid locale issues
  on.exit(close(con), add = TRUE)
  
  result_list <- list()
  total_rows_read <- 0L
  chunk_num <- 0L
  chunk_batches <- list()
  buffer <- ""
  
  repeat {
    # Read raw chunk of bytes (~10 million bytes)
    raw_bytes <- readBin(con, what = "raw", n = 1e7)
    if (length(raw_bytes) == 0) break
    
    # Convert raw bytes to char vector safely
    raw_text <- rawToChar(raw_bytes, multiple = TRUE)
    raw_text <- paste(raw_text, collapse = "")
    raw_text <- iconv(raw_text, from = "UTF-8", to = "UTF-8", sub = " ")
    
    # Clean CR and LF (handles CR, LF, or CRLF)
    raw_text <- gsub("\r\n|\r|\n", " ", raw_text, perl = TRUE)
    
    # Append to buffer (handle partial chunks)
    buffer <- paste0(buffer, raw_text)
    
    # Split by row separator
    rows <- unlist(strsplit(buffer, row_sep, fixed = TRUE))
    
    # The last element may be partial row - save it for next iteration
    buffer <- rows[length(rows)]
    rows <- rows[-length(rows)]
    
    # Replace multi-char field delimiter with \x01 (ASCII SOH)
    rows <- gsub(field_sep, "\x01", rows, fixed = TRUE)
    
    # Add rows to chunk_batches
    chunk_batches <- c(chunk_batches, rows)
    
    while (length(chunk_batches) >= chunk_size) {
      chunk_num <- chunk_num + 1L
      current_chunk <- chunk_batches[1:chunk_size]
      chunk_batches <- chunk_batches[-(1:chunk_size)]
      
      # Yield the chunk for processing
      result_list[[chunk_num]] <- future({
        tmp_file <- tempfile(fileext = ".txt")
        current_chunk <- as.character(current_chunk)
        writeLines(current_chunk, tmp_file, useBytes = TRUE)
        
        df <- tryCatch({
          vroom::vroom(tmp_file,
                       delim = "\x01",
                       col_names = FALSE,
                       progress = FALSE,
                       quote = "",
                       escape_double = FALSE,
                       trim_ws = FALSE,
                       col_types = vroom::cols(.default = "c"))
        }, error = function(e) {
          warning("Error parsing chunk: ", e$message)
          NULL
        })
        
        unlink(tmp_file)
        df
      })
      
      total_rows_read <- total_rows_read + chunk_size
      if (!is.null(max_rows) && total_rows_read >= max_rows) {
        break
      }
    }
    
    if (!is.null(max_rows) && total_rows_read >= max_rows) {
      break
    }
  }
  
  # DEBUG: Print buffer and chunk_batches length before leftover processing
  cat("Buffer length (chars):", nchar(buffer), "\n")
  cat("Chunk_batches length before leftover processing:", length(chunk_batches), "\n")
  
  # Process any remaining partial rows in buffer
  if (!is.null(buffer) && is.character(buffer) && nzchar(buffer)) {
    last_rows <- unlist(strsplit(buffer, row_sep, fixed = TRUE))
    # Remove empty strings (just in case)
    last_rows <- last_rows[nzchar(last_rows)]
    cat("Last rows length from buffer split:", length(last_rows), "\n")
    if (length(last_rows) > 0) {
      last_rows <- gsub(field_sep, "\x01", last_rows, fixed = TRUE)
      chunk_batches <- c(chunk_batches, last_rows)
    }
  }
  
  # DEBUG: Print chunk_batches length after adding leftover last_rows
  cat("Chunk_batches length after adding last_rows:", length(chunk_batches), "\n")
  
  # Process leftover chunk_batches less than chunk_size
  if (length(chunk_batches) > 0 && (is.null(max_rows) || total_rows_read < max_rows)) {
    chunk_num <- chunk_num + 1L
    leftover_rows <- if (is.null(max_rows)) {
      chunk_batches
    } else {
      needed <- max_rows - total_rows_read
      chunk_batches[1:min(needed, length(chunk_batches))]
    }
    leftover_rows <- unlist(leftover_rows)  # <-- force character vector
    
    # DEBUG: Check leftover_rows type and length before writing
    cat("Leftover rows length before writing:", length(leftover_rows), "\n")
    cat("Leftover rows is.character:", is.character(leftover_rows), "\n")
    
    if (length(leftover_rows) > 0 && is.character(leftover_rows)) {
      result_list[[chunk_num]] <- future({
        tmp_file <- tempfile(fileext = ".txt")
        writeLines(leftover_rows, tmp_file, useBytes = TRUE)
        
        df <- tryCatch({
          vroom::vroom(tmp_file,
                       delim = "\x01",
                       col_names = FALSE,
                       progress = FALSE,
                       quote = "",
                       escape_double = FALSE,
                       trim_ws = FALSE,
                       col_types = vroom::cols(.default = "c"))
        }, error = function(e) {
          warning("Error parsing leftover chunk: ", e$message)
          NULL
        })
        
        unlink(tmp_file)
        df
      })
    } else {
      warning("Skipping empty or non-character leftover chunk")
    }
  }
  
  # Collect futures results
  result_dfs <- lapply(result_list, future::value)
  
  # Combine all data frames into one data.table
  combined <- data.table::rbindlist(result_dfs, use.names = TRUE, fill = TRUE)
  
  # Reset future plan to sequential (optional)
  plan(sequential)
  
  combined
}

#Function to summarize data frame columns
summarize_column_lengths <- function(df, df_name = deparse(substitute(df))) {
  results <- data.frame(
    dataframe = character(),
    column = character(),
    class = character(),
    min_length = integer(),
    max_length = integer(),
    stringsAsFactors = FALSE
  )
  
  for (col_name in names(df)) {
    col_data <- df[[col_name]]
    col_class <- class(col_data)[1]
    
    if (is.character(col_data) || is.factor(col_data)) {
      char_data <- as.character(col_data)
      lengths <- nchar(char_data)
      min_len <- min(lengths, na.rm = TRUE)
      max_len <- max(lengths, na.rm = TRUE)
    } else {
      min_len <- NA
      max_len <- NA
    }
    
    results <- rbind(results, data.frame(
      dataframe = df_name,
      column = col_name,
      class = col_class,
      min_length = min_len,
      max_length = max_len,
      stringsAsFactors = FALSE
    ))
  }
  
  return(results)
}


#### Load REF_RaceEthnicityCode data file ####

race_codes <- read_custom_large_file_parallel_vroom(
  file_path = file.path(read_path, "dbo.REF_RaceEthnicityCode.Data.txt"),
  row_sep = "~@~",
  field_sep = "|@|",
  chunk_size = 10000,
  workers = 4,
  max_rows = Inf
)

race_colnames <- read_column_names(
  file.path(read_path, "dbo.REF_RaceEthnicityCode.HeaderOnly.txt"),
  row_sep = "~@~",
  field_sep = "|@|")

setnames(race_codes, race_colnames)
rm(race_colnames)

race_codes_summary <- summarize_column_lengths(race_codes)
rm(race_codes)


#### Load CCD_Header data file ####

system.time(header <- read_custom_large_file_parallel_vroom(
  file.path(read_path, "dbo.CCD_Header.Data.txt"),
  row_sep = "~@~",
  field_sep = "|@|",
  workers = 4,
  max_rows = 1000000
))

header_colnames <- read_column_names(
  file.path(read_path, "dbo.CCD_Header.HeaderOnly.txt"),
  row_sep = "~@~",
  field_sep = "|@|")

setnames(header, header_colnames)
rm(header_colnames)

header_summary <- summarize_column_lengths(header)
rm(header)


#### Load Allergies data file ####

system.time(allergies <- read_custom_large_file_parallel_vroom(
  file.path(read_path, "dbo.CHR_Allergies.Data.txt"),
  row_sep = "~@~",
  field_sep = "|@|",
  workers = 4,
  max_rows = 1000000
))

allergies_colnames <- read_column_names(
  file.path(read_path, "dbo.CHR_Allergies.HeaderOnly.txt"),
  row_sep = "~@~",
  field_sep = "|@|")

setnames(allergies, allergies_colnames)
rm(allergies_colnames)

allergies_summary <- summarize_column_lengths(allergies)
rm(allergies)


#### Load Labs data file ####
#Run time: ~2.3 min to load 1 million rows with fread (requires reading raw text to replace delimiters)
#Run time: ~1.4 min to load 1 million rows with vroom (no raw text reading required)

system.time(labs <- read_custom_large_file_parallel_vroom(
  file.path(read_path, "dbo.CHR_Labs.Data.txt"),
  row_sep = "~@~",
  field_sep = "|@|",
  workers = 4,
  max_rows = 100000000
))

labs_colnames <- read_column_names(
  file.path(read_path, "dbo.CHR_Labs.HeaderOnly.txt"),
  row_sep = "~@~",
  field_sep = "|@|")
labs_rows <- nrow(labs)
setnames(labs, labs_colnames)
rm(labs_colnames)
nrow(labs)
labs_summary <- summarize_column_lengths(labs)


rm(labs)


#### Load MedicationAndImmunizations data file ####

system.time(meds <- read_custom_large_file_parallel_vroom(
  file.path(read_path, "dbo.CHR_MedicationAndImmunizations.Data.txt"),
  row_sep = "~@~",
  field_sep = "|@|",
  workers = 4,
  max_rows = 1000000
))

meds_colnames <- read_column_names(
  file.path(read_path, "dbo.CHR_MedicationAndImmunizations.HeaderOnly.txt"),
  row_sep = "~@~",
  field_sep = "|@|")

setnames(meds, meds_colnames)
rm(meds_colnames)

count(meds, `CCD Available/Deprecated Flag`) # if rows were correctly delimited, there should only be 2 values (A or D)
meds_summary <- summarize_column_lengths(meds)
rm(meds)


#### Load Problems data file ####

system.time(problems <- read_custom_large_file_parallel_vroom(
  file.path(read_path, "dbo.CHR_Problems.Data.txt"),
  row_sep = "~@~",
  field_sep = "|@|",
  workers = 4,
  max_rows = 100000000
))
problems_rows <- nrow(problems)

problems_colnames <- read_column_names(
  file.path(read_path, "dbo.CHR_Problems.HeaderOnly.txt"),
  row_sep = "~@~",
  field_sep = "|@|")

setnames(problems, problems_colnames)
rm(problems_colnames)

count(problems, `CCD Available/Deprecated Flag`) # if rows were correctly delimited, there should only be 2 values (A or D)
problems_summary <- summarize_column_lengths(problems)


rm(problems)


#### Load Procedures data file ####

system.time(procedures <- read_custom_large_file_parallel_vroom(
  file.path(read_path, "dbo.CHR_Procedures.Data.txt"),
  row_sep = "~@~",
  field_sep = "|@|",
  workers = 4,
  max_rows = 100000000
))
procedures_rows <- nrow(procedures)

procedures_colnames <- read_column_names(
  file.path(read_path, "dbo.CHR_Procedures.HeaderOnly.txt"),
  row_sep = "~@~",
  field_sep = "|@|")

setnames(procedures, procedures_colnames)
rm(procedures_colnames)

count(procedures, `Available or Deprecated Flag`) # if rows were correctly delimited, there should only be 2 values (A or D)
procedures_summary <- summarize_column_lengths(procedures)
rm(procedures)


#### Load VitalSigns data file ####

system.time(vitals <- read_custom_large_file_parallel_vroom(
  file.path(read_path, "dbo.CHR_VitalSigns.Data.txt"),
  row_sep = "~@~",
  field_sep = "|@|",
  workers = 4,
  max_rows = 100000000
))
vitals_rows <- nrow(vitals)

write.table(vitals, paste0(dir_gz, "CHR_VitalSigns_20250421.txt"),
            sep = "\t", na = "", row.names = F, col.names = F)
gzip(paste0(dir_gz, "CHR_VitalSigns_20250421.txt"))

vitals_colnames <- read_column_names(
  file.path(read_path, "dbo.CHR_VitalSigns.HeaderOnly.txt"),
  row_sep = "~@~",
  field_sep = "|@|")

setnames(vitals, vitals_colnames)
rm(vitals_colnames)

count(vitals, `CCD Available/Deprecated Flag`) # if rows were correctly delimited, there should only be 2 values (A or D)
vitals_summary <- summarize_column_lengths(vitals)
rm(vitals)


#### Load IndexPatient data file ####

system.time(index_patient <- read_custom_large_file_parallel_vroom(
  file.path(read_path, "dbo.MPM_IndexPatient.Data.txt"),
  row_sep = "~@~",
  field_sep = "|@|",
  workers = 4,
  max_rows = 1000000
))

index_patient_colnames <- read_column_names(
  file.path(read_path, "dbo.MPM_IndexPatient.HeaderOnly.txt"),
  row_sep = "~@~",
  field_sep = "|@|")

setnames(index_patient, index_patient_colnames)
rm(index_patient_colnames)

count(index_patient, State) # if rows were correctly delimited, there should be only logical values
index_patient_summary <- summarize_column_lengths(index_patient)
rm(index_patient)


#### Load Person data file ####

system.time(person <- read_custom_large_file_parallel_vroom(
  file.path(read_path, "dbo.MPM_Person.Data.txt"),
  row_sep = "~@~",
  field_sep = "|@|",
  workers = 4,
  max_rows = 1000000
))

person_colnames <- read_column_names(
  file.path(read_path, "dbo.MPM_Person.HeaderOnly.txt"),
  row_sep = "~@~",
  field_sep = "|@|")

setnames(person, person_colnames)
rm(person_colnames)

count(person, State) # if rows were correctly delimited, there should be only logical values
person_summary <- summarize_column_lengths(person)
rm(person)


#### Bind all summary metadata ####
summary_full <- bind_rows(
  allergies_summary,
  header_summary,
  labs_summary,
  meds_summary,
  problems_summary,
  procedures_summary,
  vitals_summary,
  index_patient_summary,
  person_summary,
  race_codes_summary
)
clipr::write_clip(summary_full)






test_f <- function(x) {
  for(i in 1:x) {
    if(i%%1000 == 0) { message(i) }
  }
}
system.time(test_f(100000000))
