
# Eli Kern and Alastair Matheson
# 2018-2-5

# Code to create a SQL table dbo.mcaid_elig_demoever which holds SSN, DOB, gender, race, and language
# One row per ID, one SSN and one DOB per ID (frequency-based selection)
# Gender, race, and language are alone or in combination EVER variables
# Data elements: ID, BLANK

## 2018-05-22 updates:
# Add in multiple gender and multiple race variables
# Add in unknown gender, race, and language variables

## 2018-07-17 updates:
# Converted most code to use data.table package due to large size of data
# Removed vestigal code and other tidying

## 2019-05-10 updates:
# Using new standarized varnames
# No longer capturing SSN (will be in alias table instead)

## 2019-06-20 updates:
# Adapted to be called in from the master_mcaid scripts 
#  (i.e., packages and DB settings are assumed to already be loaded)
# Added print statements to track progress

## 2019-08-29 updates:
# Changed print statements to message ones
# Fixed SQL load so variable types work and can overwrite rather than append

## 2020-09 updates:
# Changed to run in either HHSAW or PHClaims DBs
# Converted to function


### Function elements
# conn = database connection
# server = whether we are working in HHSAW or PHClaims
# config = the YAML config file. Can be either an object already loaded into 
#   R or a URL that should be used
# get_config = if a URL is supplied, set this to T so the YAML file is loaded

load_stage_mcaid_elig_demo_f <- function(conn = NULL,
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
  
  from_schema <- config[[server]][["from_schema"]]
  from_table <- config[[server]][["from_table"]]
  to_schema <- config[[server]][["to_schema"]]
  to_table <- config[[server]][["to_table"]]
  

  message("Clear out temp tables")
  time_start <- Sys.time()
  try(DBI::dbExecute(conn,
"IF OBJECT_ID(N'tempdb..#elig_dob') IS NOT NULL DROP TABLE #elig_dob;
IF OBJECT_ID(N'tempdb..#elig_demoever') IS NOT NULL DROP TABLE #elig_demoever;
IF OBJECT_ID(N'tempdb..#elig_gender') IS NOT NULL DROP TABLE #elig_gender;
IF OBJECT_ID(N'tempdb..#elig_gender_t') IS NOT NULL DROP TABLE #elig_gender_t;
IF OBJECT_ID(N'tempdb..#elig_gender_recent') IS NOT NULL DROP TABLE #elig_gender_recent;
IF OBJECT_ID(N'tempdb..#elig_gender_sum') IS NOT NULL DROP TABLE #elig_gender_sum;
IF OBJECT_ID(N'tempdb..#elig_gender_final') IS NOT NULL DROP TABLE #elig_gender_final;
IF OBJECT_ID(N'tempdb..#elig_race') IS NOT NULL DROP TABLE #elig_race;
IF OBJECT_ID(N'tempdb..#elig_race_t') IS NOT NULL DROP TABLE #elig_race_t;
IF OBJECT_ID(N'tempdb..#elig_race_recent') IS NOT NULL DROP TABLE #elig_race_recent;
IF OBJECT_ID(N'tempdb..#elig_race_sum') IS NOT NULL DROP TABLE #elig_race_sum;
IF OBJECT_ID(N'tempdb..#elig_race_final') IS NOT NULL DROP TABLE #elig_race_final;
IF OBJECT_ID(N'tempdb..#elig_lang') IS NOT NULL DROP TABLE #elig_lang;
IF OBJECT_ID(N'tempdb..#elig_lang_t') IS NOT NULL DROP TABLE #elig_lang_t;
IF OBJECT_ID(N'tempdb..#elig_lang_sum') IS NOT NULL DROP TABLE #elig_lang_sum;
IF OBJECT_ID(N'tempdb..#elig_lang_all') IS NOT NULL DROP TABLE #elig_lang_all;
IF OBJECT_ID(N'tempdb..#elig_lang_cnt') IS NOT NULL DROP TABLE #elig_lang_cnt;
IF OBJECT_ID(N'tempdb..#elig_lang_cnt_max') IS NOT NULL DROP TABLE #elig_lang_cnt_max;
IF OBJECT_ID(N'tempdb..#elig_lang_max') IS NOT NULL DROP TABLE #elig_lang_max;
IF OBJECT_ID(N'tempdb..#elig_lang_final') IS NOT NULL DROP TABLE #elig_lang_final;"), silent = T)
  
  
  #### BRING IN MEDICAID ELIG DATA FOR DOB PROCESSING ####
  message("Bringing in DOB data")
  # select most frequently reported SSN and DOB per Medicaid ID
  DBI::dbExecute(conn, glue::glue_sql("
SELECT id.id_mcaid, dob.dob
INTO #elig_dob
FROM (
  SELECT DISTINCT MBR_H_SID AS 'id_mcaid'
  FROM {`from_schema`}.{`from_table`}
) id
LEFT JOIN (
  SELECT b.id_mcaid, cast(b.dob AS date) AS 'dob'
  FROM (
    SELECT a.id_mcaid, a.dob, row_number() OVER 
    (PARTITION BY a.id_mcaid order by a.id_mcaid, a.dob_cnt desc, a.dob) AS 'dob_rank'
    FROM (
      SELECT z.id_mcaid, z.dob, count(z.dob) AS 'dob_cnt'
      FROM (
        SELECT DISTINCT CLNDR_YEAR_MNTH, MBR_H_SID AS 'id_mcaid', BIRTH_DATE AS 'dob'
        FROM {`from_schema`}.{`from_table`}
        WHERE BIRTH_DATE is not null
        GROUP BY CLNDR_YEAR_MNTH, MBR_H_SID, BIRTH_DATE
      ) z
      GROUP BY z.id_mcaid, z.dob
    ) a
  ) b
  WHERE b.dob_rank = 1
) dob
ON id.id_mcaid = dob.id_mcaid;
                                      ", .con = conn))
  
  
  #### BRING IN MEDICAID ELIG DATA FOR GENDER, RACE, AND LANGUAGE PROCESSING ####
  ### Bring in Medicaid eligibility data
  message("Bringing in gender, race, and langauge data")
  DBI::dbExecute(conn, glue::glue_sql("
SELECT DISTINCT 
CLNDR_YEAR_MNTH AS calmo, 
MBR_H_SID AS id_mcaid, 
GENDER AS gender, 
CASE WHEN RACE1_NAME = 'NOT PROVIDED' OR RACE1_NAME = 'OTHER' THEN NULL ELSE UPPER(RACE1_NAME) END AS race1, 
CASE WHEN RACE2_NAME = 'NOT PROVIDED' OR RACE2_NAME = 'OTHER' THEN NULL ELSE UPPER(RACE2_NAME) END AS race2, 
CASE WHEN RACE3_NAME = 'NOT PROVIDED' OR RACE3_NAME = 'OTHER' THEN NULL ELSE UPPER(RACE3_NAME) END AS race3, 
CASE WHEN RACE4_NAME = 'NOT PROVIDED' OR RACE4_NAME = 'OTHER' THEN NULL ELSE UPPER(RACE4_NAME) END AS race4, 
CONCAT(CASE WHEN RACE1_NAME = 'NOT PROVIDED' OR RACE1_NAME = 'OTHER' THEN NULL ELSE UPPER(RACE1_NAME) END, 
	CASE WHEN RACE2_NAME = 'NOT PROVIDED' OR RACE2_NAME = 'OTHER' THEN NULL ELSE UPPER(RACE2_NAME) END, 
	CASE WHEN RACE3_NAME = 'NOT PROVIDED' OR RACE3_NAME = 'OTHER' THEN NULL ELSE UPPER(RACE3_NAME) END, 
	CASE WHEN RACE4_NAME = 'NOT PROVIDED' OR RACE4_NAME = 'OTHER' THEN NULL ELSE UPPER(RACE4_NAME) END) AS race_all,
CASE WHEN HISPANIC_ORIGIN_NAME = 'NOT PROVIDED' OR HISPANIC_ORIGIN_NAME = 'OTHER' THEN NULL ELSE UPPER(HISPANIC_ORIGIN_NAME) END AS hispanic, 
CASE WHEN SPOKEN_LNG_NAME = 'UNDETERMINED' OR SPOKEN_LNG_NAME = 'OTHER LANGUAGE' THEN NULL ELSE UPPER(SPOKEN_LNG_NAME) END AS 'slang', 
CASE WHEN WRTN_LNG_NAME = 'UNDETERMINED' OR WRTN_LNG_NAME = 'OTHER LANGUAGE' THEN NULL ELSE UPPER(WRTN_LNG_NAME) END AS 'wlang',
CONCAT(CASE WHEN SPOKEN_LNG_NAME = 'UNDETERMINED' OR SPOKEN_LNG_NAME = 'OTHER LANGUAGE' THEN NULL ELSE UPPER(SPOKEN_LNG_NAME) END,
	CASE WHEN WRTN_LNG_NAME = 'UNDETERMINED' OR WRTN_LNG_NAME = 'OTHER LANGUAGE' THEN NULL ELSE UPPER(WRTN_LNG_NAME) END) AS lang_all
INTO #elig_demoever
FROM {`from_schema`}.{`from_table`};                                      
                                      ", .con = conn))
  
  #### PROCESS GENDER DATA  ####
  message("Processing gender data")
  
  ### Create alone or in combination gender variables
  DBI::dbExecute(conn, glue::glue_sql("
SELECT id_mcaid, calmo, gender, 
CASE WHEN gender = 'FEMALE' THEN 1 ELSE 0 END AS gender_female,
CASE WHEN gender = 'MALE' THEN 1 ELSE 0 END AS gender_male,
CASE WHEN gender IS NULL THEN 1 ELSE 0 END AS gender_na
INTO #elig_gender
FROM #elig_demoever;                                      
                                      ", .con = conn))

  ### For each gender variable, count number of rows where variable = 1.
  ### Divide this number by total number of rows (months) where gender is non-missing.
  ### Create _t variables for each gender variable to hold this percentage.
  DBI::dbExecute(conn, glue::glue_sql("
SELECT id_mcaid,
ROUND(CAST(SUM(gender_female) AS FLOAT)/CAST(COUNT(gender_female) AS FLOAT) * 100, 1) AS gender_female_t,
ROUND(CAST(SUM(gender_male) AS FLOAT)/CAST(COUNT(gender_male) AS FLOAT) * 100, 1) AS gender_male_t
INTO #elig_gender_t
FROM #elig_gender
GROUP BY id_mcaid;                                      
                                      ", .con = conn))

  ### Find the most recent gender variable
  DBI::dbExecute(conn, glue::glue_sql("
SELECT a.id_mcaid, 
CASE WHEN a.gender_female = 1 AND a.gender_male = 1 THEN 'Multiple' 
	WHEN a.gender_female = 1 THEN 'Female' 
	WHEN a.gender_male = 1 THEN 'Male'
	ELSE 'Unknown' END AS gender_recent
INTO #elig_gender_recent
FROM #elig_gender a
INNER JOIN (SELECT id_mcaid, MAX(calmo) AS calmo FROM #elig_gender GROUP BY id_mcaid) b ON a.id_mcaid = b.id_mcaid AND a.calmo = b.calmo;                                      
                                      ", .con = conn))
  
 
  ### Copy all non-missing gender variable values to all rows within each ID
  # First make collapsed max of genders for each ID
  #Replace infinity values with NA (generated by max function applied to NA rows)

  DBI::dbExecute(conn, glue::glue_sql("
SELECT id_mcaid, 
MAX(gender_female) AS gender_female,
MAX(gender_male) AS gender_male
INTO #elig_gender_sum
FROM #elig_gender
GROUP BY id_mcaid;                                      
                                      ", .con = conn))
  
  # Now join back to main data and overwrite existing female/male vars
  
  
  ### Collapse to one row per ID given we have alone or in combo EVER gender variables
  # First remove unwanted variables
  #Add in variables for multiple gender (mutually exclusive categories) and missing gender
  DBI::dbExecute(conn, glue::glue_sql("
SELECT a.id_mcaid,
CASE WHEN b.gender_female_t > 0 AND b.gender_male_t > 0 THEN 'Multiple'
	WHEN a.gender_female = 1 THEN 'Female'
	WHEN a.gender_male = 1 THEN 'Male'
	ELSE 'Unknown' END AS gender_me,
c.gender_recent,
a.gender_female,
a.gender_male,
b.gender_female_t,
b.gender_male_t
INTO #elig_gender_final
FROM #elig_gender_sum a
INNER JOIN #elig_gender_t b ON a.id_mcaid = b.id_mcaid
INNER JOIN #elig_gender_recent c ON a.id_mcaid = c.id_mcaid                                      
                                      ", .con = conn))
  try(DBI::dbExecute(conn, glue::glue_sql("
DROP TABLE #elig_gender;
DROP TABLE #elig_gender_t;
DROP TABLE #elig_gender_recent;
DROP TABLE #elig_gender_sum;                                      
                                      ", .con = conn)), silent = T)
  
  #### PROCESS RACE DATA ####
  message("Processing race/ethnicity data")
  ### Create alone or in combination race variables
  # Same race can be listed more than once across race variables, replace sums > 1 with 1
  # Replace race vars with NA if all race vars are NA, (latino already NA if hispanic is NA)
  DBI::dbExecute(conn, glue::glue_sql("
SELECT id_mcaid, 
calmo, 
CASE WHEN race_all LIKE '%ALASKAN_NATIVE%' OR race_all LIKE '%AMERICAN INDIAN%' THEN 1 ELSE 0 END AS race_aian, 
CASE WHEN race_all LIKE '%BLACK%' THEN 1 ELSE 0 END AS race_black, 
CASE WHEN race_all LIKE '%ASIAN%' THEN 1 ELSE 0 END AS race_asian, 
CASE WHEN race_all LIKE '%HAWAIIAN%' OR race_all LIKE '%PACIFIC ISLANDER%' THEN 1 ELSE 0 END AS race_nhpi, 
CASE WHEN race_all LIKE '%WHITE%' THEN 1 ELSE 0 END AS race_white, 
CASE WHEN race_all LIKE '%HISPANIC%' THEN 1 ELSE 0 END AS race_latino, 
CASE WHEN race_all LIKE '%ALASKAN_NATIVE%' OR race_all LIKE '%AMERICAN INDIAN%' 
	OR race_all LIKE '%BLACK%' OR race_all LIKE '%ASIAN%' 
	OR race_all LIKE '%HAWAIIAN%' OR race_all LIKE '%PACIFIC ISLANDER%'  
	OR race_all LIKE '%WHITE%' OR race_all LIKE '%HISPANIC%' THEN 0 ELSE 1 END AS race_na
INTO #elig_race
FROM #elig_demoever                                      
                                      ", .con = conn))
  
  
  ### For each race variable, count number of rows where variable = 1.
  # Divide this number by total number of rows (months) where at least one race variable is non-missing.
  # Create _t variables for each race variable to hold this percentage.
  
  # Create a variable to flag if all race vars are NA and Latino also 0 or NA
  # Can just check aian since this is only NA if all race fields are NA
  # Create another var to count number of NA rows per ID
  # (saves having to calculate it each time below)
  # Create race person time vars
  DBI::dbExecute(conn, glue::glue_sql("
SELECT id_mcaid,
ROUND(CAST(SUM(race_aian) AS FLOAT)/CAST(COUNT(race_aian) AS FLOAT) * 100, 1) AS race_aian_t,
ROUND(CAST(SUM(race_black) AS FLOAT)/CAST(COUNT(race_black) AS FLOAT) * 100, 1) AS race_black_t,
ROUND(CAST(SUM(race_asian) AS FLOAT)/CAST(COUNT(race_asian) AS FLOAT) * 100, 1) AS race_asian_t,
ROUND(CAST(SUM(race_nhpi) AS FLOAT)/CAST(COUNT(race_nhpi) AS FLOAT) * 100, 1) AS race_nhpi_t,
ROUND(CAST(SUM(race_white) AS FLOAT)/CAST(COUNT(race_white) AS FLOAT) * 100, 1) AS race_white_t,
ROUND(CAST(SUM(race_latino) AS FLOAT)/CAST(COUNT(race_latino) AS FLOAT) * 100, 1) AS race_latino_t
INTO #elig_race_t
FROM #elig_race
GROUP BY id_mcaid;                                      
                                      ", .con = conn))
  
  
  ### Find most recent race
  DBI::dbExecute(conn, glue::glue_sql("
SELECT a.id_mcaid, 
CASE WHEN a.race_aian + a.race_black + a.race_asian + a.race_nhpi + a.race_white > 1 THEN 'Multiple' 
	WHEN a.race_aian = 1 THEN 'AI/AN' 
	WHEN a.race_asian = 1 THEN 'Asian'
	WHEN a.race_black = 1 THEN 'Black'
	WHEN a.race_nhpi = 1 THEN 'NH/PI'
	WHEN a.race_white = 1 THEN 'White'
	ELSE 'Unknown' END AS race_recent, 
CASE WHEN a.race_aian + a.race_black + a.race_asian + a.race_nhpi + a.race_white + a.race_latino > 1 THEN 'Multiple' 
	WHEN a.race_aian = 1 THEN 'AI/AN' 
	WHEN a.race_asian = 1 THEN 'Asian'
	WHEN a.race_black = 1 THEN 'Black'
	WHEN a.race_nhpi = 1 THEN 'NH/PI'
	WHEN a.race_white = 1 THEN 'White'
	WHEN a.race_latino = 1 THEN 'Latino'
	ELSE 'Unknown' END AS race_eth_recent 
INTO #elig_race_recent
FROM #elig_race a
INNER JOIN (SELECT id_mcaid, MAX(calmo) AS calmo FROM #elig_race GROUP BY id_mcaid) b ON a.id_mcaid = b.id_mcaid AND a.calmo = b.calmo;                                      
                                      ", .con = conn))
  
  
  ### Copy all non-missing race variable values to all rows within each ID.
  # First make collapsed max of race for each ID
  #Replace infinity values with NA (generated by max function applied to NA rows)
  ### Collapse to one row per ID given we have alone or in combo EVER race variables
  # First remove unwanted variables
  # Add in variables for multiple race (mutually exclusive categories) and missing race
  DBI::dbExecute(conn, glue::glue_sql("
SELECT id_mcaid, 
MAX(race_aian) AS race_aian,
MAX(race_asian) AS race_asian,
MAX(race_black) AS race_black,
MAX(race_nhpi) AS race_nhpi,
MAX(race_white) AS race_white,
MAX(race_latino) AS race_latino
INTO #elig_race_sum
FROM #elig_race
GROUP BY id_mcaid;                                      
                                      ", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
SELECT a.id_mcaid,
CASE WHEN a.race_aian + a.race_black + a.race_asian + a.race_nhpi + a.race_white > 1 THEN 'Multiple' 
	WHEN a.race_aian = 1 THEN 'AI/AN' 
	WHEN a.race_asian = 1 THEN 'Asian'
	WHEN a.race_black = 1 THEN 'Black'
	WHEN a.race_nhpi = 1 THEN 'NH/PI'
	WHEN a.race_white = 1 THEN 'White'
	ELSE 'Unknown' END AS race_me, 
CASE WHEN a.race_aian + a.race_black + a.race_asian + a.race_nhpi + a.race_white + a.race_latino > 1 THEN 'Multiple' 
	WHEN a.race_aian = 1 THEN 'AI/AN' 
	WHEN a.race_asian = 1 THEN 'Asian'
	WHEN a.race_black = 1 THEN 'Black'
	WHEN a.race_nhpi = 1 THEN 'NH/PI'
	WHEN a.race_white = 1 THEN 'White'
	WHEN a.race_latino = 1 THEN 'Latino'
	ELSE 'Unknown' END AS race_eth_me,
c.race_recent,
c.race_eth_recent,
a.race_aian,
a.race_asian,
a.race_black,
a.race_latino,
a.race_nhpi,
a.race_white,
CASE WHEN a.race_aian + a.race_black + a.race_asian + a.race_nhpi + a.race_white = 0 THEN 1 ELSE 0 END AS race_unk,
CASE WHEN a.race_aian + a.race_black + a.race_asian + a.race_nhpi + a.race_white + a.race_latino = 0 THEN 1 ELSE 0 END AS race_eth_unk,
b.race_aian_t,
b.race_asian_t,
b.race_black_t,
b.race_latino_t,
b.race_nhpi_t,
b.race_white_t
INTO #elig_race_final
FROM #elig_race_sum a
INNER JOIN #elig_race_t b ON a.id_mcaid = b.id_mcaid
INNER JOIN #elig_race_recent c ON a.id_mcaid = c.id_mcaid;                                      
                                      ", .con = conn))
  
  try(DBI::dbExecute(conn, glue::glue_sql("
DROP TABLE #elig_race;
DROP TABLE #elig_race_t;
DROP TABLE #elig_race_recent;
DROP TABLE #elig_race_sum;                                      
                                      ", .con = conn)), silent = T)
  
  #### PROCESS LANGUAGE DATA ####
  message("Processing language data")
  
  ### Create alone or in combination lang variables for King County tier 1 and 2 
  # translation languages with Arabic in place of Punjabi
  # Same langs can be listed more than once across written/spoken, replace sums > 1 with 1
  ##Replace lang vars with NA if slang and wlang are both NA
  DBI::dbExecute(conn, glue::glue_sql("
SELECT id_mcaid, 
calmo, 
CASE WHEN lang_all LIKE '%ENGLISH%' THEN 1 ELSE 0 END AS lang_english, 
CASE WHEN lang_all LIKE '%SPANISH%' OR lang_all LIKE '%CASTILIAN%' THEN 1 ELSE 0 END AS lang_spanish, 
CASE WHEN lang_all LIKE '%VIETNAMESE%' THEN 1 ELSE 0 END AS lang_vietnamese, 
CASE WHEN lang_all LIKE '%CHINESE%' OR lang_all LIKE '%HAKKA%' 
	OR lang_all LIKE '%MANDARIN%' OR lang_all LIKE '%CANTONESE%' THEN 1 ELSE 0 END AS lang_chinese, 
CASE WHEN lang_all LIKE '%SOMALI%' THEN 1 ELSE 0 END AS lang_somali, 
CASE WHEN lang_all LIKE '%RUSSIAN%' THEN 1 ELSE 0 END AS lang_russian, 
CASE WHEN lang_all LIKE '%ARABIC%' THEN 1 ELSE 0 END AS lang_arabic, 
CASE WHEN lang_all LIKE '%KOREAN%' THEN 1 ELSE 0 END AS lang_korean, 
CASE WHEN lang_all LIKE '%UKRAINIAN%' THEN 1 ELSE 0 END AS lang_ukrainian, 
CASE WHEN lang_all LIKE '%AMHARIC%' THEN 1 ELSE 0 END AS lang_amharic, 
CASE WHEN lang_all LIKE '%ENGLISH%' OR lang_all LIKE '%SPANISH%' 
	OR lang_all LIKE '%CASTILIAN%' OR lang_all LIKE '%VIETNAMESE%' 
	OR lang_all LIKE '%CHINESE%' OR lang_all LIKE '%HAKKA%' 
	OR lang_all LIKE '%MANDARIN%' OR lang_all LIKE '%CANTONESE%' 
	OR lang_all LIKE '%SOMALI%' OR lang_all LIKE '%RUSSIAN%' 
	OR lang_all LIKE '%ARABIC%' OR lang_all LIKE '%KOREAN%' 
	OR lang_all LIKE '%UKRAINIAN%' OR lang_all LIKE '%AMHARIC%' THEN 0 ELSE 1 END AS lang_na
INTO #elig_lang
FROM #elig_demoever;                                      
                                      ", .con = conn)) 
  
  
  ### For each language variable, count number of rows where variable = 1.
  # Divide this number by total number of rows (months) where at least one language variable is non-missing.
  # Create _t variables for each lang variable to hold this percentage.
  
  #Create a variable to flag if all lang vars are NA
  # Create another var to count number of NA rows per ID
  # (saves having to calculate it each time below)
  #Create lang person time vars
  DBI::dbExecute(conn, glue::glue_sql("
SELECT id_mcaid,
ROUND(CAST(SUM(lang_english) AS FLOAT)/CAST(COUNT(lang_english) AS FLOAT) * 100, 1) AS lang_english_t,
ROUND(CAST(SUM(lang_spanish) AS FLOAT)/CAST(COUNT(lang_spanish) AS FLOAT) * 100, 1) AS lang_spanish_t,
ROUND(CAST(SUM(lang_vietnamese) AS FLOAT)/CAST(COUNT(lang_vietnamese) AS FLOAT) * 100, 1) AS lang_vietnamese_t,
ROUND(CAST(SUM(lang_chinese) AS FLOAT)/CAST(COUNT(lang_chinese) AS FLOAT) * 100, 1) AS lang_chinese_t,
ROUND(CAST(SUM(lang_somali) AS FLOAT)/CAST(COUNT(lang_somali) AS FLOAT) * 100, 1) AS lang_somali_t,
ROUND(CAST(SUM(lang_russian) AS FLOAT)/CAST(COUNT(lang_russian) AS FLOAT) * 100, 1) AS lang_russian_t,
ROUND(CAST(SUM(lang_arabic) AS FLOAT)/CAST(COUNT(lang_arabic) AS FLOAT) * 100, 1) AS lang_arabic_t,
ROUND(CAST(SUM(lang_korean) AS FLOAT)/CAST(COUNT(lang_korean) AS FLOAT) * 100, 1) AS lang_korean_t,
ROUND(CAST(SUM(lang_ukrainian) AS FLOAT)/CAST(COUNT(lang_ukrainian) AS FLOAT) * 100, 1) AS lang_ukrainian_t,
ROUND(CAST(SUM(lang_amharic) AS FLOAT)/CAST(COUNT(lang_amharic) AS FLOAT) * 100, 1) AS lang_amharic_t
INTO #elig_lang_t
FROM #elig_lang
GROUP BY id_mcaid;                                      
                                      ", .con = conn))
  
  ### Copy all non-missing language variable values to all rows within each ID
  # First make collapsed max of lang for each ID
  #Replace infinity values with NA (generated by max function applied to NA rows)
  DBI::dbExecute(conn, glue::glue_sql("
SELECT id_mcaid, 
MAX(lang_english) AS lang_english,
MAX(lang_spanish) AS lang_spanish,
MAX(lang_vietnamese) AS lang_vietnamese,
MAX(lang_chinese) AS lang_chinese,
MAX(lang_somali) AS lang_somali,
MAX(lang_russian) AS lang_russian, 
MAX(lang_arabic) AS lang_arabic, 
MAX(lang_korean) AS lang_korean, 
MAX(lang_ukrainian) AS lang_ukrainian, 
MAX(lang_amharic) AS lang_amharic 
INTO #elig_lang_sum
FROM #elig_lang
GROUP BY id_mcaid;                                      
                                      ", .con = conn))
  
  
  ### Select most frequently reported language per ID
  # Count spoken language rows by ID and language
  #Count written language rows by ID and language
  DBI::dbExecute(conn, glue::glue_sql("
SELECT * INTO #elig_lang_all FROM (
SELECT id_mcaid, ISNULL(slang, 'Unknown') AS lang FROM #elig_demoever
	UNION ALL
SELECT id_mcaid, ISNULL(wlang, 'Unknown') FROM #elig_demoever) a;                                      
                                      ", .con = conn))
  
  DBI::dbExecute(conn, glue::glue_sql("
SELECT id_mcaid,
lang,
COUNT(*) AS lang_cnt,
NEWID() AS id
INTO #elig_lang_cnt
FROM #elig_lang_all
GROUP BY id_mcaid, lang;                                      
                                      ", .con = conn))

  #Join written and spoken language counts and sum by ID and language
  #Assign random number to each ID and language, and sort by ID and random number (this helps with selecting lang_max when tied)
  DBI::dbExecute(conn, glue::glue_sql("
SELECT a.id_mcaid,
a.lang,
a.id
INTO #elig_lang_cnt_max
FROM #elig_lang_cnt a
INNER JOIN (SELECT id_mcaid, MAX(lang_cnt) AS max_cnt FROM #elig_lang_cnt GROUP BY id_mcaid) b ON a.id_mcaid = b.id_mcaid AND a.lang_cnt = b.max_cnt;                                      
                                      ", .con = conn))
  
  # Slice data to one language per ID (most frequently reported)
  DBI::dbExecute(conn, glue::glue_sql("
SELECT a.id_mcaid,
a.lang AS lang_max
INTO #elig_lang_max
FROM #elig_lang_cnt a
INNER JOIN (SELECT id_mcaid, MAX(id) AS max_id FROM #elig_lang_cnt_max GROUP BY id_mcaid) b ON a.id_mcaid = b.id_mcaid AND a.id = b.max_id;                                      
                                      ", .con = conn))
  
  
  # Merge back with the primary data and make unknown if NA
  ### Collapse to one row per ID given we have alone or in combo EVER language variables
  DBI::dbExecute(conn, glue::glue_sql("
SELECT a.id_mcaid,
c.lang_max,
a.lang_amharic,
a.lang_arabic,
a.lang_chinese,
a.lang_korean,
a.lang_english,
a.lang_russian,
a.lang_somali,
a.lang_spanish,
a.lang_ukrainian,
a.lang_vietnamese,
b.lang_amharic_T,
b.lang_arabic_t,
b.lang_chinese_t,
b.lang_korean_t,
b.lang_english_t,
b.lang_russian_t,
b.lang_somali_T,
b.lang_spanish_t,
b.lang_ukrainian_t,
b.lang_vietnamese_t
INTO #elig_lang_final
FROM #elig_lang_sum a
INNER JOIN #elig_lang_t b ON a.id_mcaid = b.id_mcaid
INNER JOIN #elig_lang_max c ON a.id_mcaid = c.id_mcaid;                                      
                                      ", .con = conn))
  
  
  try(DBI::dbExecute(conn, glue::glue_sql("
DROP TABLE #elig_lang;
DROP TABLE #elig_lang_t;
DROP TABLE #elig_lang_sum;
DROP TABLE #elig_lang_all;
DROP TABLE #elig_lang_cnt;
DROP TABLE #elig_lang_cnt_max;
DROP TABLE #elig_lang_max;                                      
                                      ", .con = conn)), silent = T)
  
  
  #### JOIN ALL TABLES ####
  ### Add in date for last run
  #### LOAD TO SQL SERVER ####
  message("Loading to SQL")
  create_table_f(conn = conn, 
                 server = server,
                 config = config,
                 overwrite = T)
  
  DBI::dbExecute(conn, glue::glue_sql("
INSERT INTO {`to_schema`}.{`to_table`}
(id_mcaid, 
dob, 
gender_me, 
gender_recent, 
gender_female, 
gender_male, 
gender_female_t, 
gender_male_t, 
race_me, 
race_eth_me, 
race_recent, 
race_eth_recent, 
race_aian, 
race_asian, 
race_black, 
race_latino, 
race_nhpi, 
race_white, 
race_unk, 
race_eth_unk, 
race_aian_t, 
race_asian_t, 
race_black_t, 
race_latino_t, 
race_nhpi_t, 
race_white_t, 
lang_max, 
lang_amharic, 
lang_arabic, 
lang_chinese, 
lang_korean, 
lang_english, 
lang_russian, 
lang_somali, 
lang_spanish, 
lang_ukrainian, 
lang_vietnamese, 
lang_amharic_t, 
lang_arabic_t, 
lang_chinese_t, 
lang_korean_t, 
lang_english_t, 
lang_russian_t, 
lang_somali_t, 
lang_spanish_t, 
lang_ukrainian_t, 
lang_vietnamese_t, 
last_run)
SELECT
a.id_mcaid, 
a.dob, 
b.gender_me, 
b.gender_recent, 
b.gender_female, 
b.gender_male, 
b.gender_female_t, 
b.gender_male_t, 
c.race_me, 
c.race_eth_me, 
c.race_recent, 
c.race_eth_recent, 
c.race_aian, 
c.race_asian, 
c.race_black, 
c.race_latino, 
c.race_nhpi, 
c.race_white, 
c.race_unk, 
c.race_eth_unk, 
c.race_aian_t, 
c.race_asian_t, 
c.race_black_t, 
c.race_latino_t, 
c.race_nhpi_t, 
c.race_white_t, 
d.lang_max, 
d.lang_amharic, 
d.lang_arabic, 
d.lang_chinese, 
d.lang_korean, 
d.lang_english, 
d.lang_russian, 
d.lang_somali, 
d.lang_spanish, 
d.lang_ukrainian, 
d.lang_vietnamese, 
d.lang_amharic_t, 
d.lang_arabic_t, 
d.lang_chinese_t, 
d.lang_korean_t, 
d.lang_english_t, 
d.lang_russian_t, 
d.lang_somali_t, 
d.lang_spanish_t, 
d.lang_ukrainian_t, 
d.lang_vietnamese_t, 
GETDATE()
FROM #elig_dob a
INNER JOIN #elig_gender_final b ON a.id_mcaid = b.id_mcaid
INNER JOIN #elig_race_final c ON a.id_mcaid = c.id_mcaid
INNER JOIN #elig_lang_final d ON a.id_mcaid = d.id_mcaid;                                      
                                      ", .con = conn))
  
  try(DBI::dbExecute(conn, glue::glue_sql("
DROP TABLE #elig_dob;
DROP TABLE #elig_gender_final;
DROP TABLE #elig_race_final;
DROP TABLE #elig_lang_final;                                      
                                      ", .con = conn)), silent = T)
  
  
  #### CLEAN UP ####
  message(to_schema, ".", to_table, " created")
  time_end <- Sys.time()
  message(glue::glue("Table creation took {round(difftime(time_end, time_start, units = 'secs'), 2)} ",
                     " secs ({round(difftime(time_end, time_start, units = 'mins'), 2)} mins)"))
  
  rm(config)
  rm(from_schema, from_table, to_schema, to_table)
}






