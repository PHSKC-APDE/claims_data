
# Medicaid eligibility cohort function – SQL and R applications
Version 1.1

## Purpose
Script to send a SQL query to the PHClaims database on the SQL Server 51 to return a Medicaid eligibility cohort with specified parameters, either working in SQL Server Management Studio or R.

## Access/permissions required
- All required SQL and R scripts are stored on GitHub [here]()
- This function uses tables, stored procedures, and functions on SQL Server 51
- Users must be able to SELECT following tables in PHClaims database:
     - dbo.mcaid_elig_overall
     - dbo.mcaid_elig_address
     - dbo.mcaid_elig_dual
     - dbo.mcaid_elig_demoever
- Users must be able to EXECUTE stored procedures in PH_APDEStore database:
     - dbo.sp_mcaidcohort
- Users must be able to SELECT table-valued functions in PH_APDEStore database:
     - dbo.Split
- To check your permissions on any database once you’ve connected, run the SQL code [here](https://github.com/PHSKC-APDE/Medicaid/blob/master/analysis/Broad%20use%20functions/Server%20permissions.sql)

## Using the Medicaid eligibility cohort function in R
1.	You can use the R script titled [mcaid_cohort_process.R](mcaid_cohort_process.R) to get started – this will source (i.e. load) the function mcaid_cohort_f from the R script [mcaid_cohort_function.R](mcaid_cohort_function.R)
2.	Make sure to have the suggested R packages installed and loaded (RODBC, dplyr, stringr, lubridate)
3.	To pass parameters to this function, review the [Function parameters](#function-parameters) section below

## Using the Medicaid eligibility cohort function in SQL Server Management Studio
1.	Copy and paste the SQL code from the SQL script [mcaidcohort_run.sql](mcaidcohort_run.sql) and tweak the parameters to your desire.

## Illustrative example
Check out how the parameters are set in the [mcaidcohort_run.sql](mcaidcohort_run.sql) file. This will select a Medicaid eligibility cohort with the following parameters:
- Medicaid coverage between 1/1/2017 and 6/30/2017, with minimum coverage of 50% during this time period
- Members must have 0% Medicare-Medicaid dual eligibility coverage during this time
- Medicaid members must be between age 18 and 64 (inclusive), age is calculated as of the last day of the requested coverage date range
- Medicaid members must have been reported “male” alone or in combination at any point during the history of the King County Medicaid eligibility data set (hint: you can use the male_t variable to further subset this cohort based on the percentage of person time each member spent in the “male” status)
- Medicaid members must have been reported “Black” alone or in combination at any point during the history of the King County Medicaid eligibility data set
- All ZIP codes and ZIP-based regions are included
- Medicaid members must have Arabic or Somali as the most frequently reported spoken or written language alone or in combination at any point in the history of the Medicaid eligibility data set

## Function parameters

| Parameter | Definition | Input format/range | Default value |
| --- | --- | --- | --- |
| from_date | begin date for Medicaid coverage period	| “YYYY-MM-DD” | 12 months prior to today’s date
| to_date | end date for Medicaid coverage period | “YYYY-MM-DD” | 6 months prior to today’s date
| covmin | minimum coverage required during requested date range (percent scale) | 0-100 | begin
| dualmax | maximum Medicare-Medicaid dual eligibility coverage allowed during requested date range (percent scale) | 0-100 | 100
| agemin | minimum age for cohort (integer) | positive integer | 0
| agemax | maximum age for cohort (integer) | positive integer | 200
| male, female, aian…latino, english…amharic | alone or in combination EVER gender, race, and language, respectively | 0, 1 | null
| maxlang | most frequently reported spoken/written language, see list [here](#list-of-languages-in-medicaid-eligibility-data) | “SOMALI,ARABIC,etc.” (all caps, comma-separated, no spaces) | null
| zip | most frequently reported ZIP code during requested date range | “98103,98105,etc.” (all caps, comma-separated, no spaces) | null
| zregion | most frequently mapped ZIP code-based region during requested date range | “east,north,seattle,south” (all caps, comma-separated, no spaces) | null
| id | list of requested Medicaid IDs | 11-character Medicaid IDs, all caps, comma-separated, no spaces | null

## Data dictionary for data set created by Medicaid eligibility cohort function

| Variable | Definition | Type | Possible Values |
| --- | --- | --- | --- |
| id | Unique Medicaid member ID (ProviderOne ID) | character | 11 alphanumeric characters
| covd | Medicaid coverage duration during the requested date range (inclusive, integer days) | numeric | >=1
| covper | Percent of the requested date range with Medicaid coverage | numeric | >0.0 and <=100.0
| duald | Medicare-Medicaid dual eligibility coverage during requested date range (inclusive, integer days) | numeric | >=0
| dualper | Percent of the requested date range with Medicare-Medicaid dual eligibility | numeric | >=0.0 and <=100.0
| age | Integer age calculated as of the last date of the requested date range | numeric | >=0
| male | Male gender alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| female | Female gender alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| male_t | Percent of total person time (months) with male gender | numeric | >=0.0 and <=100.0
| female_t | Percent of total person time (months) with female gender | numeric | >=0.0 and <=100.0
| aian | American Indian/Alaska Native race alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| asian | Asian race alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| black | Black race alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| nhpi | Native Hawaiian/Pacific Islander race alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| white | White race alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| latino | Latino race alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| aian_t | Percent of total person time (months) with AI/AN race | numeric | >=0.0 and <=100.0
| asian_t | Percent of total person time (months) with Asian race | numeric | >=0.0 and <=100.0
| black_t | Percent of total person time (months) with Black race | numeric | >=0.0 and <=100.0
| nhpi_t | Percent of total person time (months) with NH/PI race | numeric | >=0.0 and <=100.0
| white_t | Percent of total person time (months) with White race | numeric | >=0.0 and <=100.0
| latino_t | Percent of total person time (months) with Latino race | numeric | >=0.0 and <=100.0
| zip_new | Most frequently reported ZIP code during requested date range | numeric | 5 numeric characters
| kcreg_zip | Most frequently reported ZIP-based King County region during requested date range | character | east, north, seattle, south
| homeless_e | Homeless status EVER (pulled from 1st and 2nd line address fields) | numeric | 0 = no, 1 = yes, null or N/A = missing
| maxlang | Most frequently reported spoken or written language EVER | character | see list [here](#list-of-languages-in-medicaid-eligibility-data)
| english | English language alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| spanish | Spanish language alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| vietnamese | Vietnamese language alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| chinese | Chinese language alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| somali | Somali language alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| russian | Russian language alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| arabic | Arabic language alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| korean | Korean language alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| ukrainian | Ukrainian language alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| amharic | Amharic language alone or in combination EVER | numeric | 0 = no, 1 = yes, null or N/A = missing
| english_t | Percent of total person time (months) with English language | numeric | >=0.0 and <=100.0
| spanish_t | Percent of total person time (months) with Spanish language | numeric | >=0.0 and <=100.0
| vietnamese_t | Percent of total person time (months) with Vietnamese language | numeric | >=0.0 and <=100.0
| chinese_t | Percent of total person time (months) with Chinese language | numeric | >=0.0 and <=100.0
| somali_t | Percent of total person time (months) with Somali language | numeric | >=0.0 and <=100.0
| russian_t | Percent of total person time (months) with Russian language | numeric | >=0.0 and <=100.0
| arabic_t | Percent of total person time (months) with Arabic language | numeric | >=0.0 and <=100.0
| korean_t | Percent of total person time (months) with Korean language | numeric | >=0.0 and <=100.0
| ukrainian_t | Percent of total person time (months) with Ukrainian language | numeric | >=0.0 and <=100.0
| amharic_t | Percent of total person time (months) with Amharic language | numeric | >=0.0 and <=100.0

*Note: For all _t variables, the denominator equals the total months of person time with non-missing demographics (e.g. gender, race, or language). For race, a month with "non-Hispanic" is considered missing if no other race information was available. For language, both spoken and written language must be missing simultaneously for a month not to contribute to the denominator person time.*

## List of languages in Medicaid eligibility data

- ALBANIAN
- AMHARIC
- ARABIC
- ARMENIAN
- BENGALI
- BIKOL
- BISAYAN
- BOSNIAN
- BRAILLE
- BULGARIAN
- BURMESE
- CAMBODIAN/KNMER
- CANTONESE
- CEBUANO
- CHAM/CHING/DIJIM
- CHAMIC LANGUAGES
- CHAMORRO
- CHINESE
- CHIU CHOW
- CHUUKESE
- CREOLES AND PIDGINS F
- CROATIAN
- CZECH
- DANISH
- DARI
- DUTCH; FLEMISH
- ENGLISH
- FARSI
- FIJIAN
- FILIPINO; PILIPINO
- FINNISH
- FRENCH
- GEORGIAN
- GERMAN
- GREEK, MODERN (1453-)
- GUJARATI
- HAITIAN; HAITIAN CREOLE
- HAKKA CHINESE
- HEBREW
- HILIGAYNON
- HINDI
- HMONG
- HUNGARIAN
- IGBO
- ILOKO
- INDONESIAN
- IRANIAN
- ITALIAN
- JAPANESE
- KHMER
- KHMU
- KIKUYU; GIKUYU
- KOREAN
- LAO
- LARGE PRINT
- LATIN
- MACEDONIAN
- MALAY
- MALAYALAM
- MANDARIN
- MAORI
- MARATHI
- MARSHALLESE
- MIEN
- MON-KHMER
- NEPALI
- NORWEGIAN
- OROMO
- PANGASINAN
- PANJABI; PUNJABI
- PERSIAN
- POLISH
- PORTUGUESE
- PUNJABI
- PUSHTO
- PUYALLUP
- QUECHUA
- ROMANIAN
- RUSSIAN
- SALISHAN LANGUAGES
- SAMOAN
- SANSKRIT
- SERBIAN
- SHONA
- SIGN LANGUAGES
- SLOVAK
- SLOVENIAN
- SOMALI
- SPANISH; CASTILIAN
- SUNDANESE
- SWAHILI
- SWEDISH
- TAGALOG
- TAMIL
- TELUGU
- THAI
- TIBETAN
- TIGRINYA
- TOISHANESE
- TONGA (TONGA ISLANDS)
- TONGAN
- TURKISH
- UKRAINIAN
- URDU
- VIETNAMESE
- VISAYAN
- YAO
- YORUBA
