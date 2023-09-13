-----------------------------------------------------
--Code to prep Medciaid/ProviderOne data for UW Dugan team
--Eli Kern, January 2023
--Will create new unique ID as P1_ID cannot be shared
--Will create age as of end of study period (01-01-2016 to 12-31-2021)
--Will create persisent tables with claims.tmp_ek prefix
--Export to CIFS folder once created by KCIT, then compress, then share via SFTP


-----------------------
--Step 1: Prepare list of columns from all tables to share with Dugan team
-----------------------

select table_schema, table_name, column_name, ordinal_position,
case
	when data_type = 'varchar' then data_type + '(' + cast(CHARACTER_MAXIMUM_LENGTH as varchar(255)) + ')'
	when data_type = 'numeric' then data_type + '(' + cast(NUMERIC_PRECISION as varchar(255)) + ',' + cast(NUMERIC_SCALE as varchar(255)) + ')'
	else data_type
end as data_type
from INFORMATION_SCHEMA.COLUMNS
where table_schema = 'claims' and
table_name in ('final_mcaid_elig_timevar', 'final_mcaid_elig_demo', 'final_mcaid_claim_procedure',
	'final_mcaid_claim_pharm', 'final_mcaid_claim_line', 'final_mcaid_claim_icdcm_header',
	'final_mcaid_claim_header', 'final_mcaid_claim_ccw', 'final_mcaid_claim_bh',
	'ref_date', 'ref_dx_lookup', 'ref_geo_kc_zip', 'ref_kc_claim_type', 'ref_mcaid_rac_code', 'ref_mco')
order by table_schema, table_name, ordinal_position;

--Copied to "table_column_selection.xlsx" file for flagging columns to include in extract

-----------------------
--Step 2: Create new unique ID and store in reference table to be held by PHSKC
--Restrict people by having 1 or more day of coverage during study period: 01-01-2016 to 12-31-2021
-----------------------

IF OBJECT_ID(N'claims.tmp_ek_dugan_person_id', N'U') IS NOT NULL DROP TABLE claims.tmp_ek_dugan_person_id;
select a.id_mcaid, ROW_NUMBER() over (order by a.id_mcaid) as id_uw
into claims.tmp_ek_dugan_person_id
from (select distinct id_mcaid from claims.final_mcaid_elig_timevar where from_date <= '2021-12-31' and to_date >= '2016-01-01') as a;

--confirm that no id_uw has more than 1 row or more than 1 id_mcaid value
select count(*)
from (
	select id_uw, count(*) as row_count
	from claims.tmp_ek_dugan_person_id
	group by id_uw
) as a
where a.row_count > 1;

select count(*)
from (
	select id_uw, count(distinct id_mcaid) as id_mcaid_dcount
	from claims.tmp_ek_dugan_person_id
	group by id_uw
) as a
where a.id_mcaid_dcount > 1;


-----------------------
--Step 3: Create claims and elig tables for export, limiting to people in claims.tmp_ek_dugan_person_id table
--Remove identifier columns and unecessary columns per Step 1 above
--For claims tables, restrict claims to those having first_service and last_service dates during study period
-----------------------

--------------
---Step 3A: claim_bh table
--------------
IF OBJECT_ID(N'claims.tmp_ek_mcaid_claim_bh', N'U') IS NOT NULL DROP TABLE claims.tmp_ek_mcaid_claim_bh;
select 
b.id_uw,
from_date,
to_date,
bh_cond,
last_run
into claims.tmp_ek_mcaid_claim_bh
from claims.final_mcaid_claim_bh as a
inner join claims.tmp_ek_dugan_person_id as b
on a.id_mcaid = b.id_mcaid;

--------------
---Step 3B: claim_ccw table
--------------
IF OBJECT_ID(N'claims.tmp_ek_mcaid_claim_ccw', N'U') IS NOT NULL DROP TABLE claims.tmp_ek_mcaid_claim_ccw;
select 
b.id_uw,
from_date,
to_date,
ccw_code,
ccw_desc,
last_run
into claims.tmp_ek_mcaid_claim_ccw
from claims.final_mcaid_claim_ccw as a
inner join claims.tmp_ek_dugan_person_id as b
on a.id_mcaid = b.id_mcaid;

--------------
---Step 3C: claim_header table (3 min)
--------------
IF OBJECT_ID(N'claims.tmp_ek_mcaid_claim_header', N'U') IS NOT NULL DROP TABLE claims.tmp_ek_mcaid_claim_header;
select
b.id_uw,
claim_header_id,
clm_type_mcaid_id,
claim_type_id,
first_service_date,
last_service_date,
patient_status,
admsn_source,
admsn_date,
admsn_time,
dschrg_date,
place_of_service_code,
type_of_bill_code,
clm_status_code,
billing_provider_npi,
drvd_drg_code,
insrnc_cvrg_code,
last_pymnt_date,
bill_date,
system_in_date,
claim_header_id_date,
primary_diagnosis,
icdcm_version,
primary_diagnosis_poa,
ed_perform_id,
ed_pophealth_id,
inpatient_id,
pc_visit_id,
last_run
into claims.tmp_ek_mcaid_claim_header
from claims.final_mcaid_claim_header as a
inner join claims.tmp_ek_dugan_person_id as b
on a.id_mcaid = b.id_mcaid
where a.first_service_date between '2016-01-01' and '2021-12-31'
	and a.last_service_date between '2016-01-01' and '2021-12-31';

--Confirm min and max of from_date and to_date, respectively, match study period
select min(first_service_date) as from_date_min, max(last_service_date) as to_date_max from claims.tmp_ek_mcaid_claim_header;

--------------
---Step 3D: claim_icdcm_header table (2 min)
--------------
IF OBJECT_ID(N'claims.tmp_ek_mcaid_claim_icdcm_header', N'U') IS NOT NULL DROP TABLE claims.tmp_ek_mcaid_claim_icdcm_header;
select
b.id_uw,
claim_header_id,
first_service_date,
last_service_date,
icdcm_raw,
icdcm_norm,
icdcm_version,
icdcm_number,
last_run
into claims.tmp_ek_mcaid_claim_icdcm_header
from claims.final_mcaid_claim_icdcm_header as a
inner join claims.tmp_ek_dugan_person_id as b
on a.id_mcaid = b.id_mcaid
where a.first_service_date between '2016-01-01' and '2021-12-31'
	and a.last_service_date between '2016-01-01' and '2021-12-31';

--Confirm min and max of from_date and to_date, respectively, match study period
select min(first_service_date) as from_date_min, max(last_service_date) as to_date_max from claims.tmp_ek_mcaid_claim_icdcm_header;

--------------
---Step 3E: claim_line table (5.5 min)
--------------
IF OBJECT_ID(N'claims.tmp_ek_mcaid_claim_line', N'U') IS NOT NULL DROP TABLE claims.tmp_ek_mcaid_claim_line;
select
b.id_uw,
claim_header_id,
claim_line_id,
first_service_date,
last_service_date,
rev_code,
rac_code_line,
last_run
into claims.tmp_ek_mcaid_claim_line
from claims.final_mcaid_claim_line as a
inner join claims.tmp_ek_dugan_person_id as b
on a.id_mcaid = b.id_mcaid
where a.first_service_date between '2016-01-01' and '2021-12-31'
	and a.last_service_date between '2016-01-01' and '2021-12-31';

--Confirm min and max of from_date and to_date, respectively, match study period
select min(first_service_date) as from_date_min, max(last_service_date) as to_date_max from claims.tmp_ek_mcaid_claim_line;

--------------
---Step 3F: claim_pharm table (<1 min)
--------------
IF OBJECT_ID(N'claims.tmp_ek_mcaid_claim_pharm', N'U') IS NOT NULL DROP TABLE claims.tmp_ek_mcaid_claim_pharm;
select
b.id_uw,
claim_header_id,
ndc,
rx_days_supply,
rx_quantity,
rx_fill_date,
prescriber_id_format,
prescriber_id,
pharmacy_npi,
last_run
into claims.tmp_ek_mcaid_claim_pharm
from claims.final_mcaid_claim_pharm as a
inner join claims.tmp_ek_dugan_person_id as b
on a.id_mcaid = b.id_mcaid
where a.rx_fill_date between '2016-01-01' and '2021-12-31'
	and a.rx_fill_date between '2016-01-01' and '2021-12-31';

--Confirm min and max of from_date and to_date, respectively, match study period
select min(rx_fill_date) as from_date_min, max(rx_fill_date) as to_date_max from claims.tmp_ek_mcaid_claim_pharm;

--------------
---Step 3G: claim_procedure table (4.5 min)
--------------
IF OBJECT_ID(N'claims.tmp_ek_mcaid_claim_procedure', N'U') IS NOT NULL DROP TABLE claims.tmp_ek_mcaid_claim_procedure;
select
b.id_uw,
claim_header_id,
first_service_date,
last_service_date,
procedure_code,
procedure_code_number,
modifier_1,
modifier_2,
modifier_3,
modifier_4,
last_run
into claims.tmp_ek_mcaid_claim_procedure
from claims.final_mcaid_claim_procedure as a
inner join claims.tmp_ek_dugan_person_id as b
on a.id_mcaid = b.id_mcaid
where a.first_service_date between '2016-01-01' and '2021-12-31'
	and a.last_service_date between '2016-01-01' and '2021-12-31';

--Confirm min and max of from_date and to_date, respectively, match study period
select min(first_service_date) as from_date_min, max(last_service_date) as to_date_max from claims.tmp_ek_mcaid_claim_procedure;

--------------
---Step 3H: elig_demo table (<1 min)
--------------
IF OBJECT_ID(N'claims.tmp_ek_mcaid_elig_demo', N'U') IS NOT NULL DROP TABLE claims.tmp_ek_mcaid_elig_demo;
select
b.id_uw,
case
	when floor((datediff(day, dob, '2021-12-31') + 1) / 365.25) >= 0 then floor((datediff(day, dob, '2021-12-31') + 1) / 365.25)
	when floor((datediff(day, dob, '2021-12-31') + 1) / 365.25) < 0 then 0
end as age,
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
last_run
into claims.tmp_ek_mcaid_elig_demo
from claims.final_mcaid_elig_demo as a
inner join claims.tmp_ek_dugan_person_id as b
on a.id_mcaid = b.id_mcaid;

--Confirm min and max of age
select min(age) as age_min, max(age) as age_max from claims.tmp_ek_mcaid_elig_demo;

--------------
---Step 3I: elig_timevar table (<1 min)
--------------
IF OBJECT_ID(N'claims.tmp_ek_mcaid_elig_timevar', N'U') IS NOT NULL DROP TABLE claims.tmp_ek_mcaid_elig_timevar;
select
b.id_uw,
from_date,
to_date,
contiguous,
dual,
tpl,
bsp_group_cid,
full_benefit,
cov_type,
mco_id,
geo_state,
geo_zip,
geo_county_code,
cov_time_day,
last_run
into claims.tmp_ek_mcaid_elig_timevar
from claims.final_mcaid_elig_timevar as a
inner join claims.tmp_ek_dugan_person_id as b
on a.id_mcaid = b.id_mcaid
where a.from_date <= '2021-12-31' and a.to_date >= '2016-01-01';


-----------------------
--Step 4: QA prior to export
--QA to make sure age is calculated directly
-----------------------

--check age calculation: PASS
select * from claims.tmp_ek_dugan_person_id where id_uw = 274950;
select * from claims.tmp_ek_mcaid_elig_demo where id_uw = 274950;
select * from claims.final_mcaid_elig_demo where id_mcaid = 'ENTER';


-----------------------
--Step 5: Export tables to CIFS folder setup by KCIT
-----------------------

--Waiting for guidance from Jeremy
--Tried using sqlcmd utility, which worked, but I couldn't figure out how to get rid of --- inserted between header row and values
--Don't want to try BCP, as I hate it
--Couldn't use SSMS data export wizard as I couldn't use Windows Authentication - I didn't see an Azure option