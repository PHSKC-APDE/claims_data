------------------------------
--Purpose: Identify ethnicities in APCD eligibility data that are not currently mapped to race/ethnicity values in our ETL process
--Eli Kern, PHSKC-APDE
--August 2026
------------------------------

--------------------
--STEP 1: Run this code to identify ethnicities in APCD 1st ethnicity column that are missing from ethnicity_race crosswalk

select distinct a.ethnicity_id1, b.ethnicity_desc, b.race_id, b.race_desc
from [stg_claims].[apcd_eligibility] as a
left join [stg_claims].[ref_apcd_ethnicity_race_map] as b
on a.ethnicity_id1 = cast(b.ethnicity_id as bigint)
order by a.ethnicity_id1;

select distinct a.ethnicity_id1, b.ethnicity_desc, b.race_id, b.race_desc
from [stg_claims].[apcd_eligibility] as a
left join [stg_claims].[ref_apcd_ethnicity_race_map] as b
on a.ethnicity_id1 = cast(b.ethnicity_id as bigint)
where race_desc IS NULL
order by a.ethnicity_id1;

--------------------
--STEP 2: Run this code to identify ethnicities in APCD 2nd ethnicity column that are missing from ethnicity_race crosswalk

select distinct a.ethnicity_id2, b.ethnicity_desc, b.race_id, b.race_desc
from [stg_claims].[apcd_eligibility] as a
left join[stg_claims].[ref_apcd_ethnicity_race_map] as b
on a.ethnicity_id2 = cast(b.ethnicity_id as bigint)
order by a.ethnicity_id2;

select distinct a.ethnicity_id2, b.ethnicity_desc, b.race_id, b.race_desc
from [stg_claims].[apcd_eligibility] as a
left join [stg_claims].[ref_apcd_ethnicity_race_map] as b
on a.ethnicity_id1 = cast(b.ethnicity_id as bigint)
where race_desc IS NULL
order by a.ethnicity_id2;

--Add any ethnicities with null race_id/race_desc as new rows to the following file:
--https://kc1.sharepoint.com/:x:/r/teams/DPH-APDE-Healthcare-Data-InternalOpsSharedChannel/Shared%20Documents/APCD/References/apcd_ethnicity_race_mapping.csv?d=wd2338c94ac3f4b9bb3de9925d15ad24b&csf=1&web=1&e=bbCDhO
--Use Table E-1 in the following report to determine ethnicity -> race mappings:
--https://kc1.sharepoint.com/:b:/r/teams/DPH-APDE-Healthcare-Data-InternalOpsSharedChannel/Shared%20Documents/APCD/References/Race,%20Ethnicity,%20and%20Language%20Data.pdf?d=w4233596984a7410aa7895fb49b162e77&csf=1&web=1&e=v5SeHA
--Then, return to APCD ETL protocol to recreate apcd_ethnicity_race_map table