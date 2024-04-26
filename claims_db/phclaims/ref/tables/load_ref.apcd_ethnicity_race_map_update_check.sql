--csv file is looked here:--SHAREPOINT\King County Cross-Sector Data - Documents\References\APCD\apcd_ethnicity_race_mapping.csv
--run this code to identify ethnicities in APCD data that are missing from ethnicity_race crosswalk
--1st ethnicity variable
select distinct a.ethnicity_id1, b.ethnicity_desc, b.race_id, b.race_desc
from claims.stage_apcd_eligibility_cci as a
left join claims.ref_apcd_ethnicity_race_map as b
on a.ethnicity_id1 = cast(b.ethnicity_id as bigint)
order by a.ethnicity_id1;

--2nd ethnicity variable
--run this code to identify ethnicities in APCD data that are missing from ethnicity_race crosswalk
select distinct a.ethnicity_id2, b.ethnicity_desc, b.race_id, b.race_desc
from claims.stage_apcd_eligibility_cci as a
left join claims.ref_apcd_ethnicity_race_map as b
on a.ethnicity_id2 = cast(b.ethnicity_id as bigint)
order by a.ethnicity_id2;

--Add any ethnicities with null race_id/race_desc as new rows to the following file:
--SHAREPOINT\King County Cross-Sector Data - Documents\References\APCD\apcd_ethnicity_race_mapping.csv