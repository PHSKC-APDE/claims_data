--csv file is looked here:--SHAREPOINT\King County Cross-Sector Data - General\References\APCD\apcd_ethnicity_race_mapping.csv
--run this code to identify ethnicities in APCD data that are missing from ethnicity_race crosswalk
--1st ethnicity variable
select z.ethnicity_id1, z.race_id, z.race_desc, count(*) as row_count
from (
	SELECT a.ethnicity_id1, b.ethnicity_desc, b.race_id, b.race_desc
	from stage.apcd_eligibility as a
	left join ref.apcd_ethnicity_race_map as b
	on a.ethnicity_id1 = b.ethnicity_id
) as z
group by z.ethnicity_id1, z.race_id, z.race_desc
order by row_count desc;

--2nd ethnicity variable
--run this code to identify ethnicities in APCD data that are missing from ethnicity_race crosswalk
select z.ethnicity_id2, z.race_id, z.race_desc, count(*) as row_count
from (
	SELECT a.ethnicity_id2, b.ethnicity_desc, b.race_id, b.race_desc
	from stage.apcd_eligibility as a
	left join ref.apcd_ethnicity_race_map as b
	on a.ethnicity_id2 = b.ethnicity_id
) as z
group by z.ethnicity_id2, z.race_id, z.race_desc
order by row_count desc;

--Add any ethnicities with null race_id/race_desc as new rows to the following file:
--SHAREPOINT\King County Cross-Sector Data - General\References\APCD\apcd_ethnicity_race_mapping.csv