--csv file is looked here:
--moved in 2026 to --https://kc1.sharepoint.com/:x:/r/teams/DPH-KCCross-SectorData/Shared%20Documents/APCD/References/apcd_ethnicity_race_mapping.csv?d=w1b637059fa4a4881bc564ca1a0da2ede&csf=1&web=1&e=JZoirL

--run this code to identify ethnicities in APCD data that are missing from ethnicity_race crosswalk
--1st ethnicity variable
select distinct a.ethnicity_id1, b.ethnicity_desc, b.race_id, b.race_desc
--from claims.stage_apcd_eligibility_cci as a
from [stg_claims].[apcd_eligibility] as a
--left join claims.ref_apcd_ethnicity_race_map as b
left join [stg_claims].[ref_apcd_ethnicity_race_map] as b
on a.ethnicity_id1 = cast(b.ethnicity_id as bigint)
order by a.ethnicity_id1;

select distinct a.ethnicity_id1, b.ethnicity_desc, b.race_id, b.race_desc
--from claims.stage_apcd_eligibility_cci as a
from [stg_claims].[apcd_eligibility] as a
--left join claims.ref_apcd_ethnicity_race_map as b
left join [stg_claims].[ref_apcd_ethnicity_race_map] as b
on a.ethnicity_id1 = cast(b.ethnicity_id as bigint)
where race_desc IS NULL
order by a.ethnicity_id1;


--2nd ethnicity variable
--run this code to identify ethnicities in APCD data that are missing from ethnicity_race crosswalk
select distinct a.ethnicity_id2, b.ethnicity_desc, b.race_id, b.race_desc
--from claims.stage_apcd_eligibility_cci as a
from [stg_claims].[apcd_eligibility] as a
--left join claims.ref_apcd_ethnicity_race_map as b
left join[stg_claims].[ref_apcd_ethnicity_race_map] as b
on a.ethnicity_id2 = cast(b.ethnicity_id as bigint)
order by a.ethnicity_id2;

select distinct a.ethnicity_id2, b.ethnicity_desc, b.race_id, b.race_desc
--from claims.stage_apcd_eligibility_cci as a
from [stg_claims].[apcd_eligibility] as a
--left join claims.ref_apcd_ethnicity_race_map as b
left join [stg_claims].[ref_apcd_ethnicity_race_map] as b
on a.ethnicity_id1 = cast(b.ethnicity_id as bigint)
where race_desc IS NULL
order by a.ethnicity_id2;


--Add any ethnicities with null race_id/race_desc as new rows to the following file:
--SHAREPOINT\King County Cross-Sector Data - Documents\References\APCD\apcd_ethnicity_race_mapping.csv
--Use this file beginning in 2026 -https://kc1.sharepoint.com/:x:/r/teams/DPH-KCCross-SectorData/Shared%20Documents/APCD/References/apcd_ethnicity_race_mapping.csv?d=w1b637059fa4a4881bc564ca1a0da2ede&csf=1&web=1&e=JZoirL