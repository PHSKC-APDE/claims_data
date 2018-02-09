--this generates a float-type value
select 150.0/181.0*100.0 as 'number'

--this generates a decimal-type with precision 3 and scale 1
select cast((150 * 1.0) / (181 * 1.0) * (100.0) as decimal(3,1)) as 'number'

--try to use this information to control the precision of calculated columns in my SQL code, perhaps this
--will help speed of where clause in addition to indexing of columns