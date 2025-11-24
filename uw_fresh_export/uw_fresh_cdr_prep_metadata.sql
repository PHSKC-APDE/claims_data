--Prepare metadata for UW Fresh Study tables
--Eli Kern, APDE, PHSKC
--November 2025

-----------------------
--STEP 1: Query row and column counts
-----------------------
select s.Name AS schema_name, t.NAME AS table_name, 
count(c.COLUMN_NAME) as col_count,
max(p.rows) AS row_count, --I'm taking max here because an index that is not on all rows creates two entries in this summary table
cast(GETDATE() as date) as query_date
from sys.tables t
inner join sys.indexes i on t.OBJECT_ID = i.object_id
inner join sys.partitions p on i.object_id = p.OBJECT_ID and i.index_id = p.index_id
left outer join sys.schemas s on t.schema_id = s.schema_id
left join information_schema.columns c on t.name = c.TABLE_NAME and s.name = c.TABLE_SCHEMA
where (s.name = 'stg_cdr' and t.name like 'export%')
group by s.Name, t.Name

union 
--select other useful reference tables
select s.Name AS schema_name, t.NAME AS table_name, 
count(c.COLUMN_NAME) as col_count,
max(p.rows) AS row_count, --I'm taking max here because an index that is not on all rows creates two entries in this summary table
cast(GETDATE() as date) as query_date
from sys.tables t
inner join sys.indexes i on t.OBJECT_ID = i.object_id
inner join sys.partitions p on i.object_id = p.OBJECT_ID and i.index_id = p.index_id
left outer join sys.schemas s on t.schema_id = s.schema_id
left join information_schema.columns c on t.name = c.TABLE_NAME and s.name = c.TABLE_SCHEMA
where t.name in (
	'ref_ccw_lookup',
	'ref_date',
	'ref_geo_kc_zip',
	'icdcm_codes',
	'ref_ndc_codes')
and s.name in ('stg_claims', 'stg_reference')
group by s.Name, t.Name
order by schema_name, table_name;


-----------------------
--STEP 2: Create format file for all tables
-----------------------
select s.Name AS schema_name,
	t.NAME AS table_name, 
	c.ORDINAL_POSITION as column_position,
	c.COLUMN_NAME as column_name,
	case
		when DATA_TYPE = 'varchar' then 'varchar' + '(' + cast(CHARACTER_MAXIMUM_LENGTH as varchar) + ')'
		when DATA_TYPE in ('numeric', 'decimal') then DATA_TYPE + 
			'(' + cast(NUMERIC_PRECISION as varchar) + ',' + cast(NUMERIC_SCALE as varchar) + ')'
		else data_type
	end as column_type,
	cast(GETDATE() as date) as query_date
from sys.tables t
inner join sys.indexes i on t.OBJECT_ID = i.object_id
inner join sys.partitions p on i.object_id = p.OBJECT_ID and i.index_id = p.index_id
left outer join sys.schemas s on t.schema_id = s.schema_id
left join information_schema.columns c on t.name = c.TABLE_NAME and s.name = c.TABLE_SCHEMA
where (s.name = 'stg_cdr' and t.name like 'export%')

union 
--select other useful reference tables
select s.Name AS schema_name,
	t.NAME AS table_name, 
	c.ORDINAL_POSITION as column_position,
	c.COLUMN_NAME as column_name,
	case
		when DATA_TYPE = 'varchar' then 'varchar' + '(' + cast(CHARACTER_MAXIMUM_LENGTH as varchar) + ')'
		when DATA_TYPE in ('numeric', 'decimal') then DATA_TYPE + 
			'(' + cast(NUMERIC_PRECISION as varchar) + ',' + cast(NUMERIC_SCALE as varchar) + ')'
		else data_type
	end as column_type,
	cast(GETDATE() as date) as query_date
from sys.tables t
inner join sys.indexes i on t.OBJECT_ID = i.object_id
inner join sys.partitions p on i.object_id = p.OBJECT_ID and i.index_id = p.index_id
left outer join sys.schemas s on t.schema_id = s.schema_id
left join information_schema.columns c on t.name = c.TABLE_NAME and s.name = c.TABLE_SCHEMA
where t.name in (
	'ref_ccw_lookup',
	'ref_date',
	'ref_geo_kc_zip',
	'icdcm_codes',
	'ref_ndc_codes')
and s.name in ('stg_claims', 'stg_reference')
order by schema_name, table_name, column_position;