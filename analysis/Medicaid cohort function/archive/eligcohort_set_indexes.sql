--create nonclustered index on 'id' variable for each table
create index [idx_mcaidid] on PHClaims.dbo.mcaid_elig_demoever (id)
create index [idx_mcaidid] on PHClaims.dbo.mcaid_elig_overall (MEDICAID_RECIPIENT_ID)
create index [idx_mcaidid] on PHClaims.dbo.mcaid_elig_address (id)
create index [idx_mcaidid] on PHClaims.dbo.mcaid_elig_dual (id)

--create clustered index on startdate for time-varying tables
create clustered index [idx_start] on PHClaims.dbo.mcaid_elig_overall (startdate)
create clustered index [idx_start] on PHClaims.dbo.mcaid_elig_address (from_add)
create clustered index [idx_start] on PHClaims.dbo.mcaid_elig_dual (calstart)

--create nonclustered index on enddate for time-varying tables
create index [idx_end] on PHClaims.dbo.mcaid_elig_overall (enddate)
create index [idx_end] on PHClaims.dbo.mcaid_elig_address (to_add)
create index [idx_end] on PHClaims.dbo.mcaid_elig_dual (calend)

--create table/topic-specific indexes
create clustered index [idx_lang] on PHClaims.dbo.mcaid_elig_demoever (maxlang)
create index [idx_zip] on PHClaims.dbo.mcaid_elig_address (zip_new)
create index [idx_zreg] on PHClaims.dbo.mcaid_elig_address (kcreg_zip)

--view indexes by table
use PHClaims
go
exec sp_helpindex mcaid_elig_demoever
exec sp_helpindex mcaid_elig_overall
exec sp_helpindex mcaid_elig_address
exec sp_helpindex mcaid_elig_dual

--view space used by indexes for each table
exec sp_spaceused mcaid_elig_demoever
exec sp_spaceused mcaid_elig_overall
exec sp_spaceused mcaid_elig_address
exec sp_spaceused mcaid_elig_dual

--code to drop index
drop index mcaid_elig_dual.idx_mcaidid_dual

--drop all indexes
declare @qry nvarchar(max);
select @qry = 
(SELECT  'DROP INDEX ' + ix.name + ' ON ' + OBJECT_NAME(ID) + '; '
FROM  sysindexes ix
WHERE   ix.Name IS NOT null and ix.Name like '%idx_%'
for xml path(''));
exec sp_executesql @qry