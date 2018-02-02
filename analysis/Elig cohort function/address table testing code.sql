--Testing code for address table

declare @begin date, @end date
set @begin = '2017-01-01'
set @end = '2017-06-30'

select x.id, x.from_add, x.to_add, x.homeless, id.id_cnt, zip.zip_cnt, reg.reg_cnt, rankt.id_rank, homeless.homeless_e,

			/**if one zip per client for time period, keep zip value */
			iif(zip.zip_cnt = 1, str(x.zip_new, 5, 0), 
	
			/**if > 1 zip per client for time period, return "multiple" */
			iif(zip.zip_cnt > 1, 'multiple',

			null)) as 'zip_new',

			/**if one zip-based region per client for time period, keep zip-based region value */
			iif(reg.reg_cnt = 1, x.kcreg_zip, 
	
			/**if > 1 zip-based region per client for time period, return "multiple" */
			iif(reg.reg_cnt > 1, 'multiple',

			null)) as 'kcreg_zip'

--client level table
from (
	select id, from_add, to_add, zip_new, kcreg_zip, homeless
	from PHClaims.dbo.mcaid_elig_address
	where from_add < @end AND to_add > @begin
) as x

--count rows per client
left join (
	select id, count(id) as id_cnt
	from PHClaims.dbo.mcaid_elig_address
	where from_add < @end AND to_add > @begin
	group by id
) as id
on x.id = id.id

--count distinct ZIPs per client
left join (
	select id, count(distinct zip_new) as zip_cnt
	from PHClaims.dbo.mcaid_elig_address
	where from_add < @end AND to_add > @begin
	group by id
) as zip
on x.id = zip.id

--count distinct regions per client
left join (
	select id, count(distinct kcreg_zip) as reg_cnt
	from PHClaims.dbo.mcaid_elig_address
	where from_add < @end AND to_add > @begin
	group by id
) as reg
on x.id = reg.id

--rank rows per client ID for keeping only one
left join (
	select id, from_add, kcreg_zip, rank() over (partition by id order by from_add) as id_rank
	from PHClaims.dbo.mcaid_elig_address
	where from_add < @end AND to_add > @begin
) as rankt
on x.id = rankt.id and x.from_add = rankt.from_add

--take max of homeless value (ever homeless)
left join (
	select id, max(homeless) as homeless_e
	from PHClaims.dbo.mcaid_elig_address
	group by id
) as homeless
on x.id = homeless.id

order by id.id_cnt desc, x.id, x.from_add

