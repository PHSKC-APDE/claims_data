--QA of ref.king_provider_master
--9/12/19
--Eli Kern
--Run time: 1 min

use phclaims
go

--No NPI should have more than one row
select count(*) as row_count
from (
	select npi, count(*) as row_count
	FROM ref.king_provider_master
	group by npi
) as a
where a.row_count >1;

--No NPI should be any length other than 10 digits
select count(*) as row_count
from ref.king_provider_master
where len(npi) != 10;

--All NPIs should have an entity type
select count(*) as row_count
from ref.king_provider_master
where entity_type is null;

--Taxonomy should be 10 digits long
select count(*) as row_count
from ref.king_provider_master
where len(primary_taxonomy) != 10 or len(secondary_taxonomy) != 10;

--ZIP codes should be 5 digits long
select count(*) as row_count
from ref.king_provider_master
where len(geo_zip_practice) != 5;