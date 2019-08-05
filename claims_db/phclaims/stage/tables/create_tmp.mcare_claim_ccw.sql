--Code to create tmp.mcare_claim_ccw table
--Person-level CCW condition status by time period
--Eli Kern (PHSKC-APDE)
--2019-7-31

if object_id('PHClaims.tmp.mcare_claim_ccw', 'U') is not null drop table PHClaims.tmp.mcare_claim_ccw;
create table PHClaims.tmp.mcare_claim_ccw (
	id_mcare varchar(255),
	from_date date,
	to_date date,
	ccw_code tinyint,
	ccw_desc varchar(200)
);



