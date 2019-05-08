--Code to create stage.apcd_claim_ccw table
--Person-level CCW condition status by time period
--Eli Kern (PHSKC-APDE)
--2019-5-8 

if object_id('PHClaims.stage.apcd_claim_ccw', 'U') is not null drop table PHClaims.stage.apcd_claim_ccw;
create table PHClaims.stage.apcd_claim_ccw (
	id_apcd bigint,
	from_date date,
	to_date date,
	ccw_code tinyint,
	ccw_desc varchar(200)
);



