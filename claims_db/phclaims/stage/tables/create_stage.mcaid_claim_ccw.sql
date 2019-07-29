--Code to create stage.mcaid_claim_ccw table
--Person-level CCW condition status by time period
--Eli Kern (PHSKC-APDE)
--2019-7-29 

if object_id('PHClaims.stage.mcaid_claim_ccw', 'U') is not null drop table PHClaims.stage.mcaid_claim_ccw;
create table PHClaims.stage.mcaid_claim_ccw (
	id_mcaid bigint,
	from_date date,
	to_date date,
	ccw_code tinyint,
	ccw_desc varchar(200)
);



