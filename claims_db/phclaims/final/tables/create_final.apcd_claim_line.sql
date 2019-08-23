--Code to create final.apcd_claim_line table
--Distinct line-level claim variables that do not have a dedicated table (e.g. revenue code). In other words elements for which there is only one distinct 
--value per claim line.
--Eli Kern (PHSKC-APDE)
--2019-8-22 


if object_id('PHClaims.final.apcd_claim_line', 'U') is not null drop table PHClaims.final.apcd_claim_line;
create table PHClaims.final.apcd_claim_line (
	id_apcd bigint,
	extract_id int,
	claim_header_id bigint,
	claim_line_id bigint,
	line_counter int,
	first_service_date date,
	last_service_date date,
	charge_amt numeric(38,2),
	revenue_code varchar(255),
	place_of_service_code varchar(255)
);



