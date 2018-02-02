--code to run stored procedure to create Medicaid eligbility cohort
use PH_APDEStore
go

exec PH_APDEStore.[PH\KERNELI].sp_mcaidcohort 
	@begin = '2017-01-01', 
	@end = '2017-06-30',
	@agemin = 18,
	@agemax = 64