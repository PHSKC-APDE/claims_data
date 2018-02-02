--code to run stored procedure to create Medicaid eligbility cohort
use PH_APDEStore
go

--exec [PH_APDESTORE].[PH\SONGL].sp_genhosp1 @ThisYear=2015;
exec PH_APDEStore.[PH\KERNELI].sp_mcaidcohort 
	@begin = '2017-01-01', 
	@end = '2017-06-30',
	@duration = 181,
	@covmin = 50,
	@agemin = 18,
	@agemax = 64,
	@female = null,
	@male = 1,
	@aian = null,
	@asian = null,
	@black = 1,
	@nhpi = null,
	@white = null,
	@latino = null,
	@zip = '98103,98105',
	@region = null