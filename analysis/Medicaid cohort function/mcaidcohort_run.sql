--code to run stored procedure to create Medicaid eligbility cohort
use PH_APDEStore
go

--Note that @duration variable must be entered manually - this is the difference between @begin and @end plus 1 day
exec PH_APDEStore.dbo.sp_mcaidcohort 
	@from_date = '2017-01-01', 
	@to_date = '2017-06-30',
	@duration = 181, /**must define this MANUALLY**/
	@covmin = 50,
	@dualmax = 0,
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
	@zip = null,
	@region = null,
	@english = null,
	@spanish = null,
	@vietnamese = null,
	@chinese = null,
	@somali = null,
	@russian = null,
	@arabic = null,
	@korean = null,
	@ukrainian = null,
	@amharic = null,
	@maxlang = 'ARABIC,SOMALI',
	@id = null