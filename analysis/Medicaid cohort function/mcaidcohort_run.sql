--code to run stored procedure to create Medicaid eligbility cohort
use PH_APDEStore
go

--Note that @duration variable must be entered manually - this is the difference between @begin and @end plus 1 day
exec PH_APDEStore.dbo.sp_mcaidcohort 
	@from_date = '2017-01-01', 
	@to_date = '2017-06-30',
	@duration = 181, /**must define this MANUALLY**/
	@covmin = 0,
	@dualmax = 100,
	@agemin = 0,
	@agemax = 200,
	@female = null,
	@male = null,
	@aian = null,
	@asian = null,
	@black = null,
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
	@maxlang = null,
	@id = null