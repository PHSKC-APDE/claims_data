--Eli Kern
--Assessment, Policy Development & Evaluation, Public Health - Seattle & King County
--2/27/18
--Code to return a demographic subset of the King County Medicaid member population for a specific time period

--Refer to README file on GitHub to understand parameters below
--https://github.com/PHSKC-APDE/Medicaid/tree/master/analysis/Medicaid%20cohort%20function

use PH_APDEStore
go

--Note that @duration variable must be entered manually - this is the difference between @begin and @end plus 1 day
exec PH_APDEStore.dbo.sp_mcaidcohort
	@from_date = '2017-01-01', 
	@to_date = '2017-06-30',
	@duration = 181, /*must calculate and input manually based on from and to dates*/
	@covmin = 0,
	@ccov_min = 1,
	@covgap_max = null,
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