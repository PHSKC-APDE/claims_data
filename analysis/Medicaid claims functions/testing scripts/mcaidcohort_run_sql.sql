--Eli Kern
--Assessment, Policy Development & Evaluation, Public Health - Seattle & King County
--2/27/18
--Code to return a demographic subset of the King County Medicaid member population for a specific time period

--Refer to README file on GitHub to understand parameters below
--https://github.com/PHSKC-APDE/Medicaid/tree/master/analysis/Medicaid%20cohort%20function

use PHClaims
go

--Note that @duration variable must be entered manually - this is the difference between @begin and @end plus 1 day
--Note that resulting table will be saved as a global temp table named: ##mcaidcohort
exec PHClaims.dbo.sp_mcaidcohort_sql
	@from_date = '2016-04-01', 
	@to_date = '2017-03-31',
	@duration = 365, /*must calculate and input manually based on from and to dates*/
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