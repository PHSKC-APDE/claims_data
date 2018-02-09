--user defined function to convert CSV lists into tables that SQL Server can use in WHERE IN statement
/****** Object:  UserDefinedFunction [dbo].[CSVToTable]    Script Date: 04/28/2013 10:45:17 ******/
use PH_APDEStore
go

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[CSVToTable] (@InStr VARCHAR(MAX))
RETURNS @TempTab TABLE
   (id varchar(max) not null)
AS
BEGIN
    ;-- Ensure input ends with comma
	SET @InStr = REPLACE(@InStr + ',', ',,', ',')
	DECLARE @SP INT
DECLARE @VALUE VARCHAR(1000)
WHILE PATINDEX('%,%', @INSTR ) <> 0 
BEGIN
   SELECT  @SP = PATINDEX('%,%',@INSTR)
   SELECT  @VALUE = LEFT(@INSTR , @SP - 1)
   SELECT  @INSTR = STUFF(@INSTR, 1, @SP, '')
   INSERT INTO @TempTab(id) VALUES (@VALUE)
END
	RETURN
END
GO