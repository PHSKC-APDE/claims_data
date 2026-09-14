
SET NOCOUNT ON;

IF OBJECT_ID('[ref].[num]', 'U') IS NOT NULL
DROP TABLE [ref].[num];

CREATE TABLE [ref].[num]
(n INT NOT NULL
,CONSTRAINT [PK_ref_num] PRIMARY KEY CLUSTERED([n])
);

DECLARE @max AS INT,
        @rc  AS INT;
SET @max = 2000000;
SET @rc  = 1;

INSERT INTO [ref].[num] VALUES(1);
-- TABLE WILL DOUBLE IN SIZE EACH ITERATION IF CURRENTLY < 1/2 OF @max
WHILE @rc * 2 <= @max
BEGIN

	INSERT INTO [ref].[num] 
	SELECT n + @rc 
	FROM [ref].[num];
	-- TABLE WILL DOUBLE IN SIZE EACH ITERATION IF CURRENTLY < 1/2 OF @max
	SET @rc = @rc * 2;

	--SELECT @rc AS New@rc;
END