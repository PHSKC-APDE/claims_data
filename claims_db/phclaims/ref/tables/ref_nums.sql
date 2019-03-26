
SET NOCOUNT ON;

IF OBJECT_ID('[ref].[nums]', 'U') IS NOT NULL
DROP TABLE [ref].[nums];

CREATE TABLE [ref].[nums]
(n INT NOT NULL
,CONSTRAINT [PK_ref_nums] PRIMARY KEY CLUSTERED([n])
);

DECLARE @max AS INT,
        @rc  AS INT;
SET @max = 2000000;
SET @rc  = 1;

INSERT INTO [ref].[nums] VALUES(1);
-- TABLE WILL DOUBLE IN SIZE EACH ITERATION IF CURRENTLY < 1/2 OF @max
WHILE @rc * 2 <= @max
BEGIN

	INSERT INTO [ref].[nums] 
	SELECT n + @rc 
	FROM [ref].[nums];
	-- TABLE WILL DOUBLE IN SIZE EACH ITERATION IF CURRENTLY < 1/2 OF @max
	SET @rc = @rc * 2;

	--SELECT @rc AS New@rc;
END