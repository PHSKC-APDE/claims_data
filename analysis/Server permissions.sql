/****** Server permissions  ******/
USE PH_APDEStore;
SELECT * FROM fn_my_permissions(NULL, 'DATABASE');  
GO

USE PHClaims;
SELECT * FROM fn_my_permissions(NULL, 'DATABASE');  
GO