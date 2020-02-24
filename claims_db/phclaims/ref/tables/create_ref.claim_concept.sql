
USE [PHClaims];
GO

IF OBJECT_ID('[ref].[claim_concept]') IS NOT NULL
DROP TABLE [ref].[claim_concept];
CREATE TABLE [ref].[claim_concept]
([concept_id] SMALLINT NOT NULL
,[concept_column_name] VARCHAR(255)
,[concept_name] VARCHAR(255)
,[desc] VARCHAR(1000)
,[reference] VARCHAR(255)
,CONSTRAINT [pk_claim_concept_concept_id] PRIMARY KEY CLUSTERED ([concept_id]));
GO