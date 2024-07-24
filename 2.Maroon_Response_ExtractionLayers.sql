--**********************************************************************************************************************************************--
-- Title: Maroon_Response Report Views
-- Desc: This file creates views from Maroon_Response
-- Change Log: When,Who,What
-- 2024-05-06,LuisValderrama/ Noor Yassin,Created Script for Views
--**********************************************************************************************************************************************--
USE [Maroon_Response];
GO
SET NOCOUNT ON;

--********************************************************************--
-- Extraction Layers in the form of Reporting Views
--********************************************************************--

-- 1. vDimCountry reporting view
IF (OBJECT_ID('vDimCountry') IS NOT NULL) DROP VIEW vDimCountry;
GO
-- Desc: This code
-- Change Log: When,Who,What
-- 2024-05-06,LuisValderrama/ Noor Yassin,Created Script for Views
-- Select ALL attributes from DimCountry
CREATE VIEW vDimCountry
	AS
		SELECT * FROM DimCountry;
GO

-- 2. vDimCountry reporting view
IF (OBJECT_ID('vDimDate') IS NOT NULL) DROP VIEW vDimDate;
GO
-- Desc: This code
-- Change Log: When,Who,What
-- 2024-05-06,LuisValderrama/ Noor Yassin,Created Script for Views
-- Select ALL attributes from DimDate
CREATE VIEW vDimDate
	AS
		SELECT * FROM DimDate;
GO

-- 3. vDimState reporting view
IF (OBJECT_ID('vDimState') IS NOT NULL) DROP VIEW vDimState;
GO
-- Desc: This code
-- Change Log: When,Who,What
-- 2024-05-06,LuisValderrama/ Noor Yassin,Created Script for Views
-- Select ALL attributes from DimState
CREATE VIEW vDimState
	AS
		SELECT * FROM DimState;
GO

-- 4. vDimResponseKey reporting view
IF (OBJECT_ID('vDimResponseKey') IS NOT NULL) DROP VIEW vDimResponseKey;
GO
-- Desc: This code
-- Change Log: When,Who,What
-- 2024-05-06,LuisValderrama/ Noor Yassin,Created Script for Views
-- Select ALL attributes from DimResponseKey
CREATE VIEW vDimResponseKey
	AS
		SELECT * FROM DimResponseKey;
GO

-- 5. vFactCovid reporting view
IF (OBJECT_ID('vFactCovid') IS NOT NULL) DROP VIEW vFactCovid;
GO
-- Desc: This code
-- Change Log: When,Who,What
-- 2024-05-06,LuisValderrama/ Noor Yassin,Created Script for Views
-- Select ALL attributes from FactCovid
CREATE VIEW vFactCovid
	AS
		SELECT * FROM FactCovid;
GO

-- 6. vFactResponse reporting view
IF (OBJECT_ID('vFactResponse') IS NOT NULL) DROP VIEW vFactResponse;
GO
-- Desc: This code
-- Change Log: When,Who,What
-- 2024-05-06,LuisValderrama/ Noor Yassin,Created Script for Views
-- Select ALL attributes from FactResponse
CREATE VIEW vFactResponse
	AS
		SELECT * FROM FactResponse;
GO

--********************************************************************--
-- Review Reporting Views
--********************************************************************--

SELECT * FROM vDimCountry;
SELECT * FROM vDimDate;
SELECT * FROM vDimState;
SELECT * FROM vFactCovid;
SELECT * FROM vFactResponse;
SELECT * FROM vDimResponseKey;
SELECT * FROM vETLLog;
GO