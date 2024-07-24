--*************************************************************************--
-- Title: Maroon_Response ETL Process
-- Author: Luis Valderrama/Noor Yassin
-- Desc: This file creates a Flush and Fill ETL process with SQL code
-- Change Log: When,Who,What
-- 2024-05-05,LValderrama,NYassin Created File
--**************************************************************************--
USE [Maroon_Response]
GO

If NOT Exists(Select * From Sys.tables where Name = 'ETLLog')
  Create Table ETLLog
  (ETLLogID int identity Primary Key
  ,ETLDateAndTime datetime Default GetDate()
  ,ETLAction varchar(100)
  ,ETLLogMessage varchar(2000)
  );
go

Create or Alter View vETLLog
As
  Select
   ETLLogID
  ,ETLDate = Format(ETLDateAndTime, 'D', 'en-us')
  ,ETLTime = Format(Cast(ETLDateAndTime as datetime2), 'HH:mm', 'en-us')
  ,ETLAction
  ,ETLLogMessage
  From ETLLog;
go


Create or Alter Proc pInsETLLog
 (@ETLAction varchar(100), @ETLLogMessage varchar(2000))
--*************************************************************************--
-- Desc:This Sproc creates an admin table for logging ETL metadata. 
-- Change Log: When,Who,What
-- 2024-05-09,NYASSIN,created Sproc
--*************************************************************************--
As
Begin
  Declare @ReturnCode int = 0;
  Begin Try
    Begin Tran;
      Insert Into ETLLog
       (ETLAction,ETLLogMessage)
      Values
       (@ETLAction,@ETLLogMessage)
    Commit Tran;
    Set @ReturnCode = 1;
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Set @ReturnCode = -1;
  End Catch
  Return @ReturnCode;
End
Go
-------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE OR ALTER PROCEDURE [dbo].[sp_FactCovid]
AS
/**************
*Developer: Luis Valderrama/ Noor Yassin
*Date: 2024-05-06
*Description: ETL Process for Raw Fact Covid 
*****************/
BEGIN
  Declare @ReturnCode int = 0;
  Begin Try
    -- ETL Processing Code --
    Begin Tran;
--Step 1: Drop Table
	DROP TABLE IF EXISTS [dbo].[FactCovid]

--Step 2: Create Table with updated types
		CREATE TABLE [dbo].[FactCovid]
		(
			FactCovid_Key int IDENTITY (1,1) --future Surrogate Key
			,ID INT
			,Updated date
			,Confirmed int
			,Confirmed_Change int
			,Deaths int
			,Deaths_Change int
			,Recovered int
			,Recovered_Change int
			,Latitude nvarchar(50)
			,Longitude nvarchar(50)
			,Iso2 nvarchar(50)
			,Iso3  nvarchar(50)
			,Country_Region nvarchar(50)
			,Admin_Region_1 nvarchar(50)
			,Iso_Subdivision nvarchar(50)
			,Admin_Region_2 nvarchar(50)
			--,Load_Time date
		)

--Step 3:Write ETL fixing datatypes
		INSERT INTO [dbo].[FactCovid]
			SELECT
			--TOP 10  --Please start with TOP 10 rows. This will speed up your attempts
			CAST([id] as INT)
			, CAST([updated] as date)
			, CAST([confirmed] as INT)
			, CAST([confirmed_change] as INT)
			, CAST([deaths] as INT)
			, CAST([deaths_change] as INT)
			, CAST([recovered] as INT)
			, CAST([recovered_change] as INT)
			, [latitude]
			, [longitude]
			, [iso2]
			, [iso3]
			, [country_region]
			, [admin_region_1]
			, [iso_subdivision]
			, [admin_region_2]
			--, CAST([load_time] as date)
		FROM [Maroon_Response].[dbo].[Raw_BingCovid] RAW WITH (NOLOCK);
Commit Tran;
    -- ETL Logging Code --
    Exec pInsETLLog
	        @ETLAction = 'sp_FactCovid'
	       ,@ETLLogMessage = 'FactCovid Filled';
    Set @ReturnCode = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'sp_FactCovid'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @ReturnCode = -1
  End Catch
  Return @ReturnCode;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = sp_FactCovid;
 Print @Status;
 Select * From factcovid 
 select * from vETLLog
*/
------------------------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE [dbo].[sp_FactResponse]

AS
/**************
*Developer: Luis Valderrama/ Noor Yassin
*Date: 2024-05-06
*Description: ETL FactResponse
***************/
BEGIN
Declare @ReturnCode int = 0;
  Begin Try
    -- ETL Processing Code --
    Begin Tran;
--Step 1: Drop Table
	DROP TABLE IF EXISTS [dbo].[FactResponse]

--Step 2: Create Table with updated types
		CREATE TABLE [dbo].[FactResponse]
		(
			 [FactResponse_Key] INT IDENTITY (1,1) --Surrogate Key
			,[CountryName] NVARCHAR(50)
			,[CountryCode] NVARCHAR(50)
			,[State] NVARCHAR(50) --This will load data from RegionName
			,[RegionCode] NVARCHAR(50)
			,[Jurisdiction] NVARCHAR(50)
			,[DATE] DATE
			,[SchoolClosure] INT --This will load data from C1_School closing	
			,[WorkplaceClosure] INT --This will load data from C2_Workplace closing			
			,[CancelPublicEvents] INT --This will load data from C3_Cancel public events		
			,[RestrictionOnGatherings] INT --This will load data from C4_Restrictions on gatherings		
			,[ClosePublicTransport] INT --This will load data from C5_Close public transport			

		)

--Step 3:Write ETL fixing datatypes
		INSERT INTO [dbo].[FactResponse]
			SELECT  
			   [CountryName]
			  ,[CountryCode]
			  ,[RegionName]
			  ,[RegionCode]
			  ,[Jurisdiction]
			  ,CAST([Date] As DATE)
			  ,CONVERT(FLOAT,[C1_School closing])AS SchoolClosure
			  ,CONVERT(FLOAT,[C2_Workplace closing])AS WorkplaceClosure
			  ,CONVERT(FLOAT,[C3_Cancel public events])AS CancelPublicEvents
			  ,CONVERT(FLOAT,[C4_Restrictions on gatherings])AS RestrictionOnGatherings
			  ,CONVERT(FLOAT,[C5_Close public transport])AS ClosePublicTransport
			FROM [Maroon_Response].[dbo].[Raw_CovidPolicyTracker] RAW WITH (NOLOCK);

Commit Tran;
    -- ETL Logging Code --
    Exec pInsETLLog
	        @ETLAction = 'sp_FactResponse'
	       ,@ETLLogMessage = 'FactResponse Filled';
    Set @ReturnCode = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'sp_FactResponse'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @ReturnCode = -1
  End Catch
  Return @ReturnCode;
 End
go

/* Testing Code:
 Declare @Status int;
 Exec @Status = sp_FactResponse;
 Print @Status;
 Select * From factResponse
 select * from vETLLog
*/
------------------------------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[sp_DimResponseKey]

AS
/**************
*Developer: Luis Valderrama/ Noor Yassin
*Date: 2024-05-06
*Description: ETL DimResponseKey
***************/
BEGIN
Declare @ReturnCode int = 0;
  Begin Try
    -- ETL Processing Code --
    Begin Tran;

--Step 1: Drop Table
	DROP TABLE IF EXISTS [dbo].[DimResponseKey]

--Step 2: Create Table with updated types
		CREATE TABLE [dbo].[DimResponseKey]
		(
			 [DimResponse_Key] INT IDENTITY (1,1) --Surrogate Key
			,[C1_School closing] INT
			,[SchoolClosureRec] NVARCHAR(50) --This will load data from C1_School closing key.CovidPolicyTrackerRef	
			,[C2 Workplace closing] INT
			,[WorkplaceClosureRec] NVARCHAR(50) --This will load data from C2 Workplace closing key.CovidPolicyTrackerRef	
			,[C3 Cancel Public Event] INT
			,[CancelPublicEventRec] NVARCHAR(50) --This will load data from C3 Cancel Public Event key.CovidPolicyTrackerRef	
			,[C4 Restriction on Gathering] INT
			,[RestrictionOnGatheringRec] NVARCHAR(100) --This will load data from C4 Restriction on Gathering key.CovidPolicyTrackerRef	
			,[C5 Close Public Transport] INT
			,[ClosePublicTransportRec] NVARCHAR(50) --This will load data from C5 Close Public Transport key.CovidPolicyTrackerRef	
		)

--Step 3:Write ETL fixing datatypes
		INSERT INTO [dbo].[DimResponseKey]
			SELECT  
			  CONVERT(INT,[C1 School closing])AS C1_School_Closing			  
			 ,[C1 School closing key]
			 ,CONVERT(INT,[C2 Workplace closing])AS C2_Workplace_closing
			 ,[C2 Workplace closing Key]
			 ,CONVERT(INT,[C3 Cancel Public Event])AS C3_Cancel_Public_Event
			 ,[C3 Cancel Public Event Key]
			 ,CONVERT(INT,[C4 Restriction on Gathering])AS C4_Restriction_on_Gathering
			 ,[C4 Restriction on Gathering Key]
			 ,CONVERT(INT,[C5 Close Public Transport])AS C5_Close_Public_Transport
			 ,[C5 Close public transport key]
			FROM [Maroon_Response].[dbo].[Raw_CovidPolicyTrackerRef] RAW WITH (NOLOCK);

Commit Tran;
    -- ETL Logging Code --
    Exec pInsETLLog
	        @ETLAction = 'sp_DimResponseKey'
	       ,@ETLLogMessage = 'DimResponseKey Filled';
    Set @ReturnCode = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'sp_DimResponseKey'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @ReturnCode = -1
  End Catch
  Return @ReturnCode;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = sp_DimResponseKey;
 Print @Status;
 Select * From DimResponseKey 
 select * from vETLLog
*/
-------------------------------------------------------------------------------------------------------------------------
go

CREATE OR ALTER PROCEDURE [dbo].[sp_DimState]
AS
/***********
*Developer: Luis Valderrama/ Noor Yassin
*Date: 2024-05-06
*Description: ETL Dim State using FactResponse
************/
BEGIN
Declare @ReturnCode int = 0;
  Begin Try
    -- ETL Processing Code --
    Begin Tran;
--Step 1: Drop Table
	DROP TABLE IF EXISTS [dbo].[DimState]

--Step 2: Create Table with updated types
		CREATE TABLE [dbo].[DimState]
		(
			DimState_Key int IDENTITY (1,1) --future Surrogate Key
			,State nvarchar(50)
		)

--Step 3:Write ETL fixing datatypes
	INSERT INTO [dbo].[DimState]
		SELECT DISTINCT --Please start with TOP 10 rows. This will speed up your attempts
		[State]
		FROM [Maroon_Response].[dbo].[FactResponse] FACT WITH (NOLOCK);
Commit Tran;
    -- ETL Logging Code --
    Exec pInsETLLog
	        @ETLAction = 'sp_DimState'
	       ,@ETLLogMessage = 'DimState Filled';
    Set @ReturnCode = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'sp_DimState'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @ReturnCode = -1
  End Catch
  Return @ReturnCode;
 End
go

/* Testing Code:
 Declare @Status int;
 Exec @Status = sp_DimState;
 Print @Status;
 Select * From Dimstate 
 select * from vETLLog
 */
----------------------------------------------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE [dbo].[sp_DimCountry]
AS
/***********
*Developer: Luis Valderrama/ Noor Yassin
*Date: 2024-05-06
*Description: ETL Dim Country using Fact Covid
************/
BEGIN
Declare @ReturnCode int = 0;
  Begin Try
    -- ETL Processing Code --
    Begin Tran;
--Step 1: Drop Table
	DROP TABLE IF EXISTS [dbo].[DimCountry]

--Step 2: Create table with updated types
		CREATE TABLE [dbo].[DimCountry] 
		(
			DimID INT IDENTITY (1, 1),
			[Country] NVARCHAR (100)
		);

--Step 3: Write ETL fixing datatypes
		INSERT INTO [dbo].[DimCountry]
		SELECT DISTINCT [Country_Region]
		FROM   [Maroon_Response].[dbo].[FactCovid];
Commit Tran;
    -- ETL Logging Code --
    Exec pInsETLLog
	        @ETLAction = 'sp_DimCountry'
	       ,@ETLLogMessage = 'DimCountry Filled';
    Set @ReturnCode = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'sp_DimCountry'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @ReturnCode = -1
  End Catch
  Return @ReturnCode;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = sp_DimCountry;
 Print @Status;
 Select * From DimCountry 
*/
----------------------------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE [dbo].[sp_DimDate]
AS
/**************
*Developer: Luis Valderrama/ Noor Yassin
*Date: 2024-05-06
*Descrption: DimDate
********************/
BEGIN
Declare @ReturnCode int = 0;
  Begin Try
    -- ETL Processing Code --
    Begin Tran;
--declare when to start
DECLARE @StartDate  date = '20200101'; --Covid cases started roughly this date
		
--declare when to stop
DECLARE @CutoffDate date = GETDATE(); --We could stop when the dataset stopped refreshing. Perhaps this could be an update
		
--create table
	DROP TABLE IF EXISTS DimDate
		CREATE TABLE DimDate
		(
			TheDate date
			,TheDay int
			,TheDayName nvarchar(100)
			,TheWeek  int
			,TheISOWeek int
			,TheDayOfWeek int
			,TheMonth int
			,TheMonthName nvarchar(100)
			,TheQuarter int
			,TheYear int
			,TheFirstOfMonth date
			,TheLastOfYear date
			,TheDayOfYear int
		)
		
--write code to get all the useable calendar 
		;WITH seq(n) AS 
		(
		SELECT 0 UNION ALL SELECT n + 1 FROM seq
		WHERE n < DATEDIFF(DAY, @StartDate, @CutoffDate)
		),
		d(d) AS 
		(
		SELECT DATEADD(DAY, n, @StartDate) FROM seq
		),
		src AS
		(
		SELECT
			TheDate         = CONVERT(date, d),
			TheDay          = DATEPART(DAY,       d),
			TheDayName      = DATENAME(WEEKDAY,   d),
			TheWeek         = DATEPART(WEEK,      d),
			TheISOWeek      = DATEPART(ISO_WEEK,  d),
			TheDayOfWeek    = DATEPART(WEEKDAY,   d),
			TheMonth        = DATEPART(MONTH,     d),
			TheMonthName    = DATENAME(MONTH,     d),
			TheQuarter      = DATEPART(Quarter,   d),
			TheYear         = DATEPART(YEAR,      d),
			TheFirstOfMonth = DATEFROMPARTS(YEAR(d), MONTH(d), 1),
			TheLastOfYear   = DATEFROMPARTS(YEAR(d), 12, 31),
			TheDayOfYear    = DATEPART(DAYOFYEAR, d)
		FROM d
		)
		
		INSERT INTO DimDate
		SELECT * FROM src
		ORDER BY TheDate
		OPTION (MAXRECURSION 0);
 Commit Tran;
       
    Exec pInsETLLog
	        @ETLAction = 'sp_DimDate'
	       ,@ETLLogMessage = 'DimDates filled';
    Set @ReturnCode = +1
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Declare @ErrorMessage nvarchar(1000) = Error_Message();
	  Exec pInsETLLog 
	     @ETLAction = 'sp_DimDate'
	    ,@ETLLogMessage = @ErrorMessage;
    Set @ReturnCode = -1;
  End Catch
  Return @ReturnCode;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = sp_DimDate;
 Print @Status;
 Select * From DimDate;
 Select * From vETLLog;
*/
---------------------------------------------------------------------------------------------------------------------------------
go


----Execute Sprocs
--EXECUTE [Maroon_Response].[dbo].[sp_FactResponse]
--EXECUTE [Maroon_Response].[dbo].[sp_FactCovid]
--EXECUTE [Maroon_Response].[dbo].[sp_DimResponseKey]
--EXECUTE [Maroon_Response].[dbo].[sp_DimDate]
--EXECUTE [Maroon_Response].[dbo].[sp_DimCountry]
--EXECUTE [Maroon_Response].[dbo].[sp_DimState]
----GO

----Drop Sprocs before exporting the backup file
--DROP PROCEDURE [dbo].[sp_FactResponse]
--DROP PROCEDURE [dbo].[sp_FactCovid]
--DROP PROCEDURE [dbo].[sp_DimResponseKey]
--DROP PROCEDURE [dbo].[sp_DimDate]
--DROP PROCEDURE [dbo].[sp_DimCountry]
--DROP PROCEDURE [dbo].[sp_DimState]
----GO

---- Select Statement
--Select * from [Maroon_Response].[dbo].[FactResponse]
--Select * from [Maroon_Response].[dbo].[FactCovid]
--Select * from [Maroon_Response].[dbo].[DimResponseKey]
--Select * from [Maroon_Response].[dbo].[DimDate]
--Select * from [Maroon_Response].[dbo].[DimCountry]
--Select * from [Maroon_Response].[dbo].[DimState]
----GO