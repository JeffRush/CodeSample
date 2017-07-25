--/****** Object:  StoredProcedure [dbo].Import_JnJ_Step12_DuplicateApplicants    Script Date: 12/04/2016 17:03:32 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Import_JnJ_Step12_DuplicateApplicants]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].Import_JnJ_Step12_DuplicateApplicants
GO

/****** Object:  StoredProcedure [dbo].Import_JnJ_Step12_DuplicateApplicants    Script Date: 12/04/2016 17:03:32 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].Import_JnJ_Step12_DuplicateApplicants
(
@BatchID as int
)
WITH RECOMPILE  
AS
BEGIN

-- Delete Existing
create table #ToDelete
(
		PlanDate DateTime
	,	Employer_ID varchar(50)
)

insert into #ToDelete (Employer_ID, PlanDate)

select distinct EmployerID, [Monitoringdate] as EffectiveDate
from stage.ModifiedJnJApplicants

delete fa
from #ToDelete del
inner join DimEmployer de on de.Employer_ID = del.Employer_ID
inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
	and mpy.LookupDate = del.PlanDate
inner join FACT_DuplicateApplicant fa on fa.Employer_Key = de.Employer_Key
	and fa.SnapShotDate = del.PlanDate
	
	
create table #JobCodes
( 
		JobGroupCode varchar(50)
	,	SnapShotDate datetime
	,	Employer_ID varchar(50)
	,	JobGroup_Key int
)

insert into #JobCodes
(JobGroupCode, SnapShotDate, Employer_ID)
select distinct [Req  EEO Job Group], [Monitoringdate], [EmployerID] from stage.ModifiedJnJApplicants


update j
set JobGroup_Key = djg.JobGroup_Key
from
#JobCodes j
	inner join DimEmployer de on de.Employer_ID = j.Employer_ID
	inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
		and mpy.LookupDate = j.SnapShotDate
	inner join DimJobGroup djg on djg.Employer_Key = de.Employer_Key
		and djg.PlanYear = mpy.PlanYear
		and djg.JobGroup_ID = j.JobGroupCode

insert into FACT_DuplicateApplicant (Employer_Key, SnapShotDate, FirstName,LastName,Race_Code,RaceDescription,Gender,RequisitionNumber
,PlanYear,WWID,RequisitionTitle,CandidateFullName,TotalCount,BatchID,ReqState,EEOCategory,JobTitle,JobCode,[Position ID],EmpStatus
,MultiHireReqNo,ApplicantID,CandidateID,AppStep,AppStatus,Disposition,Plan_Key,ReportingLocationCode,JobGroup,ZipCode)

select 
distinct
		de.Employer_Key
	,	MonitoringDate as 'SnapShotDate'
	,	CAST(dbo.FormatName([Candidate Full Name], 'F') AS VARCHAR(50)) as 'FirstName'
	,	CAST(dbo.FormatName([Candidate Full Name], 'L') AS VARCHAR(50)) as 'LastName'
	,	EditedRaceCode as 'Race_Code'
	,	EditedRace as 'RaceDescription'
	,	EditedGender as 'Gender'
	,	cast(isnull([Requisition NO],'Position ID: ' + cast([Position ID] as varchar(20))) as varchar(50)) as 'RequisitionNumber'
	,	mpy.PlanYear
	,	[Employee Number (WWID)] as 'WWID'
	,	[Requisition Title (BL)] as 'RequisitionTitle'
	,	[Candidate Full Name] as 'CandidateFullName'
	,	COUNT(*) as 'TotalCount'
	,	@BatchID
	,	[Position State] as 'ReqState'
	,	[Req  EEO Job Category] as 'EEOCategory'
	,	cast(LTRIM(RTRIM(Job)) as varchar(50))  as 'JobTitle'
	,	[Job Code] as 'JobCode'
	,	[Position ID] as 'Position ID'
	,	[Full Time Part Time] as 'EmpStatus'
	,	[UR Evergreen or Specific Requisition #] as 'MultiHireReqNo'
	,	[Application ID] as 'ApplicantID'
	,	[Candidate ID] as 'CandidateID'
	,	[Application Current CSW Step] as 'AppStep'
	,	[Application Current CSW Status] as 'AppStatus'
	,	[Application Tracking Reject Decline Motives] as 'Disposition'
	,	dp.Plan_Key
	,	cast(LTRIM(RTRIM([Personnel Area])) as varchar(50))  as 'ReportingLocationCode'
	,	cast(LTRIM(RTRIM([Req  EEO Job Group])) as varchar(50)) as 'JobGroup'
	,	ZipCode
	
		 from stage.ModifiedJnJApplicants sa
			inner join DimEmployer de on de.Employer_ID = sa.EmployerID
			inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
				and mpy.LookupDate = sa.[Monitoringdate]
			inner join #JobCodes djg on djg.JobGroupCode = sa.[Req  EEO Job Group]
				and djg.SnapShotDate = sa.[Monitoringdate]
				and djg.Employer_ID = sa.[EmployerID]
			left join Map_Plan_To_Location mptl on mptl.Employer_Key = de.Employer_Key
				and mptl.PlanYear = mpy.PlanYear
				and mptl.ReportingLocationCode = sa.[Personnel Area]
			left join DimPlan dp on dp.Employer_ID = de.Employer_ID
				and dp.JobGroup_ID = sa.[Req  EEO Job Group]
				and dp.PlanCode = mptl.PlanCode	
				and dp.PlanYear = mpy.PlanYear
			
	
		 group by
	 
		de.Employer_Key
	,	MonitoringDate
	,	mpy.PlanYear
	,	dp.Plan_Key
	,	cast(LTRIM(RTRIM([Personnel Area])) as varchar(50))
	,	cast(LTRIM(RTRIM([Req  EEO Job Group])) as varchar(50))
	,	cast(isnull([Requisition NO],'Position ID: ' + cast([Position ID] as varchar(20))) as varchar(50))
	,	[Requisition Title (BL)]
	,	[Position State]
	,	[Req  EEO Job Category]
	,	cast(LTRIM(RTRIM(Job)) as varchar(50))
	,	[Job Code]
	,	[Position ID]
	,	[Full Time Part Time]
	,	[UR Evergreen or Specific Requisition #]
	,	[Candidate Full Name] 
	,	CAST(dbo.FormatName([Candidate Full Name], 'F') AS VARCHAR(50))
	,	CAST(dbo.FormatName([Candidate Full Name], 'L') AS VARCHAR(50))
	,	[Application ID]
	,	[Candidate ID]
	,	[Employee Number (WWID)]
	,	[Application Current CSW Step]
	,	[Application Current CSW Status]
	,	[Application Tracking Reject Decline Motives]
	,	EditedRace
	,	EditedRaceCode
	,	EditedGender
	,	ZipCode
	
	
	having COUNT(*) > 1
	
	
drop table #ToDelete
drop table #JobCodes

END


GO
