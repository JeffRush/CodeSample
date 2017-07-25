--/****** Object:  StoredProcedure [dbo].[Import_JnJ_Step9_Applicants]    Script Date: 12/04/2016 17:03:32 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Import_JnJ_Step9_Applicants]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Import_JnJ_Step9_Applicants]
GO

/****** Object:  StoredProcedure [dbo].[Import_JnJ_Step9_Applicants]    Script Date: 12/04/2016 17:03:32 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[Import_JnJ_Step9_Applicants]
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
inner join FACT_Applicant fa on fa.Employer_Key = de.Employer_Key
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



create table #TempApps
(
		Employer_Key int
	,	SnapShotDate datetime
	,	FirstName varchar(50)
	,	LastName varchar(50)
	,	Race_Code char(1)
	,	RaceDescription varchar(20)
	,	Gender varchar(7)
	,	JobTitle varchar(100)
	,	ReferralSource varchar(50)
	,	Disposition varchar(250)
	,	RequisitionNumber varchar(50)
	,	Recruiter varchar(50)
	,	JobGroup varchar(50)
	,	PlanYear int
	,	Plan_Key int
	,	ReportingLocationCode varchar(50)
	,	ApplicantID varchar(50)
	,	WWID varchar(50)
	,	IsInternal bit
	,	BatchID int
	,	Materialized bit
	,	MultiHireReqNo varchar(25)
	,	PositionID varchar(50)
	,	RequisitionTitle varchar(255)
	,	JobCode varchar(50)
	
	,	JobGroup_Key int
	,	Disabled varchar(1)
	,	ProtectedVeteran varchar(1)
	,	HiringManager varchar(100)
	,	ReqState varchar(25)
	,	EEOCategory varchar(100)
	,	EmpStatus varchar(100)
	,	CandidateID varchar(100)
	,	LatestReviewDate date
	,	OfferDate date
	,	HiredDate date
	,	StartDate date
	,	AppStep varchar(100)
	,	AppStatus varchar(100)
	,	HRContact varchar(100)
	,	UDF1 varchar(100)
	,	UDF2 varchar(100)
	,	UDF3 varchar(100)
	,	UDF4 varchar(100)
	,	UDF5 varchar(100)
	,	UDF6 varchar(100)
	,	UDF7 varchar(100)
	,	UDF8 varchar(100)
	,	UDF9 varchar(100)
	,	UDF10 varchar(100)
	,	NonCompetitive bit
	,	ZipCode varchar(5)	
	)
	


insert into #TempApps (Employer_Key, SnapShotDate, FirstName, LastName,Race_Code,RaceDescription,Gender,JobTitle,ReferralSource,Disposition,RequisitionNumber,Recruiter
,JobGroup,PlanYear, Plan_Key,ReportingLocationCode,ApplicantID,WWID,IsInternal,BatchID,Materialized,MultiHireReqNo,PositionID,RequisitionTitle,JobCode,JobGroup_Key
,Disabled,ProtectedVeteran,HiringManager,ReqState,EEOCategory,EmpStatus,CandidateID,LatestReviewDate,OfferDate,HiredDate,StartDate,AppStep,AppStatus,HRContact
,UDF1,UDF2,UDF3,UDF4,UDF5,UDF6,UDF7,UDF8,UDF9,UDF10,NonCompetitive,ZipCode)

select 
distinct
		de.Employer_Key
	,	MonitoringDate as 'SnapShotDate'
	,	CAST(dbo.FormatName([Candidate Full Name], 'F') AS VARCHAR(50)) as 'FirstName'
	,	CAST(dbo.FormatName([Candidate Full Name], 'L') AS VARCHAR(50)) as 'LastName'
	,	EditedRaceCode as 'Race_Code'
	,	EditedRace as 'RaceDescription'
	,	EditedGender as 'Gender'
	,	Job  as 'JobTitle'
	,	Max([Application Source (BL)]) as 'ReferralSource'
	,	[Application Tracking Reject Decline Motives] as 'Disposition'
	,	cast(isnull([Requisition NO],'Position ID: ' + cast([Position ID] as varchar(20))) as varchar(50)) as 'RequisitionNumber'
	,	Max([Req  Recruiter Name]) as 'Recruiter'
	,	[Req  EEO Job Group] as 'JobGroup'
	,	mpy.PlanYear as 'PlanYear'
	,	dp.Plan_Key as 'Plan_Key'
	,	[Personnel Area]  as 'ReportingLocationCode'
	,	[Application ID] as 'ApplicantID'
	,	[Employee Number (WWID)] as 'WWID'
	,	CONVERT(BIT,CASE UPPER([Application Is Internal]) WHEN 'YES' THEN 1 ELSE 0 END)	AS IsInternal
	,	@BatchID
	,	isnull(sa.Materialized, 0) as 'Materialized'
	,	[UR Evergreen or Specific Requisition #] as 'MultiHireReqNo'
	,	[Position ID] as 'Position ID'
	,	[Requisition Title (BL)] as 'RequisitionTitle'
	,	[Job Code] as 'JobCode'
	--,	djg.JobGroup_Key as 'JobGroup_Key'
	,	null as 'JobGroup_Key'
	,	case
			when [Candidate Is Disabled] = 'Disability - Yes' then 'Y'
			else null
		end as 'Disabled'
	,	case
			when [Candidate Is Other Protected Veteran] = 'Protected Veteran - Yes' then 'Y'
			else null
		end as 'ProtectedVet'
	,	Max([Req  Hiring Manager Name]) as 'HiringManager'
	,	[Position State] as 'ReqState'
	,	[Req  EEO Job Category] as 'EEOCategory'
	,	[Full Time Part Time] as 'EmpStatus'
	,	[Candidate ID] as 'CandidateID'
	,	Max([Application Latest Review Date]) as 'LatestReviewDate'
	,	Max([Application Latest Offer Date]) as 'OfferDate'
	,	Max([Application Latest Hired Date]) as 'HiredDate'
	,	Max([Application Hire Start Date]) as 'StartDate'
	,	[Application Current CSW Step] as 'AppStep'
	,	[Application Current CSW Status] as 'AppStatus'
	,	Max(BBHR) as 'HRContact'
	,	Max([Legal Entity]) as 'UDF1'
	,	Max([Employee Group Subgroup]) as 'UDF2'
	,	Max([Req  EEO Job Group])  as 'UDF3'
	,	Max([Leadership Development Program (LDP)]) as 'UDF4'
	,	Max([Cost Center Number]) as 'UDF5'
	,	Max([MRC Code]) as 'UDF6'
	,	Max([Application Source Type]) as 'UDF7'
	,	null as 'UDF8'
	,	null as 'UDF9'
	,	null as 'UDF10'
	,	isnull(Inline_Promo,0) as 'NonCompetative'
	, ZipCode
		
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
	,	mpy.PlanYear
	,	dp.Plan_Key
	,	djg.JobGroup_Key
	,	MonitoringDate
	,	cast(isnull([Requisition NO],'Position ID: ' + cast([Position ID] as varchar(20))) as varchar(50))
	,	[Requisition Title (BL)]
	,	[Position State]
	,	[Req  EEO Job Category]
	,	Job
	,	[Job Code]
	,	[Position ID]
	,	[Full Time Part Time]
	,	[UR Evergreen or Specific Requisition #]
	,	CAST(dbo.FormatName([Candidate Full Name], 'F') AS VARCHAR(50))
	,	CAST(dbo.FormatName([Candidate Full Name], 'L') AS VARCHAR(50))
	,	[Application ID]
	,	[Candidate ID]
	,	CONVERT(BIT,CASE UPPER([Application Is Internal]) WHEN 'YES' THEN 1 ELSE 0 END)
	,	[Employee Number (WWID)]
	,	[Application Current CSW Step]
	,	[Application Current CSW Status]
	,	[Application Tracking Reject Decline Motives]
	,	EditedRace
	,	EditedRaceCode
	,	EditedGender
	,	isnull(Inline_Promo,0)
	,	isnull(sa.Materialized, 0)
	,	[Personnel Area]
	,	[Req  EEO Job Group]
	,	case
			when [Candidate Is Disabled] = 'Disability - Yes' then 'Y'
			else null
		end
	,	case
			when [Candidate Is Other Protected Veteran] = 'Protected Veteran - Yes' then 'Y'
			else null
		end
	, ZipCode
	
	


insert into FACT_Applicant
(
	 Employer_Key
	,SnapShotDate
	,FirstName
	,LastName
	,Race_Code
	,RaceDescription
	,Gender
	,JobTitle
	,ReferralSource
	,Disposition
	,RequisitionNumber
	,Recruiter
	,JobGroup
	,PlanYear
	,Plan_Key
	,ReportingLocationCode
	,ApplicantID
	,WWID
	,IsInternal
	,BatchID
	,Materialized
	,MultiHireReqNo
	,PositionID
	,RequisitionTitle
	,JobCode
	,JobGroup_Key
	,Disabled
	,ProtectedVeteran
	,HiringManager
	,ReqState
	,EEOCategory
	,EmpStatus
	,CandidateID
	,LatestReviewDate
	,OfferDate
	,HiredDate
	,StartDate
	,AppStep
	,AppStatus
	,HRContact
	,UDF1
	,UDF2
	,UDF3
	,UDF4
	,UDF5
	,UDF6
	,UDF7
	,UDF8
	,UDF9
	,UDF10
	,NonCompetitive
	,ZipCode
)

select distinct
	 Employer_Key
	,SnapShotDate
	,FirstName
	,LastName
	,Race_Code
	,RaceDescription
	,Gender
	,JobTitle
	,ReferralSource
	,Disposition
	,RequisitionNumber
	,Recruiter
	,JobGroup
	,PlanYear
	,Plan_Key
	,ReportingLocationCode
	,ApplicantID
	,WWID
	,IsInternal
	,BatchID
	,Materialized
	,MultiHireReqNo
	,PositionID
	,RequisitionTitle
	,JobCode
	,JobGroup_Key
	,Disabled
	,ProtectedVeteran
	,HiringManager
	,ReqState
	,EEOCategory
	,EmpStatus
	,CandidateID
	,LatestReviewDate
	,OfferDate
	,HiredDate
	,StartDate
	,AppStep
	,AppStatus
	,HRContact
	,UDF1
	,UDF2
	,UDF3
	,UDF4
	,UDF5
	,UDF6
	,UDF7
	,UDF8
	,UDF9
	,UDF10
	,NonCompetitive
	,ZipCode
	
	from #TempApps
  

drop table #ToDelete
drop table #TempApps
drop table #JobCodes

END


GO
