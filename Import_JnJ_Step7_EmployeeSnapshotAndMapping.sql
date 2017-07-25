IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Import_JnJ_Step7_EmployeeSnapshotAndMapping]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].Import_JnJ_Step7_EmployeeSnapshotAndMapping
GO

create proc Import_JnJ_Step7_EmployeeSnapshotAndMapping
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

insert into #ToDelete (PlanDate, Employer_ID)
select distinct SnapshotDate, EmployerID from stage.JnJEmployees



delete fes
from
	#ToDelete del
	inner join DimEmployer de on de.Employer_ID = del.Employer_ID
	inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
		and mpy.LookupDate = del.PlanDate
	inner join FACT_EmployeeSnapshot fes on fes.Employer_Key = de.Employer_Key
		and fes.PlanYear = mpy.PlanYear
		
delete mptl
from
	#ToDelete del
	inner join DimEmployer de on de.Employer_ID = del.Employer_ID
	inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
		and mpy.LookupDate = del.PlanDate
	inner join Map_Plan_To_Location mptl on mptl.Employer_Key = de.Employer_Key
		and mptl.PlanYear = mpy.PlanYear

-- Insert New
--		Employees

insert into FACT_EmployeeSnapshot (Plan_Key, Employer_Key, Employee_Key, SnapshotDate, PlanCode, ReportingLocationCode, JobGroup_ID, JobGroupDescription, Job_ID, JobDescription
,	Salary, SalaryGrade, CensusCode, PlanYear, BatchID, JobGroup_Key, Disabled, ProtectedVeteran)

select  distinct

		dp.Plan_Key
	,	de.Employer_Key
	,	demp.Employee_Key
	,	sse.SnapShotDate
	,	dp.PlanCode
	,	sse.ReportingLocCode
	,	sse.JobGroup
	,	sse.JobGroupDesc
	,	sse.JobCode
	,	sse.JobTitle
	,	case when sse.Salary = '' then null else sse.Salary end
	,	left(sse.SalaryGrade,10)
	,	sse.Census
	,	mpy.PlanYear
	,	@BatchID as 'BatchID'
	,	djg.JobGroup_Key as 'JobGroup_Key'
	,	case
			when sse.Disabled = '' then null
			else sse.Disabled
		end as Disabled
	,	case
			when sse.ProtectedVeteran = '' then null
			else sse.ProtectedVeteran
		end as ProtectedVeteran
	from stage.JnJEmployees sse
		inner join DimEmployer de on de.Employer_ID = sse.EmployerID
		inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
			and mpy.LookupDate = sse.SnapShotDate
		inner join DimPlan dp on dp.Employer_ID = sse.EmployerID
			and dp.PlanCode = sse.PlanName
			and dp.JobGroup_ID = sse.JobGroup
			and dp.PlanYear = mpy.PlanYear
		inner join DimEmployee demp on demp.Employer_Key = de.Employer_Key
			and demp.Employee_ID = sse.EmpID
			and demp.RowIsCurrent = 'Y'
		inner join DimJobGroup djg on djg.Employer_Key = de.Employer_Key
			and djg.JobGroup_ID = sse.JobGroup
			and djg.PlanYear = mpy.PlanYear

		
		

--===========================================
--  ACQUISITION FILE / MATERIALIZED PLANS  --
--===========================================


-- Materialize Plans
insert into DimPlan (Employer_ID, PlanDate, PlanCode, JobGroup_ID, PlanYear, BatchID, Materialized, AcquisitionPlan)
select distinct

		de.Employer_ID
	,	ssa.SnapShotDate as 'PlanDate'
	,	ssa.PlanName
	,	ssa.JobGroup
	,	mpy.PlanYear
	,	@BatchID as 'BatchID'
	,	1
	,	1
	
	from stage.JnJSnapshotAcquisition ssa
		inner join DimEmployer de on de.Employer_ID = ssa.EmployerID
		inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
			and mpy.LookupDate = ssa.SnapShotDate
		left join DimPlan dp on dp.Employer_ID = ssa.EmployerID
			and dp.PlanCode = ssa.PlanName
			and dp.JobGroup_ID = ssa.JobGroup

		where dp.Employer_ID is null

-- Insert Acquisition Employees

insert into FACT_EmployeeSnapshot (Plan_Key, Employer_Key, Employee_Key, SnapshotDate, PlanCode, ReportingLocationCode, JobGroup_ID, JobGroupDescription, Job_ID, JobDescription
,	Salary, SalaryGrade, CensusCode, PlanYear, BatchID, JobGroup_Key, Disabled, ProtectedVeteran)
select  distinct

		dp.Plan_Key
	,	de.Employer_Key
	,	demp.Employee_Key
	,	ssa.SnapShotDate
	,	dp.PlanCode
	,	ssa.ReportingLocCode
	,	ssa.JobGroup
	,	ssa.JobGroupDesc
	,	ssa.JobCode
	,	ssa.JobTitle
	,	case when ssa.Salary = '' then null else ssa.Salary end
	,	left(ssa.SalaryGrade,10)
	,	ssa.Census
	,	mpy.PlanYear
	,	@BatchID as 'BatchID'
	,	djg.JobGroup_Key as 'JobGroup_Key'
	,	case
			when ssa.Disabled = '' then null
			else ssa.Disabled
		end as Disabled
	,	case
			when ssa.ProtectedVeteran = '' then null
			else ssa.ProtectedVeteran
		end as ProtectedVeteran
	from stage.JnJSnapshotAcquisition ssa
		inner join DimEmployer de on de.Employer_ID = ssa.EmployerID
		inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
			and mpy.LookupDate = ssa.SnapShotDate
		inner join DimPlan dp on dp.Employer_ID = ssa.EmployerID
			and dp.PlanCode = ssa.PlanName
			and dp.JobGroup_ID = ssa.JobGroup
		inner join DimEmployee demp on demp.Employer_Key = de.Employer_Key
			and demp.Employee_ID =  ssa.EmpID
			and demp.RowIsCurrent = 'Y'
		inner join DimJobGroup djg on djg.Employer_Key = de.Employer_Key
			and djg.JobGroup_ID = ssa.JobGroup
			and djg.PlanYear = mpy.PlanYear

--======================
-- Location / Mapping
--======================

		
-- Insert New
--		Map Entries
create table #MapEntries
( 
		EmployerID varchar(50)
	,	SnapShotDate datetime
	,	ReportingLocCode varchar(255)
	,	PlanName varchar(255)
)
insert into #MapEntries

select distinct 
		EmployerID
	,	SnapShotDate
	,	Replace(ReportingLocCode,'"','') as ReportingLocCode
	,	Replace(PlanName,'"','') as 'PlanName'

	
	from stage.JnJEmployees

union

select distinct
		EmployerID
	,	SnapShotDate
	,	Replace(ReportingLocCode,'"','') as ReportingLocCode
	,	Replace(PlanName,'"','') as 'PlanName'

	from stage.JnJSnapshotAcquisition
union

select distinct
		EmployerID
	,	SnapShotDate
	,	Replace(LocationCode,'"','') as ReportingLocCode
	,	Replace([Plan],'"','') as 'PlanName'

	from stage.ModifiedJnJLocation

insert into Map_Plan_To_Location (Employer_Key, SnapshotDate, PlanCode, ReportingLocationCode, PlanYear, BatchID)
select de.Employer_Key, me.SnapShotDate, me.PlanName, me.ReportingLocCode, mpy.PlanYear, @BatchID
	from #MapEntries me
		inner join DimEmployer de on de.Employer_ID = me.EmployerID
		inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
			and mpy.LookupDate = me.SnapShotDate
		left join Map_Plan_To_Location mptl on mptl.Employer_Key = de.Employer_Key
			and mptl.PlanYear = mpy.PlanYear
			and mptl.PlanCode = me.PlanName
			and mptl.ReportingLocationCode = me.ReportingLocCode
			
		where mptl.Employer_Key is null



drop table #ToDelete
drop table #MapEntries

END
