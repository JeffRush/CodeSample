IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Import_JnJ_Step6_DimJobGroup]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].Import_JnJ_Step6_DimJobGroup
GO

create proc Import_JnJ_Step6_DimJobGroup
(
@BatchID as int
)
AS
BEGIN

create table #ToDelete
(
		PlanDate DateTime
	,	Employer_ID varchar(50)
)

insert into #ToDelete (PlanDate, Employer_ID)
select distinct SnapshotDate, EmployerID from stage.JnJEmployees

-- Delete existing

delete djg
from #ToDelete del
	inner join DimEmployer de on de.Employer_ID = del.Employer_ID
	inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
		and mpy.LookupDate = del.PlanDate
	inner join DimJobGroup djg on djg.Employer_Key = de.Employer_Key
		and djg.PlanYear = mpy.PlanYear



insert into DimJobGroup (Employer_Key, JobGroup_ID, JobGroupDescription, PlanYear, PlanDate, BatchID)

select de.Employer_Key, JobGroup, JobGroupDescription, mpy.PlanYear, SnapShotDate, @BatchID
from
(

	select distinct
			LTRIM(RTRIM(EmployerID)) as EmployerID
		,	SnapShotDate
		,	cast(LTRIM(RTRIM(Replace(JobGroup,'"',''))) as varchar(10)) as JobGroup
		,	 case 
				when left(JobGroupDesc,LEN(JobGroup)) = JobGroup then JobGroupDesc
				else coalesce(JobGroup, '') + ' - ' + coalesce(JobGroupDesc, '')
			end as JobGroupDescription
		
		from stage.JnJEmployees
	union
	select distinct
			LTRIM(RTRIM(EmployerID)) as EmployerID
		,	SnapShotDate
		,	cast(LTRIM(RTRIM(Replace(JobGroup,'"',''))) as varchar(10)) as JobGroup
		,	 case 
				when left(JobGroupDesc,LEN(JobGroup)) = JobGroup then JobGroupDesc
				else coalesce(JobGroup, '') + ' - ' + coalesce(JobGroupDesc, '')
			end as JobGroupDescription
		
		from stage.JnJSnapshotAcquisition
) as Result
inner join DimEmployer de on de.Employer_ID = Result.EmployerID
inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
	and mpy.LookupDate = REsult.SnapShotDate
	
order by SnapshotDate

drop table #ToDelete

END

