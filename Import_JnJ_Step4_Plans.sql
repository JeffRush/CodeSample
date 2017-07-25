IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Import_JnJ_Step4_Plans]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].Import_JnJ_Step4_Plans
GO

create proc Import_JnJ_Step4_Plans
(
@BatchID as int
)
WITH RECOMPILE  
AS
BEGIN


create table #ToDelete
(
		PlanDate DateTime
	,	Employer_ID varchar(50)
)

insert into #ToDelete (PlanDate, Employer_ID)
select 
	distinct	
	PlanDate, EmployerID
	from stage.JnJPlanGoals
	
-- Delete existing ties

delete fas
from	#ToDelete del

	inner join DimEmployer de on de.Employer_ID = del.Employer_ID
	inner join DimPlanYear dpy on dpy.Employer_Key = de.Employer_Key
		and dpy.BeginDate = del.PlanDate
	inner join FACT_ActivitySummary fas on fas.Employer_Key = de.Employer_Key
		and fas.PlanYear = dpy.PlanYear

delete fa
from	#ToDelete del

	inner join DimEmployer de on de.Employer_ID = del.Employer_ID
	inner join DimPlanYear dpy on dpy.Employer_Key = de.Employer_Key
		and dpy.BeginDate = del.PlanDate
	inner join FACT_Applicant fa on fa.Employer_Key = de.Employer_Key
		and fa.PlanYear = dpy.PlanYear

delete fea
from	#ToDelete del

	inner join DimEmployer de on de.Employer_ID = del.Employer_ID
	inner join DimPlanYear dpy on dpy.Employer_Key = de.Employer_Key
		and dpy.BeginDate = del.PlanDate
	inner join FACT_EmployeeActivity fea on fea.Employer_Key = de.Employer_Key
		and fea.PlanYear = dpy.PlanYear

delete fes
from	#ToDelete del

	inner join DimEmployer de on de.Employer_ID = del.Employer_ID
	inner join DimPlanYear dpy on dpy.Employer_Key = de.Employer_Key
		and dpy.BeginDate = del.PlanDate
	inner join FACT_EmployeeSnapshot fes on fes.Employer_Key = de.Employer_Key
		and fes.PlanYear = dpy.PlanYear
		
delete ffps
from	#ToDelete del

	inner join DimEmployer de on de.Employer_ID = del.Employer_ID
	inner join DimPlanYear dpy on dpy.Employer_Key = de.Employer_Key
		and dpy.BeginDate = del.PlanDate
	inner join FACT_FeederPoolSummary ffps on ffps.Employer_Key = de.Employer_Key
		and ffps.PlanYear = dpy.PlanYear

delete fpg
from	#ToDelete del
	
	inner join DimEmployer de on de.Employer_ID = del.Employer_ID
	inner join DimPlanYear dpy on dpy.Employer_Key = de.Employer_Key
		and dpy.BeginDate = del.PlanDate
	inner join DimPlan dp on dp.Employer_ID = del.Employer_ID
		and dp.PlanYear = dpy.PlanYear
	inner join FACT_PlanGoal fpg on fpg.Plan_Key = dp.Plan_Key

delete mpl
from	#ToDelete del

	inner join DimEmployer de on de.Employer_ID = del.Employer_ID
	inner join DimPlanYear dpy on dpy.Employer_Key = de.Employer_Key
		and dpy.BeginDate = del.PlanDate
	inner join Map_Plan_To_Location mpl on mpl.Employer_Key = de.Employer_Key
		and mpl.PlanYear = dpy.PlanYear

delete dp
from	#ToDelete del
	
	inner join DimEmployer de on de.Employer_ID = del.Employer_ID
	inner join DimPlanYear dpy on dpy.Employer_Key = de.Employer_Key
		and dpy.BeginDate = del.PlanDate
	inner join DimPlan dp on dp.Employer_ID = del.Employer_ID
		and dp.PlanYear = dpy.PlanYear

-- Add New Entries
insert into DimPlan(Employer_ID, PlanDate, PlanCode, JobGroup_ID, PlanYear, BatchID)

SELECT	DISTINCT	 
		cast(spg.EmployerID as nvarchar(50)) as EmployerID
	,	spg.PlanDate
	,	spg.[Plan] as [Plan]
	,	spg.JobGroup as 'JobGroup'
	,	dpy.PlanYear
	,	@BatchID
	

	from stage.JnJPlanGoals spg
		inner join DimEmployer de on de.Employer_ID = spg.EmployerID
		inner join DimPlanYear dpy on spg.PlanDate between dpy.BeginDate and dpy.EndDate
			and dpy.Employer_Key = de.Employer_Key
		left join DimPlan dp on dp.Employer_ID = spg.EmployerID
			and dp.PlanDate = spg.PlanDate
			and [Plan] = dp.PlanCode
			and spg.JobGroup = dp.JobGroup_ID
			and dp.PlanYear = dpy.PlanYear
		
		
		where dp.Plan_Key is null


drop table #ToDelete

END

