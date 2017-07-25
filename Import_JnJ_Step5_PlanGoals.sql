IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Import_JnJ_Step5_PlanGoals]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].Import_JnJ_Step5_PlanGoals
GO

create proc Import_JnJ_Step5_PlanGoals
(
@BatchID as int
)
AS
BEGIN

insert into FACT_PlanGoal (Plan_Key, PlanGoalTarget_Key, PlacementGoal, Employment, Availability, AnnualGoal, BatchID)

select 

		dp.Plan_Key
	,	dpgt.PlanGoalTarget_Key
	,	spg.PlacementGoal
	,	spg.Employment
	,	spg.Availability
	,	spg.AnnualGoal
	,	@BatchID
from stage.JnJPlanGoals spg
	inner join DimEmployer de on de.Employer_ID = spg.EmployerID
	inner join DimPlanYear dpy on dpy.Employer_Key = de.Employer_Key
		and dpy.BeginDate = spg.PlanDate
	inner join DimPlan dp on dp.Employer_ID = de.Employer_ID
		and dp.PlanYear = dpy.PlanYear
		and dp.PlanCode = LTRIM(RTRIM(Replace(spg.[Plan],'"','')))
		and dp.JobGroup_ID = Replace(spg.JobGroup,'"','')
	inner join DimPlanGoalTarget dpgt 
		on dpgt.PlanGoalTargetCode = Case when spg.MinorityType = 'Min' then 'M' else spg.MinorityType end

END

