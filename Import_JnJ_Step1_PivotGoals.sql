IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Import_JnJ_Step1_PivotGoals]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].Import_JnJ_Step1_PivotGoals
GO

create proc Import_JnJ_Step1_PivotGoals
(
@BatchID as int
)
AS
BEGIN

-- Step 1 Pivot Plans to Goals

truncate table stage.JnJPlanGoals

insert into Stage.JnJPlanGoals(EmployerID, PlanDate, [Plan], JobGroup, MinorityType, Employment, Availability, PlacementGoal, AnnualGoal, GoalNum)

select EmployerID, PlanDate, [Plan], JobGroup, Replace(recs,'Employment','') as 'MinorityType', Employment, Availability, PlacementGoal, case when AnnualGoal = '' then null else AnnualGoal end as AnnualGoal, case when GoalNum = '' then null else GoalNum end as GoalNum
from 
	stage.JnJPlans
UNPIVOT
	(
	Employment FOR Recs IN (EmploymentMin, EmploymentF, EmploymentB, EmploymentH, EmploymentA, EmploymentI, EmploymentP, EmploymentD)
	)
	as Employment
UNPIVOT
	(
	Availability FOR Recs2 IN (AvailabilityMin, AvailabilityF, AvailabilityB, AvailabilityH, AvailabilityA, AvailabilityI, AvailabilityP, AvailabilityD)
	)
	as Availability
UNPIVOT
	(
	PlacementGoal FOR Recs3 IN (PlacementGoalMin, PlacementGoalF, PlacementGoalB, PlacementGoalH, PlacementGoalA, PlacementGoalI, PlacementGoalP, PlacementGoalD)
	)
	as PlacementGoal
UNPIVOT
	(
	AnnualGoal FOR Recs4 IN (AnnualGoalMin, AnnualGoalF, AnnualGoalB, AnnualGoalH, AnnualGoalA, AnnualGoalI, AnnualGoalP, AnnualGoalD)
	)
	as AnnualGoal
UNPIVOT
	(
	GoalNum FOR Recs5 IN (GoalNumMin, GoalNumF, GoalNumB, GoalNumH, GoalNumA, GoalNumI, GoalNumP, GoalNumD)
	)
	as GoalNum
	
where Replace(recs,'Employment','') = Replace(recs2,'Availability','')
	and Replace(recs,'Employment','') = Replace(recs3,'PlacementGoal','')
	and Replace(recs,'Employment','') = Replace(recs4,'AnnualGoal','')
	and Replace(recs,'Employment','') = Replace(recs5,'GoalNum','')
	
order by [Plan],JobGroup,recs;

END

