--/****** Object:  StoredProcedure [dbo].[Import_JnJ_Step9_Applicants]    Script Date: 12/04/2016 17:03:32 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Import_JnJ_Step9d_ApplicantsUpdate]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[Import_JnJ_Step9d_ApplicantsUpdate]
GO

/****** Object:  StoredProcedure [dbo].[Import_JnJ_Step9d_ApplicantsUpdate]    Script Date: 12/04/2016 17:03:32 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[Import_JnJ_Step9d_ApplicantsUpdate]
(
@BatchID as int
)
WITH RECOMPILE 
AS
BEGIN


create table #ToUpdate
(
		PlanDate DateTime
	,	Employer_ID varchar(50)
)

insert into #ToUpdate (Employer_ID, PlanDate)

select distinct EmployerID, [Monitoringdate] as EffectiveDate
from stage.ModifiedJnJApplicants

---- Second post import update

update fa
set MissingHiredApplicant = 
 Case
  when fapp.Employer_Key is null and fapp2.Employer_Key is null then 1
  else 0
 end
 
from #ToUpdate tu
	inner join DimEmployer de on de.Employer_ID = tu.Employer_ID
	inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
		and mpy.LookupDate = tu.PlanDate
	inner join FACT_EmployeeActivity fa on fa.Employer_Key = de.Employer_Key
	and fa.SnapShotDate = tu.PlanDate
 inner join DimEmployee demp on demp.Employee_Key = fa.Employee_Key
 left join FACT_Applicant fapp on fapp.Employer_Key = fa.Employer_Key
  and fapp.RequisitionNumber = fa.RequisitionID
  and fapp.WWID = demp.Employee_ID
  and fapp.AppStatus in ('Cleared for Hire','Mergers & Acquisitions - Cleared for Hire','Cleared for Hire - SC','Cleared for Hire - GM','Cleared for Hire - GE','Cleared for Hire - Mergers and Acquisitions')
  and fapp.PlanYear = fa.PlanYear
  and fapp.SnapShotDate = fa.SnapShotDate
 left join FACT_Applicant fapp2 on fapp2.Employer_Key = fa.Employer_Key
  and fapp2.RequisitionNumber = fa.OriginalRequisitionID
  and fapp2.WWID = demp.Employee_ID
  and fapp2.AppStatus in ('Cleared for Hire','Mergers & Acquisitions - Cleared for Hire','Cleared for Hire - SC','Cleared for Hire - GM','Cleared for Hire - GE','Cleared for Hire - Mergers and Acquisitions')
  and fapp2.PlanYear = fa.PlanYear
  and fapp2.SnapShotDate = fa.SnapShotDate
 where ActivityTypeCode in ('HI','PR','TR')
  and ActivityQty = 1




END


GO
