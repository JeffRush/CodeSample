IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Import_JnJ_Step11_FeederPools]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].Import_JnJ_Step11_FeederPools
GO

create proc Import_JnJ_Step11_FeederPools
(
@BatchID as int
)
AS

BEGIN

create table #Employer
(Employer_ID varchar(50)
, Employer_Key int
,	ID int identity(1,1)
)
insert into #Employer (Employer_ID)
select distinct EmployerID as 'EmployerID' from stage.JnJEmployees
union 
select distinct EmployerID as 'EmployerID' from stage.JnJPlans
union 
select distinct EmployerID as 'EmployerID' from stage.ModifiedJnJHires
union 
select distinct EmployerID as 'EmployerID' from stage.ModifiedJnJPromotions
union 
select distinct EmployerID as 'EmployerID' from stage.ModifiedJnJTransfers
union 
select distinct EmployerID as 'EmployerID' from stage.ModifiedJnJTerminations


Update e
set Employer_Key = de.Employer_Key
from #Employer e
	inner join DimEmployer de on de.Employer_ID = e.Employer_ID




declare @i as int
declare @max as int
declare @SQL as varchar(max)

set @i = 1
set @max = (select MAX(ID) from #Employer)

While @i <= @max
BEGIN
-- Job Title IRA prep
set @SQL = (select 'exec usp_GenerateAndUpdateJobTitleIRASubPlanKey ' + cast(Employer_Key as varchar)
from #Employer where ID = @i)
exec(@SQL)

-- Req ID IRA prep
set @SQL = (select 'exec usp_GenerateAndUpdateReqIDIRASubPlanKey ' + cast(Employer_Key as varchar)
from #Employer where ID = @i)
exec(@SQL)

-- Job Title IRA Work
set @SQL = (select 'exec usp_MaterializeFeederPoolAndStdDev_JobTitleSubPlan ' + cast(Employer_Key as varchar) + ', ' + CAST(@BatchID as varchar)
from #Employer where ID = @i)
exec(@SQL)

-- Job Title IRA Work
set @SQL = (select 'exec dbo.usp_MaterializeFeederPoolAndStdDev_ReqIDIRASubPlan ' + cast(Employer_Key as varchar) + ', ' + CAST(@BatchID as varchar)
from #Employer where ID = @i)
exec(@SQL)

set @i = @i + 1
END


drop table #Employer

END