IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Import_JnJ_Step2_UpdateAndCreateEmployers]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].Import_JnJ_Step2_UpdateAndCreateEmployers
GO

create proc Import_JnJ_Step2_UpdateAndCreateEmployers
(
@BatchID as int
)
AS
BEGIN

create table #EmpIDs
(
EmployerID varchar(50),
EmployerName varchar(250)
)

insert into #EmpIDs
select distinct cast(EmployerID as varchar) as 'EmployerID', EmployerID as 'EmployerName' from stage.JnJEmployees
union 
select distinct cast(EmployerID as varchar) as 'EmployerID', EmployerID as 'EmployerName' from stage.JnJPlans
union 
select distinct cast(EmployerID as varchar) as 'EmployerID', EmployerID as 'EmployerName' from stage.ModifiedJnJHires
union 
select distinct cast(EmployerID as varchar) as 'EmployerID', EmployerID as 'EmployerName' from stage.ModifiedJnJPromotions
union 
select distinct cast(EmployerID as varchar) as 'EmployerID', EmployerID as 'EmployerName' from stage.ModifiedJnJTransfers
union 
select distinct cast(EmployerID as varchar) as 'EmployerID', EmployerID as 'EmployerName' from stage.ModifiedJnJTerminations


update de
set EmployerName = c.ClientName
	from #EmpIDs e
	inner join DimEmployer de on de.Employer_ID = e.EmployerID
	inner join mgr.Client c on c.Client_ID = e.EmployerID

where de.RowIsCurrent = 'Y'
	and de.EmployerName != c.ClientName
	
insert into DimEmployer (Employer_ID, EmployerName, BatchID)

select e.EmployerID, c.ClientName, @BatchID
from #EmpIDs e
	inner join mgr.Client c on c.Client_ID = e.EmployerID
	left join DimEmployer de on de.Employer_ID = e.EmployerID
	
	where de.Employer_ID is null


drop table #EmpIDs

exec [usp_CreateDimPlanYearEntries]
exec dbo.usp_MaintainMapPlanYear
END