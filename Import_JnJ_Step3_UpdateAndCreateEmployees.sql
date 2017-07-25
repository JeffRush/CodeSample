IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Import_JnJ_Step3_UpdateAndCreateEmployees]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].Import_JnJ_Step3_UpdateAndCreateEmployees
GO

create proc Import_JnJ_Step3_UpdateAndCreateEmployees
(
@BatchID as int
)
AS
BEGIN



truncate table TempDimEmployee


insert into TempDimEmployee (Employee_ID, Employer_Key, FirstName, LastName, DateOfBirth, Title, HireDate, TerminationDate, Gender, Race, BatchID, Disabled, ProtectedVeteran, Materialized)
select distinct
		right(replace(EmpID,'-',''),(len(replace(EmpID,'-','')) - PATINDEX('%[1-9]%',replace(EmpID,'-','')) + 1)) as 'EmpID'
	,	de.Employer_Key
	,	dbo.Formatname(Name, 'F') as 'FirstName'
	,	dbo.Formatname(Name, 'L') as 'LastName'  
	,	isnull(DOB,'18991231') as DOB
	,	cast(Replace(JobTitle,'"','') as varchar(50)) as 'JobTitle'
	,	HireDate
	,	TermDate
	,	case
			when cast(Gender as varchar(10)) = 'M' then 'Male'
			when cast(Gender as varchar(10)) = 'F' then 'Female'			
			else CAST(Gender as varchar(10)) 
		end as 'Gender'
	,	Case 
			when cast(Race as varchar(20)) = 'A' then 'Asian'
			when cast(Race as varchar(20)) = 'B' then 'Black'
			when cast(Race as varchar(20))  = 'H' then 'Hispanic'
			when cast(Race as varchar(20))  = 'I' then 'Indian'
			when cast(Race as varchar(20))  = 'P' then 'Pacific Islander'
			when cast(Race as varchar(20))  = 'T' then 'Two or More'
			when cast(Race as varchar(20))  = 'W' then 'White'
			else CAST(Race as varchar(20))
		end as 'Race'
	,	@BatchID
	,	Case
			when Disabled = '' then null
			Else Disabled
		end as Disabled
	,	Case
			when ProtectedVeteran = '' then null
			Else ProtectedVeteran
		end as ProtectedVeteran
	,	null as 'Materialized'
	
	from stage.JnJEmployees sse
		inner join DimEmployer de on de.Employer_ID = sse.EmployerID
union
select 
		right(replace(EmpID,'-',''),(len(replace(EmpID,'-','')) - PATINDEX('%[1-9]%',replace(EmpID,'-','')) + 1)) as 'EmpID'
	,	de.Employer_Key
	,	dbo.Formatname(Name, 'F') as 'FirstName'
	,	dbo.Formatname(Name, 'L') as 'LastName'  
	,	isnull(DOB,'18991231') as DOB
	,	cast(Replace(JobTitle,'"','') as varchar(50)) as 'JobTitle'
	,	HireDate
	,	TermDate
	,	case
			when cast(Gender as varchar(10)) = 'M' then 'Male'
			when cast(Gender as varchar(10)) = 'F' then 'Female'			
			else CAST(Gender as varchar(10)) 
		end as 'Gender'
	,	Case 
			when cast(Race as varchar(20)) = 'A' then 'Asian'
			when cast(Race as varchar(20)) = 'B' then 'Black'
			when cast(Race as varchar(20))  = 'H' then 'Hispanic'
			when cast(Race as varchar(20))  = 'I' then 'Indian'
			when cast(Race as varchar(20))  = 'P' then 'Pacific Islander'
			when cast(Race as varchar(20))  = 'T' then 'Two or More'
			when cast(Race as varchar(20))  = 'W' then 'White'
			else CAST(Race as varchar(20))
		end as 'Race'
	,	@BatchID
	,	Case
			when Disabled = '' then null
			Else Disabled
		end as Disabled
	,	Case
			when ProtectedVeteran = '' then null
			Else ProtectedVeteran
		end as ProtectedVeteran
	,	null as 'Materialized'
	from stage.JnJSnapshotAcquisition ssa
		inner join DimEmployer de on de.Employer_ID = ssa.EmployerID
		
exec dbo.usp_ProcessDimEmployeeMerge

END