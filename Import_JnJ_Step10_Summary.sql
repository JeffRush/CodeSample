IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Import_JnJ_Step10_Summary]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].Import_JnJ_Step10_Summary
GO

create proc Import_JnJ_Step10_Summary
(
@BatchID as int
)
AS
BEGIN

-- Delete Existing
create table #ToDelete
(
		PlanDate DateTime
	,	Employer_ID varchar(50)
)

insert into #ToDelete (Employer_ID, PlanDate)

select distinct
		cast(ts.EmployerID as nvarchar(50)) as 'Employer_ID'
	,	ts.Date
	from stage.SummaryAppsHires ts
	where ts.Date is not null

union

select distinct
		cast(ts.EmployerID as nvarchar(50)) as 'Employer_ID'
	,	ts.Date
	from stage.SummaryPromos ts
	where ts.Date is not null
union

select distinct
		cast(ts.EmployerID as nvarchar(50)) as 'Employer_ID'
	,	ts.Date
	from stage.SummaryPromosIn ts
	where ts.Date is not null
union

select distinct
		cast(ts.EmployerID as nvarchar(50)) as 'Employer_ID'
	,	ts.Date
	from stage.SummaryTerms ts
	where ts.Date is not null
union

select distinct
		cast(ts.EmployerID as nvarchar(50)) as 'Employer_ID'
	,	ts.Date
	from stage.SummaryTransIn ts
	where ts.Date is not null

delete fas
from #ToDelete del
inner join DimEmployer de on de.Employer_ID = del.Employer_ID
inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
	and mpy.LookupDate = del.PlanDate
inner join FACT_ActivitySummary fas on fas.Employer_Key = de.Employer_Key
	and fas.MonitoringDate = del.PlanDate


delete fps
from #ToDelete del
inner join DimEmployer de on de.Employer_ID = del.Employer_ID
inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
	and mpy.LookupDate = del.PlanDate
inner join FACT_FeederPoolSummary fps on fps.Employer_Key = de.Employer_Key
	and fps.MonitoringDate = del.PlanDate
	
-- Add new
create table #ActivitySummary
(
		EmployerID varchar(50)
	,	PlanDate datetime
	,	[Plan] varchar(50)
	,	FeederPoolTypeCode char(2)
	,	JobGroup_ID varchar(20)
	,	ActivityTotalTotalGender int
	,	ActivityTotalFemale int
	,	ActivityTotalMale int
	,	ActivityTotalUnknownGender int
	,	ActivityTotalTotalRace int
	,	ActivityTotalTotalMinority int
	,	ActivityTotalBlack int
	,	ActivityTotalHispanic int
	,	ActivityTotalAsian int
	,	ActivityTotalIndian int
	,	ActivityTotalPacificIslander int
	,	ActivityTotalTwoOrMore int
	,	ActivityTotalWhite int
	,	ActivityTotalUnknownRace int
	,	ActivityTotalDisabled int
	,	ActivityTotalProtectedVeteran int
	,	UseForIRA int
	,	UseForPTG int
	)

insert into #ActivitySummary
select 
		EmployerID
	,	[DATE] as 'PlanDate'
	,	Replace([Plan Name],'"','') as 'Plan'
	,	FeederPoolTypeCode
	,	[Job Group Code] as 'JobGroup_ID'
	,	isnull([Female Activity],0) + isnull( [Male Activity],0) as ActivityTotalTotalGender	
	,	isnull([Female Activity],0) as 'ActivityTotalFemale'
	,	isnull([Male Activity] ,0)as 'ActivityTotalMale'
	,	isnull([Unknown Gender],0) as ActivityTotalUnknownGender
	,	isnull([White Activity],0) + isnull([Black Activity],0) + isnull([Hispanic Activity],0) + isnull([Asian Activity],0) + isnull([Nat Am Activity],0) + isnull([HI or PI Activity],0) + isnull([2 or More Activity],0) as 'ActivityTotalTotalRace'
	,	isnull([Black Activity],0) + isnull([Hispanic Activity],0) + isnull([Asian Activity],0) + isnull([Nat Am Activity],0) + isnull([HI or PI Activity],0) + isnull([2 or More Activity],0) as 'ActivityTotalTotalMinority'
	,	isnull([Black Activity],0) as ActivityTotalBlack	
	,	isnull([Hispanic Activity],0) as ActivityTotalHispanic	
	,	isnull([Asian Activity],0) as ActivityTotalAsian	
	,	isnull([Nat Am Activity],0) as ActivityTotalIndian	
	,	isnull([HI or PI Activity],0) as ActivityTotalPacificIslander	
	,	isnull([2 or More Activity],0) as ActivityTotalTwoOrMore
	,	isnull([White Activity],0) as ActivityTotalWhite	
	,	isnull([Unknown Race],0) as ActivityTotalUnknownRace
	,	isnull([Disabled Activity],0) as ActivityTotalDisabled
	,	isnull([Protected Veteran Activity],0) as ActivityTotalProtectedVeteran
	,	1 as 'UseForIRA'
	,	0 as 'UseForPTG'
	from stage.SummaryTerms
union
select 

		EmployerID
	,	[DATE] as 'PlanDate'
	,	Replace([Plan Name],'"','') as 'Plan'
	,	FeederPoolTypeCode
	,	[Job Group Code] as 'JobGroup_ID'
	,	isnull([Female Hires],0) + isnull([Male Hires],0) as HiresTotalTotalGender	
	,	isnull([Female Hires],0) as 'HiresTotalFemale'
	,	isnull([Male Hires],0) as 'HiresTotalMale'
	,	0 as HiresTotalUnknownGender
	,	isnull([White Hires],0) + isnull([Black Hires],0) + isnull([Hispanic Hires],0) + isnull([Asian Hires],0) + isnull([Nat Am Hires],0) + isnull([HI or PI Hires],0) + isnull([2 or More Hires],0) as 'HiresTotalTotalRace'
	,	isnull([Black Hires],0) + isnull([Hispanic Hires],0) + isnull([Asian Hires],0) + isnull([Nat Am Hires],0) + isnull([HI or PI Hires],0) + isnull([2 or More Hires],0) as 'HiresTotalTotalMinority'
	,	isnull([Black Hires],0) as HiresTotalBlack	
	,	isnull([Hispanic Hires],0) as HiresTotalHispanic	
	,	isnull([Asian Hires],0) as HiresTotalAsian	
	,	isnull([Nat Am Hires],0) as HiresTotalIndian	
	,	isnull([HI or PI Hires],0) as HiresTotalPacificIslander	
	,	isnull([2 or More Hires],0) as HiresTotalTwoOrMore
	,	isnull([White Hires],0) as HiresTotalWhite	
	,	0 as HiresTotalUnknownRace
	,	isnull([Disabled Hires] ,0) as HiresTotalDisabled
	,	isnull([Protected Veteran Hires],0) as HiresTotalProtectedVeteran
	,	1 as 'UseForIRA'
	,	1 as 'UseForPTG'
	from stage.SummaryAppsHires
	
UNION

select 

		EmployerID
	,	[DATE] as 'PlanDate'
	,	Replace([Plan Name],'"','') as 'Plan'
	,	FeederPoolTypeCode
	,	[Job Group Code] as 'JobGroup_ID'
	,	isnull([Female Activity],0) +isnull( [Male Activity],0) as ActivityTotalTotalGender	
	,	isnull([Female Activity],0) as 'ActivityTotalFemale'
	,	isnull([Male Activity],0) as 'ActivityTotalMale'
	,	isnull([Unknown Gender],0) as ActivityTotalUnknownGender
	,	isnull([White Activity],0) + isnull([Black Activity],0) + isnull([Hispanic Activity],0) + isnull([Asian Activity],0) + isnull([Nat Am Activity],0) + isnull([HI or PI Activity],0) + isnull([2 or More Activity],0) as 'ActivityTotalTotalRace'
	,	isnull([Black Activity],0) + isnull([Hispanic Activity],0) + isnull([Asian Activity],0) + isnull([Nat Am Activity],0) + isnull([HI or PI Activity],0) + isnull([2 or More Activity],0) as 'ActivityTotalTotalMinority'
	,	isnull([Black Activity],0) as ActivityTotalBlack	
	,	isnull([Hispanic Activity],0) as ActivityTotalHispanic	
	,	isnull([Asian Activity],0) as ActivityTotalAsian	
	,	isnull([Nat Am Activity],0) as ActivityTotalIndian	
	,	isnull([HI or PI Activity],0) as ActivityTotalPacificIslander	
	,	isnull([2 or More Activity],0) as ActivityTotalTwoOrMore
	,	isnull([White Activity],0) as ActivityTotalWhite	
	,	isnull([Unknown Race],0) as ActivityTotalUnknownRace
	,	isnull([Disabled Activity],0) as ActivityTotalDisabled
	,	isnull([Protected Veteran Activity],0) as ActivityTotalProtectedVeteran
	,	1 as 'UseForIRA'
	,	0 as 'UseForPTG'
	from stage.SummaryPromos
	
UNION
select 
		EmployerID
	,	[DATE] as 'PlanDate'
	,	Replace([Plan Name],'"','') as 'Plan'
	,	FeederPoolTypeCode
	,	[Job Group Code] as 'JobGroup_ID'
	,	isnull([Female Activity],0) +isnull( [Male Activity],0) as ActivityTotalTotalGender	
	,	isnull([Female Activity],0) as 'ActivityTotalFemale'
	,	isnull([Male Activity],0) as 'ActivityTotalMale'
	,	0 as ActivityTotalUnknownGender
	,	isnull([White Activity],0) + isnull([Black Activity],0) + isnull([Hispanic Activity],0) + isnull([Asian Activity],0) + isnull([Nat Am Activity],0) + isnull([HI or PI Activity],0) + isnull([2 or More Activity],0) as 'ActivityTotalTotalRace'
	,	isnull([Black Activity],0) + isnull([Hispanic Activity],0) + isnull([Asian Activity],0) + isnull([Nat Am Activity],0) + isnull([HI or PI Activity],0) + isnull([2 or More Activity],0) as 'ActivityTotalTotalMinority'
	,	isnull([Black Activity],0) as ActivityTotalBlack	
	,	isnull([Hispanic Activity],0) as ActivityTotalHispanic	
	,	isnull([Asian Activity],0) as ActivityTotalAsian	
	,	isnull([Nat Am Activity],0) as ActivityTotalIndian	
	,	isnull([HI or PI Activity],0) as ActivityTotalPacificIslander	
	,	isnull([2 or More Activity],0) as ActivityTotalTwoOrMore
	,	isnull([White Activity],0) as ActivityTotalWhite	
	,	0 as ActivityTotalUnknownRace
	,	isnull([Disabled Activity],0) as ActivityTotalDisabled
	,	isnull([Protected Veteran Activity],0) as ActivityTotalProtectedVeteran
	,	0 as 'UseForIRA'
	,	1 as 'UseForPTG'
	from stage.SummaryPromosIn
	
UNION

select 

		EmployerID
	,	[DATE] as 'PlanDate'
	,	Replace([Plan Name],'"','') as 'Plan'
	,	FeederPoolTypeCode
	,	[Job Group Code] as 'JobGroup_ID'
	,	isnull([Female Activity],0) +isnull( [Male Activity],0) as ActivityTotalTotalGender	
	,	isnull([Female Activity],0) as 'ActivityTotalFemale'
	,	isnull([Male Activity],0) as 'ActivityTotalMale'
	,	0 as ActivityTotalUnknownGender
	,	isnull([White Activity],0) + isnull([Black Activity],0) + isnull([Hispanic Activity],0) + isnull([Asian Activity],0) + isnull([Nat Am Activity],0) + isnull([HI or PI Activity],0) + isnull([2 or More Activity],0) as 'ActivityTotalTotalRace'
	,	isnull([Black Activity],0) + isnull([Hispanic Activity],0) + isnull([Asian Activity],0) + isnull([Nat Am Activity],0) + isnull([HI or PI Activity],0) + isnull([2 or More Activity],0) as 'ActivityTotalTotalMinority'
	,	isnull([Black Activity],0) as ActivityTotalBlack	
	,	isnull([Hispanic Activity],0) as ActivityTotalHispanic	
	,	isnull([Asian Activity],0) as ActivityTotalAsian	
	,	isnull([Nat Am Activity],0) as ActivityTotalIndian	
	,	isnull([HI or PI Activity],0) as ActivityTotalPacificIslander	
	,	isnull([2 or More Activity],0) as ActivityTotalTwoOrMore
	,	isnull([White Activity],0) as ActivityTotalWhite	
	,	0 as ActivityTotalUnknownRace
	,	isnull([Disabled Activity],0) as ActivityTotalDisabled
	,	isnull([Protected Veteran Activity],0) as ActivityTotalProtectedVeteran
	,	1 as 'UseForIRA'
	,	1 as 'UseForPTG'
	from stage.SummaryTransIn
	
	
	
	
	insert into FACT_ActivitySummary
	([Plan_Key],	[Employer_Key],	[PlanYear],	[MonitoringDate],	[JobGroup_ID],	[FeederPoolTypeCode],	[ActivityTotalTotalGender],	[ActivityTotalFemale],	
	[ActivityTotalMale],	[ActivityTotalUnknownGender],	[ActivityTotalTotalRace],	[ActivityTotalTotalMinority],	[ActivityTotalBlack],
	[ActivityTotalHispanic],	[ActivityTotalAsian],	[ActivityTotalIndian],	[ActivityTotalPacificIslander],	[ActivityTotalTwoOrMore],	[ActivityTotalWhite],
	[ActivityTotalUnknownRace],	[UseForIRA],	[UseForPTG],	[BatchID],	[ActivityTotalDisabled],	[ActivityTotalProtectedVeteran])
	
	
	select 
			dp.Plan_Key
		,	de.Employer_Key
		,	mpy.PlanYear
		,	asum.PlanDate
		,	asum.JobGroup_ID
		,	FeederPoolTypeCode
		,	ActivityTotalTotalGender
		,	ActivityTotalFemale
		,	ActivityTotalMale
		,	ActivityTotalUnknownGender
		,	ActivityTotalTotalRace
		,	ActivityTotalTotalMinority
		,	ActivityTotalBlack
		,	ActivityTotalHispanic
		,	ActivityTotalAsian
		,	ActivityTotalIndian
		,	ActivityTotalPacificIslander
		,	ActivityTotalTwoOrMore
		,	ActivityTotalWhite
		,	ActivityTotalUnknownRace
		,	UseForIRA
		,	UseForPTG
		,	@BatchID
		,	ActivityTotalDisabled
		,	ActivityTotalProtectedVeteran
	 from #ActivitySummary asum 
		inner join DimEmployer de on de.Employer_ID = asum.EmployerID
		inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
		inner join DimPlan dp on dp.Employer_ID = asum.EmployerID
			and dp.PlanYear = mpy.PlanYear
			and dp.PlanCode = asum.[Plan]
			and dp.JobGroup_ID = asum.JobGroup_ID
	
	
	
-- Feeder pool summary

CREATE TABLE #FeederPoolSummary(
	Employer_ID varchar(50),
	[PlanDate] [date] NULL,
	[Plan] varchar(50),
	[FeederPoolTypeCode] [char](2) NULL,
	[JobGroup_ID] [varchar](50) NULL,

	[PoolTotalGender] [int] NULL,
	[PoolFemale] [int] NULL,
	[PoolMale] [int] NULL,
	[PoolUnknownGender] [int] NULL,
	[PoolTotalRace] [int] NULL,
	[PoolTotalMinority] [int] NULL,
	[PoolBlack] [int] NULL,
	[PoolHispanic] [int] NULL,
	[PoolAsian] [int] NULL,
	[PoolIndian] [int] NULL,
	[PoolPacificIslander] [int] NULL,
	[PoolTwoOrMore] [int] NULL,
	[PoolWhite] [int] NULL,
	[PoolUnknownRace] [int] NULL,
	[BatchID] [int] NULL,
	[PoolDisabled] [int] NULL,
	[PoolProtectedVeteran] [int] NULL
	)
	
	insert into #FeederPoolSummary(Employer_ID,[PlanDate],[Plan],[FeederPoolTypeCode],[JobGroup_ID],
	[PoolTotalGender],[PoolFemale],	[PoolMale],	[PoolUnknownGender],
		[PoolTotalRace],	[PoolTotalMinority],
	[PoolBlack],
	[PoolHispanic],	[PoolAsian],	[PoolIndian],	[PoolPacificIslander],	[PoolTwoOrMore],	[PoolWhite],	[PoolUnknownRace],	[BatchID],
	[PoolDisabled],	[PoolProtectedVeteran])
	select 
		EmployerID
	,	[DATE] as 'PlanDate'
	,	Replace([Plan Name],'"','') as 'Plan'
	,	FeederPoolTypeCode
	,	[Job Group Code] as 'JobGroup_ID'
	,	isnull([Female Pool],0) + isnull( [Male Pool],0) as PoolTotalGender	
	,	isnull([Female Pool],0) as 'PoolFemale'
	,	isnull([Male Pool] ,0)as 'PoolMale'
	,	isnull([Unknown Gender],0) as PoolUnknownGender
	,	isnull([White Pool],0) + isnull([Black Pool],0) + isnull([Hispanic Pool],0) + isnull([Asian Pool],0) + isnull([Nat Am Pool],0) + isnull([HI or PI Pool],0) + isnull([2 or More Pool],0) as 'PoolTotalRace'
	,	isnull([Black Pool],0) + isnull([Hispanic Pool],0) + isnull([Asian Pool],0) + isnull([Nat Am Pool],0) + isnull([HI or PI Pool],0) + isnull([2 or More Pool],0) as 'PoolTotalMinority'
	,	isnull([Black Pool],0) as PoolBlack	
	,	isnull([Hispanic Pool],0) as PoolHispanic	
	,	isnull([Asian Pool],0) as PoolAsian	
	,	isnull([Nat Am Pool],0) as PoolIndian	
	,	isnull([HI or PI Pool],0) as PoolPacificIslander	
	,	isnull([2 or More Pool],0) as PoolTwoOrMore
	,	isnull([White Pool],0) as PoolWhite	
	,	isnull([Unknown Race],0) as PoolUnknownRace
	,	@BatchID
	,	ISNULL([Disabled Pool],0) as PoolDisabled
	,	ISNULL([Protected Veteran Pool],0) as PoolProtectedVeteran
	
	
	from stage.SummaryTerms
union
select 

		EmployerID
	,	[DATE] as 'PlanDate'
	,	Replace([Plan Name],'"','') as 'Plan'
	,	FeederPoolTypeCode
	,	[Job Group Code] as 'JobGroup_ID'
	,	isnull([Female Apps],0) + isnull([Male Apps],0) as AppsTotalTotalGender	
	,	isnull([Female Apps],0) as 'AppsTotalFemale'
	,	isnull([Male Apps],0) as 'AppsTotalMale'
	,	ISNULL([Unknown Gender],0) as AppsTotalUnknownGender
	,	isnull([White Apps],0) + isnull([Black Apps],0) + isnull([Hispanic Apps],0) + isnull([Asian Apps],0) + isnull([Nat Am Apps],0) + isnull([HI or PI Apps],0) + isnull([2 or More Apps],0) as 'AppsTotalTotalRace'
	,	isnull([Black Apps],0) + isnull([Hispanic Apps],0) + isnull([Asian Apps],0) + isnull([Nat Am Apps],0) + isnull([HI or PI Apps],0) + isnull([2 or More Apps],0) as 'AppsTotalTotalMinority'
	,	isnull([Black Apps],0) as AppsTotalBlack	
	,	isnull([Hispanic Apps],0) as AppsTotalHispanic	
	,	isnull([Asian Apps],0) as AppsTotalAsian	
	,	isnull([Nat Am Apps],0) as AppsTotalIndian	
	,	isnull([HI or PI Apps],0) as AppsTotalPacificIslander	
	,	isnull([2 or More Apps],0) as AppsTotalTwoOrMore
	,	isnull([White Apps],0) as AppsTotalWhite	
	,	ISNULL([Unknown Race],0) as AppsTotalUnknownRace
	,	@BatchID
	,	isnull([Disabled Apps],0) as AppsTotalDisabled
	,	isnull([Protected Veteran Apps],0) as AppsTotalProtectedVeteran 
	
	
	from stage.SummaryAppsHires
	
UNION

select 

		EmployerID
	,	[DATE] as 'PlanDate'
	,	Replace([Plan Name],'"','') as 'Plan'
	,	FeederPoolTypeCode
	,	[Job Group Code] as 'JobGroup_ID'
	,	isnull([Female Pool],0) +isnull( [Male Pool],0) as PoolTotalGender	
	,	isnull([Female Pool],0) as 'PoolFemale'
	,	isnull([Male Pool],0) as 'PoolMale'
	,	isnull([Unknown Gender],0) as PoolUnknownGender
	,	isnull([White Pool],0) + isnull([Black Pool],0) + isnull([Hispanic Pool],0) + isnull([Asian Pool],0) + isnull([Nat Am Pool],0) + isnull([HI or PI Pool],0) + isnull([2 or More Pool],0) as 'PoolTotalRace'
	,	isnull([Black Pool],0) + isnull([Hispanic Pool],0) + isnull([Asian Pool],0) + isnull([Nat Am Pool],0) + isnull([HI or PI Pool],0) + isnull([2 or More Pool],0) as 'PoolTotalMinority'
	,	isnull([Black Pool],0) as PoolBlack	
	,	isnull([Hispanic Pool],0) as PoolHispanic	
	,	isnull([Asian Pool],0) as PoolAsian	
	,	isnull([Nat Am Pool],0) as PoolIndian	
	,	isnull([HI or PI Pool],0) as PoolPacificIslander	
	,	isnull([2 or More Pool],0) as PoolTwoOrMore
	,	isnull([White Pool],0) as PoolWhite	
	,	isnull([Unknown Race],0) as PoolUnknownRace
	,	@BatchID
	,	ISNULL([Disabled Pool],0) as PoolDisabled
	,	ISNULL([Protected Veteran Pool],0) as PoolProtectedVeteran
	
	
	from stage.SummaryPromos
	
	
	
	insert into FACT_FeederPoolSummary
	(Plan_Key, Employer_Key, PlanYear, MonitoringDate, JobGroup_ID, FeederPoolTypeCode
	,[PoolTotalGender],[PoolFemale],	[PoolMale],	[PoolUnknownGender],
		[PoolTotalRace],	[PoolTotalMinority],
	[PoolBlack],
	[PoolHispanic],	[PoolAsian],	[PoolIndian],	[PoolPacificIslander],	[PoolTwoOrMore],	[PoolWhite],	[PoolUnknownRace],	[BatchID],
	[PoolDisabled],	[PoolProtectedVeteran])
	
	select dp.Plan_Key, de.Employer_Key, mpy.PlanYear, fsum.PlanDate, fsum.JobGroup_ID, fsum.FeederPoolTypeCode
	,[PoolTotalGender],[PoolFemale],	[PoolMale],	[PoolUnknownGender],
		[PoolTotalRace],	[PoolTotalMinority],
	[PoolBlack],
	[PoolHispanic],	[PoolAsian],	[PoolIndian],	[PoolPacificIslander],	[PoolTwoOrMore],	[PoolWhite],	[PoolUnknownRace],	fsum.BatchID,
	[PoolDisabled],	[PoolProtectedVeteran]
	from #FeederPoolSummary fsum
	inner join DimEmployer de on de.Employer_ID = fsum.Employer_ID
		inner join MapPlanYear mpy on mpy.Employer_Key = de.Employer_Key
		inner join DimPlan dp on dp.Employer_ID = fsum.Employer_ID
			and dp.PlanYear = mpy.PlanYear
			and dp.PlanCode = fsum.[Plan]
			and dp.JobGroup_ID = fsum.JobGroup_ID
	

	drop table #ToDelete
	drop table #ActivitySummary
	drop table #FeederPoolSummary

END