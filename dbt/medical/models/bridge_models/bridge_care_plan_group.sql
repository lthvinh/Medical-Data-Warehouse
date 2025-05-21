with enriched_encounters as (
	select
		ID
	from
		{{ enriched_table('enriched_encounters') }}
)
, enriched_careplans as (
 	select distinct
 		ENCOUNTER
 		, cast(coalesce(CODE, 0) as UInt64) as CODE
 	from
		{{ enriched_table('enriched_careplans') }}
	
)
, dim_care_plans as (
	select
		Care_Plan_Key
		, Care_Plan_Code 
	from
		{{ ref('dim_care_plans') }}
)

, encounters_without_care_plan as (
	select distinct 
		ID as ENCOUNTER
		, cast(0 as UInt64) as CODE
	from 
		enriched_encounters as e
	left anti join 
		enriched_careplans as a on e.ID = a.ENCOUNTER
)

, care_plan_group_union as(
	select * from enriched_careplans
	union all
	select * from encounters_without_care_plan
)
, bridge_care_plan_group as (
	select
		hex(MD5(ENCOUNTER)) as Care_Plan_Group_Key
		, Care_Plan_Key
	from
		care_plan_group_union as ag
	join
		dim_care_plans as da on ag.CODE == da.Care_Plan_Code
)

select * from bridge_care_plan_group


