with enriched_encounters as (
	select
		ID
	from
		{{ enriched_table('enriched_encounters') }}
)
, enriched_conditions as (
 	select distinct
 		ENCOUNTER
 		, cast(coalesce(CODE, 0) as UInt64) as CODE
 	from
		{{ enriched_table('enriched_conditions') }}
	
)
, dim_conditions as (
	select
		Condition_Key
		, Condition_Code 
	from
		{{ ref('dim_conditions') }}
)

, encounters_without_condition as (
	select distinct 
		ID as ENCOUNTER
		, cast(0 as UInt64) as CODE
	from 
		enriched_encounters as e
	left anti join 
		enriched_conditions as a on e.ID = a.ENCOUNTER
)

, condition_group_union as(
	select * from enriched_conditions
	union all
	select * from encounters_without_condition
)
, bridge_condition_group as (
	select
		hex(MD5(ENCOUNTER)) as Condition_Group_Key
		, Condition_Key
	from
		condition_group_union as ag
	join
		dim_conditions as da on ag.CODE == da.Condition_Code
)

select * from bridge_condition_group


