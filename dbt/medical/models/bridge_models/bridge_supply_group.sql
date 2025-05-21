with enriched_encounters as (
	select
		ID
	from
		{{ enriched_table('enriched_encounters') }}
)
, enriched_supplies as (
 	select distinct
 		ENCOUNTER
 		, cast(coalesce(CODE, 0) as UInt64) as CODE
 	from
		{{ enriched_table('enriched_supplies') }}
	
)
, dim_supplies as (
	select
		Supply_Key
		, Supply_Code 
	from
		{{ ref('dim_supplies') }}
)

, encounters_without_supply as (
	select distinct 
		ID as ENCOUNTER
		, cast(0 as UInt64) as CODE
	from 
		enriched_encounters as e
	left anti join 
		enriched_supplies as a on e.ID = a.ENCOUNTER
)

, supply_group_union as(
	select * from enriched_supplies
	union all
	select * from encounters_without_supply
)
, bridge_supply_group as (
	select
		hex(MD5(ENCOUNTER)) as Supply_Group_Key
		, Supply_Key
	from
		supply_group_union as ag
	join
		dim_supplies as da on ag.CODE == da.Supply_Code
)

select * from bridge_supply_group