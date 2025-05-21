with enriched_encounters as (
	select
		ID
	from
		{{ enriched_table('enriched_encounters') }}
)
, enriched_immunizations as (
 	select distinct
 		ENCOUNTER
 		, cast(coalesce(CODE, 0) as UInt64) as CODE
 	from
		{{ enriched_table('enriched_immunizations') }}
	
)
, dim_immunizations as (
	select
		Immunization_Key
		, Immunization_Code 
	from
		{{ ref('dim_immunizations') }}
)

, encounters_without_immuization as (
	select distinct 
		ID as ENCOUNTER
		, cast(0 as UInt64) as CODE
	from 
		enriched_encounters as e
	left anti join 
		enriched_immunizations as a on e.ID = a.ENCOUNTER
)

, immunization_group_union as(
	select * from enriched_immunizations
	union all
	select * from encounters_without_immuization
)
, bridge_immunization_group as (
	select
		hex(MD5(ENCOUNTER)) as Immunization_Group_Key
		, Immunization_Key
	from
		immunization_group_union as ag
	join
		dim_immunizations as da on ag.CODE == da.Immunization_Code
)

select * from bridge_immunization_group


