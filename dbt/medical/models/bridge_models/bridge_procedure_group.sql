with enriched_encounters as (
	select
		ID
	from
		{{ enriched_table('enriched_encounters') }}
)
, enriched_procedures as (
 	select distinct
 		ENCOUNTER
 		, cast(coalesce(CODE, 0) as UInt64) as CODE
 	from
		{{ enriched_table('enriched_procedures') }}
	
)
, dim_procedures as (
	select
		Procedure_Key
		, Procedure_Code
	from
		{{ ref('dim_procedures') }}
)

, encounters_without_procedure as (
	select distinct 
		ID as ENCOUNTER
		, cast(0 as UInt64) as CODE
	from 
		enriched_encounters as e
	left anti join 
		enriched_procedures as a on e.ID = a.ENCOUNTER
)

, procedure_group_union as(
	select * from enriched_procedures
	union all
	select * from encounters_without_procedure
)
, bridge_procedure_group as (
	select
		hex(MD5(ENCOUNTER)) as Procedure_Group_Key
		, Procedure_Key
	from
		procedure_group_union as ag
	join
		dim_procedures as da on ag.CODE == da.Procedure_Code
)

select * from bridge_procedure_group
