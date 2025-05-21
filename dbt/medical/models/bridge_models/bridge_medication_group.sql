with enriched_encounters as (
	select
		ID
	from
		{{ enriched_table('enriched_encounters') }}
)
, enriched_medications as (
 	select distinct
 		ENCOUNTER
 		, cast(coalesce(CODE, 0) as UInt64) as CODE
 	from
		{{ enriched_table('enriched_medications') }}
	
)
, dim_medications as (
	select
		Medication_Key
		, Medication_Code 
	from
		{{ ref('dim_medications') }}
)

, encounters_without_medication as (
	select distinct 
		ID as ENCOUNTER
		, cast(0 as UInt64) as CODE
	from 
		enriched_encounters as e
	left anti join 
		enriched_medications as a on e.ID = a.ENCOUNTER
)

, medication_group_union as(
	select * from enriched_medications
	union all
	select * from encounters_without_medication
)
, bridge_medication_group as (
	select
		hex(MD5(ENCOUNTER)) as Medication_Group_Key
		, Medication_Key
	from
		medication_group_union as ag
	join
		dim_medications as da on ag.CODE == da.Medication_Code
)

select * from bridge_medication_group