{{
	config(
		materialized = 'incremental'
		, incremental_strategy = "insert_overwrite"
		, engine = 'MergeTree()'
		, partition_by = '(Allergy_Group_Key)'
		, order_by = '(Allergy_Group_Key)'
	)
}}
with enriched_encounters as (
	select
		ID
	from
		{{ enriched_table('enriched_encounters') }}
)
, enriched_allergies as (
 	select distinct
 		ENCOUNTER
 		, cast(coalesce(CODE, 0) as UInt64) as CODE
 	from
		{{ enriched_table('enriched_allergies') }}
	
)
, dim_allergies as (
	select
		Allergy_Key
		, Allergy_Code 
	from
		{{ ref('dim_allergies') }}
)

, encounters_without_allergy as (
	select distinct 
		ID as ENCOUNTER
		, cast(0 as UInt64) as CODE
	from 
		enriched_encounters as e
	left anti join 
		enriched_allergies as a on e.ID = a.ENCOUNTER
)

, allergy_group_union as(
	select * from enriched_allergies
	union all
	select * from encounters_without_allergy
)
, bridge_allergy_group as (
	select
		hex(MD5(ENCOUNTER)) as Allergy_Group_Key
		, Allergy_Key
	from
		allergy_group_union as ag
	join
		dim_allergies as da on ag.CODE == da.Allergy_Code
)

select 
	*
from 
	bridge_allergy_group as source

