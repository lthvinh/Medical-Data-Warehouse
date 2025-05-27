{{
	config(
		materialized = 'incremental'
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Allergy_Code)"
	)
}}

with dim_default_allergies as(
	select
	    '0000000000000000000000000000000000000000000000000000000000000000' as Allergy_Key
	    , cast(0 as UInt64) as Allergy_Code
	    , 'Unknown' as Allergy_System
	    , 'Unknown' as Allergy_Description
)

 , dim_source_allergies as (
 	select distinct
	    cast(CODE as UInt64) as Allergy_Code
	    , coalesce(SYSTEM, 'Unknown') as Allergy_System
	    , coalesce(DESCRIPTION, 'Unknown') as Allergy_Description
 	from
		{{ enriched_table('enriched_allergies') }}
)

, hashed_key as (
	select
		hex(MD5(concat_ws('|', *))) as Allergy_Key
		, *
	from
		dim_source_allergies
)
, dim_allergies as (
	select * from dim_default_allergies
	union all
	select * from hashed_key
)

select source.* from dim_allergies as source

{% if is_incremental() %}

left join {{ this }} as target
	on source.Allergy_Key = target.Allergy_Key
where
	target.Allergy_Key is null

{% endif %}

order by Allergy_Code

