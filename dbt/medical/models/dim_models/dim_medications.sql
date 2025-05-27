{{
	config(
		materialized = "incremental"
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Medication_Key)"
	)
}}

with dim_default_medications as(
	select
	    '0000000000000000000000000000000000000000000000000000000000000000' AS Medication_Key
	    , cast(0 AS UInt64) AS Medication_Code
	    , 'Unknown' AS Medication_Description
	    ,  cast(0.0 as Decimal32(2)) AS Medication_Base_Cost
)

 , dim_source_medications as (
 	select distinct
 		cast(CODE AS UInt64) as Medication_Code
	    , coalesce(DESCRIPTION, 'Unknown') as Medication_Description
	    ,  cast(coalesce(BASE_COST, 0.9) as Decimal32(2)) AS Medication_Base_Cost
 	from
		{{ enriched_table('enriched_medications') }}
)
, hashed_key as (
	select
		hex(MD5(concat_ws('|', *))) as Medication_Key
		, *
	from
		dim_source_medications
)
, dim_medications as (
	select * from dim_default_medications
	union all
	select * from hashed_key
)

select * from dim_medications as source

{% if is_incremental() %}

left join {{ this }} as target
	on source.Medication_Key = target.Medication_Key
where
	target.Medication_Key is null

{% endif %}

order by Medication_Code

