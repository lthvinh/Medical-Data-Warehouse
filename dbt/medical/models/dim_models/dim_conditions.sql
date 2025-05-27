{{
	config(
		materialized = "incremental"
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Condition_Key)"
	)
}}

with dim_default_conditions as(
	select
	    '0000000000000000000000000000000000000000000000000000000000000000' AS Condition_Key
	    , CAST(0 AS UInt64) AS Condition_Code
	    , 'Unknown' AS Condition_Description
)

 , dim_source_conditions as (
 	select distinct
 		CAST(CODE AS UInt64) as Condition_Code
	    , coalesce(DESCRIPTION, 'Unknown') as Condition_Description
 	from
		{{ enriched_table('enriched_conditions') }}

)
, hashed_key as (
	select
		hex(MD5(concat_ws('|', *))) as Condition_Key
		, *
	from
		dim_source_conditions
)
, dim_conditions as (
	select * from dim_default_conditions
	union all
	select * from hashed_key
)

select * from dim_conditions as source
{% if is_incremental() %}

left join {{ this }} as target
	on source.Condition_Key = target.Condition_Key
where
	target.Condition_Key is null

{% endif %}
order by Condition_Code

