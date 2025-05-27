{{
	config(
		materialized = "incremental"
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Observation_Key)"
	)
}}

with dim_default_observations as(
	select
	    '0000000000000000000000000000000000000000000000000000000000000000' AS Observation_Key
	    , 'Unknown' AS Observation_Code
	    , 'Unknown' AS Observation_Category
	    , 'Unknown' AS Observation_Description
)

 , dim_source_observations as (
 	select distinct
 		CODE as Observation_Code
	    , coalesce(CATEGORY, 'Unknown') AS Observation_Category
	    , coalesce(DESCRIPTION, 'Unknown') AS Observation_Description
 	from
		{{ enriched_table('enriched_observations') }}
)

, hashed_key as (
	select
		hex(MD5(concat_ws('|', *))) as Observation_Key
		, *
	from
		dim_source_observations
)
, dim_observations as (
	select * from dim_default_observations
	union all
	select * from hashed_key
)

select source.* from dim_observations as source

{% if is_incremental() %}

left join {{ this }} as target
	on source.Observation_Key = target.Observation_Key
where
	target.Observation_Key is null
{% endif %}

order by Observation_Code


