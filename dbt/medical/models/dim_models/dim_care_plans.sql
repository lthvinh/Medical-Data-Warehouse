{{
	config(
		materialized = "incremental"
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Care_Plan_Key)"
	)
}}

with dim_default_care_plans as(
	select
	    '0000000000000000000000000000000000000000000000000000000000000000' AS Care_Plan_Key
	    , CAST(0 AS UInt64) AS Care_Plan_Code
	    , 'Unknown' AS Care_Plan_Description
)

, dim_source_care_plans as (
 	select distinct
 		cast(CODE as UInt64) as Care_Plan_Code
	    , coalesce(DESCRIPTION, 'Unknown') as Care_Plan_Description
 	from
		{{ enriched_table('enriched_careplans') }}
)
, hashed_key as (
	select
		hex(MD5(concat_ws('|', *))) as Allergy_Key
		, *
	from
		dim_source_care_plans
)

, dim_care_plans as (
	select * from dim_default_care_plans
	union all
	select * from hashed_key
)
select source.* from dim_care_plans as source

{% if is_incremental() %}

left join {{ this }} as target
	on source.Care_Plan_Key = target.Care_Plan_Key
where
	target.Care_Plan_Key is null

{% endif %}

order by Care_Plan_Code

