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

select * from dim_conditions
order by Condition_Code

