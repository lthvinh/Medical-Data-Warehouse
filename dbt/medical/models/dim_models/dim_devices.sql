{{
	config(
		materialized = "incremental"
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Device_Key)"
	)
}}

with dim_default_devices as(
	select
	    '0000000000000000000000000000000000000000000000000000000000000000' AS Device_Key
	    , CAST(0 AS UInt64) AS Device_Code
	    , 'Unknown' AS Device_UDI
	    , 'Unknown' AS Device_Description
)

 , dim_source_devices as (
 	select distinct
 		CAST(CODE AS UInt64) as Device_Code
	    , coalesce(UDI, 'Unknown') as Device_UDI
	    , coalesce(DESCRIPTION, 'Unknown') as Device_Description
 	from
		{{ enriched_table('enriched_devices') }}
)
, hashed_key as (
	select
		hex(MD5(concat_ws('|', *))) as Allergy_Key
		, *
	from
		dim_source_devices
)
, dim_devices as (
	select * from dim_default_devices
	union all
	select * from hashed_key
)

select * from dim_devices as source

{% if is_incremental() %}

left join {{ this }} as target
	on source.Device_Key = target.Device_Key
where
	target.Device_Key is null
	
{% endif %}

order by Device_Code

