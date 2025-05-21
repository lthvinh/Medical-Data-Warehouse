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
select * from dim_devices
order by Device_Code

