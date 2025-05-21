with enriched_encounters as (
	select
		ID
	from
		{{ enriched_table('enriched_encounters') }}
)
, enriched_devices as (
 	select distinct
 		ENCOUNTER
 		, cast(coalesce(CODE, 0) as UInt64) as CODE
 	from
		{{ enriched_table('enriched_devices') }}
	
)
, dim_devices as (
	select
		Device_Key
		, Device_Code 
	from
		{{ ref('dim_devices') }}
)

, encounters_without_device as (
	select distinct 
		ID as ENCOUNTER
		, cast(0 as UInt64) as CODE
	from 
		enriched_encounters as e
	left anti join 
		enriched_devices as a on e.ID = a.ENCOUNTER
)

, condition_group_union as(
	select * from enriched_devices
	union all
	select * from encounters_without_device
)
, bridge_device_group as (
	select
		hex(MD5(ENCOUNTER)) as Device_Group_Key
		, Device_Key
	from
		condition_group_union as ag
	join
		dim_devices as da on ag.CODE == da.Device_Code
)

select * from bridge_device_group


