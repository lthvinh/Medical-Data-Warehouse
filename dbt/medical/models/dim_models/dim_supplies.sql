with dim_default_supplies as(
	select
	    '0000000000000000000000000000000000000000000000000000000000000000' as Supply_Key
	    , CasT(0 as UInt64) as Supply_Code
	    , 'Unknown' as Supply_Description
	    , CasT(0 as UInt64) as Supply_Quantity
)

, dim_source_supplies as (
	select
 		cast(coalesce(CODE, 0) as UInt64) as Supply_Code
	    , coalesce(DESCRIPTION, 'Unknown') as Supply_Description
	    , cast(coalesce(QUANTITY, 0) as UInt64) as Supply_Quantity
 	from
		{{ enriched_table('enriched_supplies') }}
)

, hashed_key as (
 	select distinct
		hex(MD5(concat_ws('|', *))) as Supply_Key
		, *
	from
		dim_source_supplies
)

, dim_supplies as (
	select * from dim_default_supplies
	union all
	select * from hashed_key
)

select * from dim_supplies
order by Supply_Code
