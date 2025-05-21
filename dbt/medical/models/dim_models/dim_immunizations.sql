with dim_default_immunizations as(
	select
	    '0000000000000000000000000000000000000000000000000000000000000000' AS Immunization_Key
	    , cast(0 AS UInt64) AS Immunization_Code
	    , 'Unknown' AS Immunization_Description
	    ,  cast(0.0 as Decimal32(2)) AS Immunization_Base_Cost
)

 , dim_source_immunizations as (
 	select distinct
 		cast(CODE AS UInt64) as Immunization_Code
	    , coalesce(DESCRIPTION, 'Unknown') as Immunization_Description
	    ,  cast(coalesce(BASE_COST, 0.9) as Decimal32(2)) AS Immunization_Base_Cost
 	from
		{{ enriched_table('enriched_immunizations') }}
)
, hashed_key as (
	select
		hex(MD5(concat_ws('|', *))) as Immunization_Key
		, *
	from
		dim_source_immunizations
)
, dim_immunizations as (
	select * from dim_default_immunizations
	union all
	select * from hashed_key
)

select * from dim_immunizations


 