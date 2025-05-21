with dim_default_procedures as (
    select
        '0000000000000000000000000000000000000000000000000000000000000000' AS Procedure_Key
        , CAST(0 AS UInt64) as Procedure_Code
        , 'Unknown' AS Procedure_Description
        , CAST(0.0 AS Decimal32(2)) AS Procedure_Base_Cost
)

, dim_source_procedures as (
	select
 		cast(coalesce(CODE, 0) as UInt64) as Procedure_Code
	    , coalesce(DESCRIPTION, 'Unknown') as Procedure_Description
	    , cast(coalesce(BASE_COST, 0.0) as Decimal32(2)) as Procedure_Base_Cost
 	from
		{{ enriched_table('enriched_procedures') }}
)
, hashed_key as (
 	select distinct
		hex(MD5(concat_ws('|', *))) as Procedure_Key
		, *
	from
		dim_source_procedures
)
, dim_procedures as (
    select * from dim_default_procedures
    union all
    select * from hashed_key
)

select * from dim_procedures