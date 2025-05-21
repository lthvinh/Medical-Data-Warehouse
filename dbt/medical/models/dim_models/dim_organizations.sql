with dim_default_organizations as (
    select
        '0000000000000000000000000000000000000000000000000000000000000000' as Organization_Key
        , '00000000-0000-0000-0000-000000000000' as Organization_Id
        , 'Unknown' as Organization_Name
        , 'Unknown' as Organization_Address
        , 'Unknown' as Organization_City
        , 'Unknown' as Organization_State
        , 0 as Organization_Zip
        , cast(0.0 as Decimal64(6)) as Organization_Lat
        , cast(0.0 as Decimal64(6)) as Organization_Lon
        , 'Unknown' as Organization_Phone
        , cast(0.0 as Decimal64(2)) as Organization_Revenue
        , cast(0 as UInt64) as Organization_Utilization
)
, dim_source_organizations as (
    select
        ID  as Organization_Id
        , coalesce(NAME , 'Unknown') as Organization_Name
        , coalesce(ADDRESS , 'Unknown') as Organization_Address
        , coalesce(CITY , 'Unknown') as Organization_City
        , coalesce(STATE , 'Unknown') as Organization_State
        , cast(coalesce(ZIP , 0) as UInt16) as Organization_Zip
        , cast(coalesce(LAT , 0.0) as Decimal64(6)) as Organization_Lat
        , cast(coalesce(LON , 0.0) as Decimal64(6)) as Organization_Lon
        , coalesce(PHONE , 'Unknown') as Organization_Phone
        , cast(coalesce(REVENUE , 0.0) as Decimal64(2)) as Organization_Revenue
        , cast(coalesce(UTILIZATION , 0) as UInt64) as Organization_Utilization
    from
        {{ enriched_table('enriched_organizations') }}
)
, hashed_key as (
	select
		hex(MD5(concat_ws('|', *))) as Organization_Key
		, *
	from
		dim_source_organizations
)
, dim_organizations as (
    select * from dim_default_organizations
    union all
    select * from hashed_key
)

select * from dim_organizations