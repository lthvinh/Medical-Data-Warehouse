with dim_default_providers as (
    select
        '0000000000000000000000000000000000000000000000000000000000000000' AS Provider_Key
        , '00000000-0000-0000-0000-000000000000' AS Provider_Id
        , 'Unknown' AS Provider_Name
        , 'Unknown' AS Provider_Gender
        , 'Unknown' AS Provider_Speciality
        , 'Unknown' AS Provider_Address
        , 'Unknown' AS Provider_City
        , 'Unknown' AS Provider_State
        , cast(0 as UInt16) AS Provider_Zip
        , cast(0.0 as Decimal64(6)) AS Provider_Lat
        , cast(0.0 as Decimal64(6)) AS Provider_Lon
        , cast(0 as UInt64) AS Provider_Utilization
        , 'Unknown' as Organization_Name
        , 'Unknown' as Organization_Address
        , 'Unknown' as Organization_City
        , 'Unknown' as Organization_State
        , cast(0 as UInt16) as Organization_Zip
        , cast(0.0 as Decimal64(6)) as Organization_Lat
        , cast(0.0 as Decimal64(6)) as Organization_Lon
        , 'Unknown' as Organization_Phone
)
, enriched_organizations as (
    select
        ID
        , NAME
        , ADDRESS
        , CITY
        , STATE
        , ZIP
        , LAT
        , LON
        , cast(PHONE as String) as PHONE
        , REVENUE
        , UTILIZATION
    from
        {{ enriched_table('enriched_organizations')}}
)
, dim_source_providers as (
    select
        coalesce(providers.ID, '00000000-0000-0000-0000-000000000000') as Provider_Id
        , coalesce(providers.NAME, 'Unknown') as Provider_Name
        , coalesce(providers.GENDER, 'Unknown') as Provider_Gender
        , coalesce(providers.SPECIALITY, 'Unknown') as Provider_Speciality
        , coalesce(providers.ADDRESS, 'Unknown') as Provider_Address
        , coalesce(providers.CITY, 'Unknown') as Provider_City
        , coalesce(providers.STATE, 'Unknown') as Provider_State
        , cast(coalesce(providers.ZIP , 0) as UInt16) as Provider_Zip
        , cast(coalesce(providers.LAT, 0.0) as Decimal64(6)) as Provider_Lat
        , cast(coalesce(providers.LON, 0.0) as Decimal64(6)) as Provider_Lon
        , cast(coalesce(providers.ENCOUNTERS, 0) as UInt64)  as Provider_Utilization
        , coalesce(organizations.NAME, 'Unknown') as Organization_Name
        , coalesce(organizations.ADDRESS, 'Unknown') as Organization_Address
        , coalesce(organizations.CITY, 'Unknown') as Organization_City
        , coalesce(organizations.STATE, 'Unknown') as Organization_State
        , cast(coalesce(organizations.ZIP , 0) as UInt16) as Organization_Zip
        , cast(coalesce(organizations.LAT, 0.0) as Decimal64(6)) as Organization_Lat
        , cast(coalesce(organizations.LON, 0.0) as Decimal64(6)) as Organization_Lon
        , coalesce(organizations.PHONE, 'Unknown') as Organization_Phone
    from
        {{ enriched_table('enriched_providers') }} as providers
    left join
        enriched_organizations as organizations on providers.ORGANIZATION = organizations.ID
)

, hashed_key as (
 	select distinct
		hex(MD5(concat_ws('|', *))) as Provider_Key
		, *
	from
		dim_source_providers
)

, dim_providers as (
	select * from dim_default_providers
	union all
	select * from hashed_key
)

select * from dim_providers
order by Provider_Id


