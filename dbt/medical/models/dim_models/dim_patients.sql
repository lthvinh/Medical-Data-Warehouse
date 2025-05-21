with dim_default_patients as(
    select
        '0000000000000000000000000000000000000000000000000000000000000000' as Patient_Key
        , '00000000-0000-0000-0000-000000000000' as Patient_Id
        , toDate32('1900-01-01') as Patient_Birth_Date
        , toDate32('9999-12-31') as Patient_Death_Date
        , 'Unknown' as Patient_SSN
        , 'Unknown' as Patient_Drivers
        , 'Unknown' as Patient_Passport
        , '' as Patient_Prefix
        , 'Unknown' as Patient_First
        , '' as Patient_Middle
        , '' as Patient_Last
        , '' as Patient_Suffix
        , '' as Patient_Maiden
        , 'Unknown' as Patient_Marital
        , 'Unknown' as Patient_Race
        , 'Unknown' as Patient_Ethnicity
        , 'Unknown' as Patient_Gender
        , 'Unknown' as Patient_BirthPlace
        , 'Unknown' as Patient_Address
        , 'Unknown' as Patient_City
        , 'Unknown' as Patient_State
        , 'Unknown' as Patient_County
        , toFixedString('00000', 5) as Patient_FIPS_County_Code
        , cast(0 as UInt16) as Patient_Zip
        , cast(0.0 as Decimal64(6)) as Patient_Lat
        , cast(0.0 as Decimal64(6)) as Patient_Lon
        , cast(0.0 as Decimal64(2)) as Patient_Healthcare_Expenses
        , cast(0.0 as Decimal64(2)) as Patient_Healthcare_Coverage
        , cast(0 as UInt32) as Patient_Income
)
, dim_source_patients as (
    select
        coalesce(ID, '00000000-0000-0000-0000-000000000000') as Patient_Id
        , cast(coalesce(BIRTHDATE, toDate32('2100-01-01')) as Date32) as Patient_Birth_Date
        , cast(coalesce(DEATHDATE, toDate32('2100-01-01')) as Date32) as Patient_Death_Date
        , coalesce(SSN, 'Unknown') as Patient_SSN
        , coalesce(DRIVERS, 'Unknown') as Patient_Drivers
        , coalesce(PASSPORT, 'Unknown') as Patient_Passport
        , coalesce(PREFIX, '') as Patient_Prefix
        , coalesce(FIRST, 'Unknown') as Patient_First
        , coalesce(MIDDLE, '') as Patient_Middle
        , coalesce(LAST, '') as Patient_Last
        , coalesce(SUFFIX, '') as Patient_Suffix
        , coalesce(MAIDEN, '') as Patient_Maiden
        , coalesce(MARITAL, 'Unknown') as Patient_Marital
        , coalesce(RACE, 'Unknown') as Patient_Race
        , coalesce(ETHNICITY, 'Unknown') as Patient_Ethnicity
        , coalesce(GENDER, 'Unknown') as Patient_Gender
        , coalesce(BIRTHPLACE, 'Unknown') as Patient_BirthPlace
        , coalesce(ADDRESS, 'Unknown') as Patient_Address
        , coalesce(CITY, 'Unknown') as Patient_City
        , coalesce(STATE, 'Unknown') as Patient_State
        , coalesce(COUNTY, 'Unknown') as Patient_County
        , toFixedString(coalesce(toString(FIPS), '00000'), 5) as Patient_FIPS_County_Code
        , cast(coalesce(ZIP, 0) as UInt16) as Patient_Zip
        , cast(coalesce(LAT, 0.0) as Decimal64(6)) as Patient_Lat
        , cast(coalesce(LON, 0.0) as Decimal64(6)) as Patient_Lon
        , cast(coalesce(HEALTHCARE_EXPENSES, 0.0) as Decimal64(2)) as Patient_Healthcare_Expenses
        , cast(coalesce(HEALTHCARE_COVERAGE, 0.0) as Decimal64(2)) as Patient_Healthcare_Coverage
        , cast(coalesce(INCOME, 0) as UInt32) as Patient_Income
    from
        {{ enriched_table('enriched_patients')}}
)

, hashed_key as (
 	select distinct
 		hex(MD5(concat_ws('|', *))) as Patient_Key
		, *
	from
		dim_source_patients
)

, dim_patients as (
	select * from dim_default_patients
	union all
	select * from hashed_key
)

select * from dim_patients
order by Patient_Id