with dim_default_payers as (
    select
        '0000000000000000000000000000000000000000000000000000000000000000' as Payer_Key
        , '00000000-0000-0000-0000-000000000000' as Payer_Id
        , 'Unknown' as Payer_Name
        , 'Unknown' as Payer_Ownership
        , 'Unknown' as Payer_Address
        , 'Unknown' as Payer_City
        , 'Unknown' as Payer_State_Headquartered
        , 'Unknown' as Payer_Zip
        , 'Unknown' as Payer_Phone
        , cast(0.0 as Decimal32(2)) as Payer_Amount_Covered
        , cast(0.0 as Decimal32(2)) as Payer_Amount_Uncovered
        , cast(0.0 as Decimal32(2)) as Payer_Revenue
        , cast(0 as UInt64) as Payer_Covered_Encounters
        , cast(0 as UInt64) as Payer_Uncovered_Encounters
        , cast(0 as UInt64) as Payer_Covered_Medications
        , cast(0 as UInt64) as Payer_Uncovered_Medications
        , cast(0 as UInt64) as Payer_Covered_Procedures
        , cast(0 as UInt64) as Payer_Uncovered_Procedures
        , cast(0 as UInt64) as Payer_Covered_Immunizations
        , cast(0 as UInt64) as Payer_Uncovered_Immunizations
        , cast(0 as UInt64) as Payer_Unique_Customers
        , cast(0.0 as Decimal32(2)) as Payer_QOLS_Avg
        , cast(0 as UInt64) as Payer_Member_Months
)
, dim_source_payers as (
    select
        coalesce(ID, '00000000-0000-0000-0000-000000000000') as Payer_Id
        , coalesce(NAME, 'Unknown') as Payer_Name
        , coalesce(OWNERSHIP, 'Unknown') as Payer_Ownership
        , coalesce(ADDRESS, 'Unknown') as Payer_Address
        , coalesce(CITY, 'Unknown') as Payer_City
        , coalesce(STATE_HEADQUARTERED, 'Unknown') as Payer_State_Headquartered
        , coalesce(ZIP, 'Unknown') as Payer_Zip
        , coalesce(PHONE, 'Unknown') as Payer_Phone
        , cast(coalesce(AMOUNT_COVERED, 0.0) as Decimal32(2)) as Payer_Amount_Covered
        , cast(coalesce(AMOUNT_UNCOVERED, 0.0) as Decimal32(2)) as Payer_Amount_Uncovered
        , cast(coalesce(REVENUE, 0.0) as Decimal32(2)) as Payer_Revenue
        , cast(coalesce(COVERED_ENCOUNTERS, 0) as UInt64) as Payer_Covered_Encounters
        , cast(coalesce(UNCOVERED_ENCOUNTERS, 0) as UInt64) as Payer_Uncovered_Encounters
        , cast(coalesce(COVERED_MEDICATIONS, 0) as UInt64) as Payer_Covered_Medications
        , cast(coalesce(UNCOVERED_MEDICATIONS, 0) as UInt64) as Payer_Uncovered_Medications
        , cast(coalesce(COVERED_PROCEDURES, 0) as UInt64) as Payer_Covered_Procedures
        , cast(coalesce(UNCOVERED_PROCEDURES, 0) as UInt64) as Payer_Uncovered_Procedures
        , cast(coalesce(COVERED_IMMUNIZATIONS, 0) as UInt64) as Payer_Covered_Immunizations
        , cast(coalesce(UNCOVERED_IMMUNIZATIONS, 0) as UInt64) as Payer_Uncovered_Immunizations
        , cast(coalesce(UNIQUE_CUSTOMERS, 0) as UInt64) as Payer_Unique_Customers
        , cast(coalesce(QOLS_AVG, 0.0) as Decimal32(2)) as Payer_QOLS_Avg
        , cast(coalesce(MEMBER_MONTHS, 0) as UInt64) as Payer_Member_Months
    from
		{{ enriched_table('enriched_payers') }}
)
, hashed_key as (
 	select distinct
		hex(MD5(concat_ws('|', *))) as Supply_Key
		, *
	from
		dim_source_payers
)

, dim_supplies as (
	select * from dim_default_payers
	union all
	select * from hashed_key
)

select * from dim_supplies
order by Payer_Id