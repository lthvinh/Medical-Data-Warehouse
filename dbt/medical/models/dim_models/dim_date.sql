with date_sequence as (
    select * from {{ ref('_date_sequence') }}
    union all
    select toDate('2100-01-01')
)

, dim_date as (
    select  
        toYYYYMMDD(date_day) as Date_Key
        , date_day as Date
        , year(date_day) as Year
        , month(date_day) as Month_Number
        , left(monthName(date_day), 3) as Month_Name_Short
        , monthName(date_day) as Month_Name_Long
        , toDayOfWeek(date_day, 3) as Day_Of_Week_Number
        , date_format(date_day, '%W') as Day_Of_Week
        , date_format(date_day, '%a') as Day_Of_Week_Short
        , quarter(date_day) as quarter
        , concat_ws('_', year(date_day) , quarter(date_day)) as Year_Quarter
        , toLastDayOfMonth(date_day) as last_date
    from
        date_sequence
)

select * from dim_date