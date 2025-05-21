WITH hours AS (
    SELECT number as Hour from numbers(24)
)
, minutes AS (
    SELECT number as Minute from numbers(60)
)
, seconds AS (
    SELECT number as Second from numbers(60)
)
, dim_time as (
    SELECT
        CAST(CONCAT_WS('', Hour, Minute, Second) AS UInt32) AS Time_Key
        , CONCAT_WS(':', lpad(toString(Hour), 2, '0'), lpad(toString(Minute), 2, '0'), lpad(toString(Second), 2, '0')) as Time
        , cast(Hour as UInt8) as Hour
        , cast(Minute as UInt8) as Minute
        , cast(Second as UInt8) as Second
    FROM hours AS h
    CROSS JOIN minutes AS m
    CROSS JOIN seconds AS s
)
select * from dim_time
order by Hour, Minute, Second