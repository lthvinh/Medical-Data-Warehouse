{% snapshot user_profiles_snapshot_456 %}

{{
    config(
        schema = 'default'
        , target_database = 'default'
        , unique_key = 'user_id'
        , strategy = 'timestamp'
        , updated_at = 'updated_at'
    )
}}



with source as (
    select * from {{ ref('user_profiles_123') }}
)

select * from source

{% endsnapshot %}