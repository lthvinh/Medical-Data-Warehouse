{{
    config(
        
        strategy = "check"
        , check_cols = ["Organization_Key"]
        , unique_key = "Organization_Key"
        , engine = "MergeTree()"
    )
}}

{% snapshot idim_organizations %}

select * from {{ ref("dim_organizations") }}

{% endsnapshot %}


