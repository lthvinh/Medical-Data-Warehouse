{{
	config(
		materialized = "incremental"
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Encounter_Reason_Code_Key)"
	)
}}

with dim_default_encounter_reason_codes as(
	select
	    '0000000000000000000000000000000000000000000000000000000000000000' AS Encounter_Reason_Code_Key
	    , CAST(0 AS UInt64) AS Encounter_Reason_Code
	    , 'Unknown' AS Encounter_Reason_Code_Description
)

 , dim_source_encounter_reason_codes as (
 	select distinct
 		CAST(CODE AS UInt64) as Encounter_Reason_Code
	    , coalesce(DESCRIPTION, 'Unknown') as Encounter_Reason_Code_Description
 	from
		{{ enriched_table('enriched_encounters') }}
)
, hashed_key as (
	select
		hex(MD5(concat_ws('|', *))) as Encounter_Reason_Code_Key
		, *
	from
		dim_source_encounter_reason_codes
)
, dim_encounter_reason_codes as (
	select * from dim_default_encounter_reason_codes
	union all
	select * from hashed_key
)

select source.* from dim_encounter_reason_codes as source

{% if is_incremental() %}

left join {{ this }} as target
	on source.Encounter_Reason_Code_Key = target.Encounter_Reason_Code_Key
where
	target.Encounter_Reason_Code_Key is null
	
{% endif %}

order by Encounter_Reason_Code

