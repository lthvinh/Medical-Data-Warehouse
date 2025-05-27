{{
	config(
		materialized = "incremental"
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Device_Group_Key)"
	)
}}

WITH dim_device_group AS (
 	SELECT
 		Encounter_Group_Key as Device_Group_Key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
 )
 
SELECT * FROM dim_device_group

{% if is_incremental() %}

left join {{ this }} as target
	on source.Device_Group_Key = target.Device_Group_Key
where
	target.Device_Group_Key is null

{% endif %}