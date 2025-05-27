{{
	config(
		materialized = "incremental"
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Condition_Group_Key)"
	)
}}

WITH dim_condition_group AS (
 	SELECT
 		Encounter_Group_Key as Condition_Group_Key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
 )
 
SELECT * FROM dim_condition_group

{% if is_incremental() %}

left join {{ this }} as target
	on source.Condition_Group_Key = target.Condition_Group_Key
where
	target.Condition_Group_Key is null

{% endif %}