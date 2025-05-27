{{
	config(
		materialized = "incremental"
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Obsercation_Group_Key)"
	)
}}

WITH dim_observation_group AS (
 	SELECT
 		Encounter_Group_Key as Obsercation_Group_Key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
 )
 
SELECT * FROM dim_observation_group

{% if is_incremental() %}

left join {{ this }} as target
	on source.Obsercation_Group_Key = target.Obsercation_Group_Key
where
	target.Obsercation_Group_Key is null

{% endif %}