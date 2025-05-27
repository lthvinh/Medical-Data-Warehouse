{{
	config(
		materialized = "incremental"
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Medication_Group_key)"
	)
}}

WITH dim_medication_group AS (
 	SELECT
 		Encounter_Group_Key as Medication_Group_key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
 )
 
SELECT * FROM dim_medication_group

{% if is_incremental() %}

left join {{ this }} as target
	on source.Medication_Group_key = target.Medication_Group_key
where
	target.Medication_Group_key is null

{% endif %}