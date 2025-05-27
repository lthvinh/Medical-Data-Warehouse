{{
	config(
		materialized = "incremental"
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Immunization_Group_Key)"
	)
}}

WITH dim_immunization_group AS (
 	SELECT
 		Encounter_Group_Key as Immunization_Group_Key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
 )
 
SELECT * FROM dim_immunization_group

{% if is_incremental() %}

left join {{ this }} as target
	on source.Immunization_Group_Key = target.Immunization_Group_Key
where
	target.Immunization_Group_Key is null

{% endif %}