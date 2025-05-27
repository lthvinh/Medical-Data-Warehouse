{{
	config(
		materialized = "incremental"
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Allergy_Group_Key)"
	)
}}

WITH dim_allergy_group AS (
 	SELECT
 		Encounter_Group_Key as Allergy_Group_Key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
)
 
SELECT source.* FROM dim_allergy_group as source

{% if is_incremental() %}

left join {{ this }} as target
	on source.Allergy_Group_Key = target.Allergy_Group_Key
where
	target.Allergy_Group_Key is null

{% endif %}