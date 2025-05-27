{{
	config(
		materialized = "incremental"
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Supply_Group_Key)"
	)
}}

WITH dim_supply_group AS (
 	SELECT
 		Encounter_Group_Key as Supply_Group_Key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
 )
 
SELECT * FROM dim_supply_group

{% if is_incremental() %}

left join {{ this }} as target
	on source.Supply_Group_Key = target.Supply_Group_Key
where
	target.Supply_Group_Key is null

{% endif %}