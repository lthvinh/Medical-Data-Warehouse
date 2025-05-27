{{
	config(
		materialized = "incremental"
		, incremental_strategy = "append"
		, engine = "MergeTree()"
		, unique_key = "(Care_Plan_Group_Key)"
	)
}}


WITH dim_care_plan_group AS (
 	SELECT
 		Encounter_Group_Key as Care_Plan_Group_Key
 	FROM	
 		{{ ref('_dim_encounter_group') }}
 )
 
SELECT * FROM dim_care_plan_group

{% if is_incremental() %}

left join {{ this }} as target
	on source.Care_Plan_Group_Key = target.Care_Plan_Group_Key
where
	target.Care_Plan_Group_Key is null

{% endif %}